#!/usr/bin/env python3
"""
verificar_coordenadas.py
──────────────────────────────────────────────────────────────────────────────
Ferramenta de verificação de coordenadas dos pontos de balneabilidade da SEMACE.

Realiza três verificações cruzadas:

1. Compara as coordenadas do scraper_semace.py (PONTOS_META) com o mapa
   oficial de balneabilidade da SEMACE (se acessível via ArcGIS REST API).

2. Para cada ponto, consulta a API Nominatim (OpenStreetMap) para confirmar
   que a coordenada está sobre areia/calçadão e não sobre o mar.

3. Cruza as coordenadas do PONTOS_META com o arquivo KML oficial de pontos
   de balneabilidade do Ceará (SEMACE 2021), calculando a distância entre
   cada ponto do scraper e o respectivo ponto KML.

Dependências:
    pip install requests

Uso:
    python verificar_coordenadas.py              # verificação completa
    python verificar_coordenadas.py --nominatim  # apenas verificação OSM
    python verificar_coordenadas.py --semace     # apenas verificação SEMACE
    python verificar_coordenadas.py --kml        # apenas cruzamento com KML
    python verificar_coordenadas.py --json-output relatorio.json
──────────────────────────────────────────────────────────────────────────────
"""
import argparse
import json
import logging
import re
import time
import unicodedata
import xml.etree.ElementTree as ET
from pathlib import Path

try:
    import requests
except ImportError:
    print("Instale as dependências: pip install requests")
    import sys
    sys.exit(1)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Configurações ─────────────────────────────────────────────────────────────

# URL do mapa ArcGIS da SEMACE (Infraestrutura de Dados Espaciais do Ceará - IDE-CE)
# O mapa oficial pode estar publicado no ArcGIS Online ou no GeoServer da SEMACE.
# URLs conhecidas para tentar:
SEMACE_MAP_URLS = [
    # ArcGIS REST — camada de pontos de balneabilidade
    "https://www.semace.ce.gov.br/geoserver/wfs"
    "?service=WFS&version=1.0.0&request=GetFeature&typeName=semace:balneabilidade"
    "&outputFormat=application/json",
    # Alternativa via IDE-CE
    "https://idece.semace.ce.gov.br/geoserver/wfs"
    "?service=WFS&version=1.0.0&request=GetFeature&typeName=balneabilidade"
    "&outputFormat=application/json",
]

# Nominatim (OSM) — reverse geocoding
NOMINATIM_URL = "https://nominatim.openstreetmap.org/reverse"
NOMINATIM_HEADERS = {
    "User-Agent": "SemaceBalneabilidadeVerificador/1.0 (+https://github.com/JoaoClaudiano/bal)"
}

# Arquivo scraper local
SCRAPER_FILE = Path(__file__).parent / "scraper_semace.py"
DATA_FILE    = Path(__file__).parent / "data" / "balneabilidade.json"
KML_FILE     = Path(__file__).parent / "data" / "0701_ce_banho_mar_2021_pto.kml"

# Namespace KML padrão
_KML_NS = {"kml": "http://www.opengis.net/kml/2.2"}

# Apelidos explícitos: chave = nome normalizado em PONTOS_META,
# valor = nome normalizado equivalente no KML.
# Necessário quando os nomes diferem além do que a normalização automática cobre.
ALIASES_PRAIA = {
    "icarai de amontada": "icaraizinho de amontada",
    "lagamar do cauipe":  "cauipe",
    "pontal de maceio":   "pontal do maceio",
}

# Distância (metros) acima da qual o cruzamento é sinalizado como divergência
KML_DIST_AVISO_M = 5_000

# Categorias OSM que indicam que o ponto está na água (deve ser evitado)
CATEGORIAS_MAR = {"sea", "water", "bay", "coastline", "wetland", "river"}

# Delay entre chamadas Nominatim (política de uso: máx 1 req/s)
NOMINATIM_DELAY = 1.2  # segundos


# ── Extração de coordenadas do scraper local ──────────────────────────────────

def carregar_pontos_scraper() -> dict:
    """
    Extrai o dicionário PONTOS_META do arquivo scraper_semace.py.
    Retorna dict {cod: {"praia":..., "ref":..., "lat":..., "lng":..., ...}}
    """
    if not SCRAPER_FILE.exists():
        log.error("Arquivo não encontrado: %s", SCRAPER_FILE)
        return {}

    content = SCRAPER_FILE.read_text(encoding="utf-8")

    pattern = re.compile(
        r'"(\w+)":\s*\{"praia":"([^"]+)",\s*"ref":"([^"]+)",\s*'
        r'"lat":(-[\d.]+),"lng":(-[\d.]+),'
        r'"setor":"([^"]+)",\s*"municipio":"([^"]+)"\}'
    )

    pontos = {}
    for m in pattern.finditer(content):
        cod = m.group(1)
        pontos[cod] = {
            "praia":     m.group(2),
            "ref":       m.group(3),
            "lat":       float(m.group(4)),
            "lng":       float(m.group(5)),
            "setor":     m.group(6),
            "municipio": m.group(7),
        }

    log.info("Pontos carregados do scraper: %d", len(pontos))
    return pontos


# ── Verificação 1: Mapa oficial da SEMACE ────────────────────────────────────

def buscar_pontos_semace_mapa() -> dict:
    """
    Tenta obter as coordenadas oficiais do mapa de balneabilidade da SEMACE
    via WFS/ArcGIS REST. Retorna dict {cod: {"lat":..., "lng":...}} ou {}
    se o serviço não estiver acessível.
    """
    headers = {
        "User-Agent": (
            "SemaceBalneabilidadeVerificador/1.0 "
            "(+https://github.com/JoaoClaudiano/bal)"
        )
    }

    for url in SEMACE_MAP_URLS:
        log.info("Tentando mapa SEMACE: %s", url[:80] + "...")
        try:
            resp = requests.get(url, headers=headers, timeout=15)
            if resp.status_code == 200:
                try:
                    data = resp.json()
                    pontos_mapa = _parse_geojson_semace(data)
                    if pontos_mapa:
                        log.info("Mapa SEMACE acessível: %d pontos", len(pontos_mapa))
                        return pontos_mapa
                except (ValueError, KeyError) as e:
                    log.warning("Resposta inválida de %s: %s", url[:60], e)
        except requests.RequestException as e:
            log.warning("Falha ao acessar %s: %s", url[:60], e)

    log.warning("Mapa oficial da SEMACE não acessível. Pulando verificação 1.")
    return {}


def _parse_geojson_semace(data: dict) -> dict:
    """
    Tenta extrair pontos de um GeoJSON retornado pelo WFS da SEMACE.
    Retorna dict {cod: {"lat":..., "lng":...}} ou {} se não reconhecido.
    """
    pontos = {}
    features = data.get("features", [])
    for feat in features:
        props = feat.get("properties", {})
        geom  = feat.get("geometry", {})
        coords = geom.get("coordinates", [])

        # Tenta identificar o código do ponto (campo pode variar)
        cod = (
            props.get("codigo") or props.get("cod") or
            props.get("CODIGO") or props.get("id") or ""
        )
        if not cod or not coords:
            continue

        lng, lat = coords[0], coords[1]
        pontos[str(cod).strip()] = {"lat": lat, "lng": lng}

    return pontos


def cruzar_com_mapa_semace(pontos_scraper: dict, pontos_mapa: dict) -> list:
    """
    Compara as coordenadas do scraper com as do mapa oficial.
    Retorna lista de dicts com divergências encontradas.
    """
    divergencias = []

    for cod, meta in pontos_scraper.items():
        if cod not in pontos_mapa:
            continue  # ponto não encontrado no mapa oficial

        lat_s = meta["lat"]
        lng_s = meta["lng"]
        lat_m = pontos_mapa[cod]["lat"]
        lng_m = pontos_mapa[cod]["lng"]

        dist_lat = abs(lat_s - lat_m)
        dist_lng = abs(lng_s - lng_m)
        # ~111 m por grau — divergência > 200 m é suspeita
        dist_m = ((dist_lat * 111000) ** 2 + (dist_lng * 111000) ** 2) ** 0.5

        if dist_m > 200:
            divergencias.append({
                "cod": cod,
                "praia": meta["praia"],
                "ref": meta["ref"],
                "scraper": {"lat": lat_s, "lng": lng_s},
                "mapa_semace": {"lat": lat_m, "lng": lng_m},
                "distancia_m": round(dist_m),
            })

    divergencias.sort(key=lambda x: x["distancia_m"], reverse=True)
    return divergencias


# ── Verificação 2: Nominatim (OSM) ───────────────────────────────────────────

def verificar_ponto_nominatim(lat: float, lng: float) -> dict:
    """
    Consulta o Nominatim para identificar o tipo de terreno em lat/lng.
    Retorna dict com campos: category, type, display_name, no_mar (bool)
    """
    params = {
        "lat":    lat,
        "lon":    lng,
        "format": "jsonv2",
        "zoom":   17,
    }
    try:
        resp = requests.get(
            NOMINATIM_URL,
            params=params,
            headers=NOMINATIM_HEADERS,
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        category  = data.get("category", "desconhecido")
        type_val  = data.get("type", "desconhecido")
        no_mar    = category in CATEGORIAS_MAR or type_val in CATEGORIAS_MAR
        return {
            "category":     category,
            "type":         type_val,
            "display_name": data.get("display_name", ""),
            "no_mar":       no_mar,
        }
    except requests.RequestException as e:
        log.warning("Nominatim falhou para (%.6f, %.6f): %s", lat, lng, e)
        return {"category": "erro", "type": "erro", "display_name": str(e), "no_mar": False}


def verificar_todos_nominatim(pontos: dict) -> list:
    """
    Verifica todos os pontos via Nominatim. Respeita o rate-limit.
    Retorna lista de dicts com resultado por ponto.
    """
    resultados = []
    total = len(pontos)

    for i, (cod, meta) in enumerate(pontos.items(), 1):
        lat = meta["lat"]
        lng = meta["lng"]
        log.info("[%d/%d] Verificando %s (%s) — %.6f, %.6f",
                 i, total, cod, meta["praia"], lat, lng)

        resultado = verificar_ponto_nominatim(lat, lng)
        resultados.append({
            "cod":          cod,
            "praia":        meta["praia"],
            "ref":          meta["ref"],
            "municipio":    meta["municipio"],
            "lat":          lat,
            "lng":          lng,
            "category":     resultado["category"],
            "type":         resultado["type"],
            "display_name": resultado["display_name"],
            "no_mar":       resultado["no_mar"],
            "ok":           not resultado["no_mar"],
        })

        if resultado["no_mar"]:
            log.warning(
                "⚠ %s (%s): ponto pode estar no mar! "
                "category=%s, type=%s",
                cod, meta["praia"], resultado["category"], resultado["type"]
            )

        if i < total:
            time.sleep(NOMINATIM_DELAY)

    return resultados


# ── Verificação 3: KML oficial SEMACE ────────────────────────────────────────

def _normalizar_praia(nome: str) -> str:
    """
    Normaliza um nome de praia para comparação case/accent-insensitive.

    Etapas:
    - Expande abreviações comuns: "P. do" → "Praia do", etc.
    - Converte para minúsculas e remove acentos.
    - Remove prefixo "praia do/da/de..." ou "prainha do/da/de...".
    - Remove sufixo a partir de "/" (ex: "Titanzinho/Farol" → "titanzinho").
    """
    # Expande abreviação "P. do/da/de"
    nome = re.sub(r"\bP\. do\b", "Praia do", nome, flags=re.IGNORECASE)
    nome = re.sub(r"\bP\. da\b", "Praia da", nome, flags=re.IGNORECASE)
    nome = re.sub(r"\bP\. de\b", "Praia de", nome, flags=re.IGNORECASE)
    # Minúsculas e remove acentos
    nome = nome.lower()
    nome = unicodedata.normalize("NFKD", nome)
    nome = "".join(c for c in nome if not unicodedata.combining(c))
    # Remove prefixo articulado de "praia" ou "prainha"
    nome = re.sub(r"^(praia|prainha) (do|da|de|dos|das) ", "", nome)
    # Remove variante após barra (ex: "titanzinho/farol" → "titanzinho")
    nome = re.sub(r"\s*/.*", "", nome)
    return nome.strip()


def _normalizar_municipio(nome: str) -> str:
    """Normaliza município para comparação case/accent-insensitive."""
    nome = nome.lower()
    nome = unicodedata.normalize("NFKD", nome)
    nome = "".join(c for c in nome if not unicodedata.combining(c))
    return nome.strip()


def _distancia_m(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """
    Aproximação plana da distância em metros entre dois pontos geográficos.
    Adequada para distâncias < 100 km na costa do Ceará (latitude ~3°S a 5°S).
    O fator de correção longitudinal usa cos(3°) ≈ 0.9986.
    """
    dlat = (lat2 - lat1) * 111_000
    dlng = (lng2 - lng1) * 111_000 * 0.9986  # cos(3°) ≈ 0.9986 para a costa do Ceará
    return (dlat ** 2 + dlng ** 2) ** 0.5


def carregar_pontos_kml(kml_path: Path = KML_FILE) -> list:
    """
    Lê o arquivo KML e retorna lista de dicts com os campos:
    praia, municipio, coord_lng, coord_lat, praia_norm, municipio_norm.

    O arquivo é UTF-8. Campos adicionais da seção ExtendedData (longitude,
    latitude arredondados) são preservados caso existam, mas as coordenadas
    usadas para cálculo são sempre as do elemento <coordinates> (coord_lng,
    coord_lat), que têm precisão completa.
    """
    if not kml_path.exists():
        log.error("Arquivo KML não encontrado: %s", kml_path)
        return []

    try:
        tree = ET.parse(str(kml_path))
    except ET.ParseError as e:
        log.error("Erro ao analisar KML: %s", e)
        return []

    root = tree.getroot()
    placemarks = root.findall(".//kml:Placemark", _KML_NS)

    pontos = []
    for pm in placemarks:
        ed    = pm.find(".//kml:ExtendedData", _KML_NS)
        coord = pm.find(".//kml:coordinates", _KML_NS)
        data: dict = {}

        if ed is not None:
            for sd in ed.findall(".//kml:SimpleData", _KML_NS):
                data[sd.get("name")] = (sd.text or "").strip()

        if coord is not None:
            parts = coord.text.strip().split(",")
            try:
                data["coord_lng"] = float(parts[0])
                data["coord_lat"] = float(parts[1])
            except (IndexError, ValueError):
                pass

        praia    = data.get("praia", "")
        municipio = data.get("municipio", "")
        data["praia_norm"]     = _normalizar_praia(praia)
        data["municipio_norm"] = _normalizar_municipio(municipio)
        pontos.append(data)

    log.info("Pontos carregados do KML: %d", len(pontos))
    return pontos


def cruzar_com_kml(pontos_scraper: dict, kml_pontos: list) -> tuple:
    """
    Cruza cada entrada de PONTOS_META com o ponto KML de mesmo
    (municipio normalizado, praia normalizada).

    Retorna dois valores:
    - matches: lista de dicts com cod, praia, municipio, coordenadas
               scraper e KML, e distância em metros.
    - nao_encontrados: lista de cods que não tiveram correspondência no KML.
    """
    # Indexa KML por (municipio_norm, praia_norm)
    kml_idx: dict = {}
    for pt in kml_pontos:
        chave = (pt["municipio_norm"], pt["praia_norm"])
        kml_idx[chave] = pt

    matches = []
    nao_encontrados = []

    for cod, meta in pontos_scraper.items():
        praia_n = _normalizar_praia(meta["praia"])
        muni_n  = _normalizar_municipio(meta["municipio"])

        # Aplica alias, se houver
        praia_busca = ALIASES_PRAIA.get(praia_n, praia_n)

        kml_pt = kml_idx.get((muni_n, praia_busca))

        if kml_pt is None:
            # Tenta match apenas por praia (ignora município) — possível divergência
            # de atribuição municipal entre as fontes
            candidatos = [
                pt for pt in kml_pontos
                if pt["praia_norm"] == praia_busca
            ]
            nao_encontrados.append({
                "cod":        cod,
                "praia":      meta["praia"],
                "municipio":  meta["municipio"],
                "lat":        meta["lat"],
                "lng":        meta["lng"],
                "candidatos": [
                    {
                        "praia":     c.get("praia", ""),
                        "municipio": c.get("municipio", ""),
                        "lat":       c.get("coord_lat"),
                        "lng":       c.get("coord_lng"),
                    }
                    for c in candidatos
                ],
            })
            continue

        dist = _distancia_m(
            meta["lat"], meta["lng"],
            kml_pt["coord_lat"], kml_pt["coord_lng"],
        )
        matches.append({
            "cod":       cod,
            "praia":     meta["praia"],
            "municipio": meta["municipio"],
            "scraper":   {"lat": meta["lat"],          "lng": meta["lng"]},
            "kml":       {"lat": kml_pt["coord_lat"],  "lng": kml_pt["coord_lng"]},
            "kml_praia": kml_pt.get("praia", ""),
            "distancia_m": round(dist),
            "aviso":     dist >= KML_DIST_AVISO_M,
        })

    matches.sort(key=lambda x: x["distancia_m"], reverse=True)
    return matches, nao_encontrados


# ── Relatório ─────────────────────────────────────────────────────────────────

def imprimir_relatorio(
    pontos_scraper: dict,
    divergencias_mapa: list,
    resultados_nominatim: list,
    matches_kml: list = None,
    nao_encontrados_kml: list = None,
):
    """Imprime resumo do relatório de verificação no console."""
    print("\n" + "=" * 70)
    print("RELATÓRIO DE VERIFICAÇÃO DE COORDENADAS — SEMACE Balneabilidade")
    print("=" * 70)

    print(f"\n📍 Total de pontos verificados: {len(pontos_scraper)}")

    # Divergências com mapa SEMACE
    if divergencias_mapa:
        print(f"\n⚠  Divergências com mapa oficial SEMACE: {len(divergencias_mapa)}")
        for d in divergencias_mapa:
            print(f"   {d['cod']:6s} {d['praia']:<20s} "
                  f"Δ={d['distancia_m']}m  "
                  f"scraper=({d['scraper']['lat']:.6f},{d['scraper']['lng']:.6f})  "
                  f"mapa=({d['mapa_semace']['lat']:.6f},{d['mapa_semace']['lng']:.6f})")
    elif divergencias_mapa == []:
        print("\n✅ Nenhuma divergência com mapa SEMACE (ou mapa não acessível).")

    # Resultados Nominatim
    if resultados_nominatim:
        no_mar = [r for r in resultados_nominatim if r["no_mar"]]
        ok     = [r for r in resultados_nominatim if r["ok"]]
        print(f"\n🗺  Verificação OSM/Nominatim:")
        print(f"   ✅ OK (em terra/areia): {len(ok)}")
        print(f"   ⚠  Possivelmente no mar: {len(no_mar)}")
        if no_mar:
            print("\n   Pontos suspeitos:")
            for r in no_mar:
                print(f"   {r['cod']:6s} {r['praia']:<20s} "
                      f"lat={r['lat']:.6f} lng={r['lng']:.6f}  "
                      f"category={r['category']} type={r['type']}")
    else:
        print("\n   (Verificação Nominatim não executada)")

    # ── Cruzamento com KML ────────────────────────────────────────────────────
    if matches_kml is not None:
        avisos = [m for m in matches_kml if m["aviso"]]
        ok_kml = [m for m in matches_kml if not m["aviso"]]
        print(f"\n📂 Cruzamento com KML SEMACE:")
        print(f"   ✅ Dentro do limiar ({KML_DIST_AVISO_M / 1000:.0f} km): {len(ok_kml)}")
        print(f"   ⚠  Distância elevada (≥ {KML_DIST_AVISO_M / 1000:.0f} km): {len(avisos)}")
        print(f"   ❌ Sem correspondência no KML: {len(nao_encontrados_kml or [])}")

        if avisos:
            print("\n   Pontos com distância elevada:")
            for m in avisos:
                print(f"   {m['cod']:6s} {m['praia']:<22s} "
                      f"Δ={m['distancia_m']:>5}m  "
                      f"scraper=({m['scraper']['lat']:.5f},{m['scraper']['lng']:.5f})  "
                      f"kml=({m['kml']['lat']:.5f},{m['kml']['lng']:.5f})")

        if nao_encontrados_kml:
            print("\n   Pontos sem correspondência no KML:")
            for p in nao_encontrados_kml:
                candidatos = p.get("candidatos", [])
                sufixo = ""
                if candidatos:
                    c = candidatos[0]
                    sufixo = f"  → candidato: {c['praia']} ({c['municipio']})"
                print(f"   {p['cod']:6s} {p['praia']:<22s} ({p['municipio']}){sufixo}")

        if ok_kml:
            print("\n   Pontos dentro do limiar (ordenados por distância):")
            for m in sorted(ok_kml, key=lambda x: x["distancia_m"], reverse=True):
                print(f"   {m['cod']:6s} {m['praia']:<22s} "
                      f"Δ={m['distancia_m']:>5}m")

    print("\n" + "=" * 70)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Verifica coordenadas dos pontos de balneabilidade da SEMACE"
    )
    parser.add_argument(
        "--semace", action="store_true",
        help="Apenas verificação contra o mapa oficial SEMACE"
    )
    parser.add_argument(
        "--nominatim", action="store_true",
        help="Apenas verificação via OSM Nominatim (reverse geocoding)"
    )
    parser.add_argument(
        "--kml", action="store_true",
        help="Cruzamento com o arquivo KML oficial de pontos de balneabilidade"
    )
    parser.add_argument(
        "--json-output", metavar="ARQUIVO",
        help="Salva relatório completo em JSON"
    )
    args = parser.parse_args()

    # Modo padrão: executa todas as verificações exceto Nominatim (lenta)
    qualquer_flag = args.semace or args.nominatim or args.kml
    fazer_semace    = args.semace    or not qualquer_flag
    fazer_nominatim = args.nominatim
    fazer_kml       = args.kml       or not qualquer_flag

    # 1. Carrega pontos do scraper local
    pontos_scraper = carregar_pontos_scraper()
    if not pontos_scraper:
        log.error("Nenhum ponto carregado. Encerrando.")
        return

    # 2. Verifica contra mapa SEMACE
    divergencias_mapa = []
    if fazer_semace:
        log.info("─── Verificação 1: Mapa oficial SEMACE ───")
        pontos_mapa = buscar_pontos_semace_mapa()
        if pontos_mapa:
            divergencias_mapa = cruzar_com_mapa_semace(pontos_scraper, pontos_mapa)
            log.info("Divergências encontradas: %d", len(divergencias_mapa))
        else:
            log.info("Mapa SEMACE não disponível, pulando cruzamento.")

    # 3. Verifica via Nominatim
    resultados_nominatim = []
    if fazer_nominatim:
        log.info("─── Verificação 2: OSM Nominatim (reverse geocoding) ───")
        log.info("Respeitando rate-limit: %.1fs entre requisições", NOMINATIM_DELAY)
        resultados_nominatim = verificar_todos_nominatim(pontos_scraper)

    # 4. Cruzamento com KML
    matches_kml = None
    nao_encontrados_kml = None
    if fazer_kml:
        log.info("─── Verificação 3: Cruzamento com KML ───")
        kml_pontos = carregar_pontos_kml()
        if kml_pontos:
            matches_kml, nao_encontrados_kml = cruzar_com_kml(pontos_scraper, kml_pontos)
            log.info(
                "KML: %d matches, %d sem correspondência",
                len(matches_kml), len(nao_encontrados_kml),
            )
        else:
            log.warning("KML não carregado, pulando cruzamento.")

    # 5. Relatório
    imprimir_relatorio(
        pontos_scraper, divergencias_mapa, resultados_nominatim,
        matches_kml, nao_encontrados_kml,
    )

    # 6. Salva JSON (opcional)
    if args.json_output:
        relatorio = {
            "pontos_verificados": len(pontos_scraper),
            "divergencias_mapa_semace": divergencias_mapa,
            "resultados_nominatim": resultados_nominatim,
            "matches_kml": matches_kml,
            "nao_encontrados_kml": nao_encontrados_kml,
        }
        out_path = Path(args.json_output)
        out_path.write_text(
            json.dumps(relatorio, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        log.info("Relatório salvo em: %s", out_path)


if __name__ == "__main__":
    main()
