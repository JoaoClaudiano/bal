#!/usr/bin/env python3
"""
verificar_coordenadas.py
──────────────────────────────────────────────────────────────────────────────
Ferramenta de verificação de coordenadas dos pontos de balneabilidade da SEMACE.

Realiza duas verificações cruzadas:

1. Compara as coordenadas do scraper_semace.py (PONTOS_META) com o mapa
   oficial de balneabilidade da SEMACE (se acessível via ArcGIS REST API).

2. Para cada ponto, consulta a API Nominatim (OpenStreetMap) para confirmar
   que a coordenada está sobre areia/calçadão e não sobre o mar.

Dependências:
    pip install requests

Uso:
    python verificar_coordenadas.py              # verificação completa
    python verificar_coordenadas.py --nominatim  # apenas verificação OSM
    python verificar_coordenadas.py --semace     # apenas verificação SEMACE
    python verificar_coordenadas.py --json-output relatorio.json
──────────────────────────────────────────────────────────────────────────────
"""
import argparse
import json
import logging
import re
import time
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


# ── Relatório ─────────────────────────────────────────────────────────────────

def imprimir_relatorio(
    pontos_scraper: dict,
    divergencias_mapa: list,
    resultados_nominatim: list,
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
        "--json-output", metavar="ARQUIVO",
        help="Salva relatório completo em JSON"
    )
    args = parser.parse_args()

    # Modo padrão: executa ambas as verificações
    fazer_semace    = args.semace    or (not args.semace and not args.nominatim)
    fazer_nominatim = args.nominatim or (not args.semace and not args.nominatim)

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

    # 4. Relatório
    imprimir_relatorio(pontos_scraper, divergencias_mapa, resultados_nominatim)

    # 5. Salva JSON (opcional)
    if args.json_output:
        relatorio = {
            "pontos_verificados": len(pontos_scraper),
            "divergencias_mapa_semace": divergencias_mapa,
            "resultados_nominatim": resultados_nominatim,
        }
        out_path = Path(args.json_output)
        out_path.write_text(
            json.dumps(relatorio, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        log.info("Relatório salvo em: %s", out_path)


if __name__ == "__main__":
    main()
