#!/usr/bin/env python3
"""
scraper_semace.py
─────────────────────────────────────────────────────────────────────────────
Scraper automático dos boletins de balneabilidade da SEMACE (Ceará).
Baixa os PDFs mais recentes, extrai os status de cada ponto de monitoramento
e atualiza o arquivo data/balneabilidade.json com histórico de 52 semanas.

Dependências:
    pip install requests pdfplumber

Uso:
    python scraper_semace.py            # atualiza data/balneabilidade.json
    python scraper_semace.py --dry-run  # imprime resultado sem salvar

Rodado automaticamente via GitHub Actions toda segunda-feira às 10h BRT.
─────────────────────────────────────────────────────────────────────────────
"""
import json
import os
import re
import sys
import tempfile
import datetime
import argparse
import logging
from pathlib import Path
from typing import Optional

try:
    import requests
    import pdfplumber
except ImportError:
    print("Instale as dependências: pip install requests pdfplumber")
    sys.exit(1)

# ── Configuração ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

BASE_URL   = "https://www.semace.ce.gov.br/boletim-de-balneabilidade/"
OUTPUT_DIR = Path(__file__).parent / "data"
OUTPUT_FILE = OUTPUT_DIR / "balneabilidade.json"
MAX_HISTORICO = 52  # semanas

# Headers para não ser bloqueado
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; SemaceBalneabilidadeBot/1.0; "
        "+https://github.com/seu-usuario/balneabilidade-ceara)"
    )
}

# ── Coordenadas e metadados de cada ponto ────────────────────────────────────
# Fonte: SEMACE + geocodificação na linha da praia (OSM)
PONTOS_META = {
    # Fortaleza — Setor Leste
    "67L": {"praia":"Sabiaguaba",      "ref":"Rua Sabiaguaba",                          "lat":-3.817520,"lng":-38.394025,"setor":"Leste",        "municipio":"Fortaleza"},
    "32L": {"praia":"Abreulândia",     "ref":"Rua Teófilo Ramos",                       "lat":-3.794012,"lng":-38.429580,"setor":"Leste",        "municipio":"Fortaleza"},
    "10L": {"praia":"P. do Futuro",    "ref":"Rua Ismael Pordeus",                      "lat":-3.774825,"lng":-38.448250,"setor":"Leste",        "municipio":"Fortaleza"},
    "09L": {"praia":"P. do Futuro",    "ref":"Areninha Praia do Futuro I",              "lat":-3.769248,"lng":-38.447850,"setor":"Leste",        "municipio":"Fortaleza"},
    "08L": {"praia":"P. do Futuro",    "ref":"Rua Clóvis Mota – Clube dos Oficiais",    "lat":-3.763815,"lng":-38.448900,"setor":"Leste",        "municipio":"Fortaleza"},
    "07L": {"praia":"P. do Futuro",    "ref":"Rua Gerôncio Brígido Neto – GV 01",       "lat":-3.758812,"lng":-38.448350,"setor":"Leste",        "municipio":"Fortaleza"},
    "06L": {"praia":"P. do Futuro",    "ref":"Av. Carlos Jereissati",                   "lat":-3.753515,"lng":-38.449250,"setor":"Leste",        "municipio":"Fortaleza"},
    "05L": {"praia":"P. do Futuro",    "ref":"Rua Antônio Atualpa Rodrigues",           "lat":-3.748820,"lng":-38.449850,"setor":"Leste",        "municipio":"Fortaleza"},
    "04L": {"praia":"P. do Futuro",    "ref":"Rua Francisco Montenegro – GV 06",        "lat":-3.743812,"lng":-38.450250,"setor":"Leste",        "municipio":"Fortaleza"},
    "03L": {"praia":"P. do Futuro",    "ref":"Rua Embratel – GV 08",                   "lat":-3.738812,"lng":-38.450850,"setor":"Leste",        "municipio":"Fortaleza"},
    "02L": {"praia":"P. do Futuro",    "ref":"Capela de Santa Terezinha – GV 09",       "lat":-3.733850,"lng":-38.450650,"setor":"Leste",        "municipio":"Fortaleza"},
    "11L": {"praia":"Titanzinho",      "ref":"Praia do Titanzinho",                     "lat":-3.726985,"lng":-38.443250,"setor":"Leste",        "municipio":"Fortaleza"},
    "01L": {"praia":"P. do Futuro",    "ref":"Caça e Pesca – Rua Germiniano Jurema",    "lat":-3.721285,"lng":-38.441520,"setor":"Leste",        "municipio":"Fortaleza"},
    # Fortaleza — Setor Centro
    "12C": {"praia":"Mucuripe",        "ref":"Porto dos Botes – Rua Interna",           "lat":-3.719850,"lng":-38.455240,"setor":"Centro",       "municipio":"Fortaleza"},
    "13C": {"praia":"Mucuripe",        "ref":"Mercado dos Peixes do Mucuripe",          "lat":-3.720125,"lng":-38.461250,"setor":"Centro",       "municipio":"Fortaleza"},
    "14C": {"praia":"Mucuripe",        "ref":"Estátua Iracema do Mucuripe",             "lat":-3.721985,"lng":-38.467825,"setor":"Centro",       "municipio":"Fortaleza"},
    "15C": {"praia":"Mucuripe",        "ref":"Jardim Japonês / Arena Beira Mar",        "lat":-3.725025,"lng":-38.477850,"setor":"Centro",       "municipio":"Fortaleza"},
    "16C": {"praia":"Meireles",        "ref":"Av. Desembargador Moreira – Feirinha",    "lat":-3.726580,"lng":-38.484025,"setor":"Centro",       "municipio":"Fortaleza"},
    "17C": {"praia":"Meireles",        "ref":"Rua José Vilar – GV 06",                  "lat":-3.727285,"lng":-38.491250,"setor":"Centro",       "municipio":"Fortaleza"},
    "18C": {"praia":"Meireles",        "ref":"Av. Rui Barbosa – Aterro",               "lat":-3.726850,"lng":-38.498450,"setor":"Centro",       "municipio":"Fortaleza"},
    "19C": {"praia":"Iracema",         "ref":"Estátua de Iracema Guardiã – Aterrinho",  "lat":-3.722180,"lng":-38.506250,"setor":"Centro",       "municipio":"Fortaleza"},
    "20C": {"praia":"Iracema",         "ref":"Av. Almirante Tamandaré – Ponte Metálica","lat":-3.720450,"lng":-38.512350,"setor":"Centro",       "municipio":"Fortaleza"},
    "69C": {"praia":"Iracema",         "ref":"Praia dos Crush – C. Cultural Belchior",  "lat":-3.719985,"lng":-38.517825,"setor":"Centro",       "municipio":"Fortaleza"},
    # Fortaleza — Setor Oeste
    "22O": {"praia":"Leste Oeste",     "ref":"Próximo à Igreja de Santa Edwiges",       "lat":-3.717185,"lng":-38.531250,"setor":"Oeste",        "municipio":"Fortaleza"},
    "23O": {"praia":"Pirambu",         "ref":"Praia da Leste – Av. Filomeno Gomes",     "lat":-3.713520,"lng":-38.551285,"setor":"Oeste",        "municipio":"Fortaleza"},
    "24O": {"praia":"Pirambu",         "ref":"Praia da Formosa – PS Guiomar Arruda",    "lat":-3.709825,"lng":-38.572025,"setor":"Oeste",        "municipio":"Fortaleza"},
    "25O": {"praia":"Colônia",         "ref":"Av. Pasteur – Est. Elevatória Arpoador",  "lat":-3.706025,"lng":-38.591250,"setor":"Oeste",        "municipio":"Fortaleza"},
    "26O": {"praia":"Colônia",         "ref":"Praia do 'L' – Rua Dr. Theberge",        "lat":-3.702525,"lng":-38.608250,"setor":"Oeste",        "municipio":"Fortaleza"},
    "27O": {"praia":"Barra do Ceará",  "ref":"Coqueirinho – Projeto 4 Varas",           "lat":-3.698825,"lng":-38.624850,"setor":"Oeste",        "municipio":"Fortaleza"},
    "28O": {"praia":"Barra do Ceará",  "ref":"Goiabeiras – Rua Coqueiro Verde",         "lat":-3.694525,"lng":-38.641025,"setor":"Oeste",        "municipio":"Fortaleza"},
    "29O": {"praia":"Barra do Ceará",  "ref":"Rua Bom Jesus",                          "lat":-3.690025,"lng":-38.656525,"setor":"Oeste",        "municipio":"Fortaleza"},
    "30O": {"praia":"Barra do Ceará",  "ref":"Rua Rita das Goiabeiras",                "lat":-3.685825,"lng":-38.670025,"setor":"Oeste",        "municipio":"Fortaleza"},
    "31O": {"praia":"Barra do Ceará",  "ref":"Foz do Rio Ceará",                       "lat":-3.678525,"lng":-38.688250,"setor":"Oeste",        "municipio":"Fortaleza"},
    # Litoral Leste
    "68LE":{"praia":"Porto das Dunas", "ref":"Rua Antônio Alencar",                    "lat":-3.806225,"lng":-38.390850,"setor":"Litoral Leste","municipio":"Aquiraz"},
    "33LE":{"praia":"Porto das Dunas", "ref":"Porto das Dunas",                        "lat":-3.809525,"lng":-38.387450,"setor":"Litoral Leste","municipio":"Aquiraz"},
    "34LE":{"praia":"Prainha",         "ref":"Prainha",                                "lat":-3.843250,"lng":-38.364025,"setor":"Litoral Leste","municipio":"Aquiraz"},
    "35LE":{"praia":"Presídio",        "ref":"Presídio",                               "lat":-3.861250,"lng":-38.352025,"setor":"Litoral Leste","municipio":"Aquiraz"},
    "36LE":{"praia":"Iguape",          "ref":"Iguape",                                 "lat":-3.899825,"lng":-38.321825,"setor":"Litoral Leste","municipio":"Aquiraz"},
    "37LE":{"praia":"Barro Preto",     "ref":"Barro Preto",                            "lat":-3.941250,"lng":-38.298825,"setor":"Litoral Leste","municipio":"Aquiraz"},
    "38LE":{"praia":"Batoque",         "ref":"Batoque",                                "lat":-3.982250,"lng":-38.274025,"setor":"Litoral Leste","municipio":"Aquiraz"},
    "39LE":{"praia":"Barra Nova",      "ref":"Barra Nova",                             "lat":-4.021825,"lng":-38.251250,"setor":"Litoral Leste","municipio":"Cascavel"},
    "40LE":{"praia":"Tabubinha",       "ref":"Tabubinha",                              "lat":-4.061850,"lng":-38.231850,"setor":"Litoral Leste","municipio":"Cascavel"},
    "41LE":{"praia":"Morro Branco",    "ref":"Morro Branco Velho",                     "lat":-4.175250,"lng":-38.129825,"setor":"Litoral Leste","municipio":"Beberibe"},
    "42LE":{"praia":"Praia das Fontes","ref":"Praia das Fontes",                       "lat":-4.188250,"lng":-38.118825,"setor":"Litoral Leste","municipio":"Beberibe"},
    "43LE":{"praia":"Canto Verde",     "ref":"Canto Verde",                            "lat":-4.231250,"lng":-38.080825,"setor":"Litoral Leste","municipio":"Beberibe"},
    "44LE":{"praia":"Pontal de Maceió","ref":"Pontal de Maceió",                       "lat":-4.458250,"lng":-37.804225,"setor":"Litoral Leste","municipio":"Fortim"},
    "46LE":{"praia":"Majorlândia",     "ref":"Majorlândia",                            "lat":-4.324850,"lng":-37.805850,"setor":"Litoral Leste","municipio":"Aracati"},
    "45LE":{"praia":"Canoa Quebrada",  "ref":"Canoa Quebrada",                         "lat":-4.350850,"lng":-37.727250,"setor":"Litoral Leste","municipio":"Aracati"},
    "47LE":{"praia":"Quixaba",         "ref":"Quixaba",                                "lat":-4.361850,"lng":-37.645850,"setor":"Litoral Leste","municipio":"Icapuí"},
    "48LE":{"praia":"Redonda",         "ref":"Redonda",                                "lat":-4.411250,"lng":-37.356825,"setor":"Litoral Leste","municipio":"Icapuí"},
    # Litoral Oeste
    "49OE":{"praia":"Icaraí",          "ref":"Icaraí (Caucaia)",                       "lat":-3.706825,"lng":-38.635025,"setor":"Litoral Oeste","municipio":"Caucaia"},
    "50OE":{"praia":"Tabuba",          "ref":"Tabuba",                                 "lat":-3.669850,"lng":-38.671825,"setor":"Litoral Oeste","municipio":"Caucaia"},
    "51OE":{"praia":"Cumbuco",         "ref":"Cumbuco",                                "lat":-3.625250,"lng":-38.726825,"setor":"Litoral Oeste","municipio":"Caucaia"},
    "52OE":{"praia":"Lagamar do Cauípe","ref":"Lagamar do Cauípe",                     "lat":-3.590825,"lng":-38.758825,"setor":"Litoral Oeste","municipio":"Caucaia"},
    "53OE":{"praia":"Pecém",           "ref":"Pecém",                                  "lat":-3.542250,"lng":-38.799825,"setor":"Litoral Oeste","municipio":"São Gonçalo do Amarante"},
    "54OE":{"praia":"Taíba",           "ref":"Taíba",                                  "lat":-3.504250,"lng":-38.872825,"setor":"Litoral Oeste","municipio":"São Gonçalo do Amarante"},
    "55OE":{"praia":"Paracuru",        "ref":"Paracuru",                               "lat":-3.413250,"lng":-39.027825,"setor":"Litoral Oeste","municipio":"Paracuru"},
    "58OE":{"praia":"Mundaú",          "ref":"Mundaú",                                 "lat":-3.292250,"lng":-39.205250,"setor":"Litoral Oeste","municipio":"Trairi"},
    "57OE":{"praia":"Flecheiras",      "ref":"Flecheiras",                             "lat":-3.267850,"lng":-39.242825,"setor":"Litoral Oeste","municipio":"Trairi"},
    "59OE":{"praia":"Baleia",          "ref":"Baleia",                                 "lat":-3.255850,"lng":-39.311825,"setor":"Litoral Oeste","municipio":"Itapipoca"},
    "56OE":{"praia":"Lagoinha",        "ref":"Lagoinha",                               "lat":-3.049250,"lng":-39.464825,"setor":"Litoral Oeste","municipio":"Paraipaba"},
    "60OE":{"praia":"Icaraí de Amontada","ref":"Icaraí de Amontada",                  "lat":-2.966850,"lng":-39.588825,"setor":"Litoral Oeste","municipio":"Amontada"},
    "61OE":{"praia":"Almofala",        "ref":"Almofala",                               "lat":-2.906250,"lng":-39.814825,"setor":"Litoral Oeste","municipio":"Itarema"},
    "62OE":{"praia":"Arpoeiras",       "ref":"Arpoeiras",                              "lat":-2.868850,"lng":-39.943825,"setor":"Litoral Oeste","municipio":"Acaraú"},
    "66OE":{"praia":"Praia do Preá",   "ref":"Cruz – Praia do Preá",                  "lat":-2.588850,"lng":-40.417825,"setor":"Litoral Oeste","municipio":"Cruz"},
    "63OE":{"praia":"Jericoacoara",    "ref":"Jericoacoara",                           "lat":-2.795850,"lng":-40.511825,"setor":"Litoral Oeste","municipio":"Jijoca de Jericoacoara"},
    "64OE":{"praia":"Camocim",         "ref":"Camocim – Travessia das balsas",         "lat":-2.901250,"lng":-40.838825,"setor":"Litoral Oeste","municipio":"Camocim"},
    "65OE":{"praia":"Bitupitá",        "ref":"Bitupitá",                               "lat":-3.098250,"lng":-41.276825,"setor":"Litoral Oeste","municipio":"Barroquinha"},
}

# ── Regex de extração dos PDFs ────────────────────────────────────────────────
# Padrão boletim Fortaleza: "01L - P. do Futuro - ... P"  ou "... I"
RE_FORTALEZA = re.compile(
    r"(\d{2}[CLO])\s*[-–]\s*[^.]+\.?\s*[-–]?\s*[^\n]+\s+(P|I|EA)\b"
)
# Padrão litoral leste/oeste: "33LE - Porto das Dunas A" ou "EA" ou "I"
RE_ESTADO = re.compile(
    r"(\d{2,3}[A-Z]{1,3})\s*[-–]\s*[^\n]+\s+(A|EA|I)\b"
)
# Mapeamento de letras dos PDFs para nosso padrão
STATUS_MAP = {"P": "P", "A": "P", "EA": "EA", "I": "I"}


def fetch_page_links() -> dict:
    """
    Acessa a página de boletins da SEMACE e extrai URLs dos PDFs mais recentes.
    Retorna dict com chaves: 'fortaleza', 'leste', 'oeste'
    """
    log.info("Acessando página de boletins: %s", BASE_URL)
    resp = requests.get(BASE_URL, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    html = resp.text

    pdfs = {}

    # Extrai links de PDF da página
    links = re.findall(r'href="(https://[^"]+\.pdf)"', html)
    log.info("PDFs encontrados na página: %d", len(links))

    for link in links:
        llow = link.lower()
        if "fortaleza" in llow or "bol1" in llow:
            if "fortaleza" not in pdfs:
                pdfs["fortaleza"] = link
        elif "oeste" in llow or "bol4" in llow:
            if "oeste" not in pdfs:
                pdfs["oeste"] = link
        elif "leste" in llow or "bol5" in llow:
            if "leste" not in pdfs:
                pdfs["leste"] = link

    # Fallback: pega os 3 primeiros PDFs encontrados se o padrão falhou
    if len(pdfs) < 3 and len(links) >= 3:
        keys = ["fortaleza", "leste", "oeste"]
        for i, link in enumerate(links[:3]):
            key = keys[i]
            if key not in pdfs:
                pdfs[key] = link
                log.warning("Fallback: %s → %s", key, link)

    log.info("PDFs mapeados: %s", list(pdfs.keys()))
    return pdfs


def download_pdf(url: str) -> Optional[bytes]:
    """Baixa um PDF e retorna os bytes."""
    log.info("Baixando: %s", url)
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        return resp.content
    except requests.RequestException as e:
        log.error("Falha ao baixar %s: %s", url, e)
        return None


def extract_text_from_pdf(pdf_bytes: bytes) -> str:
    """Extrai texto de um PDF em memória usando pdfplumber."""
    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
        tmp.write(pdf_bytes)
        tmp_path = tmp.name
    try:
        text_parts = []
        with pdfplumber.open(tmp_path) as pdf:
            for page in pdf.pages:
                t = page.extract_text()
                if t:
                    text_parts.append(t)
        return "\n".join(text_parts)
    finally:
        os.unlink(tmp_path)


def parse_fortaleza(text: str) -> dict:
    """
    Extrai status dos pontos do boletim semanal de Fortaleza.
    Retorna dict {cod: status}
    """
    results = {}
    # Normaliza sufixos: "01L", "12C", "22O"
    for match in RE_FORTALEZA.finditer(text):
        cod_raw, status_raw = match.group(1), match.group(2)
        cod = cod_raw.upper().strip()
        status = STATUS_MAP.get(status_raw.upper(), "P")
        if cod in PONTOS_META:
            results[cod] = status
            log.debug("Fortaleza: %s = %s", cod, status)

    # Fallback com regex mais permissivo se poucos resultados
    if len(results) < 10:
        log.warning("Poucos resultados (%d) — tentando regex alternativo", len(results))
        alt = re.findall(r"(\d{2}[CLO])[^\n]+(P|I)\b", text)
        for cod, st in alt:
            if cod in PONTOS_META and cod not in results:
                results[cod] = STATUS_MAP.get(st, "P")

    log.info("Fortaleza: %d pontos extraídos", len(results))
    return results


def parse_estado(text: str) -> dict:
    """
    Extrai status dos pontos dos boletins mensais de Litoral Leste e Oeste.
    Retorna dict {cod: status}
    """
    results = {}
    for match in RE_ESTADO.finditer(text):
        cod_raw, status_raw = match.group(1), match.group(2)
        cod = cod_raw.upper().strip()
        status = STATUS_MAP.get(status_raw.upper(), "P")
        if cod in PONTOS_META:
            results[cod] = status
            log.debug("Estado: %s = %s", cod, status)

    log.info("Estado: %d pontos extraídos", len(results))
    return results


def get_periodo_fortaleza(text: str) -> str:
    """Extrai o período do boletim de Fortaleza (ex: '23/03/2026 a 29/03/2026')."""
    m = re.search(r"Per[íi]odo:\s*([\d/]+)\s*a\s*([\d/]+)", text)
    if m:
        return f"{m.group(1)} a {m.group(2)}"
    # fallback: semana atual
    today = datetime.date.today()
    monday = today - datetime.timedelta(days=today.weekday())
    sunday = monday + datetime.timedelta(days=6)
    return f"{monday.strftime('%d/%m/%Y')} a {sunday.strftime('%d/%m/%Y')}"


def get_periodo_estado(text: str) -> str:
    """Extrai o período do boletim de estado (ex: 'Março/2026')."""
    m = re.search(r"Per[íi]odo:\s*([^\n]+)", text)
    if m:
        return m.group(1).strip()
    return datetime.date.today().strftime("%b/%Y")


def load_existing(path: Path) -> dict:
    """Carrega JSON existente ou retorna estrutura vazia."""
    if path.exists():
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    return {"meta": {}, "pontos": {}}


def merge_historico(existing: dict, novos: dict, periodo: str) -> dict:
    """
    Mescla novos status no histórico existente.
    - Adiciona nova entrada no topo de cada ponto
    - Mantém no máximo MAX_HISTORICO semanas
    - Preserva dados de pontos não presentes no boletim atual
    """
    pontos = existing.get("pontos", {})

    for cod, meta in PONTOS_META.items():
        if cod not in pontos:
            pontos[cod] = {**meta, "cod": cod, "historico": []}

        if cod in novos:
            nova_entrada = {"periodo": periodo, "status": novos[cod]}
            historico = pontos[cod].get("historico", [])

            # Evita duplicata do mesmo período
            if historico and historico[-1]["periodo"] == periodo:
                historico[-1]["status"] = novos[cod]
                log.debug("Atualizado (mesmo período): %s", cod)
            else:
                historico.append(nova_entrada)
                if len(historico) > MAX_HISTORICO:
                    historico = historico[-MAX_HISTORICO:]
            pontos[cod]["historico"] = historico

    return pontos


def calcular_tendencia(historico: list) -> str:
    """
    Calcula tendência baseada nas últimas 4 semanas vs 4 anteriores.
    Retorna: 'melhorando' | 'piorando' | 'estável'
    """
    if len(historico) < 4:
        return "estável"

    score = {"P": 2, "EA": 1, "I": 0}
    recente = [score.get(h["status"], 1) for h in historico[-4:]]
    anterior = [score.get(h["status"], 1) for h in historico[-8:-4]] if len(historico) >= 8 else recente

    media_recente  = sum(recente)  / len(recente)
    media_anterior = sum(anterior) / len(anterior)
    delta = media_recente - media_anterior

    if delta > 0.5:
        return "melhorando"
    elif delta < -0.5:
        return "piorando"
    return "estável"


def run(dry_run: bool = False):
    """Execução principal do scraper."""
    log.info("=== Scraper SEMACE Balneabilidade iniciado ===")

    # 1. Descobre URLs dos PDFs
    try:
        pdf_links = fetch_page_links()
    except Exception as e:
        log.error("Falha ao acessar página da SEMACE: %s", e)
        sys.exit(1)

    # 2. Baixa e processa cada PDF
    todos_status = {}
    periodo_principal = ""

    for tipo, url in pdf_links.items():
        pdf_bytes = download_pdf(url)
        if not pdf_bytes:
            log.warning("Pulando boletim %s (falha no download)", tipo)
            continue

        text = extract_text_from_pdf(pdf_bytes)

        if tipo == "fortaleza":
            status = parse_fortaleza(text)
            periodo_principal = get_periodo_fortaleza(text)
        else:
            status = parse_estado(text)
            if not periodo_principal:
                periodo_principal = get_periodo_estado(text)

        todos_status.update(status)
        log.info("Boletim '%s': %d pontos processados", tipo, len(status))

    if not todos_status:
        log.error("Nenhum dado extraído. Abortando.")
        sys.exit(1)

    log.info("Total de pontos com status: %d / %d", len(todos_status), len(PONTOS_META))

    # 3. Carrega histórico existente e mescla
    existing = load_existing(OUTPUT_FILE)
    pontos_atualizados = merge_historico(existing, todos_status, periodo_principal)

    # 4. Adiciona tendência calculada
    for cod in pontos_atualizados:
        hist = pontos_atualizados[cod].get("historico", [])
        pontos_atualizados[cod]["tendencia"] = calcular_tendencia(hist)

    # 5. Monta JSON final
    output = {
        "meta": {
            "gerado_em": datetime.datetime.now(
                datetime.timezone(datetime.timedelta(hours=-3))
            ).isoformat(),
            "fonte": "SEMACE — Superintendência Estadual do Meio Ambiente do Ceará",
            "url_fonte": BASE_URL,
            "periodo_atual": periodo_principal,
            "total_pontos": len(pontos_atualizados),
            "semanas_historico": MAX_HISTORICO,
        },
        "pontos": pontos_atualizados,
    }

    # 6. Salva (ou exibe em dry-run)
    if dry_run:
        print(json.dumps(output, ensure_ascii=False, indent=2)[:3000])
        print(f"\n... (dry-run: {len(pontos_atualizados)} pontos, não salvo)")
    else:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(output, f, ensure_ascii=False, indent=2)
        size_kb = OUTPUT_FILE.stat().st_size / 1024
        log.info("Salvo em %s (%.1f KB)", OUTPUT_FILE, size_kb)

    log.info("=== Concluído: período=%s, pontos=%d ===",
             periodo_principal, len(pontos_atualizados))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scraper SEMACE Balneabilidade")
    parser.add_argument("--dry-run", action="store_true",
                        help="Imprime resultado sem salvar arquivo")
    args = parser.parse_args()
    run(dry_run=args.dry_run)
