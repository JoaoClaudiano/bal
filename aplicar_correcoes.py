#!/usr/bin/env python3
"""
Aplica as correções de coordenadas exportadas pelo mapa ao scraper_semace.py.

Uso:
    python aplicar_correcoes.py coord_corrections.json

O arquivo JSON deve ter o formato gerado pelo botão "Exportar" do mapa:
    {
      "corrections": {
        "56OE": {"lat": -3.344123, "lng": -39.134567, "praia": "Lagoinha"},
        ...
      }
    }

As linhas correspondentes em scraper_semace.py são atualizadas in-place.
"""

import json
import re
import sys
from pathlib import Path

SCRAPER = Path(__file__).parent / "scraper_semace.py"


def aplicar(json_path: str) -> None:
    data = json.loads(Path(json_path).read_text(encoding="utf-8"))

    # Aceita dois formatos:
    # 1. {"corrections": {"56OE": {"lat":..., "lng":..., "praia":...}}}  ← só corrigidas
    # 2. {"pontos": {"56OE": {"lat":..., "lng":..., ..., "corrigido": true/false}}}  ← tudo
    if "corrections" in data:
        entries = data["corrections"]
        modo = "corrections"
    elif "pontos" in data:
        # Do formato "tudo", aplica apenas os que têm corrigido=True
        entries = {k: v for k, v in data["pontos"].items() if v.get("corrigido")}
        modo = "pontos"
    else:
        print("Formato de arquivo não reconhecido. Esperado: chave 'corrections' ou 'pontos'.")
        return

    if not entries:
        msg = "Nenhuma correção encontrada no arquivo."
        if modo == "pontos":
            msg += " (nenhum ponto com corrigido=true)"
        print(msg)
        return

    texto = SCRAPER.read_text(encoding="utf-8")
    alterados = []
    nao_encontrados = []

    for cod, c in entries.items():
        lat = c["lat"]
        lng = c["lng"]
        # Procura a linha com o código e substitui lat/lng
        padrao = re.compile(
            rf'("{re.escape(cod)}":\s*\{{[^}}]*?"lat"\s*:)\s*[-\d.]+(\s*,\s*"lng"\s*:)\s*[-\d.]+'
        )
        novo_texto, n = padrao.subn(
            rf'\g<1>{lat}\g<2>{lng}',
            texto
        )
        if n:
            texto = novo_texto
            praia = c.get("praia", "")
            print(f"  ✔  {cod:6s}  {praia:30s}  lat={lat}  lng={lng}")
            alterados.append(cod)
        else:
            print(f"  ✗  {cod}: não encontrado em scraper_semace.py")
            nao_encontrados.append(cod)

    if alterados:
        SCRAPER.write_text(texto, encoding="utf-8")
        print(f"\n{len(alterados)} coordenada(s) atualizada(s) em scraper_semace.py.")
    if nao_encontrados:
        print(f"{len(nao_encontrados)} código(s) não encontrado(s): {nao_encontrados}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: python aplicar_correcoes.py coord_corrections.json")
        sys.exit(1)
    aplicar(sys.argv[1])
