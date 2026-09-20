"""
merge_datasets_v5_debug.py

Versão DEBUGG com logging extremamente detalhado.
Ajuda a diagnosticar por que Camada 3 não encontra resultados.

Mudanças vs v5:
  1. Remove filtro "country=BR" (pode estar bloqueando resultados)
  2. Adiciona logging de TODOS os resultados do Google Books
  3. Mostra por que cada resultado foi rejeitado
  4. Tenta busca alternativa se falhar
  5. Fallback: busca SEM langRestrict se nada funcionar

Uso:
    python merge_datasets_v5_debug.py 4
"""

import requests
import pandas as pd
import os
import sys
import time
import random
import logging
import re
from typing import Dict, Optional, List, Tuple
from dotenv import load_dotenv

load_dotenv()
GOOGLE_API_KEY = os.getenv("GOOGLE_BOOKS_API_KEY")

BASE_URL = "https://www.googleapis.com/books/v1/volumes"
OPENLIBRARY_ISBN_URL = "https://openlibrary.org/isbn/{isbn}.json"
OPENLIBRARY_EDITIONS_URL = "https://openlibrary.org{work_key}/editions.json"

HEADERS_OPENLIBRARY = {
    "User-Agent": "POC1-UFMG-BooktokDataset/1.0"
}

# =====================================================================
# CONFIGURAÇÃO
# =====================================================================
BUSCAR_POR_ISBN_HARDCOVER = True
CAMINHO_HC = "../data/dataset_hardcover_poc1.csv"
CAMINHO_SAIDA = "../data/dataset_consolidado_poc1.csv"

MAX_TENTATIVAS = 5
BACKOFF_BASE = 1.5
BACKOFF_TETO = 30
THROTTLE_MIN = 1.0
TIMEOUT_OPENLIBRARY = 5

# ⚠️ REMOVIDO country="BR" por estar muito restritivo
# PAIS_ALVO = "BR"
MAX_RESULTADOS_FALLBACK = 15  # Aumentado para debug

TOLERANCIA_YEAR = 1
TOLERANCIA_PAGES = 30
MIN_SINOPSE_MATCH = 2

LIMITE_REGISTROS = 4
CHECKPOINT_A_CADA = 5

logging.basicConfig(
    level=logging.DEBUG,  # ← CHANGED to DEBUG
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("../data/coleta_google_books_debug.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)


def _sleep_com_jitter(tentativa: int) -> float:
    teto = min(BACKOFF_TETO, BACKOFF_BASE * (2 ** tentativa))
    espera = random.uniform(0, teto)
    time.sleep(espera)
    return espera


def extrair_dados_volume(item: dict) -> dict:
    vol_info = item.get("volumeInfo", {})
    access_info = item.get("accessInfo", {})
    search_info = item.get("searchInfo", {})

    titulo = vol_info.get("title", "Sem Título")
    autores = ", ".join(vol_info.get("authors", ["Desconhecido"]))
    sinopse = vol_info.get("description", "")
    idioma = vol_info.get("language")
    ano_publicacao = vol_info.get("publishedDate", "")
    num_paginas = vol_info.get("pageCount")

    image_links = vol_info.get("imageLinks", {})
    capa_url = image_links.get("thumbnail") or image_links.get("smallThumbnail")

    identifiers = vol_info.get("industryIdentifiers", [])
    isbn_encontrado = None
    for idx in identifiers:
        if idx.get("type") in ["ISBN_13", "ISBN_10"]:
            isbn_encontrado = idx.get("identifier")
            if idx.get("type") == "ISBN_13":
                break

    viewability = access_info.get("viewability", "NONE")
    embeddable = access_info.get("embeddable", False)
    web_reader_link = access_info.get("webReaderLink")
    text_snippet = search_info.get("textSnippet")

    return {
        "ISBN": isbn_encontrado,
        "Titulo": titulo,
        "Autor": autores,
        "Idioma": idioma,
        "Sinopse": sinopse if sinopse else None,
        "Sinopse_Curta_Flag": len(sinopse) < 200,
        "Capa_URL": capa_url,
        "Viewability": viewability,
        "Embeddable": embeddable,
        "WebReaderLink": web_reader_link,
        "TextSnippet": text_snippet,
        "Ano_Publicacao": ano_publicacao,
        "Num_Paginas": num_paginas,
        "Fonte_Match": None,
    }


def _requisitar_com_backoff(params: dict, contexto: str) -> Optional[dict]:
    for tentativa in range(MAX_TENTATIVAS):
        try:
            log.debug(f"[{contexto}] Requisição: {params}")
            response = requests.get(BASE_URL, params=params, timeout=(3.05, 10))
        except requests.exceptions.RequestException as e:
            log.warning(f"[{contexto}] Erro de rede: {e}. Tentativa {tentativa+1}/{MAX_TENTATIVAS}")
            _sleep_com_jitter(tentativa)
            continue

        if response.status_code == 200:
            dados = response.json()
            num_items = len(dados.get("items", []))
            log.debug(f"[{contexto}] ✅ Status 200. {num_items} items retornados")
            return dados

        elif response.status_code == 503:
            espera = _sleep_com_jitter(tentativa)
            log.warning(
                f"[{contexto}] 503 recebido. Backoff de {espera:.1f}s "
                f"(tentativa {tentativa+1}/{MAX_TENTATIVAS})"
            )
            continue

        elif response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 0))
            espera = max(retry_after, _sleep_com_jitter(tentativa))
            log.warning(f"[{contexto}] 429 (rate limit). Aguardando {espera:.1f}s")
            time.sleep(espera if isinstance(espera, (int, float)) else 5)
            continue

        else:
            log.error(f"[{contexto}] Erro {response.status_code}: {response.text[:200]}")
            return None

    log.error(f"[{contexto}] Esgotadas {MAX_TENTATIVAS} tentativas.")
    return None


def buscar_por_isbn(isbn: str) -> Optional[dict]:
    params = {"q": f"isbn:{isbn}", "key": GOOGLE_API_KEY}
    dados = _requisitar_com_backoff(params, contexto=f"ISBN {isbn}")
    if dados and dados.get("items"):
        item = dados["items"][0]
        idioma = item.get("volumeInfo", {}).get("language")
        if idioma == "pt":
            log.info(f"✅ ISBN {isbn} retornou edição PT")
            return item
        else:
            log.debug(f"❌ ISBN {isbn} retornou '{idioma}', não PT")
    return None


def resolver_isbn_br_openlibrary(isbn_en: str) -> Optional[str]:
    try:
        r = requests.get(
            OPENLIBRARY_ISBN_URL.format(isbn=isbn_en),
            headers=HEADERS_OPENLIBRARY,
            timeout=TIMEOUT_OPENLIBRARY
        )
    except requests.exceptions.RequestException as e:
        log.debug(f"[OL ISBN {isbn_en}] Timeout: {e}")
        return None

    if r.status_code != 200:
        log.debug(f"[OL ISBN {isbn_en}] Status {r.status_code}")
        return None

    works = r.json().get("works", [])
    if not works:
        log.debug(f"[OL ISBN {isbn_en}] Nenhum work")
        return None

    work_key = works[0].get("key")
    if not work_key:
        return None

    try:
        r_ed = requests.get(
            OPENLIBRARY_EDITIONS_URL.format(work_key=work_key),
            headers=HEADERS_OPENLIBRARY,
            timeout=TIMEOUT_OPENLIBRARY
        )
    except requests.exceptions.RequestException as e:
        log.debug(f"[OL edições {work_key}] Timeout: {e}")
        return None

    if r_ed.status_code != 200:
        return None

    for edicao in r_ed.json().get("entries", []):
        idiomas = edicao.get("languages", [])
        for lang_obj in idiomas:
            lang_key = lang_obj.get("key", "") if isinstance(lang_obj, dict) else str(lang_obj)
            if "por" in lang_key.lower():
                isbns_ed = edicao.get("isbn_13") or edicao.get("isbn_10") or []
                if isbns_ed:
                    isbn_br = isbns_ed[0] if isinstance(isbns_ed, list) else isbns_ed
                    log.info(f"✅ OpenLibrary: ISBN BR {isbn_br}")
                    return isbn_br

    log.debug(f"[OL {work_key}] Nenhuma edição PT-BR")
    return None


def extrair_palavras_chave_sinopse(sinopse: str, n_palavras: int = 5) -> List[str]:
    if not sinopse or len(sinopse) < 20:
        return []

    stopwords = {
        "a", "o", "e", "é", "que", "do", "da", "de", "um", "uma", "em",
        "para", "com", "por", "as", "os", "das", "dos", "não", "mas",
        "mais", "como", "se", "na", "no", "nas", "nos", "esta", "este",
        "seu", "sua", "seus", "suas", "ao", "aos", "à", "às", "tem",
        "sido", "ser", "está", "estão", "foi", "são", "tem", "tinha",
        "há", "entre", "também", "muito", "quando", "este", "esse",
    }

    sinopse_limpa = re.sub(r'[^\w\s]', ' ', sinopse.lower())
    palavras = sinopse_limpa.split()

    palavras_validas = [
        p for p in palavras
        if p not in stopwords and len(p) > 3
    ]

    return sorted(palavras_validas[:n_palavras], key=len, reverse=True)


def validar_match_por_metadados(
    item_pt: dict,
    ano_original: Optional[str],
    num_paginas_original: Optional[int],
    sinopse_original: str,
    titulo_pt: str  # Para logging
) -> Tuple[bool, float]:
    """Valida com logging detalhado"""
    
    vol_info_pt = item_pt.get("volumeInfo", {})
    ano_pt = vol_info_pt.get("publishedDate", "")
    num_paginas_pt = vol_info_pt.get("pageCount")
    sinopse_pt = vol_info_pt.get("description", "")

    score_confianca = 0.0
    validacoes = []

    # Heurística 1: Ano
    if ano_original and ano_pt:
        try:
            year_orig = int(ano_original[:4])
            year_pt = int(ano_pt[:4])
            diff_year = abs(year_orig - year_pt)
            if diff_year <= TOLERANCIA_YEAR:
                score_confianca += 0.4
                validacoes.append(f"year {diff_year}yr_diff ✓")
            else:
                validacoes.append(f"year_diff={diff_year} ✗")
        except (ValueError, TypeError):
            validacoes.append("year_parse_fail")
    else:
        validacoes.append("year_na")

    # Heurística 2: Páginas
    if num_paginas_original and num_paginas_pt:
        try:
            pages_orig = int(num_paginas_original)
            pages_pt = int(num_paginas_pt)
            diff_pages = abs(pages_orig - pages_pt)
            if diff_pages <= TOLERANCIA_PAGES:
                score_confianca += 0.3
                validacoes.append(f"pages_diff={diff_pages} ✓")
            else:
                validacoes.append(f"pages_diff={diff_pages} ✗")
        except (ValueError, TypeError):
            validacoes.append("pages_parse_fail")
    else:
        validacoes.append("pages_na")

    # Heurística 3: Sinopse
    if sinopse_original and sinopse_pt:
        palavras_orig = extrair_palavras_chave_sinopse(sinopse_original, n_palavras=5)
        sinopse_pt_lower = sinopse_pt.lower()

        match_count = sum(1 for p in palavras_orig if p in sinopse_pt_lower)

        if match_count >= MIN_SINOPSE_MATCH:
            score_confianca += 0.3
            validacoes.append(f"sinopse={match_count}/{len(palavras_orig)} ✓")
        else:
            validacoes.append(f"sinopse={match_count}/{len(palavras_orig)} ✗")
    else:
        validacoes.append("sinopse_na")

    match_valido = score_confianca >= 0.5

    log.debug(
        f"    '{titulo_pt}': {' | '.join(validacoes)} → Score={score_confianca:.1%}, "
        f"Válido={match_valido}"
    )

    return match_valido, score_confianca


def buscar_por_autor_com_validacao(
    autor: str,
    ano_original: Optional[str],
    num_paginas_original: Optional[int],
    sinopse_original: str,
    titulo_original: str
) -> Optional[dict]:
    """
    DEBUG VERSION: Logging extremamente detalhado
    """
    if not autor:
        log.debug("Autor vazio")
        return None

    primeiro_autor = autor.split(",")[0].strip()
    query_autor = f'inauthor:"{primeiro_autor}"'

    # ─────────────────────────────────────────────────────────
    # TENTATIVA 1: Com langRestrict=pt (mas SEM country)
    # ─────────────────────────────────────────────────────────
    log.info(f"[Camada 3] Tentativa 1: inauthor + langRestrict=pt (SEM country)")
    
    params = {
        "q": query_autor,
        "langRestrict": "pt",
        # ← REMOVIDO: "country": "BR",
        "maxResults": MAX_RESULTADOS_FALLBACK,
        "key": GOOGLE_API_KEY,
    }

    log.debug(f"  Query: {query_autor}")
    log.debug(f"  Params: {params}")
    
    dados = _requisitar_com_backoff(params, contexto=f"Autor '{primeiro_autor}' (lang=pt)")

    if not dados or not dados.get("items"):
        log.info(f"[Camada 3] ❌ Nenhum resultado na Tentativa 1")
        
        # ─────────────────────────────────────────────────────────
        # TENTATIVA 2: Sem langRestrict (mais permissivo)
        # ─────────────────────────────────────────────────────────
        log.info(f"[Camada 3] Tentativa 2: inauthor (SEM langRestrict)")
        
        params = {
            "q": query_autor,
            # ← REMOVIDO: "langRestrict": "pt",
            "maxResults": MAX_RESULTADOS_FALLBACK,
            "key": GOOGLE_API_KEY,
        }
        
        log.debug(f"  Query: {query_autor}")
        log.debug(f"  Params (sem langRestrict): {params}")
        
        dados = _requisitar_com_backoff(params, contexto=f"Autor '{primeiro_autor}' (sem lang)")
        
        if not dados or not dados.get("items"):
            log.info(f"[Camada 3] ❌ Nenhum resultado em AMBAS as tentativas")
            return None

    items = dados.get("items", [])
    log.info(f"[Camada 3] {len(items)} resultados retornados. Analisando...")

    # Iterar e validar
    for idx, item in enumerate(items):
        vol_info = item.get("volumeInfo", {})
        titulo_pt = vol_info.get("title", "")
        idioma = vol_info.get("language", "")
        
        log.debug(f"  [{idx+1}/{len(items)}] '{titulo_pt}' (idioma: {idioma})")

        # Verificar idioma
        if idioma not in ["pt", "por", "pt-br", "pt-pt", "pt-BR"]:
            log.debug(f"    → Idioma '{idioma}' ≠ PT. Skip.")
            continue

        log.debug(f"    → Idioma OK (PT). Validando metadados...")

        # Validar com metadados
        match_valido, score = validar_match_por_metadados(
            item,
            ano_original,
            num_paginas_original,
            sinopse_original,
            titulo_pt
        )

        if match_valido:
            log.info(
                f"[Camada 3] ✅ MATCH VÁLIDO: '{titulo_pt}' "
                f"(score={score:.1%}, idioma={idioma})"
            )
            return item
        else:
            log.debug(f"    → Score {score:.1%} < 50%. Rejeitado.")

    log.info(f"[Camada 3] ⚠️  {len(items)} resultado(s) retornado(s), mas nenhum validado")
    return None


def _salvar_checkpoint(df_parcial: pd.DataFrame, processados: int, total: int) -> None:
    if not os.path.exists("../data"):
        os.makedirs("../data")
    df_parcial.to_csv(CAMINHO_SAIDA, index=False, encoding="utf-8")
    pct = (processados / total) * 100 if total else 0
    log.info(f"--- Checkpoint: {processados}/{total} ({pct:.0f}%) ---")


def coletar_google_books() -> pd.DataFrame:
    dataset = []

    if BUSCAR_POR_ISBN_HARDCOVER:
        log.info("Iniciando coleta DEBUG...")

        if not os.path.exists(CAMINHO_HC):
            log.error(f"Arquivo {CAMINHO_HC} não encontrado")
            return pd.DataFrame()

        df_hc = pd.read_csv(CAMINHO_HC)
        df_hc["ISBN"] = df_hc["ISBN"].dropna().astype(str).str.replace(".0", "", regex=False)

        col_titulo = "Titulo" if "Titulo" in df_hc.columns else None
        col_autor = "Autor" if "Autor" in df_hc.columns else None

        limite = LIMITE_REGISTROS
        if len(sys.argv) > 1:
            try:
                limite = int(sys.argv[1])
            except ValueError:
                pass

        if limite:
            df_hc = df_hc.head(limite)

        total = len(df_hc)
        log.info(f"Processando {total} registros (DEBUG MODE)")

        for i, row in df_hc.iterrows():
            isbn = row.get("ISBN")
            titulo_hc = row.get(col_titulo) if col_titulo else None
            autor_hc = row.get(col_autor) if col_autor else None
            ano_hc = row.get("Ano") if "Ano" in df_hc.columns else None
            paginas_hc = row.get("Paginas") if "Paginas" in df_hc.columns else None
            sinopse_hc = row.get("Sinopse") if "Sinopse" in df_hc.columns else ""

            item, fonte = None, None

            log.info(f"\n{'='*80}")
            log.info(f"[{i+1}/{total}] '{titulo_hc}' (ISBN: {isbn}, Autor: {autor_hc})")
            log.info(f"{'='*80}")

            # CAMADA 1
            if isbn:
                log.info(f"→ Camada 1 (ISBN direto)")
                item = buscar_por_isbn(str(isbn))
                fonte = "isbn" if item else None

            # CAMADA 2
            if item is None and isbn:
                log.info(f"→ Camada 2 (OpenLibrary)")
                isbn_br = resolver_isbn_br_openlibrary(str(isbn))
                if isbn_br:
                    item = buscar_por_isbn(isbn_br)
                    fonte = "openlibrary_isbn_br" if item else None

            # CAMADA 3 (COM DEBUG)
            if item is None and autor_hc:
                log.info(f"→ Camada 3 (Autor + Metadados - DEBUG)")
                item = buscar_por_autor_com_validacao(
                    autor_hc,
                    ano_hc,
                    paginas_hc,
                    sinopse_hc,
                    titulo_hc
                )
                fonte = "autor_com_validacao" if item else None

            # RESULTADO
            if item:
                registro = extrair_dados_volume(item)
                registro["Fonte_Match"] = fonte
                dataset.append(registro)
                log.info(f"✅ SUCESSO via {fonte}")
            else:
                log.warning(f"❌ SEM MATCH PT-BR para este livro")
                dataset.append({
                    "ISBN": isbn,
                    "Titulo": titulo_hc,
                    "Autor": autor_hc,
                    "Idioma": "pt-br_não_encontrado",
                    "Sinopse": None,
                    "Fonte_Match": "sem_match_ptbr",
                    "Ano_Publicacao": ano_hc,
                    "Num_Paginas": paginas_hc,
                })

            processados = i + 1
            if dataset and (processados % CHECKPOINT_A_CADA == 0 or processados == total):
                _salvar_checkpoint(pd.DataFrame(dataset), processados, total)

            time.sleep(THROTTLE_MIN)

    return pd.DataFrame(dataset)


if __name__ == "__main__":
    log.info("\n" + "="*80)
    log.info("🔍 INICIANDO DEBUG MODE")
    log.info("="*80)
    
    try:
        df_livros = coletar_google_books()

        if df_livros is None or df_livros.empty:
            raise Exception("Nenhum dado retornado")

        if not os.path.exists("../data"):
            os.makedirs("../data")
        df_livros.to_csv(CAMINHO_SAIDA, index=False, encoding="utf-8")

        log.info("\n" + "="*80)
        log.info("✅ COLETA CONCLUÍDA")
        log.info("="*80)
        log.info(f"Distribuição:\n{df_livros['Fonte_Match'].value_counts()}")

        pd.set_option("display.max_columns", None)
        pd.set_option("display.width", 1000)
        print("\n" + df_livros[["ISBN", "Titulo", "Idioma", "Fonte_Match"]].to_string())

    except Exception as e:
        log.exception(f"❌ Falha: {e}")