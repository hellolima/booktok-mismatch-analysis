"""
merge_datasets_v4.py

Pipeline de Enriquecimento e Unificação PT-BR:
1. Cascata de localização PT-BR em 3 camadas determinísticas:
   a) ISBN original (Hardcover): busca no Google Books e valida se o idioma 
      já é "pt". Se for edição em inglês, recusa e passa para as próximas camadas.
   b) OpenLibrary: resolve a obra (work) a partir do ISBN EN e procura uma edição
      brasileira ("por"). Se achar, o ISBN dessa edição BR é reinjetado no Google Books.
   c) Fallback flexível no Google Books: busca por "Título EN + Autor" sem aspas rígidas,
      restrita a country="BR" e langRestrict="pt", permitindo ao Google Books
      retornar edições nacionais com títulos comerciais traduzidos (ex: "Top Secret" -> "No Sigilo",
      "Fourth Wing" -> "Quarta Asa").
2. Rastreamento e Mapeamento de Obras sem Tradução:
   - Registra obras sem tradução PT-BR como Fonte_Match = "sem_match_ptbr".
"""

import requests
import pandas as pd
import os
import sys
import time
import random
import logging
from dotenv import load_dotenv

load_dotenv()
GOOGLE_API_KEY = os.getenv("GOOGLE_BOOKS_API_KEY")

BASE_URL = "https://www.googleapis.com/books/v1/volumes"
OPENLIBRARY_ISBN_URL = "https://openlibrary.org/isbn/{isbn}.json"
OPENLIBRARY_EDITIONS_URL = "https://openlibrary.org{work_key}/editions.json"

HEADERS_OPENLIBRARY = {
    "User-Agent": "POC1-UFMG-BooktokDataset/1.0 (pesquisa-academica-ufmg)"
}

# =====================================================================
# CONFIGURAÇÃO
# =====================================================================
BUSCAR_POR_ISBN_HARDCOVER = True
CAMINHO_HC = "../data/dataset_hardcover_poc1.csv"
CAMINHO_SAIDA = "../data/dataset_consolidado_poc1.csv"

MAX_TENTATIVAS = 5          # backoff com jitter
BACKOFF_BASE = 1.5          # segundos
BACKOFF_TETO = 30           # segundos
THROTTLE_MIN = 1.0          # segundos mínimos entre requisições OK

PAIS_ALVO = "BR"            # usado no fallback do Google Books
MAX_RESULTADOS_FALLBACK = 5 # itera até achar language == "pt"

TIMEOUT_OPENLIBRARY = 5     # timeout de 5s para não travar a fila

# --- Amostragem / checkpoint ---
LIMITE_REGISTROS = 6        # Ajustado para teste dos 6 primeiros
CHECKPOINT_A_CADA = 5       # salva o CSV parcial a cada N registros

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("../data/coleta_google_books.log", encoding="utf-8"),
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
        "Fonte_Match": None,
    }


def _requisitar_com_backoff(params: dict, contexto: str) -> dict | None:
    for tentativa in range(MAX_TENTATIVAS):
        try:
            response = requests.get(BASE_URL, params=params, timeout=(3.05, 10))
        except requests.exceptions.RequestException as e:
            log.warning(f"[{contexto}] Erro de rede: {e}. Tentativa {tentativa+1}/{MAX_TENTATIVAS}")
            _sleep_com_jitter(tentativa)
            continue

        if response.status_code == 200:
            return response.json()

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
            log.error(f"[{contexto}] Erro não recuperável {response.status_code}: {response.text[:200]}")
            return None

    log.error(f"[{contexto}] Esgotadas {MAX_TENTATIVAS} tentativas.")
    return None


def buscar_por_isbn(isbn: str) -> dict | None:
    """Camada 1: busca direta pelo ISBN no Google Books."""
    params = {"q": f"isbn:{isbn}", "key": GOOGLE_API_KEY}
    dados = _requisitar_com_backoff(params, contexto=f"ISBN {isbn}")
    if dados and dados.get("items"):
        item = dados["items"][0]
        if item.get("volumeInfo", {}).get("language") == "pt":
            return item
        else:
            log.info(f"ISBN {isbn} retornou edição em '{item.get('volumeInfo', {}).get('language')}' no Google Books. Ignorando para buscar PT-BR.")
    return None


def resolver_isbn_br_openlibrary(isbn_en: str) -> str | None:
    """Camada 2: consulta a OpenLibrary para obter ISBN de edição brasileira."""
    try:
        r = requests.get(
            OPENLIBRARY_ISBN_URL.format(isbn=isbn_en),
            headers=HEADERS_OPENLIBRARY,
            timeout=TIMEOUT_OPENLIBRARY
        )
    except requests.exceptions.RequestException as e:
        log.warning(f"[OpenLibrary ISBN {isbn_en}] Indisponível ou timeout: {e}")
        return None

    if r.status_code != 200:
        return None

    works = r.json().get("works", [])
    if not works:
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
        log.warning(f"[OpenLibrary edições {work_key}] Indisponível ou timeout: {e}")
        return None

    if r_ed.status_code != 200:
        return None

    for edicao in r_ed.json().get("entries", []):
        idiomas = edicao.get("languages", [])
        if any("por" in lang.get("key", "") for lang in idiomas):
            isbns_ed = edicao.get("isbn_13") or edicao.get("isbn_10") or []
            if isbns_ed:
                return isbns_ed[0]

    return None


def buscar_por_titulo_autor(titulo: str, autor: str) -> dict | None:
    """Camada 3 (Fallback final): busca ampla por Título + Autor sem aspas rígidas."""
    partes_query = []
    if titulo:
        partes_query.append(titulo)
    if autor:
        primeiro_autor = autor.split(",")[0].strip()
        partes_query.append(primeiro_autor)

    if not partes_query:
        return None

    query_str = " ".join(partes_query)

    params = {
        "q": query_str,
        "langRestrict": "pt",
        "country": PAIS_ALVO,
        "maxResults": MAX_RESULTADOS_FALLBACK,
        "key": GOOGLE_API_KEY,
    }
    dados = _requisitar_com_backoff(params, contexto=f"Fallback BR '{query_str}'")
    if not dados:
        return None

    for item in dados.get("items", []):
        if item.get("volumeInfo", {}).get("language") == "pt":
            log.info(f" Match PT-BR encontrado via Fallback: '{item.get('volumeInfo', {}).get('title')}'")
            return item

    if dados.get("items"):
        log.info(
            f"Fallback BR '{query_str}' retornou {len(dados['items'])} item(ns), "
            f"nenhum validado em português ('pt'). Descartado."
        )
    return None


def _salvar_checkpoint(df_parcial: pd.DataFrame, processados: int, total: int) -> None:
    if not os.path.exists("../data"):
        os.makedirs("../data")
    df_parcial.to_csv(CAMINHO_SAIDA, index=False, encoding="utf-8")
    pct = (processados / total) * 100 if total else 0
    log.info(f"--- Checkpoint salvo: {processados}/{total} ({pct:.0f}%) em {CAMINHO_SAIDA} ---")


def coletar_google_books() -> pd.DataFrame:
    dataset = []

    if BUSCAR_POR_ISBN_HARDCOVER:
        log.info("Modo de Enriquecimento: lendo ISBNs (e título/autor de apoio) do Hardcover...")

        if not os.path.exists(CAMINHO_HC):
            log.error(f"Arquivo {CAMINHO_HC} não encontrado.")
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
                log.warning(f"Argumento '{sys.argv[1]}' inválido para limite; ignorando.")

        total_disponivel = len(df_hc)
        if limite is not None:
            df_hc = df_hc.head(limite)
            log.info(f"MODO AMOSTRA: processando {len(df_hc)} de {total_disponivel} registros disponíveis.")
        else:
            log.info(f"Processando os {total_disponivel} registros (lista completa).")

        total = len(df_hc)

        for i, row in df_hc.iterrows():
            isbn = row.get("ISBN")
            titulo_hc = row.get(col_titulo) if col_titulo else None
            autor_hc = row.get(col_autor) if col_autor else None

            item, fonte = None, None

            # 1) Tenta por ISBN original
            if pd.notna(isbn) and isbn:
                item = buscar_por_isbn(str(isbn))
                fonte = "isbn" if item else None

            # 2) Fallback: OpenLibrary resolve um ISBN BR a partir do ISBN EN
            if item is None and pd.notna(isbn) and isbn:
                log.info(f"ISBN {isbn} sem match PT-BR direto. Consultando OpenLibrary por edição BR...")
                isbn_br = resolver_isbn_br_openlibrary(str(isbn))
                if isbn_br:
                    log.info(f"OpenLibrary encontrou ISBN BR {isbn_br} para {isbn}. Revalidando no Google Books...")
                    item = buscar_por_isbn(isbn_br)
                    fonte = "openlibrary_isbn_br" if item else None

            # 3) Fallback final: busca livre por título + autor (country=BR, langRestrict=pt)
            if item is None and (titulo_hc or autor_hc):
                log.info(f"Sem match via ISBN/OpenLibrary. Tentando fallback por título/autor (country=BR, langRestrict=pt)...")
                item = buscar_por_titulo_autor(titulo_hc, autor_hc)
                fonte = "intitle_inauthor_br" if item else None

            if item:
                registro = extrair_dados_volume(item)
                registro["Fonte_Match"] = fonte
                dataset.append(registro)
                log.info(f"[{len(dataset)}/{total}] OK ({fonte}) — Título PT: '{registro['Titulo']}'")
            else:
                log.warning(f"[{i+1}/{total}] SEM MATCH PT-BR (ISBN={isbn}, título='{titulo_hc}')")
                dataset.append({
                    "ISBN": isbn,
                    "Titulo": titulo_hc,
                    "Autor": autor_hc,
                    "Idioma": "pt-br_não_encontrado",
                    "Sinopse": None,
                    "Sinopse_Curta_Flag": True,
                    "Capa_URL": None,
                    "Viewability": None,
                    "Embeddable": False,
                    "WebReaderLink": None,
                    "TextSnippet": None,
                    "Fonte_Match": "sem_match_ptbr",
                })

            processados = i + 1
            if dataset and (processados % CHECKPOINT_A_CADA == 0 or processados == total):
                _salvar_checkpoint(pd.DataFrame(dataset), processados, total)

            time.sleep(THROTTLE_MIN)

    return pd.DataFrame(dataset)


if __name__ == "__main__":
    try:
        df_livros = coletar_google_books()

        if df_livros is None or df_livros.empty:
            raise Exception("Nenhum dado retornado ou ocorreu um erro na coleta.")

        if not os.path.exists("../data"):
            os.makedirs("../data")
        df_livros.to_csv(CAMINHO_SAIDA, index=False, encoding="utf-8")

        log.info("Coleta realizada com sucesso!")
        log.info(f"Distribuição da fonte do match:\n{df_livros['Fonte_Match'].value_counts(dropna=False)}")
        log.info(f"Registros com sinopse curta (<200 chars): {df_livros['Sinopse_Curta_Flag'].sum()}")

        pd.set_option("display.max_columns", None)
        pd.set_option("display.width", 1000)
        print(df_livros[["ISBN", "Titulo", "Idioma", "Fonte_Match", "Sinopse_Curta_Flag", "Viewability"]].head())

    except Exception as e:
        log.exception(f"Falha na execução: {e}")