"""
merge_datasets_v3.py

Melhorias em relação à v2:
1. Cascata de localização PT-BR em 3 camadas, 100% determinística:
   a) ISBN original (Hardcover, normalmente edição EN) direto no Google Books.
   b) OpenLibrary: resolve a obra (work) a partir do ISBN EN e procura, entre
      as edições cadastradas, uma edição em português (idioma "por"). Se
      achar, o ISBN dessa edição BR é reinjetado no Google Books.
   c) Fallback intitle+inauthor no Google Books, agora com country="BR" e
      maxResults=5, validando explicitamente volumeInfo.language == "pt"
      antes de aceitar o resultado (v2 aceitava items[0] às cegas).
2. Nenhuma camada usa LLM — todo o pipeline permanece auditável e
   reproduzível sem dependência de inferência não-determinística.
3. Fonte_Match agora distingue as 3 camadas: "isbn", "openlibrary_isbn_br",
   "intitle_inauthor_br". Isso vira uma tabela de cobertura por camada no
   EDA do POC I (útil como evidência empírica da esparsidade PT-BR).
4. Mantido de v2: backoff com jitter para 503, throttle proativo,
   extração de sinais de preview/amostra, logging estruturado, modo
   amostra + checkpoint incremental.

Uso:
    python merge_datasets_v3.py            # roda a lista inteira (ou LIMITE_REGISTROS, se definido abaixo)
    python merge_datasets_v3.py 20         # roda só os 20 primeiros registros (sobrescreve LIMITE_REGISTROS)
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

# =====================================================================
# CONFIGURAÇÃO
# =====================================================================
BUSCAR_POR_ISBN_HARDCOVER = True
CAMINHO_HC = "../data/dataset_hardcover_poc1.csv"
CAMINHO_SAIDA = "../data/dataset_consolidado_poc1.csv"

MAX_TENTATIVAS = 5          # subiu de 3 -> 5, já que agora o backoff tem jitter
BACKOFF_BASE = 1.5          # segundos
BACKOFF_TETO = 30           # segundos, evita esperas absurdas em runs longos
THROTTLE_MIN = 1.0          # segundos mínimos entre requisições OK (rate limit proativo)

PAIS_ALVO = "BR"            # usado no fallback intitle+inauthor do Google Books
MAX_RESULTADOS_FALLBACK = 5 # v2 usava 1; agora iteramos até achar language == "pt"

TIMEOUT_OPENLIBRARY = 10    # segundos, para as duas chamadas à OpenLibrary

# --- Amostragem / checkpoint (para validar antes de rodar a lista toda) ---
LIMITE_REGISTROS = 4       # None = roda tudo. Comece pequeno (ex.: 15) para validar o pipeline.
CHECKPOINT_A_CADA = 5       # salva o CSV parcial a cada N registros processados (sucesso ou falha)

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
    """Exponential backoff com jitter (full jitter, padrão AWS).
    Evita que múltiplas requisições retentem exatamente no mesmo instante,
    o que agrava picos de carga e prolonga os 503."""
    teto = min(BACKOFF_TETO, BACKOFF_BASE * (2 ** tentativa))
    espera = random.uniform(0, teto)
    time.sleep(espera)
    return espera


def extrair_dados_volume(item: dict) -> dict:
    """Parseia o JSON de um volume do Google Books, incluindo sinais
    de amostra/preview para uso como fallback textual à sinopse."""
    vol_info = item.get("volumeInfo", {})
    access_info = item.get("accessInfo", {})
    search_info = item.get("searchInfo", {})

    titulo = vol_info.get("title", "Sem Título")
    autores = ", ".join(vol_info.get("authors", ["Desconhecido"]))
    sinopse = vol_info.get("description", "")
    idioma = vol_info.get("language")  # ex.: "pt", "en" — usado na validação da cascata

    image_links = vol_info.get("imageLinks", {})
    capa_url = image_links.get("thumbnail") or image_links.get("smallThumbnail")

    identifiers = vol_info.get("industryIdentifiers", [])
    isbn_encontrado = None
    for idx in identifiers:
        if idx.get("type") in ["ISBN_13", "ISBN_10"]:
            isbn_encontrado = idx.get("identifier")
            if idx.get("type") == "ISBN_13":
                break

    # --- Sinais de preview/amostra (para fallback quando sinopse é curta) ---
    viewability = access_info.get("viewability", "NONE")   # NONE / PARTIAL / ALL_PAGES
    embeddable = access_info.get("embeddable", False)
    web_reader_link = access_info.get("webReaderLink")
    text_snippet = search_info.get("textSnippet")  # só vem quando a query batia numa ocorrência no corpo

    return {
        "ISBN": isbn_encontrado,
        "Titulo": titulo,
        "Autor": autores,
        "Idioma": idioma,
        "Sinopse": sinopse if sinopse else None,
        
        "Capa_URL": capa_url,
        "Viewability": viewability,
        "Embeddable": embeddable,
        "WebReaderLink": web_reader_link,
        "TextSnippet": text_snippet,
        "Fonte_Match": None,  # preenchido pelo chamador: "isbn" | "openlibrary_isbn_br" | "intitle_inauthor_br"
    }


def _requisitar_com_backoff(params: dict, contexto: str) -> dict | None:
    """Executa a chamada GET ao Google Books com exponential backoff + jitter.
    Retorna o JSON da resposta em caso de sucesso, ou None se esgotar tentativas
    ou receber um erro não recuperável (4xx que não seja rate limit)."""
    for tentativa in range(MAX_TENTATIVAS):
        try:
            response = requests.get(BASE_URL, params=params, timeout=10)
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
            # Rate limit explícito: respeita Retry-After se vier no header
            retry_after = int(response.headers.get("Retry-After", 0))
            espera = max(retry_after, _sleep_com_jitter(tentativa))
            log.warning(f"[{contexto}] 429 (rate limit). Aguardando {espera:.1f}s")
            time.sleep(espera if isinstance(espera, (int, float)) else 5)
            continue

        else:
            # 400, 403, 404, etc. -> não adianta retentar
            log.error(f"[{contexto}] Erro não recuperável {response.status_code}: {response.text[:200]}")
            return None

    log.error(f"[{contexto}] Esgotadas {MAX_TENTATIVAS} tentativas.")
    return None


def buscar_por_isbn(isbn: str) -> dict | None:
    """Camada 1: busca direta pelo ISBN (normalmente a edição EN vinda do Hardcover)."""
    params = {"q": f"isbn:{isbn}", "key": GOOGLE_API_KEY}
    dados = _requisitar_com_backoff(params, contexto=f"ISBN {isbn}")
    if dados and dados.get("items"):
        return dados["items"][0]
    return None


def resolver_isbn_br_openlibrary(isbn_en: str) -> str | None:
    """Camada 2: usa a OpenLibrary (gratuita, sem chave) para localizar, a
    partir do ISBN EN, a obra (work) correspondente e, dentro dela, uma
    edição cujo idioma seja português. Retorna o ISBN dessa edição BR, ou
    None se a obra/edição não estiver catalogada na OpenLibrary.

    Não gera nenhum dado final por si só — apenas devolve um ISBN candidato
    que é reinjetado em buscar_por_isbn(), mantendo o Google Books como
    única fonte de sinopse/capa/metadados."""
    try:
        r = requests.get(OPENLIBRARY_ISBN_URL.format(isbn=isbn_en), timeout=TIMEOUT_OPENLIBRARY)
    except requests.exceptions.RequestException as e:
        log.warning(f"[OpenLibrary ISBN {isbn_en}] Erro de rede: {e}")
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
        r_ed = requests.get(OPENLIBRARY_EDITIONS_URL.format(work_key=work_key), timeout=TIMEOUT_OPENLIBRARY)
    except requests.exceptions.RequestException as e:
        log.warning(f"[OpenLibrary edições {work_key}] Erro de rede: {e}")
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
    """Camada 3 (fallback final): busca estruturada intitle+inauthor no Google
    Books, restrita a country="BR" e validando explicitamente que o volume
    retornado está em português. v2 aceitava items[0] às cegas — aqui
    iteramos até MAX_RESULTADOS_FALLBACK candidatos procurando um com
    volumeInfo.language == "pt", já que country=BR restringe disponibilidade
    comercial mas não garante que a única edição indexada seja a brasileira."""
    partes_query = []
    if titulo:
        partes_query.append(f'intitle:"{titulo}"')
    if autor:
        # usa só o primeiro autor listado para evitar over-constraining a query
        primeiro_autor = autor.split(",")[0].strip()
        partes_query.append(f'inauthor:"{primeiro_autor}"')

    if not partes_query:
        return None

    params = {
        "q": " ".join(partes_query),
        "key": GOOGLE_API_KEY,
        "maxResults": MAX_RESULTADOS_FALLBACK,
        "country": PAIS_ALVO,
    }
    dados = _requisitar_com_backoff(params, contexto=f"Fallback BR '{titulo}' / '{autor}'")
    if not dados:
        return None

    for item in dados.get("items", []):
        if item.get("volumeInfo", {}).get("language") == "pt":
            return item

    # Nenhum item em PT entre os candidatos: loga para auditoria, mas não
    # aceita silenciosamente uma edição EN como se fosse a resposta certa.
    if dados.get("items"):
        log.info(
            f"Fallback BR '{titulo}' retornou {len(dados['items'])} item(ns), "
            f"nenhum em português. Descartado (SEM MATCH PT-BR)."
        )
    return None


def _salvar_checkpoint(df_parcial: pd.DataFrame, processados: int, total: int) -> None:
    """Sobrescreve o CSV de saída com o progresso atual. Roda a cada
    CHECKPOINT_A_CADA registros, então você pode abrir o arquivo em outra
    janela (Excel, pandas, VS Code) enquanto a coleta continua."""
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

        # título/autor do Hardcover ficam disponíveis para o fallback --
        # ajuste os nomes das colunas abaixo conforme o schema real do seu CSV.
        col_titulo = "Titulo" if "Titulo" in df_hc.columns else None
        col_autor = "Autor" if "Autor" in df_hc.columns else None

        # Aplica o limite de amostra, se configurado (ou passado via CLI)
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

            # 1) Tenta por ISBN original (normalmente edição EN)
            if pd.notna(isbn) and isbn:
                item = buscar_por_isbn(isbn)
                fonte = "isbn" if item else None

            # 2) Fallback: OpenLibrary resolve um ISBN BR a partir do ISBN EN
            if item is None and pd.notna(isbn) and isbn:
                log.info(f"ISBN {isbn} sem match direto. Consultando OpenLibrary por edição BR...")
                isbn_br = resolver_isbn_br_openlibrary(str(isbn))
                if isbn_br:
                    log.info(f"OpenLibrary encontrou ISBN BR {isbn_br} para {isbn}. Revalidando no Google Books...")
                    item = buscar_por_isbn(isbn_br)
                    fonte = "openlibrary_isbn_br" if item else None

            # 3) Fallback final: intitle + inauthor, restrito a country=BR
            if item is None and (titulo_hc or autor_hc):
                log.info(f"Sem match via ISBN/OpenLibrary. Tentando fallback por título/autor (country=BR)...")
                item = buscar_por_titulo_autor(titulo_hc, autor_hc)
                fonte = "intitle_inauthor_br" if item else None

            if item:
                registro = extrair_dados_volume(item)
                registro["Fonte_Match"] = fonte
                dataset.append(registro)
                log.info(f"[{len(dataset)}/{total}] OK ({fonte}) — '{registro['Titulo']}'")
            else:
                log.warning(f"[{i+1}/{total}] SEM MATCH PT-BR (ISBN={isbn}, título='{titulo_hc}')")

            # --- Checkpoint incremental: permite abrir o CSV e conferir
            # resultados intermediários enquanto o script ainda roda ---
            processados = i + 1
            if dataset and (processados % CHECKPOINT_A_CADA == 0 or processados == total):
                _salvar_checkpoint(pd.DataFrame(dataset), processados, total)

            # Rate limiting proativo (throttle mínimo entre requisições OK)
            time.sleep(THROTTLE_MIN)

    else:
        log.info("Modo Genérico: buscando pela categoria 'New Adult'...")
        params = {
            "q": 'subject:"New Adult"',
            "maxResults": 40,
            "printType": "books",
            "langRestrict": "pt",
            "country": PAIS_ALVO,
            "key": GOOGLE_API_KEY,
        }
        dados = _requisitar_com_backoff(params, contexto="Busca genérica New Adult")
        if dados:
            for item in dados.get("items", []):
                registro = extrair_dados_volume(item)
                registro["Fonte_Match"] = "generico"
                dataset.append(registro)

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
        

        pd.set_option("display.max_columns", None)
        pd.set_option("display.width", 1000)
        print(df_livros[["ISBN", "Titulo", "Idioma", "Fonte_Match", "Viewability"]].head())

    except Exception as e:
        log.exception(f"Falha na execução: {e}")
