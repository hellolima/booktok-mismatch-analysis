import requests
import pandas as pd
import os
from dotenv import load_dotenv
import time

load_dotenv()
GOOGLE_API_KEY = os.getenv("GOOGLE_BOOKS_API_KEY")

# =====================================================================
# FLAG DE CONTROLE DE BUSCA
# True  -> Busca direcionada (Lê o CSV do Hardcover e busca os ISBNs)
# False -> Busca aleatória (Busca genérica pela categoria "New Adult")
# =====================================================================
BUSCAR_POR_ISBN_HARDCOVER = True


def extrair_dados_volume(item):
    """Função auxiliar para parsear o JSON de um livro do Google Books"""
    vol_info = item.get("volumeInfo", {})
    
    titulo = vol_info.get("title", "Sem Título")
    autores = ", ".join(vol_info.get("authors", ["Desconhecido"]))
    sinopse = vol_info.get("description", "Sem sinopse")
    
    image_links = vol_info.get("imageLinks", {})
    capa_url = image_links.get("thumbnail") or image_links.get("smallThumbnail")
    
    identifiers = vol_info.get("industryIdentifiers", [])
    isbn_encontrado = None
    for idx in identifiers:
        # Pega o ISBN 13 ou 10
        if idx.get("type") in ["ISBN_13", "ISBN_10"]:
            isbn_encontrado = idx.get("identifier")
            if idx.get("type") == "ISBN_13":
                break # Prefere o ISBN 13
                
    return {
        "ISBN": isbn_encontrado,
        "Titulo": titulo,
        "Autor": autores,
        "Sinopse": sinopse,
        "Capa_URL": capa_url
    }


def coletar_google_books():
    url = "https://www.googleapis.com/books/v1/volumes"
    dataset = []
    
    if BUSCAR_POR_ISBN_HARDCOVER:
        print("Modo de Enriquecimento: Lendo ISBNs do Hardcover...")
        caminho_hc = '../data/dataset_booktok_poc1.csv'
        
        if not os.path.exists(caminho_hc):
            print(f"Erro: Arquivo {caminho_hc} não encontrado.")
            return pd.DataFrame()
            
        # Lê o CSV e limpa os ISBNs
        df_hc = pd.read_csv(caminho_hc)
        isbns_alvo = df_hc['ISBN'].dropna().astype(str).str.replace('.0', '', regex=False).unique()
        
        print(f"Encontrados {len(isbns_alvo)} ISBNs únicos. Iniciando chamadas para a API...")
        
        for isbn in isbns_alvo:
            max_tentativas = 3
            sucesso = False
            
            for tentativa in range(max_tentativas):
                params = {
                    "q": f"isbn:{isbn}",
                    "key": GOOGLE_API_KEY
                }
                response = requests.get(url, params=params)
                
                if response.status_code == 200:
                    dados = response.json()
                    itens = dados.get("items", [])
                    if itens:
                        dataset.append(extrair_dados_volume(itens[0]))
                    sucesso = True
                    break # Sai do loop de tentativas porque deu certo
                    
                elif response.status_code == 503:
                    tempo_espera = 2 ** tentativa # Espera 1s, depois 2s, depois 4s
                    print(f"Servidor do Google falhou (503) no ISBN {isbn}. Tentando de novo em {tempo_espera}s... ({tentativa+1}/{max_tentativas})")
                    time.sleep(tempo_espera)
                else:
                    print(f"Erro fatal {response.status_code} no ISBN {isbn}.")
                    break # Se for erro 400 ou 404, não adianta tentar de novo
            
            if not sucesso:
                print(f" Pulando ISBN {isbn} - O Google Books não conseguiu processar este livro.")
                
            time.sleep(1.5)
                
    else:
        print("Modo Genérico: Buscando aleatoriamente pela categoria 'New Adult'...")
        params = {
            "q": 'subject:"New Adult"',
            "maxResults": 40,
            "printType": "books",
            "langRestrict": "pt",
            "key": GOOGLE_API_KEY
        }
        
        response = requests.get(url, params=params)
        if response.status_code == 200:
            dados = response.json()
            itens = dados.get("items", [])
            for item in itens:
                dataset.append(extrair_dados_volume(item))
        else:
            print(f"Erro na API: {response.status_code}")

    return pd.DataFrame(dataset)

if __name__ == "__main__":
    try:
        df_livros = coletar_google_books()
        
        if df_livros is None or df_livros.empty:
            raise Exception("Nenhum dado retornado ou ocorreu um erro na coleta.")
            
        caminho_arquivo = '../data/dataset_consolidado_poc1.csv'
        if not os.path.exists('../data'): os.makedirs('../data')
        df_livros.to_csv(caminho_arquivo, index=False, encoding='utf-8')
        
        print("Coleta realizada com sucesso!")
        
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', 1000)
        
        print(df_livros[['ISBN', 'Titulo', 'Sinopse']].head())
    except Exception as e:
        print(f"Falha na execução: {e}")