import requests
import pandas as pd
import os
from dotenv import load_dotenv
from deep_translator import GoogleTranslator

load_dotenv()
GOOGLE_API_KEY = os.getenv("GOOGLE_BOOKS_API_KEY")

def coletar_google_books_new_adult(max_results=40):
    url = "https://www.googleapis.com/books/v1/volumes"
    
    # q=subject:"New Adult"
    params = {
        "q": 'subject:"New Adult"',
        "maxResults": max_results,
        "printType": "books",
        "langRestrict": "pt", # tenta forçar o retorno de sinopses em Português (nao deu certo)
        "key": GOOGLE_API_KEY
    }
    
    response = requests.get(url, params=params)
    
    if response.status_code != 200:
        print(f"Erro na API: {response.status_code}")
        return pd.DataFrame()
        
    dados = response.json()
    livros = dados.get("items", [])
    dataset = []
    
    tradutor = GoogleTranslator(source='auto', target='pt')
    
    for item in livros:
        vol_info = item.get("volumeInfo", {})
        
        titulo = vol_info.get("title", "Sem Título")
        autores = ", ".join(vol_info.get("authors", ["Desconhecido"]))
        sinopse_original = vol_info.get("description", "Sem sinopse")
        
        sinopse_pt = "Sem sinopse"
        if sinopse_original != "Sem sinopse":
            try:
                # Traduz o texto para o português
                sinopse_pt = tradutor.translate(sinopse_original)
            except Exception as e:
                print(f"Aviso: Falha ao traduzir sinopse do livro '{titulo}'. Erro: {e}")
                sinopse_pt = sinopse_original # Fallback para o original em caso de erro
        
        image_links = vol_info.get("imageLinks", {})
        capa_url = image_links.get("thumbnail") or image_links.get("smallThumbnail")
        
        identifiers = vol_info.get("industryIdentifiers", [])
        isbn13 = None
        for idx in identifiers:
            if idx.get("type") == "ISBN_13":
                isbn13 = idx.get("identifier")
                break
        
        dataset.append({
            "ISBN13": isbn13,
            "Titulo": titulo,
            "Autor": autores,
            "Sinopse": sinopse_pt,
            "Capa_URL": capa_url
        })
        
    return pd.DataFrame(dataset)

if __name__ == "__main__":
    try:
        df_livros = coletar_google_books_new_adult()
        
        if df_livros is None or df_livros.empty:
            raise Exception("Nenhum livro retornado ou falha ao gerar o Dataset.")
            
        caminho_arquivo = 'data/dataset_googlebooks_poc1.csv'
        if not os.path.exists('data'): os.makedirs('data')
        df_livros.to_csv(caminho_arquivo, index=False, encoding='utf-8')
        
        print("Coleta realizada com sucesso!")
        
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', 1000)
        
        print(df_livros[['ISBN13', 'Titulo', 'Autor']].head())
    except Exception as e:
        print(f"Falha na execução: {e}")