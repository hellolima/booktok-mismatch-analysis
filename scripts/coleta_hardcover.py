import os
import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()
API_TOKEN = os.getenv("HARDCOVER_API_TOKEN")

def executar_query(query):
    url = "https://api.hardcover.app/v1/graphql"
    
    token = API_TOKEN.strip()
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"{token}",
        "User-Agent": "POC-BookTok-Scraper/1.0"
    }
    
    ## print(f"DEBUG: Enviando Authorization header: Bearer {token[:5]}...") # Mostra apenas o início do token
    
    response = requests.post(url, headers=headers, json={'query': query})
    
    print(f"Status Code: {response.status_code}")
    return response.json()

def processar_dados(raw_data):
    """Transforma o JSON aninhado em uma lista de dicionários planos."""
    books = raw_data['data']['books']
    dataset = []
    
    for book in books:
        author = book['contributions'][0]['author']['name'] if book.get('contributions') else None
        
        cached = book.get('cached_tags', {})
        cw_list = [cw['tag'] for cw in cached.get('Content Warning', [])]
        
        dataset.append({
            'Titulo': book.get('title'),
            'Autor': author,
            'Ano_Publicacao': book.get('release_year'),
            'Capa_URL': book['image']['url'] if book.get('image') else None,
            'Content_Warnings': ", ".join(cw_list),
            'Is_Explicit': any(tag in ["Sexual content", "sexual harassment"] for tag in cw_list)
        })
    return pd.DataFrame(dataset)

# Query principal de coleta
query = """
query ColetaPOC1 {
  books(where: { taggings: { tag: { slug: { _eq: "new-adult" } } } }, limit: 50) {
    title
    release_year
    image { url }
    contributions { author { name } }
    cached_tags
  }
}
"""

if __name__ == "__main__":
    try:
        dados_brutos = executar_query(query)
        
        if 'data' not in dados_brutos:
            raise Exception(f"A chave 'data' não foi encontrada. Resposta: {dados_brutos}")
            
        df = processar_dados(dados_brutos)
        
        if not os.path.exists('data'): os.makedirs('data')
        df.to_csv("data/dataset_booktok_poc1.csv", index=False)
        
        print("Coleta realizada com sucesso!")
        print(df.head())
    except Exception as e:
        print(f"Falha na execução: {e}")