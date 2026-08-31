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
        
        isbn_final = None
        origem_isbn = None
        
        # Tenta pegar da edição física padrão
        ed_fisica = book.get('default_physical_edition') or {}
        isbn_final = ed_fisica.get('isbn_13') or ed_fisica.get('isbn_10')
        if isbn_final:
            origem_isbn = "Edição Física Padrão"
        
        # Se não achar, tenta da edição ebook padrão
        if not isbn_final:
            ed_ebook = book.get('default_ebook_edition') or {}
            isbn_final = ed_ebook.get('isbn_13') or ed_ebook.get('isbn_10')
            if isbn_final:
                origem_isbn = "Edição Ebook Padrão"
            
        # Se ainda não achar, varre todas as edições como último recurso
        if not isbn_final:
            edicoes = book.get('editions') or []
            for ed in edicoes:
                encontrado = ed.get('isbn_13') or ed.get('isbn_10')
                if encontrado:
                    isbn_final = encontrado
                    break
        
        dataset.append({
            'ISBN': isbn_final,
            # 'Origem_ISBN': origem_isbn,
            'Titulo': book.get('title'),
            'Autor': author,
            'Ano_Publicacao': book.get('release_year'),
            'Capa_URL': book['image']['url'] if book.get('image') else None,
            'Content_Warnings': ", ".join(cw_list),
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
    default_physical_edition {
      isbn_13
      isbn_10
    }
    default_ebook_edition {
      isbn_13
      isbn_10
    }
    editions {
      isbn_13
      isbn_10
    }
  }
}
"""

# #books(
#   where: { 
#     book_genres: { 
#       genre: { 
#         slug: { _eq: "young-adult" } 
#       } 
#     } 
#   }, 
#   limit: 50
# ) 

if __name__ == "__main__":
    try:
        dados_brutos = executar_query(query)
        
        if 'data' not in dados_brutos:
            raise Exception(f"A chave 'data' não foi encontrada. Resposta: {dados_brutos}")
            
        df = processar_dados(dados_brutos)
        
        if not os.path.exists('data'): os.makedirs('data')
        df.to_csv("data/dataset_hardcover_poc1.csv", index=False)
        
        print("Coleta realizada com sucesso!")
    
        #print(df.head())
        
        print(df[['Titulo', 'Content_Warnings' ]].head(10))
    except Exception as e:
        print(f"Falha na execução: {e}")