# BookTok Mismatch Analysis

Repositório dedicado ao estudo empírico da discrepância (*mismatch*) entre a sinalização visual de capas de livros do gênero "New Adult" e a classificação real de conteúdo (nível de *spice* e alertas de gatilho/content warnings).

Este projeto é parte do **POC (Projeto Orientado em Computação)** do curso de Ciência da Computação da UFMG.

## Descrição do Problema
O fenômeno do BookTok popularizou romances com capas de estética fofa, cartunesca ou em tons pastéis, frequentemente mascarando um conteúdo interno altamente explícito. Este trabalho visa quantificar essa discrepância através da construção de um dataset e de uma análise comparativa, buscando embasamento para discussões sobre design seguro e proteção de menores no ambiente digital.

## Estrutura do Projeto
- `/data`: Datasets gerados e processados.
- `/notebooks`: Análise exploratória e protótipos.
- `/scripts`: Scripts de automação de coleta via API GraphQL.

## Tecnologias Utilizadas
- **Python 3.10+**
- **API GraphQL (Hardcover.app)**
- **Pandas** (processamento de dados)

## Como utilizar
1. Clone este repositório.
2. Instale as dependências: `pip install -r requirements.txt`
3. Configure suas credenciais (não as suba para o GitHub!):
   - Crie um arquivo `.env` com sua `HARDCOVER_API_KEY`.

---