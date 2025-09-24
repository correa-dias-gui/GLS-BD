import datetime
import psycopg2
from pathlib import Path
import db
import utils
import argparse

def print_schema(conn):
    query = """
    SELECT table_name, column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = 'public'
    ORDER BY table_name, ordinal_position;
    """
    with conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()

    current_table = None
    for table, column, dtype in rows:
        if table != current_table:
            print(f"\n Tabela: {table}")
            current_table = table
        print(f"   - {column}: {dtype}")

def run_sql_file(conn, filepath):
    with open(filepath, "r", encoding="utf-8") as f:
        sql = f.read()
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-host", default="db")
    parser.add_argument("--db-port", type=int, default=5432)
    parser.add_argument("--db-name", default="ecommerce")
    parser.add_argument("--db-user", default="postgres")
    parser.add_argument("--db-pass", default="postgres")
    parser.add_argument("--input", required=True, help="Caminho para o arquivo SNAP")
    args = parser.parse_args()

    conn = psycopg2.connect(
        host=args.db_host, port=args.db_port, dbname=args.db_name,
        user=args.db_user, password=args.db_pass
    )
    
    # Executa o esquema
    run_sql_file(conn, "/app/sql/schema.sql")
    
    # Verifica o esquema
    #print("\n=== Estrutura atual do banco ===")
    #print_schema(conn)

    # Depois faz o ETL para inserir os dados

    # similares = dict()
    # for product in utils.parse_products(filepath):
    #     if not product:
    #         continue  # Produto descontinuado, ignora
        
    #     asin = product["asin"]
    #     print(asin)
    #     title = product["title"]
    #     group_name = product["group"]
    #     salesrank = product["salesrank"]
    #     cat = len(product["categories"])
    #     rev = len(product["reviews"])
    #     down = sum(1 for r in product["reviews"] if r["rating"] <= 2)
    #     rating = (sum(r["rating"] for r in product["reviews"]) / rev) if rev > 0 else None
    #     #print(asin, title, group_name, product["similar"],salesrank, cat, rev, down, rating)
    #     db.insert_product(conn, asin, title, group_name, salesrank, cat, rev, down, rating)
    #     #db.insert_similares(conn, asin, product["similar"])
    #     #print(product["categories"])
    #     similares[asin] = product["similar"]
    #     for categoria in product["categories"]:
    #         db.insert_categoria(conn, asin, categoria)
    #     for review in product["reviews"]:
    #         db.insert_review(conn, asin, review)
    
    # for asin in similares.keys():
    #     print(asin, similares[asin])
    #     db.insert_similares(conn, asin, similares[asin])


filepath = Path(args.input)
BATCH_SIZE = 1000  # Inserir a cada 1000 produtos

# products_batch = []
# reviews_batch = []
# categories_batch = []  # Para tabela Categoria
# produto_categoria_batch = []  # Para tabela Produto_categoria
similares_batch = []
print("Iniciando ETL em modo batch...")
processed_count = 0

for products, categorias_hierarquia, categorias, reviews, similares in utils.parse_1000_products(filepath):
    processed_count += (len(products))
    print(f"Processando {processed_count} produtos...")
    if products:
        print("Inserindo produtos")
        db.insert_product_batch(conn, products)
        
    if reviews:
        print("Inserindo reviews")
        db.insert_review_batch(conn, reviews)
        
    if categorias_hierarquia:
        print("Inserindo categorias")
        print(len(categorias_hierarquia))
        db.insert_categoria_batch(conn, categorias_hierarquia)
        
    if categorias:
        print("Inserindo categorias final")
        db.insert_produto_categoria_batch(conn, categorias)

    print("Inserindo similares")
    similares_batch.extend(similares)
    
 # Inserir produtos similares (após todos os produtos existirem)
print("Inserindo produtos similares...")
if similares_batch:
    db.insert_similares_batch_corrigida(conn, similares_batch)

print(f"ETL finalizado. Total de produtos processados: {processed_count}")

conn.commit()
conn.close()

