import datetime
import psycopg
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

    conn = psycopg.connect(
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

products_batch = []
reviews_batch = []
categories_batch = []  # Para tabela Categoria
produto_categoria_batch = []  # Para tabela Produto_categoria
similares_batch = []
print("Iniciando ETL em modo batch...")
processed_count = 0

for product in utils.parse_products(filepath):
    if not product:
        continue  # Produto descontinuado, ignora
        
    # Extrair dados do produto
    asin = product["asin"]
    title = product["title"]
    group_name = product["group"]
    salesrank = product["salesrank"]
    cat_count = len(product["categories"])
    rev_count = len(product["reviews"])
    down_count = sum(1 for r in product["reviews"] if r["rating"] <= 2)
    rating_avg = (sum(r["rating"] for r in product["reviews"]) / rev_count) if rev_count > 0 else None
        
    # Acumular produto
    products_batch.append((asin, title, group_name, salesrank, cat_count, rev_count, down_count, rating_avg))
        
        # Processar categorias
    for categoria_hierarchy in product["categories"]:
        id_pai = None
        for cat_obj in categoria_hierarchy:
        # Acumular categoria
            categories_batch.append((cat_obj["id"], cat_obj["name"], id_pai))
            id_pai = cat_obj["id"]
            
            # Relacionar produto com a última categoria (mais específica)
        if categoria_hierarchy:
            last_cat = categoria_hierarchy[-1]
            produto_categoria_batch.append((asin, last_cat["id"]))
        
        # Processar reviews
        for review in product["reviews"]:
            reviews_batch.append((
                asin,
                review["customer"],
                datetime.strptime(review["date"], "%Y-%m-%d"),
                review["rating"],
                review["votes"],
                review["helpful"]
            ))
        
        # Processar produtos similares
        for similar_asin in product["similar"]:
            similares_batch.append((asin, similar_asin))
        
        processed_count += 1
        
        # Inserir em lote a cada BATCH_SIZE
        if processed_count % BATCH_SIZE == 0:
            print(f"Processando lote de {BATCH_SIZE} produtos...")
            
            # Inserir batches
            if products_batch:
                db.insert_product_batch(conn, products_batch)
                products_batch = []
            
            if reviews_batch:
                db.insert_review_batch(conn, reviews_batch)
                reviews_batch = []
            
            if categories_batch:
                # Inserir categorias (usar função corrigida)
                db.insert_categories_batch_corrigida(conn, categories_batch)
                categories_batch = []
            
            if produto_categoria_batch:
                db.insert_produto_categoria_batch(conn, produto_categoria_batch)
                produto_categoria_batch = []
            
            conn.commit()
            print(f"Produtos processados: {processed_count}")

    # Inserir último lote (incompleto)
print("Processando último lote...")
    
if products_batch:
    db.insert_product_batch(conn, products_batch)
    
if reviews_batch:
    db.insert_review_batch(conn, reviews_batch)
    
if categories_batch:
    db.insert_categories_batch_corrigida(conn, categories_batch)
    
if produto_categoria_batch:
    db.insert_produto_categoria_batch(conn, produto_categoria_batch)
    
 # Inserir produtos similares (após todos os produtos existirem)
print("Inserindo produtos similares...")
if similares_batch:
    db.insert_similares_batch_corrigida(conn, similares_batch)
    
conn.commit()
print(f"ETL concluído! Total de produtos processados: {processed_count}")
conn.close()

