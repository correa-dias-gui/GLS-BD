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
    


filepath = Path(args.input)

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

    print("acumulando produtos similares para inserir depois...")
    similares_batch.extend(similares)
    
 # Inserir produtos similares (após todos os produtos existirem)
print("Inserindo produtos similares...")
if similares_batch:
    db.insert_similares_batch(conn, similares_batch)

print(f"ETL finalizado. Total de produtos processados: {processed_count}")

conn.commit()
conn.close()

