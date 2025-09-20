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
    filepath = Path("/app/data/snap_amazon.txt")
    similares = dict()
    for product in utils.parse_products(filepath):
        if not product:
            continue  # Produto descontinuado, ignora
        
        asin = product["asin"]
        print(asin)
        title = product["title"]
        group_name = product["group"]
        salesrank = product["salesrank"]
        cat = len(product["categories"])
        rev = len(product["reviews"])
        down = sum(1 for r in product["reviews"] if r["rating"] <= 2)
        rating = (sum(r["rating"] for r in product["reviews"]) / rev) if rev > 0 else None
        #print(asin, title, group_name, product["similar"],salesrank, cat, rev, down, rating)
        db.insert_product(conn, asin, title, group_name, salesrank, cat, rev, down, rating)
        #db.insert_similares(conn, asin, product["similar"])
        #print(product["categories"])
        similares[asin] = product["similar"]
        for categoria in product["categories"]:
            db.insert_categoria(conn, asin, categoria)
        for review in product["reviews"]:
            db.insert_review(conn, asin, review)
    
    for asin in similares.keys():
        db.insert_similares(conn, asin, similares[asin])

    conn.commit()
    conn.close()
