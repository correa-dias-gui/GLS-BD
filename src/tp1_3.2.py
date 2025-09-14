import psycopg
from pathlib import Path


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
    conn = psycopg.connect(
        host="db", port=5432, dbname="ecommerce",
        user="postgres", password="postgres"
    )
    
    # Executa o esquema
    run_sql_file(conn, "/app/sql/schema.sql")
    
    # Verifica o esquema
    print("\n=== Estrutura atual do banco ===")
    print_schema(conn)

    # Depois faz o ETL para inserir os dados
    # ...
    
    conn.close()
