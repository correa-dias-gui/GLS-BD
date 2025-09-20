import argparse
import sys
import psycopg
import pandas as pd

# ==============================
# Consultas SQL (1 a 7)
# ==============================
QUERIES = {
    "1": """
    (
      SELECT r.*
      FROM Review r
      WHERE r.ASIN = %(asin)s
      ORDER BY r.Rating DESC, r.Helpful DESC
      LIMIT 5
    )
    UNION
    (
      SELECT r.*
      FROM Review r
      WHERE r.ASIN = %(asin)s
      ORDER BY r.Rating ASC, r.Helpful DESC
      LIMIT 5
    );
    """,

    "2": """
    SELECT p2.ASIN, p2.Title, p2.Salesrank
    FROM Produto p
    JOIN Produto_similaridade s ON p.ASIN = s.ASIN_p
    JOIN Produto p2 ON s.ASIN_s = p2.ASIN
    WHERE p.ASIN = %(asin)s
      AND p2.Salesrank < p.Salesrank
    ORDER BY p2.Salesrank ASC;
    """,

    "3": """
    SELECT r.Review_date, AVG(r.Rating) AS media_diaria
    FROM Review r
    WHERE r.ASIN = %(asin)s
    GROUP BY r.Review_date
    ORDER BY r.Review_date;
    """,

    "4": """
    WITH Ranked AS (
        SELECT p.Group_name, p.ASIN, p.Title, p.Salesrank,
               ROW_NUMBER() OVER (PARTITION BY p.Group_name ORDER BY p.Salesrank ASC) AS pos
        FROM Produto p
        WHERE p.Salesrank IS NOT NULL AND p.Salesrank > 0
    )
    SELECT Group_name, ASIN, Title, Salesrank
    FROM Ranked
    WHERE pos <= 10;
    """,

    "5": """
    SELECT r.ASIN, p.Title, AVG(CAST(r.Helpful AS FLOAT) / NULLIF(r.Votes, 0)) AS media_util
    FROM Review r
    JOIN Produto p ON p.ASIN = r.ASIN
    WHERE r.Votes > 0
    GROUP BY r.ASIN, p.Title
    ORDER BY media_util DESC
    LIMIT 10;
    """,

    # ✅ Consulta 6 atualizada com CTE recursiva para subir a hierarquia de categorias
    "6": """
    WITH RECURSIVE cat_hierarchy AS (
        -- ponto de partida: categorias mais profundas dos produtos
        SELECT pc.ASIN, c.Id, c.Nome, c.Id_pai
        FROM Produto_Categoria pc
        JOIN Categoria c ON c.Id = pc.Id_cat

        UNION ALL

        -- sobe na hierarquia
        SELECT ch.ASIN, c.Id, c.Nome, c.Id_pai
        FROM cat_hierarchy ch
        JOIN Categoria c ON c.Id = ch.Id_pai
    ),

    -- calcula a média de utilidade por produto
    prod_media AS (
        SELECT r.ASIN, AVG(CAST(r.Helpful AS FLOAT) / NULLIF(r.Votes, 0)) AS media_util
        FROM Review r
        WHERE r.Votes > 0
        GROUP BY r.ASIN
    )

    -- associa cada produto às suas categorias (todas da hierarquia)
    SELECT c.Id, c.Nome,
           AVG(prod_media.media_util) AS media_categoria
    FROM prod_media
    JOIN cat_hierarchy c ON c.ASIN = prod_media.ASIN
    GROUP BY c.Id, c.Nome
    ORDER BY media_categoria DESC
    LIMIT 5;
    """,

    "7": """
    WITH Ranked AS (
        SELECT p.Group_name, r.Customer_id, COUNT(*) AS qtd_comentarios,
               ROW_NUMBER() OVER (PARTITION BY p.Group_name ORDER BY COUNT(*) DESC) AS pos
        FROM Review r
        JOIN Produto p ON p.ASIN = r.ASIN
        GROUP BY p.Group_name, r.Customer_id
    )
    SELECT Group_name, Customer_id, qtd_comentarios
    FROM Ranked
    WHERE pos <= 10;
    """
}

# ==============================
# Função para executar queries
# ==============================
def run_queries(conn, asin, output_path):
    with conn.cursor() as cur:
        for key, query in QUERIES.items():
            print(f"\n=== Consulta {key} ===\n")
            cur.execute(query, {"asin": asin})
            rows = cur.fetchall()
            colnames = [desc[0] for desc in cur.description]

            # Usa pandas para imprimir bonitinho em tabela
            df = pd.DataFrame(rows, columns=colnames)
            print(df.to_string(index=False))
        
            # Salva em CSV
            csv_path = f"{output_path}_query{key}.csv"
            df.to_csv(csv_path, index=False)
            print(f"Resultados salvos em: {csv_path}")

# ==============================
# Main
# ==============================
def main():
    parser = argparse.ArgumentParser(description="Executa consultas SQL.")
    parser.add_argument("--db-host", required=True)
    parser.add_argument("--db-port", default=5432, type=int)
    parser.add_argument("--db-user", required=True)
    parser.add_argument("--db-pass", required=True)
    parser.add_argument("--db-name", required=True)
    parser.add_argument("--asin", required=True, help="ASIN do produto alvo")
    parser.add_argument("--output", required=True, help="Caminho do arquivo de saída")
    args = parser.parse_args()

    try:
        conn = psycopg.connect(
            host=args.db_host,
            port=args.db_port,
            user=args.db_user,
            password=args.db_pass,
            dbname=args.db_name
        )
        run_queries(conn, args.asin, args.output)
        conn.close()
        sys.exit(0)
    except Exception as e:
        print(f"Erro: {e}", file=sys.stderr)
        sys.exit(1)


main()