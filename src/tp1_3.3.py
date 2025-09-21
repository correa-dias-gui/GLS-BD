import argparse
import sys
import psycopg
import pandas as pd

# ==============================
# Consultas SQL (1 a 7)
# ==============================
QUERIES = {
    "1": """
    WITH top_reviews AS (
        SELECT review_id, asin, customer_id, review_date, rating, votes, helpful
        FROM Review
        WHERE asin = %(asin)s
        ORDER BY rating DESC, helpful DESC
        LIMIT 5
    ),
    low_reviews AS (
        SELECT review_id, asin, customer_id, review_date, rating, votes, helpful
        FROM Review
        WHERE asin = %(asin)s
        ORDER BY rating ASC, helpful DESC
        LIMIT 5
    )
    SELECT * FROM top_reviews
    UNION ALL
    SELECT * FROM low_reviews;
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
    SELECT
        r.review_date,
        TRUNC(
            AVG(r.rating) OVER (
                ORDER BY r.review_date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )::numeric,
            2
        ) AS media_acumulada
    FROM Review r
    WHERE r.asin = %(asin)s
    GROUP BY r.review_date, r.rating
    ORDER BY r.review_date;
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

    "6": """
    WITH RECURSIVE categoria_produto AS (
        SELECT 
            pc.asin,
            pc.categoria_id
        FROM Produto_categoria pc
        UNION ALL
        SELECT 
            cp.asin,
            c.pai_id
        FROM categoria_produto cp
        JOIN Categoria c ON cp.categoria_id = c.categoria_id
        WHERE c.pai_id IS NOT NULL
    ),
    media_produto AS (
        SELECT
            r.asin,
            AVG(r.helpful::numeric) AS media_util_positiva
        FROM Review r
        WHERE r.rating > 3
        GROUP BY r.asin
    )
    SELECT
        c.nome AS categoria,
        TRUNC(AVG(mp.media_util_positiva), 2) AS media_categoria
    FROM categoria_produto cp
    JOIN media_produto mp ON mp.asin = cp.asin
    JOIN Categoria c ON c.categoria_id = cp.categoria_id
    GROUP BY c.categoria_id, c.nome
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