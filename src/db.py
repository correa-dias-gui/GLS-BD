import psycopg
from datetime import datetime


# INSERÇÕES NAS TABELAS
def insert_product(conn, asin, title, group_name, salesrank, cat, rev, down, rating):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO Produto (asin, title, group_name, salesrank, Qtd_cat, Qtd_rev, Qtd_down, Avg_rating)
            VALUES (%s, %s, %s, %s, %s, %s, %s,%s)
            ON CONFLICT (asin) DO NOTHING
        """, (asin, title, group_name, salesrank, cat, rev, down, rating))

def insert_review(conn, asin, review):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO Review (asin, customer_id, review_date, rating, votes, helpful)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            asin,
            review["customer"],
            datetime.strptime(review["date"], "%Y-%m-%d"),
            review["rating"],
            review["votes"],
            review["helpful"]
        ))

def insert_categoria(conn, asin, categorias):
    comando = """
            INSERT INTO Categoria (Categoria_id, Nome, Pai_id)
            VALUES (%s, %s, %s)
            ON CONFLICT (Categoria_id) DO UPDATE
            SET Nome = EXCLUDED.Nome, Pai_id = EXCLUDED.Pai_id
            """
    with conn.cursor() as cur:
        id_pai = None
        for categoria in categorias:
            #print(categoria)
            cur.execute(comando, (categoria["id"], categoria["name"], id_pai))
            id_pai = categoria["id"]

        categoria = categorias[-1]
        cur.execute("""
                    INSERT INTO Produto_categoria (ASIN, Categoria_id)
                    VALUES (%s, %s)
                    """, (asin, categoria["id"]))

def insert_similares(conn, asin, similares:list):
        with conn.cursor() as cur:
            for similar in similares:
                cur.execute("""
                            INSERT INTO Produto_similaridade(ASIN_p, ASIN_s)
                            SELECT %s, %s 
                            WHERE EXISTS (SELECT 1 FROM Produto WHERE ASIN = %s)
                            ON CONFLICT DO NOTHING
                            """, (asin, similar, similar))    
