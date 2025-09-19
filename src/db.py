import psycopg
from datetime import datetime


# INSERÇÕES NAS TABELAS
def insert_product(conn, asin, title, group_name, salesrank, cat, rev, down, rating):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO product (asin, title, group_name, salesrank, Qtd_categorias, Qtd_rev, Qtd_down, Avg_rating)
            VALUES (%s, %s, %s, %s, %s, %s, %s,%s, %s)
            ON CONFLICT (asin) DO NOTHING
        """, (asin, title, group_name, salesrank, cat, rev, down, rating))

def insert_review(conn, asin, review):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO review (asin, customer_id, review_date, rating, votes, helpful)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            asin,
            review["customer"],
            datetime.strptime(review["date"], "%Y-%m-%d"),
            review["rating"],
            review["votes"],
            review["helpful"]
        ))

def insert_categoria(conn, asin, categorias:list[dict[str, str]]):
    comando = """
            INSERT INTO Categoria (id, nome, id_pai)
            VALUES (%s, %s, %s)
            """
    with conn.cursor as cur:
        id_pai = None
        for categoria in categorias:
            cur.execute(comando, (categoria["id"], categoria["name"], id_pai))
            id_pai = categoria["id"]

        categoria = categorias[-1]
        cur.execute("""
                    INSERT INTO Produto_categoria (ASIN, Categoria)
                    VALUES (%s, %s)
                    """, (asin, categoria["id"]))

def insert_similares(conn, asin, similares:list):
        with conn.cursor as cur:
            for similar in similares:
                cur.execute("""
                            INSERT INTO Produto_similaridade(ASIN_c, ASIN_s)
                            VALUES (%s, %s)
                            """, (asin, similar))    
