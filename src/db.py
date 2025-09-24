from psycopg.extras import execute_batch
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


def insert_product_batch(conn, products):
    with conn.cursor() as cur:
        execute_batch(cur, """
            INSERT INTO Produto (asin, title, group_name, salesrank, Qtd_cat, Qtd_rev, Qtd_down, Avg_rating)
            VALUES (%s, %s, %s, %s, %s, %s, %s,%s)
            ON CONFLICT (asin) DO NOTHING
    """, products, page_size=1000)

def insert_review_batch(conn, reviews):
    with conn.cursor() as cur:
        execute_batch(cur, """
            INSERT INTO Review (asin, customer_id, review_date, rating, votes, helpful)
            VALUES (%s, %s, %s, %s, %s, %s)
    """, reviews, page_size=1000)

def insert_categoria_batch(conn, categories_list):
    """Inserir uma lista de categorias na tabela Categoria.
    categories_list: lista de tuplas (Categoria_id, Nome, Pai_id)
    """
    with conn.cursor() as cur:
        execute_batch(cur, """
            INSERT INTO Categoria (Categoria_id, Nome, Pai_id)
            VALUES (%s, %s, %s)
            ON CONFLICT (Categoria_id) DO UPDATE
            SET Nome = EXCLUDED.Nome, Pai_id = EXCLUDED.Pai_id
        """, categories_list, page_size=1000)

def insert_categories_batch_corrigida(conn, categories_list):
    """Alias compatível: insere categorias no formato (Categoria_id, Nome, Pai_id)."""
    return insert_categoria_batch(conn, categories_list)

def insert_produto_categoria_batch(conn, produto_categoria_list):
    """Insere relações Produto_categoria em lote.
    produto_categoria_list: lista de tuplas (ASIN, Categoria_id)
    """
    if not produto_categoria_list:
        return
    with conn.cursor() as cur:
        execute_batch(cur, """
            INSERT INTO Produto_categoria (ASIN, Categoria_id)
            VALUES (%s, %s)
            ON CONFLICT (ASIN, Categoria_id) DO NOTHING
        """, produto_categoria_list, page_size=1000)

# def insert_categoria_batch(conn, asin, categorias):
#     comando = """
#             INSERT INTO Categoria (Categoria_id, Nome, Pai_id)
#             VALUES (%s, %s, %s)
#             ON CONFLICT (Categoria_id) DO UPDATE
#             SET Nome = EXCLUDED.Nome, Pai_id = EXCLUDED.Pai_id
#             """
#     with conn.cursor() as cur:
#         id_pai = None
#         categorias_list = [] # execute_batch precisa de lista
    
#         execute_batch(cur, comando, categorias_list, pagesize=len(categorias_list))

#         categoria = categorias[-1]
#         cur.execute("""
#                     INSERT INTO Produto_categoria (ASIN, Categoria_id)
#                     VALUES (%s, %s)
#                     """, (asin, categoria["id"]))

def insert_similares_batch(conn, asin, similares:list):
        # Versão antiga (por produto) mantida por compatibilidade; converte em lista de tuplas
        if not similares:
            return
        pares = [(asin, s) for s in similares]
        insert_similares_batch_corrigida(conn, pares)

def insert_similares_batch_corrigida(conn, similares_list:list[tuple[str, str]]):
        """Insere similaridades em lote.
        similares_list: lista de tuplas (ASIN_p, ASIN_s)
        Apenas insere quando o produto similar (ASIN_s) existe para evitar erro de FK.
        """
        if not similares_list:
            return
        with conn.cursor() as cur:
            # Precisamos de 3 placeholders por linha para checar existência de ASIN_s
            args = [(p, s, s) for (p, s) in similares_list]
            execute_batch(cur, """
                INSERT INTO Produto_similaridade(ASIN_p, ASIN_s)
                SELECT %s, %s 
                WHERE EXISTS (SELECT 1 FROM Produto WHERE ASIN = %s)
                ON CONFLICT DO NOTHING
            """, args, page_size=1000)