import psycopg
from utils import parse_products
from datetime import datetime

def insert_product(conn, asin, title, group_name, salesrank):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO product (asin, title, group_name, salesrank)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (asin) DO NOTHING
        """, (asin, title, group_name, salesrank))

def insert_review(conn, asin, review):
    with conn.cursor() as cur:
        cur.execute("INSERT INTO customer (customer_id) VALUES (%s) ON CONFLICT DO NOTHING", (review["customer"],))
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