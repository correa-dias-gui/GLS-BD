import re
from datetime import datetime

def parse_category_line(line):
    """
    Converte uma linha como:
    |Books[1]|Fantasy[3]|RPG[4]
    em uma lista de dicts: [{"id":1,"name":"Books"}, {"id":3,"name":"Fantasy"}, {"id":4,"name":"RPG"}]
    """
    categories = []
    for seg in line.split("|"):
        if not seg.strip():
            continue
        match = re.match(r"(.+)\[(\d+)\]", seg.strip())
        if match:
            name, cat_id = match.groups()
            categories.append({"id": int(cat_id), "name": name.strip()})
    return categories

def parse_products(filepath):
    """
    Lê o arquivo SNAP e gera um dicionário por produto.
    Cada produto contém: asin, title, group, salesrank, similares, categorias, reviews
    """
    product = None

    with open(filepath, "r", encoding="latin1") as f:
        for line in f:
            line = line.strip()

            # Novo produto começa quando encontra "Id:"
            if line.startswith("Id:"):
                if product:
                    yield product
                product = {
                    "id": line.split()[1],
                    "asin": None,
                    "title": None,
                    "group": None,
                    "salesrank": None,
                    "similar": [],
                    "categories": [],
                    "reviews": []
                }

            elif line.startswith("ASIN:"):
                product["asin"] = line.split()[1]

            elif line.startswith("title:"):
                product["title"] = line.split(":", 1)[1].strip()

            elif line.startswith("group:"):
                product["group"] = line.split(":", 1)[1].strip()

            elif line.startswith("salesrank:"):
                rank = line.split(":", 1)[1].strip()
                product["salesrank"] = int(rank) if rank.isdigit() else None

            elif line.startswith("similar:"):
                parts = line.split()
                #print(f"Similar parts: {parts}")
                product["similar"] = parts[2:]  # ignora o número inicial

            elif re.match(r"^\|", line):  # linha de categoria
                # cada linha é uma hierarquia completa
                product["categories"].append(parse_category_line(line))

            elif re.match(r"^\d{4}-\d{1,2}-\d{1,2}", line):  # linha de review
                parts = line.split()
                review = {
                    "date": parts[0],
                    "customer": parts[2],
                    "rating": int(parts[4]),
                    "votes": int(parts[6]),
                    "helpful": int(parts[8])
                }
                product["reviews"].append(review)
            elif "discontinued product" in line:
                # Produto descontinuado, ignora
                product = None
        # adiciona o último produto
        if product:
            yield product

def parse_1000_products(filepath):
    """
    Gera os primeiros 1000 produtos do arquivo SNAP.
    """
    products = []
    reviews = []
    similares = []
    categorias = []
    for i, product in enumerate(parse_products(filepath), start=1):
        if i % 1000:
            yield (products, categorias, reviews, similares)
            products = []
            reviews = []
            similares = []

        rev = len(product["reviews"])
        products.append((
            product["asin"], product["title"], product["group"], product["salesrank"],
            len(product["categories"]),
            rev,
            sum(1 for r in product["reviews"] if r["rating"] <= 2),
            (sum(r["rating"] for r in product["reviews"]) / rev) if rev > 0 else None
        ))

        for review in product["reviews"]:
            reviews.append((
                product["asin"],
                review["customer"], 
                datetime.strptime(review["date"], "%Y-%m-%d"), 
                review["rating"], review["votes"], review["helpful"]
            ))

        for sim in product["similar"]:
            similares.append((product["asin"], sim))    
        
        id_pai = None
        for categoria in product["categories"]:
            categorias.append((id_pai, categoria["name"], categoria["id"]))
            id_pai = categoria["id"]
        
    if products or reviews or similares or categorias:
        yield (products, categorias, reviews, similares)    
