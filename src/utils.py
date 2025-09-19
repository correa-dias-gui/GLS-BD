import re

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

            elif line.startswith("  title:"):
                product["title"] = line.split(":", 1)[1].strip()

            elif line.startswith("  group:"):
                product["group"] = line.split(":", 1)[1].strip()

            elif line.startswith("  salesrank:"):
                rank = line.split(":", 1)[1].strip()
                product["salesrank"] = int(rank) if rank.isdigit() else None

            elif line.startswith("  similar:"):
                parts = line.split()
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

        # adiciona o último produto
        if product:
            yield product

