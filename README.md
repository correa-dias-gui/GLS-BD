# **GLS-BD:**

# Configurando o data
- extraia e renomeie com:
``` bash
gunzip amazon-meta.txt.gz
mv amazon-meta.txt. snap_amazon.txt
```

# 1) Construir e subir os serviços
```bash
docker compose up -d --build
```
# 2) (Opcional) conferir saúde do PostgreSQL
```bash
docker compose ps
```
# 3) Criar esquema e carregar dados
```bash
docker compose run --rm app python src/tp1_3.2.py \
  --db-host db --db-port 5432 --db-name ecommerce --db-user postgres --db-pass postgres \
  --input /app/data/snap_amazon.txt
```
# 4) Executar o Dashboard (todas as consultas)
- A flag `--asin` é obrigatória e todas as consultas serao referentes ao produto deste asin.
```bash
docker compose run --rm app python src/tp1_3.3.py \
  --db-host db --db-port 5432 --db-name ecommerce --db-user postgres --db-pass postgres \
  --asin 0738700797 --output /app/out
  ```
