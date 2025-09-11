# Usa uma imagem Python leve
FROM python:3.11-slim  

# Define diretório de trabalho
WORKDIR /app  

# Copia lista de dependências e instala
COPY requirements.txt .  
RUN pip install --no-cache-dir -r requirements.txt  

# Copia o código-fonte
COPY src ./src  

# Comando padrão (pode ser sobrescrito no docker-compose)
CMD ["python", "src/tp1_3.3.py", "--help"]
