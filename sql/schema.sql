-- Limpeza inicial (para evitar conflitos se rodar mais de uma vez)
DROP TABLE IF EXISTS Review CASCADE;
DROP TABLE IF EXISTS Produto_categoria CASCADE;
DROP TABLE IF EXISTS Categoria CASCADE;
DROP TABLE IF EXISTS Produto_similaridade CASCADE;
DROP TABLE IF EXISTS Produto CASCADE;

-- Produto
CREATE TABLE Produto (
    id SERIAL,
    ASIN VARCHAR(20) PRIMARY KEY,
    Title TEXT NOT NULL,
    Group_name VARCHAR(50) NOT NULL,
    Salesrank INTEGER,
    Qtd_cat INTEGER,
    Qtd_rev INTEGER,
    Qtd_down INTEGER,
    Avg_rating FLOAT
);


-- Categoria (hierarquia de categorias da Amazon)
CREATE TABLE Categoria (
    Categoria_id INT PRIMARY KEY,
    Nome TEXT NOT NULL,
    Pai_id INT REFERENCES Categoria(Categoria_id) ON DELETE CASCADE
);

-- Relação produto ↔ categoria (N:N)
CREATE TABLE Produto_categoria (    
    ASIN VARCHAR(20) REFERENCES Produto(ASIN) ON DELETE CASCADE,
    Categoria_id INT REFERENCES Categoria(Categoria_id) ON DELETE CASCADE,
    PRIMARY KEY (ASIN, Categoria_id)
);

-- Produtos similares (N:N entre produtos)
CREATE TABLE Produto_similaridade (
    ASIN_c VARCHAR(20) REFERENCES Produto(ASIN) ON DELETE CASCADE,
    ASIN_s VARCHAR(20) REFERENCES Produto(ASIN) ON DELETE CASCADE,
    PRIMARY KEY (ASIN_c, ASIN_s),
    CHECK (ASIN_c <> ASIN_s)
);

-- Avaliações (reviews)
CREATE TABLE Review (
    Review_id SERIAL PRIMARY KEY,
    ASIN VARCHAR(20) REFERENCES Produto(ASIN) ON DELETE CASCADE,
    Customer_id VARCHAR(20),
    Review_date DATE NOT NULL,
    Rating INT,
    Votes INT,
    Helpful INT
);
