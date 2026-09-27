# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Silver: limpeza e padronização
# MAGIC
# MAGIC | Tabela Bronze | Tabela Silver | Principais transformações |
# MAGIC |---|---|---|
# MAGIC | customers | clientes | trim, UF em maiúsculas, cidade em minúsculas, CEP com 5 dígitos, remoção de duplicatas |
# MAGIC | orders | pedidos | tipagem das datas (TRY_CAST), status em minúsculas, deduplicação por order_id |
# MAGIC | order_reviews | avaliacoes | nota como inteiro, filtro de nota fora de 1 a 5, **uma avaliação por pedido** (a mais recente) |
# MAGIC | order_items | itens_pedido | preço e frete como DECIMAL, deduplicação por (order_id, item) |

# COMMAND ----------

CATALOG = "workspace"

def documentar(tabela, descricao, colunas):
    """Grava descrição da tabela e das colunas no Unity Catalog (catálogo de dados)."""
    esc = lambda s: s.replace("'", "\\'")
    spark.sql(f"COMMENT ON TABLE {tabela} IS '{esc(descricao)}'")
    for coluna, desc in colunas.items():
        spark.sql(f"ALTER TABLE {tabela} ALTER COLUMN {coluna} COMMENT '{esc(desc)}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## silver.clientes

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.silver.clientes AS
SELECT
  TRIM(customer_id)                        AS customer_id,
  TRIM(customer_unique_id)                 AS customer_unique_id,
  LPAD(TRIM(customer_zip_code_prefix), 5, '0') AS cep_prefixo,
  LOWER(TRIM(customer_city))               AS cidade,
  UPPER(TRIM(customer_state))              AS uf
FROM {CATALOG}.bronze.customers
WHERE customer_id IS NOT NULL AND customer_unique_id IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIM(customer_id) ORDER BY _ingestion_ts DESC) = 1
""")

documentar(f"{CATALOG}.silver.clientes",
  "Silver - cadastro de clientes limpo. Origem: bronze.customers. Granularidade: 1 linha por customer_id (1 por pedido).",
  {
    "customer_id": "Chave do cliente no pedido (texto hash, unico). Origem: customers.customer_id",
    "customer_unique_id": "Identificador real da pessoa (texto hash). Um mesmo cliente pode ter varios customer_id. Origem: customers.customer_unique_id",
    "cep_prefixo": "5 primeiros digitos do CEP, com zeros a esquerda. Origem: customers.customer_zip_code_prefix",
    "cidade": "Cidade do cliente em minusculas. Origem: customers.customer_city",
    "uf": "Sigla do estado, 27 valores possiveis (AC a TO). Origem: customers.customer_state",
  })

# COMMAND ----------

# MAGIC %md
# MAGIC ## silver.pedidos

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.silver.pedidos AS
SELECT
  TRIM(order_id)                                        AS order_id,
  TRIM(customer_id)                                     AS customer_id,
  LOWER(TRIM(order_status))                             AS status,
  TRY_CAST(order_purchase_timestamp      AS TIMESTAMP)  AS dt_compra,
  TRY_CAST(order_approved_at             AS TIMESTAMP)  AS dt_aprovacao,
  TRY_CAST(order_delivered_carrier_date  AS TIMESTAMP)  AS dt_envio_transportadora,
  TRY_CAST(order_delivered_customer_date AS TIMESTAMP)  AS dt_entrega,
  TRY_CAST(order_estimated_delivery_date AS TIMESTAMP)  AS dt_entrega_estimada
FROM {CATALOG}.bronze.orders
WHERE order_id IS NOT NULL
  AND TRY_CAST(order_purchase_timestamp AS TIMESTAMP) IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIM(order_id) ORDER BY _ingestion_ts DESC) = 1
""")

documentar(f"{CATALOG}.silver.pedidos",
  "Silver - pedidos com datas tipadas. Origem: bronze.orders. Granularidade: 1 linha por pedido.",
  {
    "order_id": "Identificador unico do pedido (texto hash). Origem: orders.order_id",
    "customer_id": "Chave para silver.clientes. Origem: orders.customer_id",
    "status": "Status do pedido: delivered, shipped, canceled, unavailable, invoiced, processing, created, approved. Origem: orders.order_status",
    "dt_compra": "Data e hora da compra (2016 a 2018). Origem: orders.order_purchase_timestamp",
    "dt_aprovacao": "Data e hora da aprovacao do pagamento, pode ser nula. Origem: orders.order_approved_at",
    "dt_envio_transportadora": "Data de entrega a transportadora, pode ser nula. Origem: orders.order_delivered_carrier_date",
    "dt_entrega": "Data de entrega ao cliente, nula se nao entregue. Origem: orders.order_delivered_customer_date",
    "dt_entrega_estimada": "Data prometida de entrega. Origem: orders.order_estimated_delivery_date",
  })

# COMMAND ----------

# MAGIC %md
# MAGIC ## silver.avaliacoes
# MAGIC Alguns pedidos têm mais de uma avaliação. Mantemos apenas a **mais recente** (maior data de resposta),
# MAGIC pois representa a opinião final do cliente sobre aquele pedido.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.silver.avaliacoes AS
SELECT
  TRIM(review_id)                                   AS review_id,
  TRIM(order_id)                                    AS order_id,
  TRY_CAST(review_score AS INT)                     AS nota,
  NULLIF(TRIM(review_comment_title), '')            AS titulo_comentario,
  NULLIF(TRIM(review_comment_message), '')          AS comentario,
  NULLIF(TRIM(review_comment_message), '') IS NOT NULL AS tem_comentario,
  TRY_CAST(review_creation_date    AS TIMESTAMP)    AS dt_envio_pesquisa,
  TRY_CAST(review_answer_timestamp AS TIMESTAMP)    AS dt_resposta
FROM {CATALOG}.bronze.order_reviews
WHERE order_id IS NOT NULL
  AND TRY_CAST(review_score AS INT) BETWEEN 1 AND 5
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY TRIM(order_id)
  ORDER BY TRY_CAST(review_answer_timestamp AS TIMESTAMP) DESC NULLS LAST,
           TRY_CAST(review_creation_date AS TIMESTAMP) DESC NULLS LAST
) = 1
""")

documentar(f"{CATALOG}.silver.avaliacoes",
  "Silver - avaliacoes limpas, 1 por pedido (a mais recente). Origem: bronze.order_reviews.",
  {
    "review_id": "Identificador da avaliacao (texto hash). Origem: order_reviews.review_id",
    "order_id": "Pedido avaliado, chave unica nesta tabela. Origem: order_reviews.order_id",
    "nota": "Nota de satisfacao, inteiro de 1 a 5. Origem: order_reviews.review_score",
    "titulo_comentario": "Titulo do comentario, nulo se nao preenchido. Origem: order_reviews.review_comment_title",
    "comentario": "Texto livre do cliente, nulo se nao preenchido. Origem: order_reviews.review_comment_message",
    "tem_comentario": "Booleano: true se o cliente escreveu comentario. Derivado de comentario",
    "dt_envio_pesquisa": "Data em que a pesquisa foi enviada. Origem: order_reviews.review_creation_date",
    "dt_resposta": "Data e hora da resposta. Origem: order_reviews.review_answer_timestamp",
  })

# COMMAND ----------

# MAGIC %md
# MAGIC ## silver.itens_pedido

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.silver.itens_pedido AS
SELECT
  TRIM(order_id)                              AS order_id,
  TRY_CAST(order_item_id AS INT)              AS item_seq,
  TRIM(product_id)                            AS product_id,
  TRIM(seller_id)                             AS seller_id,
  TRY_CAST(price         AS DECIMAL(10,2))    AS preco,
  TRY_CAST(freight_value AS DECIMAL(10,2))    AS frete
FROM {CATALOG}.bronze.order_items
WHERE order_id IS NOT NULL AND TRY_CAST(price AS DECIMAL(10,2)) > 0
QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIM(order_id), TRY_CAST(order_item_id AS INT) ORDER BY _ingestion_ts DESC) = 1
""")

documentar(f"{CATALOG}.silver.itens_pedido",
  "Silver - itens dos pedidos com valores tipados. Origem: bronze.order_items. Granularidade: 1 linha por item do pedido.",
  {
    "order_id": "Pedido ao qual o item pertence. Origem: order_items.order_id",
    "item_seq": "Numero sequencial do item dentro do pedido (1, 2, 3...). Origem: order_items.order_item_id",
    "product_id": "Produto vendido (texto hash). Origem: order_items.product_id",
    "seller_id": "Vendedor (texto hash). Origem: order_items.seller_id",
    "preco": "Preco do item em reais, maior que zero. Origem: order_items.price",
    "frete": "Valor do frete do item em reais, maior ou igual a zero. Origem: order_items.freight_value",
  })

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validação pós-Silver
# MAGIC Todas as colunas de "duplicados" e "invalidos" devem ser **zero**.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'clientes' AS tabela, COUNT(*) AS linhas, COUNT(*) - COUNT(DISTINCT customer_id) AS duplicados,
# MAGIC        SUM(CASE WHEN LENGTH(uf) <> 2 THEN 1 ELSE 0 END) AS invalidos FROM workspace.silver.clientes
# MAGIC UNION ALL
# MAGIC SELECT 'pedidos', COUNT(*), COUNT(*) - COUNT(DISTINCT order_id),
# MAGIC        SUM(CASE WHEN dt_compra IS NULL THEN 1 ELSE 0 END) FROM workspace.silver.pedidos
# MAGIC UNION ALL
# MAGIC SELECT 'avaliacoes', COUNT(*), COUNT(*) - COUNT(DISTINCT order_id),
# MAGIC        SUM(CASE WHEN nota NOT BETWEEN 1 AND 5 THEN 1 ELSE 0 END) FROM workspace.silver.avaliacoes
# MAGIC UNION ALL
# MAGIC SELECT 'itens_pedido', COUNT(*), COUNT(*) - COUNT(DISTINCT order_id, item_seq),
# MAGIC        SUM(CASE WHEN preco <= 0 THEN 1 ELSE 0 END) FROM workspace.silver.itens_pedido
