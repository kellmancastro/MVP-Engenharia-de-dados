# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Qualidade dos dados (sobre a camada Bronze)
# MAGIC
# MAGIC Para cada atributo relevante verificamos: **completude, consistência, unicidade, acurácia e outliers**.
# MAGIC Os problemas encontrados aqui justificam as transformações do notebook 03 (Silver).
# MAGIC
# MAGIC > Tire print do resultado de cada célula para o README.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "workspace"

def perfil_nulos(tabela):
    """Percentual de nulos ou vazios por coluna."""
    df = spark.table(f"{CATALOG}.bronze.{tabela}")
    total = df.count()
    colunas = [c for c in df.columns if not c.startswith("_")]
    linha = df.select([
        F.sum(F.when(F.col(c).isNull() | (F.trim(F.col(c)) == ""), 1).otherwise(0)).alias(c)
        for c in colunas
    ]).collect()[0]
    dados = [(tabela, c, total, int(linha[c]), round(100 * linha[c] / total, 2)) for c in colunas]
    return spark.createDataFrame(dados, "tabela string, coluna string, total_linhas long, nulos_ou_vazios long, pct_nulos double")

perfil = None
for t in ["customers", "orders", "order_reviews", "order_items"]:
    perfil = perfil_nulos(t) if perfil is None else perfil.unionByName(perfil_nulos(t))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Completude: nulos por coluna

# COMMAND ----------

display(perfil.orderBy(F.desc("pct_nulos")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Unicidade: chaves duplicadas

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'customers.customer_id' AS chave, COUNT(*) - COUNT(DISTINCT customer_id) AS duplicados FROM workspace.bronze.customers
# MAGIC UNION ALL
# MAGIC SELECT 'orders.order_id', COUNT(*) - COUNT(DISTINCT order_id) FROM workspace.bronze.orders
# MAGIC UNION ALL
# MAGIC SELECT 'order_reviews.review_id', COUNT(*) - COUNT(DISTINCT review_id) FROM workspace.bronze.order_reviews
# MAGIC UNION ALL
# MAGIC SELECT 'order_reviews.order_id (mais de 1 avaliacao por pedido)', COUNT(*) - COUNT(DISTINCT order_id) FROM workspace.bronze.order_reviews
# MAGIC UNION ALL
# MAGIC SELECT 'order_items.(order_id, order_item_id)', COUNT(*) - COUNT(DISTINCT order_id, order_item_id) FROM workspace.bronze.order_items

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Consistência da chave de cliente
# MAGIC O dataset gera um `customer_id` novo a cada pedido. A pessoa é identificada por `customer_unique_id`.
# MAGIC Sem essa correção, a recompra seria sempre zero.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(DISTINCT customer_id)        AS qtd_customer_id,
# MAGIC   COUNT(DISTINCT customer_unique_id) AS qtd_customer_unique_id,
# MAGIC   COUNT(DISTINCT customer_id) - COUNT(DISTINCT customer_unique_id) AS diferenca
# MAGIC FROM workspace.bronze.customers

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Domínio de valores categóricos

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Status dos pedidos
# MAGIC SELECT order_status, COUNT(*) AS qtd FROM workspace.bronze.orders GROUP BY 1 ORDER BY 2 DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Nota da avaliacao: esperado inteiro de 1 a 5
# MAGIC SELECT review_score, COUNT(*) AS qtd,
# MAGIC        CASE WHEN TRY_CAST(review_score AS INT) BETWEEN 1 AND 5 THEN 'valido' ELSE 'INVALIDO' END AS situacao
# MAGIC FROM workspace.bronze.order_reviews GROUP BY 1 ORDER BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC -- UF: esperado 27 siglas em maiusculas
# MAGIC SELECT COUNT(DISTINCT customer_state) AS qtd_ufs,
# MAGIC        SUM(CASE WHEN customer_state <> UPPER(TRIM(customer_state)) THEN 1 ELSE 0 END) AS fora_do_padrao
# MAGIC FROM workspace.bronze.customers

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Acurácia: datas coerentes

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   MIN(TRY_CAST(order_purchase_timestamp AS TIMESTAMP)) AS primeira_compra,
# MAGIC   MAX(TRY_CAST(order_purchase_timestamp AS TIMESTAMP)) AS ultima_compra,
# MAGIC   SUM(CASE WHEN TRY_CAST(order_purchase_timestamp AS TIMESTAMP) IS NULL THEN 1 ELSE 0 END) AS compra_sem_data_valida,
# MAGIC   SUM(CASE WHEN TRY_CAST(order_delivered_customer_date AS TIMESTAMP) < TRY_CAST(order_purchase_timestamp AS TIMESTAMP) THEN 1 ELSE 0 END) AS entrega_antes_da_compra,
# MAGIC   SUM(CASE WHEN order_status = 'delivered' AND order_delivered_customer_date IS NULL THEN 1 ELSE 0 END) AS entregue_sem_data_entrega
# MAGIC FROM workspace.bronze.orders

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Integridade referencial

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   (SELECT COUNT(*) FROM workspace.bronze.orders o
# MAGIC     WHERE NOT EXISTS (SELECT 1 FROM workspace.bronze.order_reviews r WHERE r.order_id = o.order_id)) AS pedidos_sem_avaliacao,
# MAGIC   (SELECT COUNT(*) FROM workspace.bronze.order_reviews r
# MAGIC     WHERE NOT EXISTS (SELECT 1 FROM workspace.bronze.orders o WHERE o.order_id = r.order_id))  AS avaliacoes_sem_pedido,
# MAGIC   (SELECT COUNT(*) FROM workspace.bronze.orders o
# MAGIC     WHERE NOT EXISTS (SELECT 1 FROM workspace.bronze.customers c WHERE c.customer_id = o.customer_id)) AS pedidos_sem_cliente

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Outliers: preço e frete dos itens

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   MIN(TRY_CAST(price AS DOUBLE)) AS preco_min,
# MAGIC   PERCENTILE(TRY_CAST(price AS DOUBLE), 0.5)  AS preco_mediana,
# MAGIC   PERCENTILE(TRY_CAST(price AS DOUBLE), 0.99) AS preco_p99,
# MAGIC   MAX(TRY_CAST(price AS DOUBLE)) AS preco_max,
# MAGIC   SUM(CASE WHEN TRY_CAST(price AS DOUBLE) <= 0 THEN 1 ELSE 0 END) AS preco_nao_positivo,
# MAGIC   MIN(TRY_CAST(freight_value AS DOUBLE)) AS frete_min,
# MAGIC   MAX(TRY_CAST(freight_value AS DOUBLE)) AS frete_max
# MAGIC FROM workspace.bronze.order_items
