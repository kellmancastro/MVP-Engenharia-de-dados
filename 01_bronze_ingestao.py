# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze: ingestão dos dados brutos
# MAGIC
# MAGIC **Objetivo:** criar os schemas do Lakehouse (bronze, silver, gold), o volume onde os CSVs do Kaggle
# MAGIC são enviados e gravar cada arquivo como tabela Delta **sem nenhuma transformação**.
# MAGIC
# MAGIC Apenas duas colunas de controle são adicionadas: `_ingestion_ts` (momento da carga) e
# MAGIC `_source_file` (arquivo de origem), garantindo rastreabilidade.
# MAGIC
# MAGIC **Pré-requisito:** baixar o dataset em https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
# MAGIC e enviar os 4 CSVs abaixo para o volume `workspace.bronze.raw_files`
# MAGIC (Catalog > workspace > bronze > Volumes > raw_files > Upload to this volume).

# COMMAND ----------

CATALOG = "workspace"

for schema in ["bronze", "silver", "gold"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.bronze.raw_files")
RAW_PATH = f"/Volumes/{CATALOG}/bronze/raw_files"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conferência dos arquivos enviados ao volume

# COMMAND ----------

display(dbutils.fs.ls(RAW_PATH))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Leitura dos CSVs e gravação como Delta
# MAGIC
# MAGIC - Todas as colunas são lidas como **texto** (sem `inferSchema`), preservando o dado como veio.
# MAGIC - `multiLine` e `escape` são necessários porque os comentários das avaliações têm quebras de linha e aspas.

# COMMAND ----------

from pyspark.sql import functions as F

arquivos = {
    "customers": "olist_customers_dataset.csv",
    "orders": "olist_orders_dataset.csv",
    "order_reviews": "olist_order_reviews_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
}

for tabela, arquivo in arquivos.items():
    df = (
        spark.read
        .option("header", True)
        .option("multiLine", True)
        .option("quote", '"')
        .option("escape", '"')
        .csv(f"{RAW_PATH}/{arquivo}")
        .withColumn("_ingestion_ts", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
    (df.write.mode("overwrite")
       .option("overwriteSchema", True)
       .saveAsTable(f"{CATALOG}.bronze.{tabela}"))
    print(f"{CATALOG}.bronze.{tabela}: {spark.table(f'{CATALOG}.bronze.{tabela}').count():,} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Documentação das tabelas Bronze no Unity Catalog

# COMMAND ----------

descricoes = {
    "customers": "Bronze - clientes Olist, copia fiel de olist_customers_dataset.csv (Kaggle, CC BY-NC-SA 4.0). Um customer_id por pedido.",
    "orders": "Bronze - pedidos Olist, copia fiel de olist_orders_dataset.csv (Kaggle, CC BY-NC-SA 4.0).",
    "order_reviews": "Bronze - avaliacoes dos pedidos, copia fiel de olist_order_reviews_dataset.csv (Kaggle, CC BY-NC-SA 4.0).",
    "order_items": "Bronze - itens dos pedidos, copia fiel de olist_order_items_dataset.csv (Kaggle, CC BY-NC-SA 4.0).",
}
for tabela, desc in descricoes.items():
    spark.sql(f"COMMENT ON TABLE {CATALOG}.bronze.{tabela} IS '{desc}'")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN workspace.bronze
