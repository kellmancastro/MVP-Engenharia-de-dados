# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Gold: modelo estrela para análise de recompra
# MAGIC
# MAGIC - **gold.fato_pedido**: 1 linha por pedido entregue, com a nota, dados de entrega e os indicadores de recompra.
# MAGIC - **gold.dim_cliente**: 1 linha por pessoa (`customer_unique_id`).
# MAGIC - **gold.dim_data**: calendário diário.
# MAGIC - **gold.agg_recompra_por_nota**: métrica pré-calculada que responde a pergunta principal.
# MAGIC
# MAGIC ### Regra de negócio da recompra
# MAGIC Para cada pedido (chamado de *pedido de referência*), olhamos se o **mesmo cliente** fez outro pedido
# MAGIC **em uma data posterior**. Assim, a nota do pedido de referência é sempre a nota da *última compra
# MAGIC feita até aquele momento*, que é exatamente a pergunta do trabalho.
# MAGIC
# MAGIC Pedidos feitos no mesmo dia não contam como recompra (costumam ser um mesmo carrinho dividido).
# MAGIC Para não penalizar pedidos recentes, que não tiveram tempo de gerar recompra, usamos uma **janela de 180 dias**:
# MAGIC só entram na métrica pedidos feitos pelo menos 180 dias antes da última data do dataset.

# COMMAND ----------

CATALOG = "workspace"
JANELA_DIAS = 180

def documentar(tabela, descricao, colunas):
    esc = lambda s: s.replace("'", "\\'")
    spark.sql(f"COMMENT ON TABLE {tabela} IS '{esc(descricao)}'")
    for coluna, desc in colunas.items():
        spark.sql(f"ALTER TABLE {tabela} ALTER COLUMN {coluna} COMMENT '{esc(desc)}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## gold.fato_pedido

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.gold.fato_pedido AS
WITH itens AS (
  SELECT order_id, COUNT(*) AS qtd_itens, SUM(preco) AS valor_produtos, SUM(frete) AS valor_frete
  FROM {CATALOG}.silver.itens_pedido
  GROUP BY order_id
),
base AS (
  SELECT
    p.order_id,
    c.customer_unique_id,
    p.dt_compra,
    CAST(p.dt_compra AS DATE)                                  AS data_compra,
    a.nota,
    CASE WHEN a.nota >= 4 THEN 'Boa (4-5)'
         WHEN a.nota = 3  THEN 'Neutra (3)'
         WHEN a.nota <= 2 THEN 'Ruim (1-2)' END                AS faixa_nota,
    a.tem_comentario,
    i.qtd_itens,
    i.valor_produtos,
    i.valor_frete,
    DATEDIFF(p.dt_entrega, p.dt_compra)                        AS dias_entrega,
    CAST(p.dt_entrega AS DATE) > CAST(p.dt_entrega_estimada AS DATE) AS entregue_com_atraso
  FROM {CATALOG}.silver.pedidos p
  JOIN {CATALOG}.silver.clientes c ON c.customer_id = p.customer_id
  LEFT JOIN {CATALOG}.silver.avaliacoes a ON a.order_id = p.order_id
  LEFT JOIN itens i ON i.order_id = p.order_id
  WHERE p.status = 'delivered'
),
seq AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY customer_unique_id ORDER BY dt_compra, order_id) AS seq_pedido_cliente,
    -- menor data de compra do mesmo cliente em um DIA POSTERIOR ao pedido atual
    MIN(data_compra) OVER (
      PARTITION BY customer_unique_id
      ORDER BY UNIX_DATE(data_compra)
      RANGE BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING
    ) AS data_proxima_compra
  FROM base
),
limite AS (SELECT MAX(data_compra) AS data_max FROM base)
SELECT
  s.*,
  DATEDIFF(s.data_proxima_compra, s.data_compra)                          AS dias_ate_proxima_compra,
  s.data_proxima_compra IS NOT NULL                                       AS teve_recompra,
  COALESCE(DATEDIFF(s.data_proxima_compra, s.data_compra) <= {JANELA_DIAS}, FALSE) AS recompra_{JANELA_DIAS}d,
  s.data_compra <= DATE_SUB(l.data_max, {JANELA_DIAS})                    AS elegivel_janela_{JANELA_DIAS}d
FROM seq s CROSS JOIN limite l
""")

documentar(f"{CATALOG}.gold.fato_pedido",
  "Gold - fato de pedidos entregues com nota e indicadores de recompra. Origem: silver.pedidos + silver.clientes + silver.avaliacoes + silver.itens_pedido. Granularidade: 1 linha por pedido com status delivered.",
  {
    "order_id": "Chave do pedido. Origem: silver.pedidos.order_id",
    "customer_unique_id": "Chave para gold.dim_cliente. Origem: silver.clientes via JOIN por customer_id",
    "dt_compra": "Data e hora da compra. Origem: silver.pedidos.dt_compra",
    "data_compra": "Data da compra, chave para gold.dim_data. Derivada de dt_compra",
    "nota": "Nota da avaliacao do pedido, 1 a 5, nula se o pedido nao foi avaliado. Origem: silver.avaliacoes.nota",
    "faixa_nota": "Categoria da nota: Boa (4-5), Neutra (3), Ruim (1-2) ou nula. Derivada de nota",
    "tem_comentario": "Booleano: cliente deixou comentario escrito. Origem: silver.avaliacoes",
    "qtd_itens": "Quantidade de itens no pedido, inteiro maior ou igual a 1. Agregado de silver.itens_pedido",
    "valor_produtos": "Soma do preco dos itens em reais. Agregado de silver.itens_pedido.preco",
    "valor_frete": "Soma do frete em reais. Agregado de silver.itens_pedido.frete",
    "dias_entrega": "Dias entre compra e entrega (normalmente 0 a 200). Derivado de silver.pedidos",
    "entregue_com_atraso": "Booleano: entrega apos a data prometida. Derivado de dt_entrega e dt_entrega_estimada",
    "seq_pedido_cliente": "Ordem do pedido na historia do cliente (1 = primeira compra). Calculado por janela",
    "data_proxima_compra": "Data da proxima compra do cliente em dia posterior, nula se nao recomprou. Calculado por janela",
    "dias_ate_proxima_compra": "Dias ate a proxima compra, nulo se nao recomprou. Derivado",
    "teve_recompra": "Booleano: cliente comprou novamente em qualquer data posterior. Derivado",
    f"recompra_{JANELA_DIAS}d": f"Booleano: cliente comprou novamente em ate {JANELA_DIAS} dias. Derivado",
    f"elegivel_janela_{JANELA_DIAS}d": f"Booleano: pedido tem pelo menos {JANELA_DIAS} dias de observacao ate o fim do dataset. Usar como filtro na analise",
  })

# COMMAND ----------

# MAGIC %md
# MAGIC ## gold.dim_cliente

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.gold.dim_cliente AS
SELECT
  f.customer_unique_id,
  MAX_BY(c.cidade, f.dt_compra)            AS cidade,
  MAX_BY(c.uf, f.dt_compra)                AS uf,
  COUNT(DISTINCT f.order_id)               AS qtd_pedidos,
  COUNT(DISTINCT f.data_compra)            AS qtd_dias_com_compra,
  COUNT(DISTINCT f.data_compra) > 1        AS cliente_recorrente,
  MIN(f.data_compra)                       AS data_primeira_compra,
  MAX(f.data_compra)                       AS data_ultima_compra,
  MIN_BY(f.nota, f.dt_compra)              AS nota_primeiro_pedido,
  MAX_BY(f.nota, f.dt_compra)              AS nota_ultimo_pedido
FROM {CATALOG}.gold.fato_pedido f
JOIN {CATALOG}.silver.pedidos p  ON p.order_id = f.order_id
JOIN {CATALOG}.silver.clientes c ON c.customer_id = p.customer_id
GROUP BY f.customer_unique_id
""")

documentar(f"{CATALOG}.gold.dim_cliente",
  "Gold - dimensao de clientes (pessoa unica). Origem: gold.fato_pedido + silver.clientes. Granularidade: 1 linha por customer_unique_id.",
  {
    "customer_unique_id": "Chave da pessoa (texto hash). Origem: silver.clientes",
    "cidade": "Cidade do pedido mais recente. Origem: silver.clientes.cidade",
    "uf": "UF do pedido mais recente, 27 valores. Origem: silver.clientes.uf",
    "qtd_pedidos": "Total de pedidos entregues, inteiro maior ou igual a 1. Agregado",
    "qtd_dias_com_compra": "Quantidade de dias distintos com compra. Agregado",
    "cliente_recorrente": "Booleano: comprou em mais de um dia. Derivado de qtd_dias_com_compra",
    "data_primeira_compra": "Data da primeira compra. Agregado",
    "data_ultima_compra": "Data da ultima compra. Agregado",
    "nota_primeiro_pedido": "Nota do primeiro pedido, 1 a 5 ou nula. Agregado de fato_pedido.nota",
    "nota_ultimo_pedido": "Nota do ultimo pedido, 1 a 5 ou nula. Agregado de fato_pedido.nota",
  })

# COMMAND ----------

# MAGIC %md
# MAGIC ## gold.dim_data

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.gold.dim_data AS
SELECT
  d                          AS data,
  YEAR(d)                    AS ano,
  QUARTER(d)                 AS trimestre,
  MONTH(d)                   AS mes,
  DATE_FORMAT(d, 'yyyy-MM')  AS ano_mes,
  DAYOFWEEK(d)               AS dia_semana,
  DAYOFWEEK(d) IN (1, 7)     AS fim_de_semana
FROM (
  SELECT EXPLODE(SEQUENCE(MIN(data_compra), MAX(data_compra), INTERVAL 1 DAY)) AS d
  FROM {CATALOG}.gold.fato_pedido
)
""")

documentar(f"{CATALOG}.gold.dim_data",
  "Gold - calendario diario cobrindo o periodo dos pedidos. Gerado a partir de min e max de fato_pedido.data_compra.",
  {
    "data": "Data (chave). Gerada por SEQUENCE",
    "ano": "Ano, 2016 a 2018",
    "trimestre": "Trimestre, 1 a 4",
    "mes": "Mes, 1 a 12",
    "ano_mes": "Ano e mes no formato yyyy-MM",
    "dia_semana": "Dia da semana, 1 = domingo a 7 = sabado",
    "fim_de_semana": "Booleano: sabado ou domingo",
  })

# COMMAND ----------

# MAGIC %md
# MAGIC ## gold.agg_recompra_por_nota

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.gold.agg_recompra_por_nota AS
SELECT
  nota,
  faixa_nota,
  COUNT(*)                                                    AS pedidos_referencia,
  SUM(CAST(recompra_{JANELA_DIAS}d AS INT))                   AS pedidos_com_recompra,
  ROUND(100 * AVG(CAST(recompra_{JANELA_DIAS}d AS INT)), 2)   AS pct_recompra_{JANELA_DIAS}d
FROM {CATALOG}.gold.fato_pedido
WHERE elegivel_janela_{JANELA_DIAS}d AND nota IS NOT NULL
GROUP BY nota, faixa_nota
""")

documentar(f"{CATALOG}.gold.agg_recompra_por_nota",
  f"Gold - taxa de recompra em {JANELA_DIAS} dias por nota do pedido de referencia. Origem: gold.fato_pedido filtrado por elegivel_janela_{JANELA_DIAS}d.",
  {
    "nota": "Nota do pedido de referencia, 1 a 5",
    "faixa_nota": "Boa (4-5), Neutra (3) ou Ruim (1-2)",
    "pedidos_referencia": "Quantidade de pedidos elegiveis com essa nota",
    "pedidos_com_recompra": f"Quantos desses pedidos tiveram nova compra em ate {JANELA_DIAS} dias",
    f"pct_recompra_{JANELA_DIAS}d": "Percentual de recompra, 0 a 100",
  })

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN workspace.gold

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.gold.agg_recompra_por_nota ORDER BY nota
