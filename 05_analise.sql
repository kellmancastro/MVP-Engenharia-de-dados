-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 05 - Análise: a nota da última compra influencia a recompra?
-- MAGIC
-- MAGIC Todas as consultas usam a camada Gold. Em cada resultado, use o botão **+ > Visualization**
-- MAGIC do Databricks para gerar o gráfico de barras e tire print para o README.
-- MAGIC
-- MAGIC O intervalo de confiança de 95% é calculado por aproximação normal:
-- MAGIC `p ± 1,96 × raiz(p × (1 − p) / n)`. Se os intervalos de duas faixas não se sobrepõem,
-- MAGIC a diferença é estatisticamente relevante.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## P1. Qual é a taxa geral de recompra dos clientes Olist?

-- COMMAND ----------

SELECT
  COUNT(*)                                               AS clientes,
  SUM(CAST(cliente_recorrente AS INT))                   AS clientes_recorrentes,
  ROUND(100 * AVG(CAST(cliente_recorrente AS INT)), 2)   AS pct_clientes_recorrentes
FROM workspace.gold.dim_cliente

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## P2. Quando a última avaliação é boa, neutra ou ruim, qual é a chance de recompra em 180 dias?
-- MAGIC **Pergunta principal do MVP.**

-- COMMAND ----------

SELECT
  faixa_nota,
  COUNT(*)                                              AS pedidos_referencia,
  SUM(CAST(recompra_180d AS INT))                       AS com_recompra,
  ROUND(100 * AVG(CAST(recompra_180d AS INT)), 2)       AS pct_recompra,
  ROUND(100 * (AVG(CAST(recompra_180d AS INT))
        - 1.96 * SQRT(AVG(CAST(recompra_180d AS INT)) * (1 - AVG(CAST(recompra_180d AS INT))) / COUNT(*))), 2) AS ic95_inferior,
  ROUND(100 * (AVG(CAST(recompra_180d AS INT))
        + 1.96 * SQRT(AVG(CAST(recompra_180d AS INT)) * (1 - AVG(CAST(recompra_180d AS INT))) / COUNT(*))), 2) AS ic95_superior
FROM workspace.gold.fato_pedido
WHERE elegivel_janela_180d AND nota IS NOT NULL
GROUP BY faixa_nota
ORDER BY faixa_nota

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## P3. Como a recompra varia em cada nota, de 1 a 5?

-- COMMAND ----------

SELECT nota, pedidos_referencia, pedidos_com_recompra, pct_recompra_180d
FROM workspace.gold.agg_recompra_por_nota
ORDER BY nota

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## P4. O resultado se mantém olhando só a primeira compra de cada cliente?
-- MAGIC Checagem de robustez: aqui cada cliente conta uma única vez.

-- COMMAND ----------

SELECT
  faixa_nota,
  COUNT(*)                                          AS clientes,
  ROUND(100 * AVG(CAST(recompra_180d AS INT)), 2)   AS pct_recompra
FROM workspace.gold.fato_pedido
WHERE seq_pedido_cliente = 1 AND elegivel_janela_180d AND nota IS NOT NULL
GROUP BY faixa_nota
ORDER BY faixa_nota

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## P5. Entregas atrasadas geram notas piores e menos recompra?

-- COMMAND ----------

SELECT
  CASE WHEN entregue_com_atraso THEN 'Com atraso' ELSE 'No prazo' END  AS entrega,
  COUNT(*)                                                             AS pedidos,
  ROUND(AVG(nota), 2)                                                  AS nota_media,
  ROUND(100 * AVG(CASE WHEN nota <= 2 THEN 1 ELSE 0 END), 2)           AS pct_nota_ruim,
  ROUND(100 * AVG(CAST(recompra_180d AS INT)), 2)                      AS pct_recompra
FROM workspace.gold.fato_pedido
WHERE elegivel_janela_180d AND nota IS NOT NULL AND entregue_com_atraso IS NOT NULL
GROUP BY 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## P6. Entre quem recomprou, a nota muda o tempo até a nova compra?

-- COMMAND ----------

SELECT
  faixa_nota,
  COUNT(*)                                          AS recompras,
  ROUND(AVG(dias_ate_proxima_compra), 1)            AS media_dias,
  PERCENTILE(dias_ate_proxima_compra, 0.5)          AS mediana_dias
FROM workspace.gold.fato_pedido
WHERE recompra_180d AND nota IS NOT NULL
GROUP BY faixa_nota
ORDER BY faixa_nota
