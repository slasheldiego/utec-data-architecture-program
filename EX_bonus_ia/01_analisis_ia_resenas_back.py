# Databricks notebook source
# MAGIC %md
# MAGIC # EX · Bonus — IA sobre datos no estructurados (reseñas)
# MAGIC **Cierra con:** M8 IA y Datos No Estructurados · **Puntaje:** bonus (no bloquea la nota base)
# MAGIC
# MAGIC El texto de las reseñas es el activo no estructurado más valioso del caso. Aquí se
# MAGIC aplican las **AI Functions** de Databricks SQL (ai_analyze_sentiment, ai_classify,
# MAGIC ai_summarize) para enriquecer la capa gold — arquitectura de IA gobernada: el
# MAGIC resultado del modelo se persiste como tabla catalogada, con lineage y calidad.
# MAGIC
# MAGIC ⚠️ FinOps: las AI Functions consumen cuota. Trabajar con una muestra pequeña (≤500 reseñas).

# COMMAND ----------

# DBTITLE 1: Sentimiento y clasificación de una muestra de reseñas
%sql
CREATE OR REPLACE TABLE workspace.yelp_gold.gld_resenas_enriquecidas
COMMENT 'Muestra de reseñas enriquecidas con IA: sentimiento y tema. Consumidor: Trust & Safety y Producto.'
AS
WITH muestra AS (
  SELECT review_id, business_id, stars, text
  FROM workspace.yelp_silver.slv_review
  TABLESAMPLE (500 ROWS)
)
SELECT
  review_id, business_id, stars,
  ai_analyze_sentiment(text)                                            AS sentimiento,
  ai_classify(text, array('comida','servicio','precio','ambiente','otro')) AS tema
FROM muestra;

# COMMAND ----------

# DBTITLE 1: ✏️ TODO — Detección de inconsistencia rating vs sentimiento (señal de fraude)
%sql
-- Reseñas con 5 estrellas y sentimiento negativo (o viceversa) son candidatas a revisión
-- por Trust & Safety. Construir la consulta y agregar el visual al dashboard E8.
-- SELECT ... FROM workspace.yelp_gold.gld_resenas_enriquecidas WHERE ...;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definition of Done (EX)
# MAGIC - [ ] Tabla enriquecida creada, catalogada y con COMMENT.
# MAGIC - [ ] Análisis de inconsistencia rating↔sentimiento con conclusiones para T&S.
# MAGIC - [ ] Reflexión de gobierno de IA: ¿qué riesgos introduce persistir salidas de un LLM
# MAGIC       como datos productivos y qué controles pondrían? (½ página)