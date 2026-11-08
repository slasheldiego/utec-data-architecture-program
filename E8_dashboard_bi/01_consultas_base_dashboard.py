# Databricks notebook source
# MAGIC %md
# MAGIC # E8 · Dashboard de BI (AI/BI Dashboard + Genie)
# MAGIC **Cierra con:** M9 BI y Visualización
# MAGIC
# MAGIC El entregable son DOS artefactos en el workspace:
# MAGIC 1. **AI/BI Dashboard** "Plataforma de Datos YelpConnect — Grupo NN" con 2 páginas:
# MAGIC    - *Página Negocio:* KPIs para los stakeholders del caso (rating, reseñas, check-ins).
# MAGIC    - *Página Gobierno:* el dashboard que le mostrarían a la CDO — salud de calidad (E5),
# MAGIC      completitud de metadata (E4), cobertura de ownership (E6).
# MAGIC 2. **Genie Space** sobre la capa gold, con instrucciones y sinónimos del glosario E4,
# MAGIC    para que un usuario de negocio pregunte en lenguaje natural.
# MAGIC
# MAGIC Reglas: el dashboard consume SOLO `yelp_gold` y `yelp_gov` (nunca silver/bronze —
# MAGIC eso valida la arquitectura de capas), y cada visual declara a qué stakeholder responde.
# MAGIC
# MAGIC Este notebook deja listas las consultas base; el ensamblado se hace en la UI.
# MAGIC
# MAGIC > **Trazabilidad —** este entregable **implementa** la sección «Requerimientos de información y KPIs por stakeholder» del
# MAGIC > **Documento Formal de Arquitectura de Datos**. Cada visual de este dashboard debe responder a un requerimiento declarado en el documento; un KPI sin stakeholder que lo pidió no va al tablero.

# COMMAND ----------

# DBTITLE 1: Q1 — Evolución de reseñas y rating (Producto)
%sql
SELECT mes, sum(n_resenas) AS resenas, round(avg(rating_promedio),2) AS rating
FROM workspace.yelp_gold.gld_kpi_negocio_mensual
GROUP BY mes ORDER BY mes;

# COMMAND ----------

# DBTITLE 1: Q2 — Salud de calidad de datos (CDO)
%sql
SELECT r.dimension, round(avg(res.pct_cumplimiento),2) AS cumplimiento,
       sum(CASE WHEN NOT res.paso THEN 1 ELSE 0 END)  AS reglas_en_rojo
FROM workspace.yelp_gov.dq_resultados res
JOIN workspace.yelp_gov.dq_reglas r USING (regla_id)
WHERE res.ejecucion_ts = (SELECT max(ejecucion_ts) FROM workspace.yelp_gov.dq_resultados)
GROUP BY r.dimension;

# COMMAND ----------

# DBTITLE 1: Q3 — KPIs de gobierno (CDO)
%sql
SELECT
  (SELECT round(100.0*sum(CASE WHEN descripcion_columna IS NOT NULL THEN 1 ELSE 0 END)/count(*),1)
     FROM workspace.yelp_gov.diccionario_datos)                       AS pct_columnas_documentadas,
  (SELECT count(*) FROM workspace.yelp_gov.matriz_raci)               AS entidades_con_owner,
  (SELECT count(*) FROM workspace.yelp_gov.glosario_negocio)          AS terminos_glosario;

# COMMAND ----------

# DBTITLE 1: ✏️ TODO — Q4 a Q8
# Mínimo: top negocios por rating ponderado, check-ins diarios (Ops Comerciales),
# reseñas en cuarentena/duplicadas (Trust & Safety), tendencia de la bitácora del pipeline.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checklist de ensamblado en la UI
# MAGIC - [ ] Dashboard con 2 páginas, ≥6 visuales, filtro por ciudad/mes y textos de contexto.
# MAGIC - [ ] Cada visual tiene subtítulo con el stakeholder al que responde.
# MAGIC - [ ] Genie Space creado sobre `yelp_gold` con ≥5 instrucciones/sinónimos del glosario;
# MAGIC       incluir captura de 3 preguntas de negocio respondidas correctamente
# MAGIC       (y 1 donde Genie falló + cómo lo corrigieron con instrucciones → esa iteración se evalúa).
# MAGIC - [ ] Publicar el dashboard y programar refresco alineado al job E7.4.
# MAGIC
# MAGIC ### Definition of Done (E8)
# MAGIC - [ ] Dashboard publicado consumiendo solo gold/gov.
# MAGIC - [ ] Genie Space con evidencia de iteración.
# MAGIC - [ ] Storytelling: 1 slide-resumen por página explicando la decisión que habilita.