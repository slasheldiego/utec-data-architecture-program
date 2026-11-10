# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # E8 · Dashboard de BI (AI/BI Dashboard + Genie) — Grupo 06
# MAGIC **Cierra con:** M9 BI y Visualización
# MAGIC
# MAGIC El tablero tiene **dos páginas** (sección 7.2 del Documento Formal). Cada consulta de este notebook es un
# MAGIC *dataset* del tablero y declara el requerimiento (REQ-nn de 7.1) y el stakeholder al que responde.
# MAGIC
# MAGIC | Página | Audiencia | Requerimientos | Fuentes |
# MAGIC |---|---|---|---|
# MAGIC | **Integridad y calidad de datos** | Todas las áreas | REQ-01, REQ-04, REQ-05, REQ-06, REQ-07 + KPIs de gobierno de la CDO | `gld_kpi_resenas_sospechosas_negocio`, `gld_kpi_calidad_datos`, `gld_checkins_diarios`, `vw_rating_verificado_negocio`, `gld_dim_negocio`, `yelp_gov` (gobierno) |
# MAGIC | **Alertas de Trust & Safety** | Solo `data_stewards` | REQ-02, REQ-03 (y REQ-08 sin visual) | `gld_dim_usuario_riesgo`, `gld_salud_resenas` (Restringido) |
# MAGIC
# MAGIC Reglas: el tablero consume **solo `yelp_gold` y `yelp_gov`**, nunca silver ni bronze.
# MAGIC
# MAGIC > **Cómo se cumple "visible solo para data_stewards".** Un AI/BI Dashboard no permite ocultar una página a
# MAGIC > algunos lectores. Por eso la página de Trust & Safety se publica como **un segundo tablero**, compartido solo con
# MAGIC > `data_stewards` y publicado **sin credenciales embebidas** (cada lector consulta con sus propios permisos, así E6
# MAGIC > decide qué ve). Proponer al grupo ajustar la redacción de 7.2: "dos tableros" o "un tablero por audiencia".
# MAGIC
# MAGIC > **Trazabilidad —** implementa la sección 7 «Requerimientos de información y KPIs». Un KPI sin stakeholder que lo pidió no va al tablero.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Página 1 · Integridad y calidad de datos (todas las áreas)

# COMMAND ----------

# DBTITLE 1,Q1 · REQ-01 — Negocios con más reseñas sospechosas en los últimos 12 meses (Rafael Nakamura, T&S)
# MAGIC %sql
# MAGIC -- Ventana de 12 meses: un solo mes tiene muy pocas reseñas por negocio y el porcentaje no es estable
# MAGIC -- (el último mes de la muestra, enero 2022, está incompleto). La evolución mes a mes está en Q2.
# MAGIC -- Mínimo de 20 reseñas para que un negocio con pocas reseñas no encabece el ranking.
# MAGIC
# MAGIC WITH ult AS (SELECT max(mes) AS fin FROM workspace.yelp_gold.gld_kpi_resenas_sospechosas_negocio)
# MAGIC SELECT n.name AS negocio,
# MAGIC         sum(k.n_resenas) AS resenas,
# MAGIC         sum(k.n_sospechosas) AS sospechosas,
# MAGIC         round(try_divide(100.0 * sum(k.n_sospechosas), sum(k.n_resenas)), 2) AS pct_sospechosas
# MAGIC  FROM workspace.yelp_gold.gld_kpi_resenas_sospechosas_negocio k
# MAGIC  JOIN workspace.yelp_gold.gld_dim_negocio n USING (business_id)
# MAGIC  CROSS JOIN ult
# MAGIC  WHERE k.mes > add_months(ult.fin, -12)
# MAGIC  GROUP BY n.name, k.business_id
# MAGIC  HAVING sum(k.n_resenas) >= 20 AND sum(k.n_sospechosas) > 0
# MAGIC  ORDER BY pct_sospechosas DESC, sospechosas DESC
# MAGIC  LIMIT 20;

# COMMAND ----------

# DBTITLE 1,Q2 · REQ-01 — Evolución mensual del porcentaje de reseñas sospechosas (Rafael Nakamura, T&S)
# MAGIC %sql
# MAGIC SELECT mes,
# MAGIC        sum(n_resenas)                                                       AS resenas,
# MAGIC        sum(n_sospechosas)                                                   AS sospechosas,
# MAGIC        round(try_divide(100.0 * sum(n_sospechosas), sum(n_resenas)), 2)     AS pct_sospechosas
# MAGIC FROM workspace.yelp_gold.gld_kpi_resenas_sospechosas_negocio
# MAGIC GROUP BY mes
# MAGIC ORDER BY mes;

# COMMAND ----------

# DBTITLE 1,Q3 · REQ-04 — Cumplimiento de reglas críticas en la última ejecución (Laura Hernández, CDO)
# MAGIC %sql
# MAGIC -- KPI: reglas de criticidad alta que cumplen su umbral o tienen excepción vigente / total de reglas altas (meta 100 %, CE-04)
# MAGIC WITH ultima AS (
# MAGIC   SELECT * FROM workspace.yelp_gold.gld_kpi_calidad_datos
# MAGIC   WHERE ejecucion_ts = (SELECT max(ejecucion_ts) FROM workspace.yelp_gold.gld_kpi_calidad_datos))
# MAGIC SELECT max(ejecucion_ts)                                                         AS ejecucion,
# MAGIC        count_if(criticidad = 'alta')                                              AS reglas_criticas,
# MAGIC        count_if(criticidad = 'alta' AND paso)                                     AS cumplen,
# MAGIC        count_if(criticidad = 'alta' AND NOT paso AND con_excepcion)               AS con_excepcion_vigente,
# MAGIC        count_if(criticidad = 'alta' AND NOT paso AND NOT con_excepcion)           AS fallan_sin_excepcion,
# MAGIC        round(try_divide(100.0 * count_if(criticidad = 'alta' AND (paso OR con_excepcion)),
# MAGIC                         count_if(criticidad = 'alta')), 1)                        AS pct_cumplimiento_criticas
# MAGIC FROM ultima;

# COMMAND ----------

# DBTITLE 1,Q4 · REQ-04 — Detalle de reglas fallidas en la última ejecución (Laura Hernández, CDO)
# MAGIC %sql
# MAGIC -- Tabla para decidir si una excepción se renueva o se cierra. Las fechas vienen de dq_excepciones (Acta 05).
# MAGIC SELECT k.regla_id, k.dimension, k.criticidad, k.umbral_pct, k.pct_cumplimiento, k.con_excepcion,
# MAGIC        ex.fecha_caducidad,
# MAGIC        datediff(ex.fecha_caducidad, current_date())  AS dias_para_caducar,
# MAGIC        ex.acta
# MAGIC FROM workspace.yelp_gold.gld_kpi_calidad_datos k
# MAGIC LEFT JOIN workspace.yelp_gov.dq_excepciones ex ON ex.regla_id = k.regla_id
# MAGIC   AND current_date() BETWEEN ex.fecha_aprobacion AND ex.fecha_caducidad
# MAGIC WHERE k.ejecucion_ts = (SELECT max(ejecucion_ts) FROM workspace.yelp_gold.gld_kpi_calidad_datos)
# MAGIC   AND NOT k.paso
# MAGIC ORDER BY CASE k.criticidad WHEN 'alta' THEN 1 ELSE 2 END, k.pct_cumplimiento;

# COMMAND ----------

# DBTITLE 1,Q5 · REQ-04 — Tendencia de cumplimiento por dimensión de calidad (Laura Hernández, CDO)
# MAGIC %sql
# MAGIC -- Reemplaza la Q2 del esqueleto del curso: usa gold (gld_kpi_calidad_datos) y no dq_reglas, que guarda el
# MAGIC -- parámetro Restringido de DQ-016 (5.3).
# MAGIC SELECT ejecucion_ts, dimension,
# MAGIC        round(avg(pct_cumplimiento), 2)          AS cumplimiento_promedio,
# MAGIC        count_if(NOT paso)                       AS reglas_en_rojo
# MAGIC FROM workspace.yelp_gold.gld_kpi_calidad_datos
# MAGIC GROUP BY ejecucion_ts, dimension
# MAGIC ORDER BY ejecucion_ts, dimension;

# COMMAND ----------

# DBTITLE 1,Q6 · REQ-05 — Brecha entre rating publicado y rating verificado (Andrew Collins, Producto)
# MAGIC %sql
# MAGIC -- Rating verificado de toda la historia del negocio = promedio de los meses ponderado por reseñas verificadas.
# MAGIC SELECT n.name                                                                       AS negocio,
# MAGIC        v.rating_publicado,
# MAGIC        round(sum(v.rating_verificado * v.n_resenas_verificadas) / sum(v.n_resenas_verificadas), 2) AS rating_verificado,
# MAGIC        round(v.rating_publicado
# MAGIC              - sum(v.rating_verificado * v.n_resenas_verificadas) / sum(v.n_resenas_verificadas), 2) AS brecha_rating,
# MAGIC        sum(v.n_resenas_verificadas)                                                 AS resenas_verificadas
# MAGIC FROM workspace.yelp_gold.vw_rating_verificado_negocio v
# MAGIC JOIN workspace.yelp_gold.gld_dim_negocio n USING (business_id)
# MAGIC GROUP BY n.name, v.business_id, v.rating_publicado
# MAGIC HAVING sum(v.n_resenas_verificadas) >= 10
# MAGIC ORDER BY abs(brecha_rating) DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# DBTITLE 1,Q7 · REQ-06 — Reseñas verificadas por mes (Jessica Martinez, Growth)
# MAGIC %sql
# MAGIC -- En el tablero: filtro por negocio (gld_dim_negocio.name) para medir campañas sobre opiniones reales.
# MAGIC SELECT v.mes, n.name AS negocio, v.n_resenas_verificadas, v.rating_verificado
# MAGIC FROM workspace.yelp_gold.vw_rating_verificado_negocio v
# MAGIC JOIN workspace.yelp_gold.gld_dim_negocio n USING (business_id)
# MAGIC ORDER BY v.mes, negocio;

# COMMAND ----------

# DBTITLE 1,Q8 · REQ-07 — Volumen diario de check-ins válidos (Carla Romero, Ads Strategy)
# MAGIC %sql
# MAGIC SELECT k.fecha, t.es_fin_de_semana, n.name AS negocio, k.n_checkins
# MAGIC FROM workspace.yelp_gold.gld_checkins_diarios k
# MAGIC JOIN workspace.yelp_gold.gld_dim_negocio n USING (business_id)
# MAGIC JOIN workspace.yelp_gold.gld_dim_tiempo  t ON t.fecha = k.fecha
# MAGIC ORDER BY k.fecha;

# COMMAND ----------

# DBTITLE 1,Q9 · REQ-07 — Picos de check-ins que no acompañan las reseñas (Carla Romero → aviso a T&S)
# MAGIC %sql
# MAGIC -- Pico: día con check-ins por encima de la media + 3 desviaciones estándar del propio negocio.
# MAGIC -- "No acompaña las reseñas": ese mes el negocio recibió menos reseñas que su promedio mensual.
# MAGIC WITH estad AS (
# MAGIC   SELECT business_id, avg(n_checkins) AS media, stddev(n_checkins) AS desv
# MAGIC   FROM workspace.yelp_gold.gld_checkins_diarios GROUP BY business_id HAVING count(*) >= 30),
# MAGIC resenas_mes AS (
# MAGIC   SELECT business_id, mes, n_resenas,
# MAGIC          avg(n_resenas) OVER (PARTITION BY business_id) AS media_resenas_mes
# MAGIC   FROM workspace.yelp_gold.gld_kpi_resenas_sospechosas_negocio)
# MAGIC SELECT n.name AS negocio, c.fecha, c.n_checkins, round(e.media, 1) AS media_checkins,
# MAGIC        coalesce(r.n_resenas, 0) AS resenas_del_mes, round(r.media_resenas_mes, 1) AS media_resenas_mes
# MAGIC FROM workspace.yelp_gold.gld_checkins_diarios c
# MAGIC JOIN estad e USING (business_id)
# MAGIC JOIN workspace.yelp_gold.gld_dim_negocio n USING (business_id)
# MAGIC LEFT JOIN resenas_mes r ON r.business_id = c.business_id AND r.mes = trunc(c.fecha, 'MM')
# MAGIC WHERE e.desv > 0 AND c.n_checkins > e.media + 3 * e.desv
# MAGIC   AND coalesce(r.n_resenas, 0) < coalesce(r.media_resenas_mes, 1)
# MAGIC ORDER BY c.n_checkins / e.media DESC
# MAGIC LIMIT 50;

# COMMAND ----------

# DBTITLE 1,Q10 · KPIs de gobierno (Laura Hernández, CDO)
# MAGIC %sql
# MAGIC -- Corrige la Q3 del esqueleto: en el diccionario la columna se llama descripcion_negocio (no descripcion_columna).
# MAGIC SELECT
# MAGIC   (SELECT round(try_divide(100.0 * count_if(descripcion_negocio IS NOT NULL), count(*)), 1)
# MAGIC      FROM workspace.yelp_gov.diccionario_datos)                                   AS pct_columnas_documentadas,
# MAGIC   (SELECT round(try_divide(100.0 * count_if(responsable IS NOT NULL AND sensibilidad IS NOT NULL), count(*)), 1)
# MAGIC      FROM workspace.yelp_gov.diccionario_datos
# MAGIC      WHERE nombre_fisico LIKE 'workspace.yelp_gold.gld_salud_resenas.%')          AS pct_ce05_salud_resenas,
# MAGIC   (SELECT count(*) FROM workspace.yelp_gov.matriz_raci WHERE data_owner IS NOT NULL) AS entidades_con_owner,
# MAGIC   (SELECT count(*) FROM workspace.yelp_gov.glosario_negocio)                      AS terminos_glosario,
# MAGIC   (SELECT count(DISTINCT ejecucion_ts) FROM workspace.yelp_gold.gld_kpi_calidad_datos) AS ejecuciones_calidad;

# COMMAND ----------

# DBTITLE 1,Q11 · Operación del pipeline — duración por capa y corrida (Equipo de Data Platform · FinOps 8.2)
# MAGIC %sql
# MAGIC SELECT date_trunc('minute', ejecucion_ts) AS corrida, capa, max(duracion_seg) AS duracion_seg, sum(filas) AS filas
# MAGIC FROM workspace.yelp_gov.pipeline_bitacora
# MAGIC GROUP BY ALL
# MAGIC ORDER BY corrida, capa;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Página 2 · Alertas de Trust & Safety (solo `data_stewards` — tablero aparte)

# COMMAND ----------

# DBTITLE 1,Q12 · REQ-02 — Cuentas por nivel de riesgo (Rafael Nakamura, T&S)
# MAGIC %sql
# MAGIC SELECT nivel_riesgo, count(*) AS cuentas, max(ejecucion_ts) AS ejecucion
# MAGIC FROM workspace.yelp_gold.gld_dim_usuario_riesgo
# MAGIC GROUP BY nivel_riesgo
# MAGIC ORDER BY CASE nivel_riesgo WHEN 'Alto' THEN 1 WHEN 'Medio' THEN 2 WHEN 'Bajo' THEN 3 ELSE 4 END;

# COMMAND ----------

# DBTITLE 1,Q13 · REQ-02 — Cuentas de riesgo alto y medio con sus señales (Rafael Nakamura, T&S)
# MAGIC %sql
# MAGIC -- Prioridad de revisión, no sanción (6.4). El user_id es Confidencial; el nombre no está en gold.
# MAGIC SELECT user_id, nivel_riesgo, n_senales, array_join(senales, ', ') AS senales,
# MAGIC        n_resenas_cargadas, yelping_since, version_reglas, ejecucion_ts
# MAGIC FROM workspace.yelp_gold.gld_dim_usuario_riesgo
# MAGIC WHERE nivel_riesgo IN ('Alto', 'Medio')
# MAGIC ORDER BY CASE nivel_riesgo WHEN 'Alto' THEN 1 ELSE 2 END, n_senales DESC, n_resenas_cargadas DESC
# MAGIC LIMIT 200;

# COMMAND ----------

# DBTITLE 1,Q14 · REQ-03 — ¿Por qué se marcó esta reseña? (Rafael Nakamura, T&S)
# En el tablero: dataset con el parámetro :review_id (widget de texto). Aquí se prueba con un widget del notebook.
dbutils.widgets.text("review_id", "", "review_id a explicar (REQ-03)")
rid = dbutils.widgets.get("review_id").strip()
if not rid:   # sin valor: se toma una reseña sospechosa cualquiera para la demostración
    rid = spark.sql("SELECT review_id FROM workspace.yelp_gold.gld_salud_resenas WHERE es_sospechosa LIMIT 1").first()
    rid = rid.review_id if rid else ""
display(spark.sql("""
  SELECT s.review_id, n.name AS negocio, s.user_id, s.fecha_resena, s.stars,
         s.es_sospechosa, array_join(s.regla_disparada, ', ') AS senales,
         s.version_reglas, s.ejecucion_ts, u.nivel_riesgo AS riesgo_de_la_cuenta
  FROM workspace.yelp_gold.gld_salud_resenas s
  JOIN workspace.yelp_gold.gld_dim_negocio n USING (business_id)
  JOIN workspace.yelp_gold.gld_dim_usuario_riesgo u USING (user_id)
  WHERE s.review_id = :review_id""", args={"review_id": rid}))

# COMMAND ----------

# DBTITLE 1,Q15 · CE-06 — Alertas explicables (meta 100 %)
# MAGIC %sql
# MAGIC SELECT count(*)                                                                         AS resenas_sospechosas,
# MAGIC        round(try_divide(100.0 * count_if(size(regla_disparada) > 0 AND version_reglas IS NOT NULL
# MAGIC                                          AND ejecucion_ts IS NOT NULL), count(*)), 1)   AS pct_explicables
# MAGIC FROM workspace.yelp_gold.gld_salud_resenas
# MAGIC WHERE es_sospechosa;

# COMMAND ----------

# DBTITLE 1,REQ-08 (sin visual) — Con qué versión de datos y reglas se etiquetó un dataset (Mei Lin, Data Science)
# MAGIC %sql
# MAGIC -- Cada versión de gld_salud_resenas queda en el historial de Delta durante 30 días (ADR-04).
# MAGIC -- Para reproducir un entrenamiento: SELECT * FROM workspace.yelp_gold.gld_salud_resenas VERSION AS OF <n>
# MAGIC DESCRIBE HISTORY workspace.yelp_gold.gld_salud_resenas;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Genie Space (solo tablas de la página 1)
# MAGIC Crear en **Genie → New** con estas tablas: `gld_kpi_resenas_sospechosas_negocio`, `gld_kpi_calidad_datos`,
# MAGIC `gld_checkins_diarios`, `vw_rating_verificado_negocio` y las dimensiones `gld_dim_negocio` y `gld_dim_tiempo`
# MAGIC (sin ellas Genie no puede mostrar nombres de negocio ni filtrar por fines de semana).
# MAGIC **No agregar** `gld_salud_resenas` ni `gld_dim_usuario_riesgo`: nadie debe llegar por preguntas a datos Restringidos (7.2).
# MAGIC
# MAGIC **Instrucciones sugeridas** (copiar en *Instructions*; salen del Glosario, Anexo A):
# MAGIC ```
# MAGIC - "Negocio", "local", "establecimiento" o "comercio" = gld_dim_negocio; mostrar siempre name, no business_id.
# MAGIC - "Negocio activo" o "abierto" = is_open = 1.
# MAGIC - "Reseña sospechosa", "reseña marcada" o "posible reseña falsa" = n_sospechosas de gld_kpi_resenas_sospechosas_negocio.
# MAGIC - "Tasa de sospecha" o "% de reseñas sospechosas" = pct_sospechosas (ya viene en porcentaje, 0 a 100).
# MAGIC - "Reseña verificada", "reseña válida" o "reseña confiable" = n_resenas_verificadas de vw_rating_verificado_negocio.
# MAGIC - "Rating real" o "rating verificado" = rating_verificado; "rating publicado" o "estrellas del negocio" = rating_publicado.
# MAGIC - "Check-in válido", "visitas diarias" o "check-ins por día" = n_checkins de gld_checkins_diarios.
# MAGIC - "Regla DQ" o "control de calidad" = gld_kpi_calidad_datos; usar siempre la última ejecucion_ts salvo que pidan historia.
# MAGIC - "Excepción", "riesgo aceptado" = con_excepcion = TRUE.
# MAGIC - Los meses se representan por su primer día (columna mes).
# MAGIC - Si preguntan por cuentas, usuarios concretos o por qué se marcó una reseña, responder que es información
# MAGIC   Restringida de Trust & Safety y no está disponible en este espacio.
# MAGIC ```
# MAGIC **Preguntas de prueba** (capturar respuesta correcta y, si falla, la instrucción que lo corrigió):
# MAGIC 1. ¿Qué 10 negocios tuvieron mayor porcentaje de reseñas sospechosas en el último mes?
# MAGIC 2. ¿Cuántas reseñas verificadas recibió cada negocio abierto en 2021?
# MAGIC 3. ¿Qué reglas de criticidad alta fallaron en la última ejecución y cuáles tienen excepción?
# MAGIC 4. ¿Qué días de fin de semana tuvieron más check-ins válidos?
# MAGIC 5. (Control) ¿Qué usuarios tienen riesgo alto? → Genie debe negarse.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checklist de ensamblado en la UI
# MAGIC - [ ] Tablero 1 «Plataforma de Datos YelpConnect — Grupo 06 · Integridad y calidad»: datasets Q1–Q11, ≥6 visuales,
# MAGIC       filtros por negocio y mes, publicado **con credenciales embebidas** y compartido con todo el equipo.
# MAGIC - [ ] Tablero 2 «… · Alertas de Trust & Safety»: datasets Q12–Q15 (Q14 con parámetro `:review_id`), compartido solo
# MAGIC       con `data_stewards` y publicado **sin credenciales embebidas**.
# MAGIC - [ ] Cada visual con subtítulo «REQ-nn · stakeholder».
# MAGIC - [ ] Refresco programado después del job de E7/04 (por ejemplo, 23:30 hora de Lima).
# MAGIC - [ ] CE-07: captura de David viendo el tablero 2 y de Juan Carlos o Edgar sin acceso a él.
# MAGIC - [ ] Genie Space con las instrucciones de arriba y evidencia de una iteración (pregunta fallida → instrucción → acierto).
# MAGIC
# MAGIC ### Definition of Done (E8)
# MAGIC - [ ] Tableros publicados consumiendo solo gold/gov.
# MAGIC - [ ] Genie Space con evidencia de iteración.
# MAGIC - [ ] Storytelling: 1 slide-resumen por página explicando la decisión que habilita (columna «Decisión que habilita» de 7.1).