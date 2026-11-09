# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # EX · Bonus — IA sobre datos no estructurados (reseñas) · Grupo 06
# MAGIC **Cierra con:** M8 IA y Datos No Estructurados · **Puntaje:** bonus (no bloquea la nota base)
# MAGIC
# MAGIC Se aplican las **AI Functions** de Databricks SQL (`ai_analyze_sentiment`, `ai_classify`) a una muestra de reseñas
# MAGIC y se busca una **señal adicional de fraude**: reseñas cuya calificación contradice su propio texto
# MAGIC (5 estrellas con texto negativo, o 1 estrella con texto positivo).
# MAGIC
# MAGIC **Decisión de gobierno: el resultado va a `yelp_gov`, no a `yelp_gold`.** Es un experimento, no un producto de
# MAGIC datos aprobado: no figura en la sección 4.3 ni en el Diccionario. Si se publicara en gold:
# MAGIC - E6 lo daría automáticamente a los consumidores (toda tabla gold no restringida es de consumo general), y
# MAGIC - E4/02 dejaría de cumplir el 100 % de cobertura del Diccionario.
# MAGIC En `yelp_gov` solo lo leen el steward y los custodians. Si el grupo lo adopta, se documenta en el Diccionario,
# MAGIC se clasifica como Restringido y se mueve a gold.
# MAGIC
# MAGIC ⚠️ **FinOps:** las AI Functions consumen cuota. Se usa una muestra de 500 reseñas y la tabla **no se regenera**
# MAGIC salvo que el widget lo pida. Si Free Edition no tiene AI Functions habilitadas, la celda falla: documentarlo como hallazgo.
# MAGIC
# MAGIC ⚠️ **Quién lo ejecuta:** un Data Custodian o el steward. Para cualquier otra cuenta el texto llega enmascarado
# MAGIC (`mascara_texto`) y el modelo analizaría el texto "[texto oculto…]".

# COMMAND ----------

# DBTITLE 1,Parámetros y verificaciones
dbutils.widgets.dropdown("regenerar", "no", ["no", "si"], "¿Volver a llamar al modelo? (consume cuota)")
REGENERAR = dbutils.widgets.get("regenerar") == "si"
TABLA = "workspace.yelp_gov.ex_resenas_enriquecidas"

enmascarado = spark.sql("""SELECT count(*) c FROM workspace.yelp_silver.slv_review
                           WHERE text = '[texto oculto: dato Confidencial]'""").first().c
assert enmascarado == 0, "❌ El texto llega enmascarado: ejecutar con una cuenta de Data Custodian o del steward."
assert spark.catalog.tableExists("workspace.yelp_gold.gld_salud_resenas"), "❌ Ejecutar antes E7/03 (gold)"
existe = spark.catalog.tableExists(TABLA)
print(f"Tabla {'existe' if existe else 'no existe'} · regenerar = {REGENERAR}")

# COMMAND ----------

# DBTITLE 1,Sentimiento y tema de una muestra estratificada (250 sospechosas + 250 verificadas)
# Muestra estratificada y reproducible (xxhash64 en lugar de TABLESAMPLE): permite comparar si la contradicción
# calificación ↔ texto es más frecuente entre las reseñas que gold ya marcó como sospechosas.
if REGENERAR or not existe:
    spark.sql(f"""
      CREATE OR REPLACE TABLE {TABLA}
      COMMENT 'EX (bonus, experimental): sentimiento y tema de 500 reseñas generados con AI Functions. No es producto de datos aprobado; acceso solo steward y custodians.'
      AS
      WITH muestra AS (
        SELECT * FROM (
          SELECT r.review_id, r.business_id, r.stars, r.text, s.es_sospechosa,
                 row_number() OVER (PARTITION BY s.es_sospechosa ORDER BY xxhash64(r.review_id)) AS n
          FROM workspace.yelp_silver.slv_review r
          JOIN workspace.yelp_gold.gld_salud_resenas s USING (review_id)
          WHERE length(r.text) BETWEEN 20 AND 3000)
        WHERE n <= 250)
      SELECT review_id, business_id, stars, es_sospechosa,
             ai_analyze_sentiment(text)                                                  AS sentimiento,
             ai_classify(text, array('comida', 'servicio', 'precio', 'ambiente', 'otro')) AS tema,
             current_timestamp()                                                         AS generado_ts,
             'ai_analyze_sentiment / ai_classify (Databricks AI Functions)'              AS modelo
      FROM muestra""")
    print(f"✔ {TABLA} generada")
else:
    print("ℹ️ Se reutiliza la tabla existente (no se consume cuota). Poner el widget en 'si' para regenerarla.")
display(spark.table(TABLA).limit(10))

# COMMAND ----------

# DBTITLE 1,Detección de inconsistencia calificación ↔ sentimiento (señal candidata para Trust & Safety)
# MAGIC %sql
# MAGIC -- Contradicción: 4-5 estrellas con texto negativo, o 1-2 estrellas con texto positivo.
# MAGIC SELECT CASE WHEN es_sospechosa THEN 'Sospechosa (6.4)' ELSE 'Verificada' END            AS grupo,
# MAGIC        count(*)                                                                         AS resenas,
# MAGIC        count_if((stars >= 4 AND sentimiento = 'negative') OR (stars <= 2 AND sentimiento = 'positive')) AS contradictorias,
# MAGIC        round(try_divide(100.0 * count_if((stars >= 4 AND sentimiento = 'negative')
# MAGIC                                       OR (stars <= 2 AND sentimiento = 'positive')), count(*)), 1) AS pct_contradictorias
# MAGIC FROM workspace.yelp_gov.ex_resenas_enriquecidas
# MAGIC GROUP BY 1 ORDER BY 1;

# COMMAND ----------

# DBTITLE 1,Detalle por tema (para el visual opcional del tablero de Trust & Safety)
# MAGIC %sql
# MAGIC SELECT tema, sentimiento, count(*) AS resenas, round(avg(stars), 2) AS estrellas_promedio
# MAGIC FROM workspace.yelp_gov.ex_resenas_enriquecidas
# MAGIC GROUP BY tema, sentimiento
# MAGIC ORDER BY tema, resenas DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conclusiones para Trust & Safety (completar con los resultados de la corrida)
# MAGIC - ✏️ % de reseñas contradictorias entre las **sospechosas**: … · entre las **verificadas**: …
# MAGIC - ✏️ Si el porcentaje es claramente mayor entre las sospechosas, la contradicción es candidata a **sexta señal**
# MAGIC   de la sección 6.4 (con peso Bajo hasta calibrarla con casos revisados). Si no, se descarta y se documenta.
# MAGIC - ✏️ Ejemplos revisados a mano (3 a 5 `review_id`) y si el modelo acertó.
# MAGIC
# MAGIC ### Reflexión de gobierno de IA (borrador para revisar en grupo)
# MAGIC Persistir la salida de un modelo de lenguaje como dato productivo introduce riesgos que las reglas de calidad de la
# MAGIC sección 6 no cubren. **Primero, no es reproducible:** el mismo texto puede recibir otro sentimiento si cambia el modelo
# MAGIC detrás de la función, así que cada fila guarda `modelo` y `generado_ts`, y una marca derivada debería guardar también
# MAGIC la versión, igual que `version_reglas` en gold (CE-06). **Segundo, puede equivocarse de forma sistemática:** ironía,
# MAGIC jerga o reseñas en otros idiomas producen falsos positivos que recaerían en usuarios legítimos; por eso la señal solo
# MAGIC marcaría y derivaría a revisión humana, nunca sancionaría (ADR-02), y se calibraría con los casos que revise Trust &
# MAGIC Safety. **Tercero, envía datos Confidenciales a un servicio de IA:** el texto puede contener datos personales (5.3), por
# MAGIC lo que solo lo ejecuta un custodian, se trabaja con una muestra mínima y el resultado no incluye el texto. **Cuarto,
# MAGIC revela cómo se detecta el fraude:** si se adopta, la señal y su umbral serían Restringidos, como DQ-016. **Quinto,
# MAGIC consume cuota:** la tabla no se regenera en cada corrida del job. Controles propuestos: registro de modelo y fecha por
# MAGIC fila, revisión humana obligatoria, clasificación Restringido, acceso solo `data_stewards`, regla de calidad propia
# MAGIC (por ejemplo, % de filas con sentimiento nulo) y re-evaluación periódica contra casos etiquetados por Trust & Safety.
# MAGIC
# MAGIC ### Definition of Done (EX)
# MAGIC - [x] Tabla enriquecida creada, catalogada y con COMMENT (en `yelp_gov`, por la decisión de gobierno de arriba).
# MAGIC - [ ] Análisis de inconsistencia calificación ↔ sentimiento con conclusiones para T&S (completar arriba).
# MAGIC - [x] Reflexión de gobierno de IA (borrador; revisar y ajustar en grupo).