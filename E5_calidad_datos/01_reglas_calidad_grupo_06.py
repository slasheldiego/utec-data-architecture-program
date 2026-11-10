# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # E5 · Reglas de Calidad de Datos + Motor de Evaluación
# MAGIC **Se define en:** M2/M3 (reglas en lenguaje de negocio) · **Se implementa tras:** M6 (cuando ya hay datos en silver)
# MAGIC
# MAGIC Enfoque *rules-as-data*: las reglas viven en la tabla `yelp_gov.dq_reglas` (no
# MAGIC hardcodeadas), un motor genérico las ejecuta y persiste resultados en
# MAGIC `yelp_gov.dq_resultados`. Ese historial alimenta el dashboard de E8 y el job de E7.
# MAGIC
# MAGIC **Mínimo 12 reglas** cubriendo las 6 dimensiones de calidad y los dolores del caso
# MAGIC (duplicidad de reseñas, check-ins inconsistentes, integridad referencial).
# MAGIC
# MAGIC > **Trazabilidad —** este entregable **implementa** la sección «Reglas de calidad de datos (definición en lenguaje de negocio)» del
# MAGIC > **Documento Formal de Arquitectura de Datos**. Aquí se implementan y ejecutan esas reglas: el documento define QUÉ debe cumplirse y por qué; la plataforma demuestra que se cumple.

# COMMAND ----------

# DBTITLE 1,Vistas de entrada (silver + cuarentena) y vistas auxiliares de agregación
SLV, GOV = "workspace.yelp_silver", "workspace.yelp_gov"

ENTRADAS = {
    "review":   "review_id, user_id, business_id, stars, `date`, text, useful",
    "business": "business_id, name, address, stars, review_count, is_open, categories",
    "user":     "user_id, name, review_count, yelping_since, useful, average_stars, compliment_writer",
    "tip":      "user_id, business_id, text, `date`, compliment_count",
    "check_in": "business_id, `date`",
}
for ent, cols in ENTRADAS.items():
    spark.sql(f"""
      CREATE OR REPLACE VIEW {GOV}.vw_dq_entrada_{ent}
      COMMENT 'E5 · Entrada evaluada por las reglas de aislamiento: silver + cuarentena de {ent}'
      AS SELECT {cols} FROM {SLV}.slv_{ent}
         UNION ALL
         SELECT {cols} FROM {GOV}.cuarentena_{ent}""")

# DQ-006: rating publicado vs promedio de reseñas válidas, redondeado a medias estrellas
spark.sql(f"""
  CREATE OR REPLACE VIEW {GOV}.vw_dq_rating_negocio
  COMMENT 'E5 · DQ-006: rating publicado frente al promedio de reseñas válidas'
  AS SELECT b.business_id,
            b.stars                              AS rating_publicado,
            round(avg(r.stars) * 2) / 2          AS rating_calculado,
            count(r.review_id)                   AS resenas_validas
     FROM {SLV}.slv_business b
     LEFT JOIN {SLV}.slv_review r ON r.business_id = b.business_id
     GROUP BY b.business_id, b.stars""")

# DQ-016: máximo de reseñas publicadas por un usuario en un solo día (dato Restringido)
spark.sql(f"""
  CREATE OR REPLACE VIEW {GOV}.vw_dq_rafagas_usuario
  COMMENT 'E5 · DQ-016: máximo diario de reseñas por usuario — Restringido (Acta 02)'
  AS SELECT user_id, max(n) AS max_resenas_dia
     FROM (SELECT user_id, `date`, count(*) AS n FROM {SLV}.slv_review GROUP BY user_id, `date`)
     GROUP BY user_id""")

# DQ-017: reseñas cargadas frente al total declarado en el perfil
spark.sql(f"""
  CREATE OR REPLACE VIEW {GOV}.vw_dq_resenas_usuario
  COMMENT 'E5 · DQ-017: reseñas cargadas frente a las declaradas en el perfil'
  AS SELECT u.user_id, u.review_count, count(r.review_id) AS resenas_cargadas
     FROM {SLV}.slv_user u
     LEFT JOIN {SLV}.slv_review r ON r.user_id = u.user_id
     GROUP BY u.user_id, u.review_count""")
print("✔ Vistas de evaluación creadas en yelp_gov")

# COMMAND ----------

# DBTITLE 1,Catálogo de reglas — las 19 de la sección 6.2
from pyspark.sql import Row

def regla(id, tabla, col, dim, desc, expr, crit, umbral):
    return Row(regla_id=id, tabla_objetivo=tabla, columna=col, dimension=dim, descripcion=desc,
               expresion_sql=expr, criticidad=crit, umbral_pct=float(umbral))

E = lambda ent: f"{GOV}.vw_dq_entrada_{ent}"      # entrada (silver + cuarentena)
S = lambda t: f"{SLV}.{t}"                          # silver
REGEX_RELLENO = r"^(.)\\1+$"                        # mismo carácter repetido: 'aaaa', '1111'
REGEX_SIN_LETRAS = r"^[0-9\\s\\W_]+$"               # solo números, espacios o símbolos: '11122'

reglas = [
  regla("DQ-001", E("review"), "stars", "validez",
        "Toda reseña debe tener una calificación entre 1 y 5 estrellas",
        "t.stars BETWEEN 1 AND 5", "alta", 100),
  regla("DQ-002", E("review"), "review_id", "unicidad",
        "No pueden existir dos reseñas con el mismo identificador",
        "t.review_id IS NOT NULL", "alta", 100),
  regla("DQ-003", E("review"), "business_id", "integridad",
        "Toda reseña debe corresponder a un negocio que exista en el catálogo",
        f"t.business_id IN (SELECT business_id FROM {S('slv_business')})", "alta", 99.5),
  regla("DQ-004", E("business"), "business_id", "unicidad",
        "Todo negocio debe tener un identificador único y no nulo",
        "t.business_id IS NOT NULL", "alta", 100),
  regla("DQ-005", E("review"), "user_id, date", "completitud",
        "Toda reseña debe tener usuario y fecha de publicación",
        "t.user_id IS NOT NULL AND t.`date` IS NOT NULL", "alta", 99.5),
  regla("DQ-006", f"{GOV}.vw_dq_rating_negocio", "rating_publicado", "consistencia",
        "El rating publicado del negocio no debe diferir en más de media estrella del promedio de sus reseñas cargadas, redondeado a medias estrellas",
        "t.resenas_validas = 0 OR abs(t.rating_publicado - t.rating_calculado) <= 0.5", "media", 95),   # v0.5: 99 → 95 (6.3)
  regla("DQ-007", S("slv_review"), "date", "actualidad",
        "La fecha de una reseña no puede ser posterior a la fecha de ejecución de la carga",
        "t.`date` <= current_date()", "media", 100),
  regla("DQ-008", S("slv_review"), "text", "completitud",
        "El texto de una reseña no puede estar vacío ni contener solo espacios",
        "t.text IS NOT NULL AND length(trim(t.text)) > 0", "media", 99),
  regla("DQ-009", S("slv_review"), "date", "consistencia",
        "La fecha de una reseña no puede ser anterior a la fecha en que su autor se registró en la plataforma",
        f"NOT EXISTS (SELECT 1 FROM {S('slv_user')} u WHERE u.user_id = t.user_id AND t.`date` < u.yelping_since)",
        "media", 99.9),   # v0.5: alta/100 → media/99,9: es señal de fraude, marca y no detiene (ADR-03)
  regla("DQ-010", S("slv_review"), "business_id, text", "unicidad",
        "No deberían existir reseñas con texto exactamente igual en registros distintos del mismo negocio",
        "t.text IS NOT NULL", "media", 99),
  regla("DQ-011", E("review"), "user_id", "integridad",
        "Toda reseña debe corresponder a un usuario que exista en el catálogo de usuarios",
        f"t.user_id IS NULL OR t.user_id IN (SELECT user_id FROM {S('slv_user')})",   # sin usuario: lo mide DQ-005
        "alta", 99.5),
  regla("DQ-012", S("slv_user"), "review_count, useful, average_stars", "validez",
        "Los conteos de actividad de un usuario no pueden ser negativos, y su promedio de estrellas debe estar entre 1 y 5",
        "coalesce(t.review_count, 0) >= 0 AND coalesce(t.useful, 0) >= 0 "
        "AND (t.average_stars IS NULL OR t.average_stars BETWEEN 1 AND 5)", "alta", 100),
  regla("DQ-013", S("slv_review"), "text", "validez",
        "El texto de una reseña no puede estar compuesto solo por caracteres o secuencias repetitivas de relleno",
        f"t.text IS NULL OR NOT (trim(t.text) RLIKE '{REGEX_RELLENO}' OR trim(t.text) RLIKE '{REGEX_SIN_LETRAS}')",
        "media", 99),
  regla("DQ-014", S("slv_user"), "name", "validez",
        "El nombre de un usuario debe tener al menos dos letras y no estar compuesto solo por caracteres repetitivos o de relleno",
        f"length(regexp_replace(coalesce(t.name, ''), '[^\\\\p{{L}}]', '')) >= 2 AND NOT lower(t.name) RLIKE '{REGEX_RELLENO}'",
        "media", 95),   # v0.5: 99 → 95: el 3,8 % de los nombres son iniciales legítimas (6.3)
  regla("DQ-015", E("user"), "user_id", "unicidad",
        "Todo usuario debe tener un identificador único y no nulo",
        "t.user_id IS NOT NULL", "alta", 100),
  regla("DQ-016", f"{GOV}.vw_dq_rafagas_usuario", "max_resenas_dia", "consistencia",
        "Un mismo usuario no debería publicar más reseñas en un día que el máximo definido por Trust & Safety",
        "t.max_resenas_dia <= 10", "media", 99.5),
  regla("DQ-017", f"{GOV}.vw_dq_resenas_usuario", "resenas_cargadas", "consistencia",
        "Las reseñas cargadas de un usuario no pueden superar el total declarado en su perfil",
        "t.resenas_cargadas <= coalesce(t.review_count, 0)", "media", 99),
  regla("DQ-018", E("tip"), "business_id, user_id", "integridad",
        "Todo tip debe corresponder a un negocio y a un usuario que existan en sus catálogos",
        f"t.business_id IN (SELECT business_id FROM {S('slv_business')}) "
        f"AND t.user_id IN (SELECT user_id FROM {S('slv_user')})", "media", 99.5),
  regla("DQ-019", E("check_in"), "business_id, date", "integridad",   # v0.5: validez → integridad referencial
        "Todo check-in debe pertenecer a un negocio existente y tener una fecha que no sea posterior a la carga",
        f"t.business_id IN (SELECT business_id FROM {S('slv_business')}) "
        "AND t.`date` IS NOT NULL AND t.`date` <= current_timestamp()", "alta", 100),
]

assert len({r.regla_id for r in reglas}) == 19
assert {r.dimension for r in reglas} == {"completitud", "unicidad", "validez", "consistencia", "actualidad", "integridad"}

(spark.createDataFrame(reglas)
      .select("regla_id", "tabla_objetivo", "columna", "dimension", "descripcion",
              "expresion_sql", "criticidad", "umbral_pct")
      .write.mode("overwrite").option("overwriteSchema", "true")
      .saveAsTable("workspace.yelp_gov.dq_reglas"))
display(spark.table("workspace.yelp_gov.dq_reglas"))

# Vista para los lectores que no son Trust & Safety (sección 5.3): el catálogo completo, salvo el
# parámetro Restringido de DQ-016 (Acta 02 y 04). E6 da acceso a esta vista y no a dq_reglas.
spark.sql(f"""
  CREATE OR REPLACE VIEW {GOV}.vw_dq_reglas_publica
  COMMENT 'E5 · Catálogo de reglas sin el parámetro Restringido de DQ-016 (sección 5.3, Acta 02)'
  AS SELECT regla_id, tabla_objetivo, columna, dimension, descripcion,
            CASE WHEN regla_id = 'DQ-016' THEN '[Restringido]' ELSE expresion_sql END AS expresion_sql,
            criticidad, umbral_pct
     FROM {GOV}.dq_reglas""")
print("✔ Vista vw_dq_reglas_publica creada (catálogo sin el parámetro de DQ-016)")

# COMMAND ----------

# DBTITLE 1,Motor genérico de evaluación (extendido: alias t y unicidad multicolumna)
# Única extensión respecto del esqueleto del curso: la tabla se evalúa con el alias `t`, lo que permite
# reglas con subconsultas correlacionadas (DQ-009). La unicidad compara filas totales vs combinaciones
# distintas de la(s) columna(s) indicadas (DQ-010 usa business_id + text).
from pyspark.sql import functions as F

def evaluar_reglas():
    resultados = []
    for r in spark.table("workspace.yelp_gov.dq_reglas").collect():
        total = spark.table(r.tabla_objetivo).count()
        if r.dimension == "unicidad" and r.columna != "*":
            ok = spark.sql(f"SELECT count(DISTINCT {r.columna}) c FROM {r.tabla_objetivo} t").first().c
        else:
            ok = spark.sql(f"SELECT count(*) c FROM {r.tabla_objetivo} t WHERE {r.expresion_sql}").first().c
        pct = round(100.0 * ok / total, 2) if total else 100.0
        resultados.append((r.regla_id, r.tabla_objetivo, total, ok, pct, pct >= r.umbral_pct))
    df = spark.createDataFrame(resultados,
        "regla_id STRING, tabla_objetivo STRING, filas_totales BIGINT, filas_ok BIGINT, pct_cumplimiento DOUBLE, paso BOOLEAN"
    ).withColumn("ejecucion_ts", F.current_timestamp())
    df.select("ejecucion_ts","regla_id","tabla_objetivo","filas_totales","filas_ok","pct_cumplimiento","paso") \
      .write.mode("append").saveAsTable("workspace.yelp_gov.dq_resultados")
    return df

resultado = evaluar_reglas()
display(resultado.orderBy("regla_id"))

# COMMAND ----------

# DBTITLE 1,Scorecard de calidad (insumo directo del dashboard E8)
# MAGIC %sql
# MAGIC SELECT r.dimension,
# MAGIC        count(*)                                    AS reglas,
# MAGIC        sum(CASE WHEN res.paso THEN 1 ELSE 0 END)   AS reglas_ok,
# MAGIC        round(avg(res.pct_cumplimiento), 2)         AS cumplimiento_promedio
# MAGIC FROM workspace.yelp_gov.dq_resultados res
# MAGIC JOIN workspace.yelp_gov.dq_reglas r USING (regla_id)
# MAGIC WHERE res.ejecucion_ts = (SELECT max(ejecucion_ts) FROM workspace.yelp_gov.dq_resultados)
# MAGIC GROUP BY r.dimension ORDER BY cumplimiento_promedio;

# COMMAND ----------

# DBTITLE 1,Excepciones vigentes (sección 5.5 · registro del Anexo D)
# Una excepción no cambia el umbral ni el resultado de la regla: solo registra que el Data Owner aceptó el
# riesgo, por qué y hasta cuándo. El gate de E7/02 la consulta. Sin fecha de caducidad no es una excepción.
spark.sql("""
  CREATE TABLE IF NOT EXISTS workspace.yelp_gov.dq_excepciones (
    regla_id          STRING COMMENT 'Regla DQ-nnn exceptuada',
    motivo            STRING COMMENT 'Causa conocida que impide cumplir el umbral',
    tratamiento       STRING COMMENT 'Qué se hace con los registros mientras dure la excepción',
    aprobado_por      STRING COMMENT 'Data Owner que acepta el riesgo',
    fecha_aprobacion  DATE,
    fecha_caducidad   DATE   COMMENT 'Máximo 30 días, renovable una vez (sección 5.5)',
    acta              STRING COMMENT 'Acta del Anexo D que la registra'
  ) COMMENT 'E5 · Registro de excepciones de calidad aprobadas (sección 5.5 del Documento Formal)'
""")
spark.sql("""
  MERGE INTO workspace.yelp_gov.dq_excepciones t
  USING (SELECT 'DQ-011' AS regla_id,
                'La muestra demo del curso incluye solo parte de los usuarios de Philadelphia: las reseñas de autores no muestreados quedan sin usuario. Es un efecto del muestreo, no un defecto del origen.' AS motivo,
                'Las reseñas sin autor permanecen en cuarentena y no llegan a gold; la regla sigue evaluándose y registrándose como falla.' AS tratamiento,
                'Andrew Collins (Data Owner, Producto) a propuesta de Rafael Nakamura (Steward)' AS aprobado_por,
                DATE'2026-10-13' AS fecha_aprobacion,
                DATE'2026-11-12' AS fecha_caducidad,
                'Acta 05' AS acta) s
  ON t.regla_id = s.regla_id AND t.acta = s.acta
  WHEN MATCHED THEN UPDATE SET *   -- si el acta ya está registrada, se alinea con las fechas del código
  WHEN NOT MATCHED THEN INSERT *
""")
# Acta 05 también cubre DQ-018 (tips): el script de muestreo no incluye a los autores de tips.
# DQ-018 es de criticidad media y no detiene el pipeline, pero la excepción se registra igual (sección 5.5).
spark.sql("""
  MERGE INTO workspace.yelp_gov.dq_excepciones t
  USING (SELECT 'DQ-018' AS regla_id,
                'El script de muestreo del curso no incluye a los autores de tips: el 26,0 % de los tips queda sin su usuario. Es un efecto del muestreo, no un defecto del origen.' AS motivo,
                'Los tips huérfanos permanecen en cuarentena y no llegan a gold; la regla sigue evaluándose y registrándose como falla.' AS tratamiento,
                'Andrew Collins (Data Owner, Producto) a propuesta de Rafael Nakamura (Steward)' AS aprobado_por,
                DATE'2026-10-13' AS fecha_aprobacion,
                DATE'2026-11-12' AS fecha_caducidad,
                'Acta 05' AS acta) s
  ON t.regla_id = s.regla_id AND t.acta = s.acta
  WHEN MATCHED THEN UPDATE SET *   -- si el acta ya está registrada, se alinea con las fechas del código
  WHEN NOT MATCHED THEN INSERT *
""")

# Acta 06: renovación única de la excepción de DQ-011 (sección 5.5). El Acta 05 se conserva como historial.
spark.sql("""
  MERGE INTO workspace.yelp_gov.dq_excepciones t
  USING (SELECT 'DQ-011' AS regla_id,
                'Renovación única de la excepción del Acta 05: la muestra sigue limitada a 150.000 autores y el 6,7 % de las reseñas queda sin usuario.' AS motivo,
                'Las reseñas sin autor permanecen en cuarentena y no llegan a gold; la regla sigue evaluándose y registrándose como falla.' AS tratamiento,
                'Andrew Collins (Data Owner, Producto) a propuesta de Rafael Nakamura (Steward)' AS aprobado_por,
                DATE'2026-11-13' AS fecha_aprobacion,
                DATE'2026-12-12' AS fecha_caducidad,
                'Acta 06' AS acta) s
  ON t.regla_id = s.regla_id AND t.acta = s.acta
  WHEN MATCHED THEN UPDATE SET *
  WHEN NOT MATCHED THEN INSERT *
""")

# Aviso de caducidad: una excepción vencida deja de proteger al gate de E7/02
for e in spark.sql("""
    SELECT regla_id, max(fecha_caducidad) AS fecha_caducidad, datediff(max(fecha_caducidad), current_date()) AS dias
    FROM workspace.yelp_gov.dq_excepciones GROUP BY regla_id ORDER BY regla_id""").collect():
    if e.dias < 0:
        print(f"❌ La excepción de {e.regla_id} venció el {e.fecha_caducidad}: el gate se detendrá si la regla es de criticidad alta. Renovarla (una sola vez, nueva acta) o ampliar la muestra.")
    elif e.dias <= 7:
        print(f"⚠️ La excepción de {e.regla_id} vence en {e.dias} día(s) ({e.fecha_caducidad}): decidir renovación o cierre.")
    else:
        print(f"✔ Excepción de {e.regla_id} vigente hasta {e.fecha_caducidad} ({e.dias} días)")
display(spark.table("workspace.yelp_gov.dq_excepciones"))

# COMMAND ----------

dbutils.notebook.exit("ok: 19 reglas evaluadas")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definition of Done (E5)
# MAGIC - [ ] ≥12 reglas en las 6 dimensiones, con criticidad y umbral justificados.
# MAGIC - [ ] Motor ejecutado con historial en `dq_resultados` (≥2 corridas en fechas distintas).
# MAGIC - [ ] Para cada regla FALLIDA: decisión documentada (cuarentena, corrección en pipeline, o aceptación del riesgo con firma del "Data Owner").
# MAGIC - [ ] Bonus: agregar constraint `CHECK` o expectativa en el pipeline E7 para la regla más crítica (calidad *preventiva* vs *detectiva*).

# COMMAND ----------

# MAGIC %md
# MAGIC ### Decisión ante cada regla fallida (última corrida)
# MAGIC | Regla | Criticidad | Umbral | % obtenido | Decisión | Firma |
# MAGIC |---|---|---|---|---|---|
# MAGIC | DQ-011 · Reseña con usuario existente | Alta | 99,5 % | 93,3 % | **Aceptación del riesgo con caducidad.** Falla por el muestreo (solo 150.000 autores), no por el origen. Las reseñas sin autor quedan en cuarentena y no llegan a gold. Excepción original hasta el 12/11/2026 (Acta 05), renovada una única vez hasta el 12/12/2026 (Acta 06). | Andrew Collins (Data Owner), a propuesta de Rafael Nakamura (Steward) |
# MAGIC | DQ-018 · Tip con negocio y usuario existentes | Media | 99,5 % | 74,0 % | **Aceptación del riesgo con caducidad.** El script de muestreo no incluye a los autores de tips. Los tips huérfanos quedan en cuarentena y no llegan a gold. Al ser de criticidad media, no detiene el pipeline. Excepción hasta el 12/11/2026 (Acta 05). | Andrew Collins (Data Owner), a propuesta de Rafael Nakamura (Steward) |
# MAGIC
# MAGIC Las otras 17 reglas cumplen su umbral en la última corrida (scorecard: 4 de 4 en validez, consistencia y unicidad; 2 de 2 en completitud; 1 de 1 en actualidad; 2 de 4 en integridad).
# MAGIC
# MAGIC **Solución de fondo para ambas:** muestrear usuarios junto con toda su actividad (recomendación del resumen ejecutivo). Mientras tanto, la regla sigue evaluándose y figurando como fallida en `dq_resultados`: la excepción no oculta la falla.
# MAGIC ### Definition of Done (E5)
# MAGIC - [x] 19 reglas en las 6 dimensiones, con criticidad y umbral justificados (sección 6.3, v0.5).
# MAGIC - [ ] Motor ejecutado con historial en `dq_resultados` (≥2 corridas en fechas distintas; CE-04 pide ≥3).
# MAGIC - [ ] Para cada regla FALLIDA: decisión documentada en la tabla de arriba.
# MAGIC - [x] Parámetro Restringido de DQ-016 fuera del alcance de los demás lectores: `vw_dq_reglas_publica` (5.3).
# MAGIC - [ ] Bonus (no adoptado): el Anexo C deja constancia de que el DDL de E3 no declara `CHECK` sobre `stars`;
# MAGIC   el rango se garantiza con DQ-001 + cuarentena. Si el grupo decide agregarlo, ver la celda comentada de E3.