# Databricks notebook source
# MAGIC %md
# MAGIC # E7.2 · Bronze → Silver: limpieza y conformación
# MAGIC **Cierra con:** M6 Big Data
# MAGIC
# MAGIC Silver materializa el **modelo físico de E3**: mismos nombres, mismos tipos,
# MAGIC mismas constraints. Las transformaciones deben atacar los dolores del caso:
# MAGIC deduplicación de reseñas (Marketing), normalización de check-ins (Ops Comerciales).
# MAGIC
# MAGIC Ejemplo resuelto: `slv_business`. El grupo completa las demás entidades.

# COMMAND ----------

# MAGIC # E7.2 · Bronze → Silver: limpieza y conformación (Grupo 06)
# MAGIC **Cierra con:** M6 Big Data
# MAGIC
# MAGIC Silver materializa el **modelo físico de E3**: mismos nombres, mismos tipos, mismas constraints.
# MAGIC Por eso las tablas **no se recrean** aquí: se llenan con `insertInto` (overwrite), que respeta el DDL,
# MAGIC los COMMENT, las PK/FK y las máscaras de E6. El rango de `stars` lo garantiza DQ-001 (cuarentena), sin CHECK (Anexo C).
# MAGIC
# MAGIC **Cuarentena.** Los registros que incumplen una regla de calidad cuya decisión es "aislar" (sección 6.2)
# MAGIC no entran a silver: van a `workspace.yelp_gov.cuarentena_<entidad>` con la regla y el motivo.
# MAGIC La cuarentena vive en `yelp_gov` y no en `yelp_silver` porque el Diccionario (E4) exige documentar el 100 % de las columnas de silver.
# MAGIC
# MAGIC | Regla | Entidad | Decisión en este notebook |
# MAGIC |---|---|---|
# MAGIC | DQ-004 · identificador de negocio único y no nulo | Negocio | Cuarentena (se conserva la versión más reciente) |
# MAGIC | DQ-015 · identificador de usuario único y no nulo | Usuario | Cuarentena (se conserva la versión más reciente) |
# MAGIC | DQ-002 · reseña sin duplicar | Reseña | Cuarentena de las repeticiones (se conserva la más reciente) |
# MAGIC | DQ-001 · estrellas entre 1 y 5 | Reseña | Cuarentena |
# MAGIC | DQ-005 · reseña con usuario y fecha | Reseña | Cuarentena |
# MAGIC | DQ-003 · reseña con negocio existente | Reseña | Cuarentena |
# MAGIC | DQ-011 · reseña con usuario existente | Reseña | Cuarentena |
# MAGIC | DQ-018 · tip con negocio y usuario existentes | Tip | Cuarentena |
# MAGIC | DQ-019 · check-in con negocio existente y fecha no futura | Check-in | Cuarentena |
# MAGIC
# MAGIC Las demás reglas (DQ-006 a DQ-010, DQ-012 a DQ-014, DQ-016, DQ-017) **marcan, no aíslan**: los registros
# MAGIC quedan en silver y E5 mide su cumplimiento. Si un registro incumple varias reglas de aislamiento, se registra
# MAGIC la primera según el orden de la tabla.
# MAGIC
# MAGIC **Criterio de duplicados lógicos (pedido del curso).** Las reseñas con el mismo negocio y texto idéntico
# MAGIC **no se eliminan**: son una señal de fraude que se mide con DQ-010 y se marca en gold (ADR-02: marcar, no eliminar).
# MAGIC
# MAGIC > **Trazabilidad —** implementa la sección «3. Arquitectura de referencia» (bloques *Transformación y conformado*
# MAGIC > y *Revisión automática de la calidad*) y las decisiones de la sección 6.2 del Documento Formal.

# COMMAND ----------

# DBTITLE 1,Configuración y verificación de prerrequisitos
from pyspark.sql import functions as F, Window
import time
T0 = time.time()   # bitácora del pipeline (E7/04)

BRZ, SLV, GOV = "workspace.yelp_bronze", "workspace.yelp_silver", "workspace.yelp_gov"

for t in ["slv_business", "slv_user", "slv_review", "slv_tip", "slv_check_in"]:
    assert spark.catalog.tableExists(f"{SLV}.{t}"), f"❌ Falta {SLV}.{t}: ejecute primero E3_modelo_fisico/01_modelo_fisico_ddl"
for t in ["brz_business", "brz_user", "brz_review", "brz_tip", "brz_checkin"]:
    assert spark.catalog.tableExists(f"{BRZ}.{t}"), f"❌ Falta {BRZ}.{t}: ejecute primero E7_pipeline_lakehouse/01_bronze_ingesta"

MOTIVOS = {
    "DQ-001": "Calificación fuera del rango 1 a 5",
    "DQ-002": "Reseña duplicada o sin identificador",
    "DQ-003": "Reseña de un negocio que no existe en el catálogo",
    "DQ-004": "Negocio duplicado o sin identificador",
    "DQ-005": "Reseña sin usuario o sin fecha",
    "DQ-011": "Reseña de un usuario que no existe en el catálogo",
    "DQ-015": "Usuario duplicado o sin identificador",
    "DQ-018": "Tip de un negocio o usuario que no existe en el catálogo",
    "DQ-019": "Check-in de un negocio inexistente, con fecha inválida o futura",
}
motivo_expr = F.create_map(*[F.lit(x) for kv in MOTIVOS.items() for x in kv])

def publicar(df, tabla_silver, tabla_cuarentena, columnas):
    """Separa válidos y cuarentena. Los válidos se insertan respetando el DDL de E3;
    la cuarentena se guarda en yelp_gov con la regla incumplida y el motivo."""
    validos = df.filter(F.col("_regla").isNull()).select(*columnas)
    cuarentena = (df.filter(F.col("_regla").isNotNull())
                    .select(*columnas,
                            F.col("_regla").alias("regla_id"),
                            motivo_expr[F.col("_regla")].alias("motivo"),
                            F.current_timestamp().alias("fecha_ejecucion")))
    validos.write.mode("overwrite").insertInto(f"{SLV}.{tabla_silver}")
    (cuarentena.write.mode("overwrite").option("overwriteSchema", "true")
               .saveAsTable(f"{GOV}.{tabla_cuarentena}"))
    n_ok, n_q = spark.table(f"{SLV}.{tabla_silver}").count(), spark.table(f"{GOV}.{tabla_cuarentena}").count()
    print(f"✔ {tabla_silver}: {n_ok:,} filas · cuarentena: {n_q:,}")

def tc(col, tipo):
    """try_cast: con ANSI activo (Databricks serverless), un valor mal formado devuelve NULL en vez de detener
    el pipeline; el registro luego cae en cuarentena por la regla que corresponda."""
    return F.expr(f"try_cast({col} AS {tipo})")

def mas_reciente(df, clave):
    """Número de orden por clave: 1 = versión más reciente según la ingesta a bronze."""
    w = Window.partitionBy(clave).orderBy(F.col("_fecha_ingesta").desc())
    return df.withColumn("_rn", F.row_number().over(w))

# COMMAND ----------

# DBTITLE 1,slv_business
COLS_BUSINESS = ["business_id", "name", "address", "stars", "review_count", "is_open", "categories"]

business = mas_reciente(
    spark.table(f"{BRZ}.brz_business").select(
        F.trim("business_id").alias("business_id"),
        F.trim("name").alias("name"),
        F.trim("address").alias("address"),
        tc("stars", "DECIMAL(2,1)").alias("stars"),
        tc("review_count", "INT").alias("review_count"),
        tc("is_open", "INT").alias("is_open"),
        F.trim(F.col("categories").cast("string")).alias("categories"),
        "_fecha_ingesta"),
    "business_id")

business = business.withColumn("_regla",
    F.when(F.col("business_id").isNull() | (F.col("business_id") == ""), "DQ-004")
     .when(F.col("_rn") > 1, "DQ-004"))

publicar(business, "slv_business", "cuarentena_business", COLS_BUSINESS)

# COMMAND ----------

# DBTITLE 1,slv_user
COLS_USER = ["user_id", "name", "review_count", "yelping_since", "useful", "average_stars", "compliment_writer"]

user = mas_reciente(
    spark.table(f"{BRZ}.brz_user").select(
        F.trim("user_id").alias("user_id"),
        F.trim("name").alias("name"),
        tc("review_count", "INT").alias("review_count"),
        tc("try_cast(yelping_since AS TIMESTAMP)", "DATE").alias("yelping_since"),
        tc("useful", "INT").alias("useful"),
        tc("average_stars", "DECIMAL(3,2)").alias("average_stars"),
        tc("compliment_writer", "INT").alias("compliment_writer"),
        "_fecha_ingesta"),
    "user_id")

user = user.withColumn("_regla",
    F.when(F.col("user_id").isNull() | (F.col("user_id") == ""), "DQ-015")
     .when(F.col("_rn") > 1, "DQ-015"))

publicar(user, "slv_user", "cuarentena_user", COLS_USER)

# COMMAND ----------

# DBTITLE 1,slv_review — deduplicación explícita + integridad referencial
COLS_REVIEW = ["review_id", "user_id", "business_id", "stars", "date", "text", "useful"]

negocios = spark.table(f"{SLV}.slv_business").select("business_id", F.lit(True).alias("_negocio_ok"))
usuarios = spark.table(f"{SLV}.slv_user").select("user_id", F.lit(True).alias("_usuario_ok"))

review = (mas_reciente(
            spark.table(f"{BRZ}.brz_review").select(
                F.trim("review_id").alias("review_id"),
                F.trim("user_id").alias("user_id"),
                F.trim("business_id").alias("business_id"),
                tc("try_cast(stars AS DOUBLE)", "INT").alias("stars"),   # Auto Loader infiere texto: "4.0" → 4
                tc("try_cast(`date` AS TIMESTAMP)", "DATE").alias("date"),
                F.col("text").cast("string").alias("text"),
                tc("useful", "INT").alias("useful"),
                "_fecha_ingesta"),
            "review_id")
          .join(F.broadcast(negocios), "business_id", "left")
          .join(usuarios, "user_id", "left"))

review = review.withColumn("_regla",
    F.when(F.col("review_id").isNull() | (F.col("_rn") > 1), "DQ-002")
     .when(F.col("stars").isNull() | ~F.col("stars").between(1, 5), "DQ-001")
     .when(F.col("user_id").isNull() | F.col("date").isNull(), "DQ-005")
     .when(F.col("_negocio_ok").isNull(), "DQ-003")
     .when(F.col("_usuario_ok").isNull(), "DQ-011"))

publicar(review, "slv_review", "cuarentena_review", COLS_REVIEW)

# COMMAND ----------

# DBTITLE 1,slv_tip
COLS_TIP = ["user_id", "business_id", "text", "date", "compliment_count"]

tip = (spark.table(f"{BRZ}.brz_tip").select(
            F.trim("user_id").alias("user_id"),
            F.trim("business_id").alias("business_id"),
            F.col("text").cast("string").alias("text"),
            tc("try_cast(`date` AS TIMESTAMP)", "DATE").alias("date"),
            tc("compliment_count", "INT").alias("compliment_count"))
        .join(F.broadcast(negocios), "business_id", "left")
        .join(usuarios, "user_id", "left"))

tip = tip.withColumn("_regla",
    F.when(F.col("_negocio_ok").isNull() | F.col("_usuario_ok").isNull(), "DQ-018"))

publicar(tip, "slv_tip", "cuarentena_tip", COLS_TIP)

# COMMAND ----------

# DBTITLE 1,slv_check_in — explosión a grano evento (1 fila = 1 visita)
# El origen trae todas las marcas de un negocio en un solo texto: "2016-04-26 19:49:16, 2016-08-30 18:36:57, ..."
# Se separa en una fila por visita y se conserva fecha y hora (TIMESTAMP), base del término "Check-in Válido".
COLS_CHECKIN = ["business_id", "date"]

checkin_eventos = (spark.table(f"{BRZ}.brz_checkin")
    .select(F.trim("business_id").alias("business_id"),
            F.explode(F.split(F.col("date"), r",\s*")).alias("_marca"))
    .withColumn("date", F.expr("try_cast(trim(_marca) AS TIMESTAMP)")))

total_marcas = checkin_eventos.count()
checkin_eventos = checkin_eventos.dropDuplicates(["business_id", "date"])   # repetición exacta: no es un check-in válido adicional
print(f"Marcas de check-in: {total_marcas:,} · duplicados exactos descartados: {total_marcas - checkin_eventos.count():,}")

checkin = (checkin_eventos.join(F.broadcast(negocios), "business_id", "left")
    .withColumn("_regla",
        F.when(F.col("_negocio_ok").isNull() | F.col("date").isNull()
               | (F.col("date") > F.current_timestamp()), "DQ-019")))

publicar(checkin, "slv_check_in", "cuarentena_check_in", COLS_CHECKIN)

# COMMAND ----------

# DBTITLE 1,Diagnóstico de la muestra (antes del gate)
# El script de muestreo limita usuarios a 150 000. Si la ciudad tiene más autores, parte de las reseñas
# queda sin usuario en la muestra y DQ-011 (criticidad alta, 99,5 %) detendrá el pipeline.
diag = spark.sql(f"""
  SELECT regla_id, motivo, count(*) AS registros
  FROM (
    SELECT regla_id, motivo FROM {GOV}.cuarentena_review UNION ALL
    SELECT regla_id, motivo FROM {GOV}.cuarentena_tip    UNION ALL
    SELECT regla_id, motivo FROM {GOV}.cuarentena_check_in UNION ALL
    SELECT regla_id, motivo FROM {GOV}.cuarentena_business UNION ALL
    SELECT regla_id, motivo FROM {GOV}.cuarentena_user)
  GROUP BY regla_id, motivo ORDER BY registros DESC
""")
display(diag)

n_rev = spark.table(f"{SLV}.slv_review").count() + spark.table(f"{GOV}.cuarentena_review").count()
n_011 = spark.table(f"{GOV}.cuarentena_review").filter("regla_id = 'DQ-011'").count()
pct_011 = 100.0 * n_011 / n_rev if n_rev else 0.0
print(f"Reseñas sin usuario en la muestra: {n_011:,} ({pct_011:.2f} %)")
if pct_011 > 0.5:
    print("⚠️ Supera el 0,5 % de DQ-011. Si la muestra no puede ampliarse, la falla se gestiona como excepción "
          "(sección 5.5): ver la celda de excepciones en E5 y el Acta 05 del Anexo D.")

# COMMAND ----------

# DBTITLE 1,Bitácora del pipeline (E7/04 · sección 8.2) — se registra antes del gate
# Se registra antes del gate para que también queden en la bitácora las corridas que el gate detiene.
spark.sql(f"""CREATE TABLE IF NOT EXISTS {GOV}.pipeline_bitacora (
  ejecucion_ts TIMESTAMP, capa STRING, tabla STRING, filas BIGINT, duracion_seg DOUBLE
) COMMENT 'Observabilidad del pipeline: volumetría y duración por corrida'""")
duracion = round(time.time() - T0, 1)
filas = [("silver", t, spark.table(f"{SLV}.{t}").count(), duracion)
         for t in ["slv_business", "slv_user", "slv_review", "slv_tip", "slv_check_in"]]
filas += [("cuarentena", f"cuarentena_{e}", spark.table(f"{GOV}.cuarentena_{e}").count(), duracion)
          for e in ["business", "user", "review", "tip", "check_in"] if spark.catalog.tableExists(f"{GOV}.cuarentena_{e}")]
(spark.createDataFrame(filas, "capa STRING, tabla STRING, filas BIGINT, duracion_seg DOUBLE")
      .select(F.current_timestamp().alias("ejecucion_ts"), "capa", "tabla", "filas", "duracion_seg")
      .write.insertInto(f"{GOV}.pipeline_bitacora"))
print(f"✔ Bitácora: {len(filas)} registros · carga a silver en {duracion} s")

# COMMAND ----------

# DBTITLE 1,Gate de calidad — se detiene si una regla de criticidad alta falla sin excepción vigente
# Una falla con excepción aprobada y vigente (sección 5.5 · Anexo D) no detiene el pipeline, pero sigue
# registrada como falla en dq_resultados: la excepción no oculta el problema, documenta quién asumió el riesgo.
resultado = dbutils.notebook.run("../E5_calidad_datos/01_reglas_calidad_exploratorio", 1200)
print(f"Motor de calidad: {resultado}")

fallas = spark.sql("""
  SELECT res.regla_id, res.pct_cumplimiento, r.umbral_pct,
         ex.aprobado_por, ex.fecha_caducidad, ex.acta,
         (ex.regla_id IS NOT NULL) AS con_excepcion_vigente
  FROM workspace.yelp_gov.dq_resultados res
  JOIN workspace.yelp_gov.dq_reglas r USING (regla_id)
  LEFT JOIN workspace.yelp_gov.dq_excepciones ex
         ON ex.regla_id = res.regla_id AND current_date() BETWEEN ex.fecha_aprobacion AND ex.fecha_caducidad
  WHERE r.criticidad = 'alta' AND NOT res.paso
    AND res.ejecucion_ts = (SELECT max(ejecucion_ts) FROM workspace.yelp_gov.dq_resultados)
""")
display(fallas)

aceptadas = fallas.filter("con_excepcion_vigente").count()
bloqueantes = fallas.filter("NOT con_excepcion_vigente").count()
if aceptadas:
    print(f"⚠️ {aceptadas} regla(s) crítica(s) fallan con riesgo aceptado y vigente (ver dq_excepciones / Anexo D)")
assert bloqueantes == 0, f"❌ {bloqueantes} reglas críticas fallaron sin excepción vigente — pipeline detenido (la calidad es un GATE, no un reporte)"
print("✔ Gate de calidad superado")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definition of Done (E7.2)
# MAGIC - [x] Las 5 entidades silver pobladas conforme al DDL de E3 (`insertInto` respeta tipos, COMMENT y constraints).
# MAGIC - [x] Deduplicación de reseñas con criterio documentado + cuarentena operativa en `yelp_gov`.
# MAGIC - [x] Check-ins explotados a grano evento.
# MAGIC - [x] Gate de calidad integrado (ejecuta E5 y se detiene si falla una regla de criticidad alta sin excepción vigente).
# MAGIC - [ ] Capturar el resultado del diagnóstico de la muestra y del gate como evidencia.


# COMMAND ----------

# MAGIC %md
# MAGIC ### Definition of Done (E7.2)
# MAGIC - [ ] Todas las entidades silver pobladas conforme al DDL de E3.
# MAGIC - [ ] Deduplicación de reseñas con criterio documentado + tabla de cuarentena operativa.
# MAGIC - [ ] Check-ins explotados a grano evento.
# MAGIC - [ ] Gate de calidad integrado y funcionando.


# COMMAND ----------

# MAGIC %md
# MAGIC ### Definition of Done (E7.2)
# MAGIC - [ ] Todas las entidades silver pobladas conforme al DDL de E3.
# MAGIC - [ ] Deduplicación de reseñas con criterio documentado + tabla de cuarentena operativa.
# MAGIC - [ ] Check-ins explotados a grano evento.
# MAGIC - [ ] Gate de calidad integrado y funcionando.