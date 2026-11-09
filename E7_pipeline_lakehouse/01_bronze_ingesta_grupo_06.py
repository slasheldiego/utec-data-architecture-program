# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # E7.1 · Ingesta a Bronze
# MAGIC **Cierra con:** M6 Big Data (batch) + M7 Integración (incremental/streaming)
# MAGIC
# MAGIC Principio de la capa bronze: **fidelidad al origen** — sin limpiar, sin filtrar,
# MAGIC pero SÍ con metadata de auditoría (archivo origen, timestamp de ingesta).
# MAGIC
# MAGIC Dos modos:
# MAGIC - **Batch full** (mínimo obligatorio): `read_files` sobre el Volume.
# MAGIC - **Incremental con Auto Loader** (obligatorio para al menos UNA entidad, se ve en M7):
# MAGIC   demuestra ingesta idempotente — si suben un archivo nuevo al Volume, solo procesa lo nuevo.
# MAGIC
# MAGIC > **Trazabilidad —** este entregable **implementa** la sección «Arquitectura de referencia (ABB / SBB)» del
# MAGIC > **Documento Formal de Arquitectura de Datos**. Este pipeline es el SBB que implementa los bloques de ingesta y almacenamiento definidos en el documento.

# COMMAND ----------

RAW = "/Volumes/workspace/yelp_bronze/raw"
import time
T0 = time.time()   # bitácora del pipeline (E7/04)
ENTIDADES = ["business", "user", "tip", "checkin"]  # review va con Auto Loader abajo

# COMMAND ----------

# DBTITLE 1: Modo batch — 4 entidades
from pyspark.sql import functions as F

for e in ENTIDADES:
    (spark.read.json(f"{RAW}/{e}.json.gz")
        .withColumn("_archivo_origen", F.col("_metadata.file_path"))
        .withColumn("_fecha_ingesta", F.current_timestamp())
        .write.mode("overwrite")
        .saveAsTable(f"workspace.yelp_bronze.brz_{e}"))
    print(f"✔ brz_{e}: {spark.table(f'workspace.yelp_bronze.brz_{e}').count():,} filas")

# COMMAND ----------

# DBTITLE 1: Modo incremental — Auto Loader para review (la entidad de mayor volumen)
# ✏️ TODO (M7): completar los parámetros marcados y explicar en markdown por qué
#               Auto Loader es idempotente (schema tracking + checkpoint + exactly-once).
CHECKPOINT = "/Volumes/workspace/yelp_bronze/raw/_checkpoints/review"

(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", CHECKPOINT + "/schema")
    .load(f"{RAW}/review*.json.gz")
    .withColumn("_archivo_origen", F.col("_metadata.file_path"))
    .withColumn("_fecha_ingesta", F.current_timestamp())
 .writeStream
    .option("checkpointLocation", CHECKPOINT)
    .trigger(availableNow=True)          # micro-batch y termina: amigable con la cuota serverless
    .toTable("workspace.yelp_bronze.brz_review")
).awaitTermination()

print(f"✔ brz_review: {spark.table('workspace.yelp_bronze.brz_review').count():,} filas")

# COMMAND ----------

# DBTITLE 1,Bitácora del pipeline (E7/04 · sección 8.2)
from pyspark.sql import functions as F
spark.sql("""CREATE TABLE IF NOT EXISTS workspace.yelp_gov.pipeline_bitacora (
  ejecucion_ts TIMESTAMP, capa STRING, tabla STRING, filas BIGINT, duracion_seg DOUBLE
) COMMENT 'Observabilidad del pipeline: volumetría y duración por corrida'""")
duracion = round(time.time() - T0, 1)
filas = [("bronze", f"brz_{e}", spark.table(f"workspace.yelp_bronze.brz_{e}").count(), duracion)
         for e in ["business", "user", "review", "tip", "checkin"]]
(spark.createDataFrame(filas, "capa STRING, tabla STRING, filas BIGINT, duracion_seg DOUBLE")
      .select(F.current_timestamp().alias("ejecucion_ts"), "capa", "tabla", "filas", "duracion_seg")
      .write.insertInto("workspace.yelp_gov.pipeline_bitacora"))
print(f"✔ Bitácora: {len(filas)} registros · ingesta a bronze en {duracion} s")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Experimento obligatorio (evidencia para el informe)
# MAGIC 1. Ejecutar este notebook completo → anotar conteos.
# MAGIC 2. Volver a ejecutar SOLO la celda de Auto Loader **sin subir archivos nuevos** → debe procesar 0 registros.
# MAGIC 3. Subir un archivo `review_2.json.gz` pequeño al Volume y re-ejecutar → solo procesa el nuevo.
# MAGIC > ✏️ TODO — Documentar el experimento con capturas: es la prueba de ingesta incremental.
# MAGIC
# MAGIC ### Definition of Done (E7.1)
# MAGIC - [ ] 5 tablas bronze pobladas con columnas de auditoría.
# MAGIC - [ ] Experimento de idempotencia documentado.
# MAGIC - [ ] Decisión batch vs streaming justificada por entidad (tabla en markdown).