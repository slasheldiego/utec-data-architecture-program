# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# dependencies = [
#   "openpyxl",
# ]
# ///
# MAGIC %md
# MAGIC # E4.1 · Glosario de Negocio
# MAGIC **Cierra con:** M3 MDM y Metadata · **Formato oficial:** plantilla institucional
# MAGIC `Glosario_de_terminos_de_negocio.xlsx` del bloque de Seguridad, Gobernanza y Compliance.
# MAGIC
# MAGIC El glosario resuelve el problema central del caso: distintas áreas llaman igual a cosas
# MAGIC distintas ("check-in", "reseña válida", "negocio activo"). Cada término tiene UNA
# MAGIC definición acordada y UN propietario.
# MAGIC
# MAGIC **Flujo del entregable:**
# MAGIC 1. El grupo negocia y redacta los términos en el **Excel institucional** (interfaz de negocio).
# MAGIC 2. Sube el Excel al Volume `/Volumes/workspace/yelp_gov/artefactos/`.
# MAGIC 3. Este notebook lo carga a `yelp_gov.glosario_negocio` (fuente consultable).
# MAGIC 4. Los validadores comprueban el Definition of Done.
# MAGIC
# MAGIC > La tabla Delta tiene **exactamente las 12 columnas de la plantilla**: el Excel no se
# MAGIC > "traduce", se carga. Ese es el principio de un glosario vivo.
# MAGIC
# MAGIC > **Trazabilidad —** este entregable **implementa** la sección «Anexo · Glosario de términos de negocio» del
# MAGIC > **Documento Formal de Arquitectura de Datos**. El Excel institucional se anexa al documento; esta tabla Delta es la versión consultable desde Genie y el dashboard de gobierno.

# COMMAND ----------

# DBTITLE 1: Volume donde el grupo sube los artefactos de gobierno
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.yelp_gov.artefactos")
RUTA = "/Volumes/workspace/yelp_gov/artefactos/Glosario_de_terminos_de_negocio_YelpConnect.xlsx"
print(f"Suba el Excel completado a: {RUTA}")

# COMMAND ----------

# MAGIC %pip install openpyxl

# COMMAND ----------

# DBTITLE 1: Carga del Excel institucional → tabla Delta
import pandas as pd

COLUMNAS = ["termino", "sinonimos", "definicion_negocio", "definicion_tecnica",
            "contexto_uso", "dominio_datos", "categoria", "ejemplo_uso",
            "relaciones", "propietario", "fuente_normativa", "fecha_actualizacion"]

# La plantilla trae 3 filas de cabecera institucional + 1 de identificación del caso;
# los encabezados de tabla están en la fila 5 y los datos comienzan en la fila 6.
pdf = pd.read_excel(RUTA, sheet_name="Glosario", skiprows=4)
pdf = pdf.dropna(how="all").iloc[:, :12]
pdf.columns = COLUMNAS
pdf["fecha_actualizacion"] = pd.to_datetime(pdf["fecha_actualizacion"]).dt.date

(spark.createDataFrame(pdf)
      .write.mode("overwrite").option("overwriteSchema", "true")
      .saveAsTable("workspace.yelp_gov.glosario_negocio"))

display(spark.table("workspace.yelp_gov.glosario_negocio"))

# COMMAND ----------

# DBTITLE 1: Validadores del Definition of Done (no modificar)
from pyspark.sql import functions as F

g = spark.table("workspace.yelp_gov.glosario_negocio")
n = g.count()
sin_owner = g.filter(F.col("propietario").isNull() | F.col("propietario").contains("✏️")).count()
sin_dominio = g.select("dominio_datos").distinct().count()
con_relaciones = g.filter(F.col("relaciones").isNotNull()).count()
duplicados = n - g.select("termino").distinct().count()

assert n >= 20, f"❌ Solo hay {n} términos; el DoD exige ≥20"
assert sin_owner == 0, "❌ Hay términos sin propietario asignado"
assert duplicados == 0, f"❌ Hay {duplicados} términos repetidos"
assert sin_dominio >= 4, "❌ El glosario debe cubrir al menos 4 dominios de datos"
assert con_relaciones / n >= 0.8, "❌ Menos del 80 % de los términos declara relaciones"
print(f"✔ Glosario válido: {n} términos · {sin_dominio} dominios · "
      f"{round(100*con_relaciones/n)} % con relaciones declaradas")

# COMMAND ----------

# DBTITLE 1: Conflictos semánticos del caso — verificación explícita
%sql
-- Los tres conflictos del enunciado deben estar resueltos con un término propio
SELECT termino, dominio_datos, propietario
FROM workspace.yelp_gov.glosario_negocio
WHERE lower(termino) LIKE '%verificada%'      -- Marketing ↔ Trust & Safety
   OR lower(termino) LIKE '%check-in válido%' -- Operaciones Comerciales
   OR lower(termino) LIKE '%duplicada%'       -- Marketing
ORDER BY termino;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definition of Done (E4.1)
# MAGIC - [ ] ≥20 términos en el formato institucional, sin duplicados y cubriendo ≥4 dominios.
# MAGIC - [ ] Todo término con propietario correspondiente a un área real del caso.
# MAGIC - [ ] Los 3 conflictos semánticos del caso resueltos con término propio.
# MAGIC - [ ] La definición de negocio no nombra tablas ni columnas (eso va en la definición técnica).
# MAGIC - [ ] Acta breve (markdown aquí) de **cómo se negociaron** las definiciones en el grupo.