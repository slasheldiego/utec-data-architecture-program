# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# dependencies = [
#   "openpyxl",
# ]
# ///
# MAGIC %md
# MAGIC # E4.2 · Diccionario de Datos
# MAGIC **Cierra con:** M3 MDM y Metadata, sobre el modelo físico de M4 · **Formato oficial:**
# MAGIC plantilla institucional `Diccionario_de_Datos.xlsx` (25 columnas).
# MAGIC
# MAGIC Lección de arquitectura: el diccionario **no se escribe entero a mano**. Se divide en dos:
# MAGIC
# MAGIC | Parte | Origen | Quién la produce |
# MAGIC |---|---|---|
# MAGIC | Metadata técnica (nombre físico, tipo, nulidad, clave, clasificación) | `information_schema` de Unity Catalog | Se **hereda** automáticamente del DDL de E3 |
# MAGIC | Metadata de negocio (descripción, dominio, uso, retención, normativa, owner) | Acuerdo entre áreas | La **redacta el grupo** en el Excel institucional |
# MAGIC
# MAGIC Si el borrador autogenerado sale con huecos, la deuda está en el DDL de E3, no aquí.
# MAGIC
# MAGIC > **Trazabilidad —** este entregable **implementa** la sección «Anexo · Diccionario de datos» del
# MAGIC > **Documento Formal de Arquitectura de Datos**. El Excel institucional se anexa al documento; esta tabla Delta permite auditar automáticamente la cobertura de documentación contra el catálogo.

# COMMAND ----------

# DBTITLE 1: Paso 1 — Borrador técnico autogenerado desde Unity Catalog
borrador = spark.sql("""
  SELECT
    c.column_name                                                    AS elemento,
    NULL                                                             AS nombre_negocio,
    c.comment                                                        AS descripcion_negocio,
    NULL                                                             AS dominio,
    NULL                                                             AS sistema_fuente,
    concat_ws('.', 'workspace', c.table_schema, c.table_name,
              c.column_name)                                         AS nombre_fisico,
    c.full_data_type                                                 AS tipo_dato,
    CASE WHEN c.is_nullable = 'NO' THEN 'Obligatorio'
         ELSE 'Opcional' END                                         AS obligatorio,
    ct.tag_value                                                     AS sensibilidad
  FROM workspace.information_schema.columns c
  LEFT JOIN workspace.information_schema.column_tags ct
    ON  ct.schema_name = c.table_schema AND ct.table_name = c.table_name
    AND ct.column_name = c.column_name  AND ct.tag_name   = 'clasificacion'
  WHERE c.table_schema IN ('yelp_silver', 'yelp_gold')
  ORDER BY c.table_schema, c.table_name, c.ordinal_position
""")
display(borrador)

# ✏️ Exporte este borrador, péguelo en la plantilla institucional y complete las
#    16 columnas de negocio restantes acordándolas con el "área" dueña de cada dominio.

# COMMAND ----------

# MAGIC %pip install openpyxl

# COMMAND ----------

# DBTITLE 1: Paso 2 — Carga del Excel institucional completado → tabla Delta
import pandas as pd

RUTA = "/Volumes/workspace/yelp_gov/artefactos/Diccionario_de_Datos_YelpConnect.xlsx"

COLUMNAS = ["elemento", "nombre_negocio", "descripcion_negocio", "dominio", "sistema_fuente",
            "nombre_fisico", "tipo_dato", "formato", "longitud", "valores_permitidos",
            "unidad_medida", "valor_defecto", "obligatorio", "clave", "reglas_calidad",
            "reglas_transformacion", "frecuencia_actualizacion", "origen_dato", "uso_procesos",
            "relacion_elementos", "responsable", "politica_retencion", "sensibilidad",
            "fuente_normativa", "fecha_actualizacion"]

pdf = pd.read_excel(RUTA, sheet_name="Diccionario", skiprows=4)
pdf = pdf.dropna(how="all").iloc[:, :25]
pdf.columns = COLUMNAS
pdf = pdf.astype({c: "string" for c in COLUMNAS[:-1]})
pdf["fecha_actualizacion"] = pd.to_datetime(pdf["fecha_actualizacion"]).dt.date

(spark.createDataFrame(pdf)
      .write.mode("overwrite").option("overwriteSchema", "true")
      .saveAsTable("workspace.yelp_gov.diccionario_datos"))

display(spark.table("workspace.yelp_gov.diccionario_datos"))

# COMMAND ----------

# DBTITLE 1: Validador 1 — Cobertura de documentación (columna física ↔ diccionario)
cobertura = spark.sql("""
  WITH fisicas AS (
    SELECT concat_ws('.', 'workspace', table_schema, table_name, column_name) AS nombre_fisico,
           table_schema AS capa
    FROM workspace.information_schema.columns
    WHERE table_schema IN ('yelp_silver', 'yelp_gold')
  )
  SELECT f.capa,
         count(*)                                          AS columnas_fisicas,
         count(d.nombre_fisico)                            AS documentadas,
         round(100.0 * count(d.nombre_fisico) / count(*), 1) AS pct_documentado
  FROM fisicas f
  LEFT JOIN workspace.yelp_gov.diccionario_datos d USING (nombre_fisico)
  GROUP BY f.capa ORDER BY f.capa
""")
display(cobertura)

faltantes = spark.sql("""
  SELECT concat_ws('.', 'workspace', table_schema, table_name, column_name) AS sin_documentar
  FROM workspace.information_schema.columns
  WHERE table_schema IN ('yelp_silver', 'yelp_gold')
    AND concat_ws('.', 'workspace', table_schema, table_name, column_name)
        NOT IN (SELECT nombre_fisico FROM workspace.yelp_gov.diccionario_datos)
""")
print(f"Columnas físicas sin entrada en el diccionario: {faltantes.count()}")
display(faltantes)

# COMMAND ----------

# DBTITLE 1: Validador 2 — Trazabilidad con el glosario (E4.1) y las reglas DQ (E5)
from pyspark.sql import functions as F

d = spark.table("workspace.yelp_gov.diccionario_datos")
sin_dueno = d.filter(F.col("responsable").isNull()).count()
sin_clase = d.filter(F.col("sensibilidad").isNull()).count()
sin_reten = d.filter(F.col("politica_retencion").isNull()).count()

# Toda regla citada en el diccionario debe existir en el catálogo de reglas de E5
citadas = (d.select(F.explode(F.expr(r"regexp_extract_all(reglas_calidad, 'DQ-[0-9]{3}', 0)")).alias("regla_id"))
            .distinct())
existentes = spark.table("workspace.yelp_gov.dq_reglas").select("regla_id").distinct()
huerfanas = citadas.join(existentes, "regla_id", "left_anti")

assert sin_dueno == 0, f"❌ {sin_dueno} elementos sin Data Owner"
assert sin_clase == 0, f"❌ {sin_clase} elementos sin clasificación de sensibilidad"
assert sin_reten == 0, f"❌ {sin_reten} elementos sin política de retención"
print(f"✔ Diccionario íntegro · reglas DQ citadas: {citadas.count()} · "
      f"sin correspondencia en E5: {huerfanas.count()}")
display(huerfanas)

# COMMAND ----------

# DBTITLE 1: Aplicar la clasificación del diccionario como tags reales en Unity Catalog
# El diccionario deja de ser documentación y se convierte en control: la columna
# "Sensibilidad / Clasificación" se propaga como tag, y E6 la usa para los permisos.
for fila in spark.sql("""
    SELECT nombre_fisico, lower(split(sensibilidad, ' ')[0]) AS clase
    FROM workspace.yelp_gov.diccionario_datos
    WHERE sensibilidad IS NOT NULL AND nombre_fisico LIKE 'workspace.yelp_%'
""").collect():
    partes = fila.nombre_fisico.split(".")
    if len(partes) == 4:
        tabla, columna = ".".join(partes[:3]), partes[3]
        try:
            spark.sql(f"ALTER TABLE {tabla} ALTER COLUMN {columna} "
                      f"SET TAGS ('clasificacion' = '{fila.clase}')")
        except Exception as e:
            print(f"⚠️ {fila.nombre_fisico}: {str(e)[:90]}")
print("Tags de clasificación aplicados ✔ (insumo directo de E6)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definition of Done (E4.2)
# MAGIC - [ ] 100 % de las columnas de `yelp_silver` y `yelp_gold` presentes en el diccionario.
# MAGIC - [ ] Todo elemento con responsable, clasificación de sensibilidad y política de retención.
# MAGIC - [ ] Toda regla citada como `DQ-nnn` existe en `yelp_gov.dq_reglas` (sin huérfanas).
# MAGIC - [ ] Los `nombre_fisico` coinciden exactamente con Unity Catalog (validador 1 en 100 %).
# MAGIC - [ ] Tags de clasificación aplicados y visibles en el catálogo.
# MAGIC - [ ] Hoja "Diccionario resumido" coherente con la hoja completa (es la de la sustentación).
# MAGIC - [ ] Párrafo de reflexión: ¿qué se hereda del catálogo y qué debe negociarse con el negocio?