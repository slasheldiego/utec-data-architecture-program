# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 00 · Configuración del Lakehouse del grupo
# MAGIC **Ejecutar UNA sola vez por grupo** (es idempotente: re-ejecutarlo no rompe nada).
# MAGIC
# MAGIC Crea la arquitectura medallón sobre Unity Catalog:
# MAGIC - Schemas `yelp_bronze` / `yelp_silver` / `yelp_gold` (datos) y `yelp_gov` (gobierno, calidad y metadata).
# MAGIC - Volume `raw` para aterrizar los archivos crudos del Yelp Open Dataset.
# MAGIC - Tablas de control que usarán los entregables E4, E5 y E6.
# MAGIC
# MAGIC > ✏️ **TODO (grupo):** completar el widget con el número de grupo antes de ejecutar.

# COMMAND ----------

dbutils.widgets.text("grupo", "06", "Número de grupo")
GRUPO = dbutils.widgets.get("grupo")
CATALOGO = "workspace"  # catálogo por defecto de Free Edition
print(f"Configurando lakehouse del Grupo {GRUPO} en el catálogo '{CATALOGO}'")

# COMMAND ----------

# DBTITLE 1: Schemas de la arquitectura medallón
for schema, comentario in [
    ("yelp_bronze", "Capa Bronze: datos crudos tal como llegan de YelpConnect"),
    ("yelp_silver", "Capa Silver: datos limpios, conformados y con calidad validada"),
    ("yelp_gold",   "Capa Gold: productos de datos para consumo analítico y BI"),
    ("yelp_gov",    "Gobierno: glosario, diccionario, reglas y resultados de calidad"),
]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{schema} COMMENT '{comentario} — Grupo {GRUPO}'")
print("Schemas creados ✔")

# COMMAND ----------

# DBTITLE 1: Volume para datos crudos
spark.sql(f"""
CREATE VOLUME IF NOT EXISTS {CATALOGO}.yelp_bronze.raw
COMMENT 'Zona de aterrizaje de archivos crudos (business, review, user, tip, checkin)'
""")
print("Volume listo → /Volumes/workspace/yelp_bronze/raw/")

# COMMAND ----------

# DBTITLE 1: Tablas de control de gobierno (se llenan en E4, E5 y E6)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOGO}.yelp_gov.glosario_negocio (
  termino               STRING COMMENT 'Nombre del Término — nombre oficial aprobado del concepto',
  sinonimos             STRING COMMENT 'Sinónimos / Alias — otras formas en que el término se conoce',
  definicion_negocio    STRING COMMENT 'Definición de Negocio — explicación comprensible sin tecnicismos',
  definicion_tecnica    STRING COMMENT 'Definición Técnica (opcional) — cómo se representa en la plataforma',
  contexto_uso          STRING COMMENT 'Contexto / Uso — áreas, procesos o dominios donde se utiliza',
  dominio_datos         STRING COMMENT 'Dominio de Datos — Negocio, Usuario, Contenido, Interacción, Publicidad, Confianza y Seguridad, Gobierno',
  categoria             STRING COMMENT 'Categoría — Entidad, Atributo, Rol, Proceso, Medida, Indicador o Concepto',
  ejemplo_uso           STRING COMMENT 'Ejemplo de Uso — frase que ilustra el término aplicado al caso',
  relaciones            STRING COMMENT 'Relaciones con otros términos — trazabilidad semántica',
  propietario           STRING COMMENT 'Propietario del Término — Data Owner del caso responsable de la definición',
  fuente_normativa      STRING COMMENT 'Fuente Normativa / Regulatoria — ley, norma o política interna que la soporta',
  fecha_actualizacion   DATE   COMMENT 'Fecha de Creación / Última Actualización'
) COMMENT 'E4 · Glosario de términos de negocio — mismo esquema que la plantilla institucional del bloque de Gobierno'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOGO}.yelp_gov.diccionario_datos (
  elemento                 STRING COMMENT 'Nombre del Elemento de Datos — nombre técnico del campo',
  nombre_negocio           STRING COMMENT 'Nombre de Negocio — nombre usado en el glosario',
  descripcion_negocio      STRING COMMENT 'Descripción de Negocio — significado del dato',
  dominio                  STRING COMMENT 'Dominio / Tema de Datos',
  sistema_fuente           STRING COMMENT 'Sistema / Aplicación Fuente donde se origina el dato',
  nombre_fisico            STRING COMMENT 'Nombre Físico — nombre calificado en Unity Catalog (catalogo.schema.tabla.columna)',
  tipo_dato                STRING COMMENT 'Tipo de Dato según el motor (STRING, INT, DOUBLE, TIMESTAMP...)',
  formato                  STRING COMMENT 'Formato / Patrón esperado',
  longitud                 STRING COMMENT 'Longitud / Precisión',
  valores_permitidos       STRING COMMENT 'Valores Permitidos / Dominio de Valores',
  unidad_medida            STRING COMMENT 'Unidad de Medida (si aplica)',
  valor_defecto            STRING COMMENT 'Valor por Defecto',
  obligatorio              STRING COMMENT 'Obligatorio / Nulo',
  clave                    STRING COMMENT 'Clave Primaria / Foránea',
  reglas_calidad           STRING COMMENT 'Reglas de Calidad / Validación — debe citar el identificador DQ-nnn de yelp_gov.dq_reglas',
  reglas_transformacion    STRING COMMENT 'Reglas de Transformación / Cálculo (si aplica)',
  frecuencia_actualizacion STRING COMMENT 'Frecuencia de Actualización',
  origen_dato              STRING COMMENT 'Origen del Dato — cómo y desde dónde se genera',
  uso_procesos             STRING COMMENT 'Uso / Procesos Relacionados',
  relacion_elementos       STRING COMMENT 'Relación con otros elementos',
  responsable              STRING COMMENT 'Responsable / Data Owner',
  politica_retencion       STRING COMMENT 'Política de Retención',
  sensibilidad             STRING COMMENT 'Sensibilidad / Clasificación — Público, Interno, Confidencial o Restringido',
  fuente_normativa         STRING COMMENT 'Fuente Normativa / Regulatoria',
  fecha_actualizacion      DATE   COMMENT 'Fecha de Creación / Última Actualización'
) COMMENT 'E4 · Diccionario de datos — mismo esquema que la plantilla institucional del bloque de Gobierno'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOGO}.yelp_gov.dq_reglas (
  regla_id         STRING  COMMENT 'Identificador único (DQ-001, DQ-002...)',
  tabla_objetivo   STRING  COMMENT 'Tabla completa: workspace.yelp_silver.slv_business',
  columna          STRING  COMMENT 'Columna evaluada (o * si es a nivel tabla)',
  dimension        STRING  COMMENT 'Dimensión DQ: completitud|unicidad|validez|consistencia|actualidad|integridad',
  descripcion      STRING  COMMENT 'Regla en lenguaje de negocio',
  expresion_sql    STRING  COMMENT 'Predicado SQL que DEBE cumplirse (ej. stars BETWEEN 1 AND 5)',
  criticidad       STRING  COMMENT 'alta|media|baja',
  umbral_pct       DOUBLE  COMMENT 'Porcentaje mínimo de filas que deben cumplir la regla (0-100)'
) COMMENT 'E5 · Catálogo de reglas de calidad de datos'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOGO}.yelp_gov.dq_resultados (
  ejecucion_ts     TIMESTAMP,
  regla_id         STRING,
  tabla_objetivo   STRING,
  filas_totales    BIGINT,
  filas_ok         BIGINT,
  pct_cumplimiento DOUBLE,
  paso             BOOLEAN COMMENT 'TRUE si pct_cumplimiento >= umbral de la regla'
) COMMENT 'E5 · Historial de ejecuciones del motor de calidad'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOGO}.yelp_gov.matriz_raci (
  entidad          STRING  COMMENT 'Entidad de datos (business, review, user...)',
  dominio          STRING,
  data_owner       STRING  COMMENT 'Área/rol accountable del caso Yelp',
  data_steward     STRING,
  data_custodian   STRING,
  consumidores     STRING  COMMENT 'Áreas que consumen la entidad',
  clasificacion    STRING  COMMENT 'publica|interna|confidencial|pii'
) COMMENT 'E6 · Asignación de roles y responsabilidades sobre entidades'
""")
print("Tablas de control creadas ✔")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Siguiente paso
# MAGIC 1. Correr en su laptop `datos/preparar_muestra_yelp.py` y subir los `.json.gz` al Volume.
# MAGIC 2. Ejecutar `00_setup/01_verificar_datos_raw_grupo_06`.