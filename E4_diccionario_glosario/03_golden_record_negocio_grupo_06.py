# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # E4.3 · Golden Record de Negocio (MDM) · Grupo 06
# MAGIC **Cierra con:** M3 MDM y Metadata (Sesión 08 · Gestión estratégica de datos maestros)
# MAGIC
# MAGIC El **Golden Record** es la versión única y confiable de una entidad maestra. Se construye en tres pasos:
# MAGIC 1. **Matching:** identificar qué registros representan al mismo negocio aunque tengan distinto `business_id`.
# MAGIC 2. **Merging con reglas de supervivencia:** elegir, atributo por atributo, el valor que sobrevive.
# MAGIC 3. **Documentar el criterio:** cada decisión queda escrita y es trazable.
# MAGIC
# MAGIC **Entidad elegida: Negocio.** Es la entidad maestra del caso (Negocio y Usuario son maestros; Reseña, Tip y Check-in son
# MAGIC transaccionales). Usuario no se consolida: su único atributo comparable es el nombre de pila, que no identifica a una persona.
# MAGIC
# MAGIC **Arquitectura MDM: Registry (registro).** No se modifica el origen ni silver: se mantiene una **tabla de correspondencia**
# MAGIC (`business_id` → `golden_id`) y el golden record se publica aparte. Silver ya resuelve los duplicados **por identificador**
# MAGIC (DQ-004, se conserva la versión más reciente); este notebook resuelve los duplicados **lógicos**: el mismo local con
# MAGIC distinto identificador.
# MAGIC
# MAGIC **Dónde vive:** en `yelp_gov`, igual que la cuarentena. Es un artefacto de gobierno, no un producto de consumo, así que no
# MAGIC entra en el Diccionario ni en los permisos de gold. Lee `brz_business` porque silver no conserva el código postal.
# MAGIC
# MAGIC > **Trazabilidad —** implementa la gestión de datos maestros de Negocio descrita en el Documento Formal (dominio Negocio,
# MAGIC > Data Owner Andrew Collins, steward Rafael Nakamura). Ejecutar después de E7/01 (bronze).

# COMMAND ----------

# DBTITLE 1,Paso 1 — Matching: clave normalizada (nombre + dirección + código postal)
# Reglas de matching (determinístico, sobre valores normalizados):
#   M1. Minúsculas y sin signos ni espacios: "Joe's Pizza" = "joes pizza" = "JOES PIZZA".
#   M2. Abreviaturas de dirección unificadas: street → st, avenue → ave, road → rd, boulevard → blvd.
#   M3. Mismo nombre normalizado + misma dirección normalizada + mismo código postal = mismo negocio.
#   M4. Sin dirección no hay match (food trucks, negocios a domicilio): el registro es su propio golden record.
spark.sql("""
CREATE OR REPLACE TEMP VIEW mdm_candidatos AS
WITH base AS (
  SELECT business_id, name, address, CAST(postal_code AS STRING) AS postal_code,
         CAST(is_open AS INT) AS is_open, CAST(categories AS STRING) AS categories,
         coalesce(CAST(review_count AS INT), 0) AS review_count,
         regexp_replace(lower(coalesce(name, '')), '[^a-z0-9]', '') AS nombre_norm,
         regexp_replace(
           replace(replace(replace(replace(lower(coalesce(address, '')),
             'boulevard', 'blvd'), 'avenue', 'ave'), 'street', 'st'), 'road', 'rd'),
           '[^a-z0-9]', '') AS direccion_norm
  FROM workspace.yelp_bronze.brz_business
  WHERE business_id IS NOT NULL
)
SELECT *,
       CASE WHEN direccion_norm <> '' AND nombre_norm <> ''
            THEN concat_ws('|', nombre_norm, direccion_norm, coalesce(trim(postal_code), ''))
       END AS clave_match
FROM base
""")

# Grupo de match: registros con la misma clave. Superviviente = el de más reseñas (el más validado por los usuarios);
# a igualdad, el business_id menor (desempate determinístico: la corrida siempre da el mismo resultado).
spark.sql("""
CREATE OR REPLACE TEMP VIEW mdm_grupos AS
SELECT *,
       coalesce(clave_match, business_id) AS grupo,
       count(*) OVER (PARTITION BY coalesce(clave_match, business_id)) AS n_registros,
       row_number() OVER (PARTITION BY coalesce(clave_match, business_id)
                          ORDER BY review_count DESC, business_id) AS rn,
       first_value(business_id) OVER (PARTITION BY coalesce(clave_match, business_id)
                                      ORDER BY review_count DESC, business_id) AS golden_id
FROM mdm_candidatos
""")

dup = spark.sql("SELECT count(DISTINCT grupo) g, count(*) r FROM mdm_grupos WHERE n_registros > 1").first()
print(f"Negocios en bronze: {spark.table('mdm_grupos').count():,} · "
      f"grupos con duplicados lógicos: {dup.g:,} ({dup.r:,} registros)")

# COMMAND ----------

# DBTITLE 1,Evidencia del matching — grupos con más de un registro
# MAGIC %sql
# MAGIC SELECT golden_id, business_id, name, address, postal_code, review_count, is_open,
# MAGIC        CASE WHEN rn = 1 THEN 'Superviviente' ELSE 'Fusionado' END AS rol
# MAGIC FROM mdm_grupos
# MAGIC WHERE n_registros > 1
# MAGIC ORDER BY golden_id, rn
# MAGIC LIMIT 100;

# COMMAND ----------

# DBTITLE 1,Paso 2 — Merging: tabla de correspondencia y golden record
# Reglas de supervivencia por atributo:
#   S1. name, postal_code → del registro superviviente (el de más reseñas).
#   S2. address → la más completa (la de mayor longitud).
#   S3. categories → unión sin repetidos de las categorías de todos los registros del grupo.
#   S4. review_count_total → suma de las reseñas de todos los registros (la actividad no se pierde al fusionar).
#   S5. is_open → 1 si alguna ficha del grupo figura abierta (la ficha nueva refleja el estado actual).
spark.sql("""
CREATE OR REPLACE TABLE workspace.yelp_gov.mdm_negocio_correspondencia
COMMENT 'E4.3 · MDM: correspondencia business_id → golden_id del Golden Record de Negocio (arquitectura Registry)'
AS SELECT business_id, golden_id, rn = 1 AS es_superviviente, n_registros,
          CASE WHEN clave_match IS NULL THEN 'Sin dirección: es su propio golden record (M4)'
               WHEN n_registros = 1     THEN 'Sin coincidencias'
               ELSE 'Mismo nombre, dirección y código postal normalizados (M1-M3)' END AS regla_match,
          current_timestamp() AS ejecucion_ts
   FROM mdm_grupos
""")

spark.sql("""
CREATE OR REPLACE TABLE workspace.yelp_gov.mdm_negocio_golden
COMMENT 'E4.3 · MDM: Golden Record de Negocio, una fila por negocio real, con reglas de supervivencia S1-S4'
AS SELECT golden_id,
          max(CASE WHEN rn = 1 THEN name END)                   AS name,
          max_by(address, length(address))                      AS address,
          max(CASE WHEN rn = 1 THEN postal_code END)            AS postal_code,
          max(is_open)                AS is_open,
          array_join(array_sort(array_distinct(flatten(collect_list(
            transform(split(categories, ','), c -> trim(c)))))), ', ') AS categories,
          sum(review_count)                                     AS review_count_total,
          count(*)                                              AS n_registros_origen,
          array_sort(collect_list(business_id))                 AS business_ids_origen,
          'S1 name/postal_code del superviviente · S2 dirección más completa · S3 unión de categorías · S4 suma de reseñas · S5 abierto si alguna ficha está abierta' AS reglas_supervivencia,
          current_timestamp()                                   AS ejecucion_ts
   FROM mdm_grupos
   GROUP BY golden_id
""")
print("✔ mdm_negocio_correspondencia y mdm_negocio_golden creadas en yelp_gov")

# COMMAND ----------

# DBTITLE 1,Paso 3 — Validadores del Golden Record
x = spark.table("workspace.yelp_gov.mdm_negocio_correspondencia")
g = spark.table("workspace.yelp_gov.mdm_negocio_golden")
n_origen = spark.table("workspace.yelp_bronze.brz_business").filter("business_id IS NOT NULL").count()

assert x.count() == n_origen, "❌ Algún negocio de origen no tiene golden_id"
assert x.select("business_id").distinct().count() == x.count(), "❌ Un business_id apunta a más de un golden record"
assert g.count() == x.select("golden_id").distinct().count(), "❌ Golden records y correspondencia no coinciden"
assert g.filter("name IS NULL").count() == 0, "❌ Hay golden records sin nombre superviviente"
assert g.agg({"n_registros_origen": "sum"}).first()[0] == n_origen, "❌ Se perdieron o duplicaron registros al fusionar"

fusionados = g.filter("n_registros_origen > 1").count()
print(f"✔ Golden Record válido: {n_origen:,} registros de origen → {g.count():,} negocios únicos "
      f"({fusionados:,} golden records consolidan más de un registro)")

# COMMAND ----------

# MAGIC %md
# MAGIC Resultado y criterio
# MAGIC Registros de origen: 14.577 · Golden records: 14.556 · Grupos consolidados: 20 (41 registros; 21 duplicados lógicos, 0,14 % del catálogo).
# MAGIC Revisión del steward de los 20 grupos:
# MAGIC 9 son negocios que reabrieron o se volvieron a registrar: una ficha cerrada y otra abierta (por ejemplo, Lolita, con 668 y 423 reseñas repartidas en dos fichas).
# MAGIC 4 son fichas duplicadas de negocios ya cerrados, y 5 son fichas duplicadas vigentes. En “Nail Polish Bar & Spa” y “HubBub Coffee”, la normalización unió variantes del nombre y de la dirección.
# MAGIC 2 son probables falsos positivos: Starbucks y XpresSpa en 8000 Essington Ave, el aeropuerto de Filadelfia, donde sucursales de distintas terminales comparten la dirección postal.
# MAGIC Ajuste de supervivencia tras la revisión: en 4 grupos la ficha con más reseñas era la antigua y cerrada. Se agregó la regla S5: el negocio figura abierto si alguna de sus fichas lo está. Cada atributo tiene su propia regla de supervivencia.
# MAGIC Limitación: el matching es exacto tras normalizar. Errores de tipeo no se detectan, y direcciones compartidas (aeropuertos, centros comerciales) generan falsos positivos. Antes de usar el golden_id en gold, el steward debe aprobar cada grupo; los dos del aeropuerto quedan excluidos.
# MAGIC
# MAGIC ### Definition of Done (E4.3)
# MAGIC - [ ] Reglas de matching (M1-M4) y de supervivencia (S1-S4) documentadas.
# MAGIC - [ ] Tabla de correspondencia y golden record creados en `yelp_gov`.
# MAGIC - [ ] Validadores en verde y captura de la celda de evidencia.