# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # E7.3 · Silver → Gold: productos de datos (Grupo 06 · Trust & Safety)
# MAGIC **Cierra con:** M6 (construcción) + M9 (consumo)
# MAGIC
# MAGIC Gold materializa el esquema estrella de la sección 4.3 y convierte las cinco señales de la sección 6.4
# MAGIC en marcas por reseña y en un nivel de riesgo por cuenta. **Marca, no elimina** (ADR-02 y ADR-03).
# MAGIC Cada tabla responde a un requerimiento de la sección 7.1 y tiene un consumidor declarado:
# MAGIC
# MAGIC | Producto de datos | Tipo | Stakeholder | Pregunta que responde | Acceso |
# MAGIC |---|---|---|---|---|
# MAGIC | `gld_salud_resenas` | Hecho | Rafael Nakamura (T&S) | REQ-03: ¿por qué se marcó una reseña, con qué reglas y en qué ejecución? | **Restringido** |
# MAGIC | `gld_dim_usuario_riesgo` | Dimensión | Rafael Nakamura (T&S) | REQ-02: ¿qué cuentas tienen riesgo alto o medio y qué señales activaron? | **Restringido** |
# MAGIC | `gld_kpi_resenas_sospechosas_negocio` | Hecho agregado | Rafael Nakamura (T&S) | REQ-01: negocios con más reseñas sospechosas y su evolución mensual | General |
# MAGIC | `vw_rating_verificado_negocio` | Vista | Andrew Collins (Producto) y Jessica Martinez (Growth) | REQ-05 y REQ-06: brecha de rating y reseñas verificadas por mes | General |
# MAGIC | `gld_checkins_diarios` | Hecho agregado | Carla Romero (Ads Strategy) | REQ-07: volumen diario oficial de check-ins válidos | General |
# MAGIC | `gld_kpi_calidad_datos` | Hecho agregado | Laura Hernández (CDO) | REQ-04: reglas críticas fallidas y excepciones vigentes | General |
# MAGIC | `gld_dim_negocio`, `gld_dim_tiempo` | Dimensiones | Todas las áreas | Ejes de análisis por negocio y fecha | General |
# MAGIC
# MAGIC REQ-08 (Mei Lin, Data Science) se responde con `version_reglas` y el historial de Delta de las tablas gold (CE-03).
# MAGIC
# MAGIC > **Importante — quién lo ejecuta.** Debe correrlo un **Data Custodian** (David o Fiorela), igual que el job de E7/04.
# MAGIC > DQ-010 y DQ-013 leen el texto de las reseñas; cualquier otra cuenta lo ve enmascarado y las marcas saldrían mal.
# MAGIC > La primera celda lo verifica y se detiene si el texto llega enmascarado.
# MAGIC
# MAGIC > **Antes de la primera corrida:** ejecutar E3 (crea las tablas gold vacías). **Después de la primera corrida:** re-ejecutar **E4/02** (tags de clasificación de gold desde el Diccionario)
# MAGIC > y **E6** (permisos de gold tabla por tabla).
# MAGIC
# MAGIC > **Trazabilidad —** implementa las secciones 4.3, 6.4 y 7 del Documento Formal. Las tablas y la vista se **definen en E3**
# MAGIC > (tipos, COMMENT, PK/FK, retención: el mismo patrón que silver) y aquí solo se **cargan** con `insertInto`, que respeta ese DDL.
# MAGIC > Las 51 columnas (45 en tablas y 6 en la vista) son las del Diccionario de Datos (Grupo06).

# COMMAND ----------

# DBTITLE 1,Configuración: versión de reglas, ejecución, bitácora y verificación de quién ejecuta
import time
from pyspark.sql import functions as F

SLV, GLD, GOV = "workspace.yelp_silver", "workspace.yelp_gold", "workspace.yelp_gov"
T0 = time.time()

# El DDL de gold (tipos, NOT NULL, COMMENT, PK/FK, retención) está en E3: aquí solo se cargan las tablas
OBJETOS_GOLD = ["gld_dim_tiempo", "gld_dim_negocio", "gld_dim_usuario_riesgo", "gld_salud_resenas",
                "gld_kpi_resenas_sospechosas_negocio", "gld_checkins_diarios", "gld_kpi_calidad_datos",
                "vw_rating_verificado_negocio"]
faltan = [t for t in OBJETOS_GOLD if not spark.catalog.tableExists(f"{GLD}.{t}")]
assert not faltan, f"❌ Faltan objetos gold: {faltan}. Ejecutar antes E3_modelo_fisico/01_modelo_fisico_ddl_grupo_06."

# Quien ejecuta debe ver el texto en claro (custodian/steward): si no, DQ-010 y DQ-013 marcarían mal
enmascarado = spark.sql(f"SELECT count(*) c FROM {SLV}.slv_review WHERE text = '[texto oculto: dato Confidencial]'").first().c
assert enmascarado == 0, ("❌ El texto de las reseñas llega enmascarado para este usuario: E7/03 debe ejecutarlo "
                          "un Data Custodian (David o Fiorela) o el job configurado con 'Run as' un custodian.")

# CE-06: cada marca guarda la versión del catálogo de reglas y la ejecución que la generó
VERSION_REGLAS = int(spark.sql(f"DESCRIBE HISTORY {GOV}.dq_reglas LIMIT 1").first()["version"])
EJECUCION_TS = spark.sql(f"SELECT max(ejecucion_ts) ts FROM {GOV}.dq_resultados").first().ts   # la corrida del gate de E7/02
assert EJECUCION_TS is not None, "❌ dq_resultados está vacía: ejecutar antes E7/02 (que llama a E5)"
print(f"version_reglas = {VERSION_REGLAS} · ejecucion_ts = {EJECUCION_TS} (coincide con dq_resultados)")

# Bitácora del pipeline (E7/04 · sección 8.2): filas y duración de cada carga
spark.sql(f"""CREATE TABLE IF NOT EXISTS {GOV}.pipeline_bitacora (
  ejecucion_ts TIMESTAMP, capa STRING, tabla STRING, filas BIGINT, duracion_seg DOUBLE
) COMMENT 'Observabilidad del pipeline: volumetría y duración por corrida'""")
BITACORA = []

def cargar(tabla, df):
    """Sobrescribe la tabla respetando su DDL (tipos, NOT NULL, COMMENT) y anota filas y duración."""
    t = time.time()
    df.write.insertInto(f"{GLD}.{tabla}", overwrite=True)
    filas = spark.table(f"{GLD}.{tabla}").count()
    BITACORA.append(("gold", tabla, filas, round(time.time() - t, 1)))
    print(f"✔ {tabla}: {filas:,} filas")

# COMMAND ----------

# DBTITLE 1,gld_dim_tiempo — calendario estático (se regenera solo si la muestra trae fechas fuera de rango)
rango = spark.sql(f"""
  SELECT make_date(year(min(d)), 1, 1) AS ini, make_date(year(max(d)), 12, 31) AS fin FROM (
    SELECT `date` AS d FROM {SLV}.slv_review UNION ALL
    SELECT `date` FROM {SLV}.slv_tip UNION ALL
    SELECT to_date(`date`) FROM {SLV}.slv_check_in)""").first()
actual = spark.sql(f"SELECT min(fecha) ini, max(fecha) fin FROM {GLD}.gld_dim_tiempo").first()
if actual.ini is None or actual.ini > rango.ini or actual.fin < rango.fin:
    cargar("gld_dim_tiempo", spark.sql(f"""
      SELECT fecha, year(fecha) AS anio, month(fecha) AS mes, dayofweek(fecha) AS dia_semana,
             dayofweek(fecha) IN (1, 7) AS es_fin_de_semana
      FROM (SELECT explode(sequence(DATE'{rango.ini}', DATE'{rango.fin}', interval 1 day)) AS fecha)"""))
else:
    print(f"✔ gld_dim_tiempo ya cubre {actual.ini} a {actual.fin}: no se regenera")

# COMMAND ----------

# DBTITLE 1,gld_dim_negocio — SCD tipo 1
cargar("gld_dim_negocio", spark.sql(f"""
  SELECT business_id, name, categories, stars, is_open FROM {SLV}.slv_business"""))

# COMMAND ----------

# DBTITLE 1,Señales de fraude (sección 6.4) — expresiones leídas de dq_reglas, no escritas aquí
# Rules-as-data: la condición de cada señal es la misma expresión que evalúa E5. Así el parámetro Restringido
# de DQ-016 no aparece en este notebook y, si Trust & Safety recalibra una regla, gold usa la nueva versión.
# Peso en el riesgo de la cuenta (tabla de la sección 6.4):
PESO = {"DQ-016": "Alto", "DQ-010": "Medio", "DQ-009": "Medio", "DQ-017": "Medio", "DQ-013": "Bajo"}
R = {r.regla_id: r for r in spark.table(f"{GOV}.dq_reglas").where(F.col("regla_id").isin(list(PESO))).collect()}
assert set(R) == set(PESO), f"❌ Faltan señales en dq_reglas: {set(PESO) - set(R)}"

# Cada consulta devuelve las reseñas que INCUMPLEN la regla (NOT expresión), siempre leyendo solo silver (CE-03)
marcas_sql = {
  # DQ-009: reseña anterior al alta de su autor
  "DQ-009": f"SELECT t.review_id FROM {SLV}.slv_review t WHERE NOT ({R['DQ-009'].expresion_sql})",
  # DQ-013: texto de relleno
  "DQ-013": f"SELECT t.review_id FROM {SLV}.slv_review t WHERE NOT ({R['DQ-013'].expresion_sql})",
  # DQ-010: texto idéntico al de otra reseña del mismo negocio → se marcan todas las del grupo repetido
  "DQ-010": f"""SELECT review_id FROM (
                  SELECT t.review_id, count(*) OVER (PARTITION BY {R['DQ-010'].columna}) AS n
                  FROM {SLV}.slv_review t WHERE {R['DQ-010'].expresion_sql}) WHERE n > 1""",
  # DQ-016: ráfaga → todas las reseñas de ese usuario en ese día. La expresión usa t.max_resenas_dia,
  # que aquí es el conteo de ese usuario en ese día.
  "DQ-016": f"""SELECT r.review_id FROM {SLV}.slv_review r
                JOIN (SELECT user_id, `date`, count(*) AS max_resenas_dia
                      FROM {SLV}.slv_review GROUP BY user_id, `date`) t
                  ON r.user_id = t.user_id AND r.`date` = t.`date`
                WHERE NOT ({R['DQ-016'].expresion_sql})""",
}
marcas = None
for regla, sql in marcas_sql.items():
    df = spark.sql(sql).select("review_id", F.lit(regla).alias("regla"))
    marcas = df if marcas is None else marcas.unionByName(df)
marcas.createOrReplaceTempView("marcas_resena")

# DQ-017 marca solo la cuenta: reseñas cargadas por encima de las declaradas en el perfil
spark.sql(f"""
  SELECT t.user_id FROM (
    SELECT u.user_id, u.review_count, count(r.review_id) AS resenas_cargadas
    FROM {SLV}.slv_user u LEFT JOIN {SLV}.slv_review r ON r.user_id = u.user_id
    GROUP BY u.user_id, u.review_count) t
  WHERE NOT ({R['DQ-017'].expresion_sql})""").createOrReplaceTempView("cuentas_dq017")

display(spark.sql("SELECT regla, count(*) AS resenas_marcadas FROM marcas_resena GROUP BY regla ORDER BY regla"))

# COMMAND ----------

# DBTITLE 1,gld_salud_resenas — una fila por reseña con sus marcas (Restringido)
cargar("gld_salud_resenas", spark.sql(f"""
  WITH m AS (SELECT review_id, array_sort(collect_set(regla)) AS reglas FROM marcas_resena GROUP BY review_id)
  SELECT r.review_id, r.business_id, r.user_id, r.`date` AS fecha_resena, r.stars,
         coalesce(size(m.reglas), 0) > 0                      AS es_sospechosa,
         coalesce(m.reglas, CAST(array() AS ARRAY<STRING>))    AS regla_disparada,
         coalesce(size(m.reglas), 0) = 0                       AS es_verificada,
         CAST({VERSION_REGLAS} AS BIGINT)                      AS version_reglas,
         CAST('{EJECUCION_TS}' AS TIMESTAMP)                   AS ejecucion_ts
  FROM {SLV}.slv_review r
  LEFT JOIN m USING (review_id)
  WHERE r.user_id IS NOT NULL AND r.`date` IS NOT NULL   -- obligatorios en gold (DQ-005 ya los aísla en silver)
"""))

# COMMAND ----------

# DBTITLE 1,gld_dim_usuario_riesgo — nivel de riesgo por cuenta (Restringido · SCD tipo 1)
ALTO  = [k for k, v in PESO.items() if v == "Alto"]
MEDIO = [k for k, v in PESO.items() if v == "Medio"]
BAJO  = [k for k, v in PESO.items() if v == "Bajo"]
lista = lambda xs: ", ".join(f"'{x}'" for x in xs)

cargar("gld_dim_usuario_riesgo", spark.sql(f"""
  WITH por_resena AS (
    SELECT user_id, count(*) AS n_resenas_cargadas,
           array_distinct(flatten(collect_list(regla_disparada))) AS senales_resenas
    FROM {GLD}.gld_salud_resenas GROUP BY user_id),
  cuentas AS (
    SELECT u.user_id, u.yelping_since,
           coalesce(p.n_resenas_cargadas, 0) AS n_resenas_cargadas,
           array_sort(array_distinct(concat(
             coalesce(p.senales_resenas, CAST(array() AS ARRAY<STRING>)),
             CASE WHEN d.user_id IS NOT NULL THEN array('DQ-017') ELSE CAST(array() AS ARRAY<STRING>) END))) AS senales
    FROM {SLV}.slv_user u
    LEFT JOIN por_resena p   ON p.user_id = u.user_id
    LEFT JOIN cuentas_dq017 d ON d.user_id = u.user_id)
  SELECT user_id, yelping_since, n_resenas_cargadas, size(senales) AS n_senales, senales,
         CASE WHEN arrays_overlap(senales, array({lista(ALTO)})) OR size(senales) >= 2 THEN 'Alto'
              WHEN size(senales) = 1 AND senales[0] IN ({lista(MEDIO)})              THEN 'Medio'
              WHEN size(senales) = 1 AND senales[0] IN ({lista(BAJO)})               THEN 'Bajo'
              ELSE 'Sin señales' END                       AS nivel_riesgo,
         CAST({VERSION_REGLAS} AS BIGINT)                  AS version_reglas,
         CAST('{EJECUCION_TS}' AS TIMESTAMP)               AS ejecucion_ts
  FROM cuentas
"""))

# COMMAND ----------

# DBTITLE 1,gld_kpi_resenas_sospechosas_negocio — REQ-01 (solo conteos)
cargar("gld_kpi_resenas_sospechosas_negocio", spark.sql(f"""
  WITH m AS (
    SELECT business_id, trunc(fecha_resena, 'MM') AS mes,
           count(*) AS n_resenas, count_if(es_sospechosa) AS n_sospechosas,
           CAST(round(100.0 * count_if(es_sospechosa) / count(*), 2) AS DECIMAL(5,2)) AS pct_sospechosas
    FROM {GLD}.gld_salud_resenas GROUP BY business_id, trunc(fecha_resena, 'MM'))
  SELECT m.business_id, m.mes, m.n_resenas, m.n_sospechosas, m.pct_sospechosas,
         ant.pct_sospechosas AS pct_sospechosas_mes_anterior   -- vacío si el negocio no tuvo reseñas el mes anterior
  FROM m LEFT JOIN m ant ON ant.business_id = m.business_id AND ant.mes = add_months(m.mes, -1)
"""))

# COMMAND ----------

# DBTITLE 1,gld_checkins_diarios — REQ-07 (volumen oficial; acumulativo)
# Se recalcula desde toda la historia de silver: el resultado es el acumulado completo de la muestra.
# Solo cuenta check-ins válidos: los que incumplen DQ-019 ya quedaron en cuarentena en E7/02.
cargar("gld_checkins_diarios", spark.sql(f"""
  SELECT business_id, to_date(`date`) AS fecha, count(*) AS n_checkins
  FROM {SLV}.slv_check_in GROUP BY business_id, to_date(`date`)"""))

# COMMAND ----------

# DBTITLE 1,gld_kpi_calidad_datos — REQ-04 (solo se agregan las ejecuciones nuevas)
# La primera corrida incorpora el historial previo de dq_resultados con el catálogo de reglas vigente.
t = time.time()
nuevas = spark.sql(f"""
  WITH exc AS (
    SELECT DISTINCT res.ejecucion_ts, res.regla_id
    FROM {GOV}.dq_resultados res JOIN {GOV}.dq_excepciones ex
      ON ex.regla_id = res.regla_id AND to_date(res.ejecucion_ts) BETWEEN ex.fecha_aprobacion AND ex.fecha_caducidad)
  SELECT res.ejecucion_ts, res.regla_id, r.dimension, r.criticidad, r.umbral_pct,
         res.pct_cumplimiento, res.paso, exc.regla_id IS NOT NULL AS con_excepcion
  FROM {GOV}.dq_resultados res
  JOIN {GOV}.dq_reglas r USING (regla_id)
  LEFT JOIN exc USING (ejecucion_ts, regla_id)
  LEFT ANTI JOIN {GLD}.gld_kpi_calidad_datos k USING (ejecucion_ts, regla_id)""")
n_nuevas = nuevas.count()
nuevas.write.insertInto(f"{GLD}.gld_kpi_calidad_datos")
BITACORA.append(("gold", "gld_kpi_calidad_datos", n_nuevas, round(time.time() - t, 1)))
print(f"✔ gld_kpi_calidad_datos: {n_nuevas:,} filas nuevas · "
      f"{spark.table(f'{GLD}.gld_kpi_calidad_datos').select('ejecucion_ts').distinct().count()} ejecuciones registradas (CE-04 pide ≥3)")

# COMMAND ----------

# DBTITLE 1,Validaciones: CE-06, integridad de las FK, contrato del Diccionario y bitácora
# CE-06: el 100 % de las reseñas sospechosas es explicable
ce06 = spark.sql(f"""
  SELECT count(*) AS sospechosas,
         count_if(size(regla_disparada) > 0 AND version_reglas IS NOT NULL AND ejecucion_ts IS NOT NULL) AS explicables
  FROM {GLD}.gld_salud_resenas WHERE es_sospechosa""").first()
citadas = spark.sql(f"SELECT DISTINCT explode(regla_disparada) AS regla_id FROM {GLD}.gld_salud_resenas")
sin_regla = citadas.join(spark.table(f"{GOV}.dq_reglas").select("regla_id"), "regla_id", "left_anti").count()
assert ce06.sospechosas == ce06.explicables and sin_regla == 0, "❌ CE-06: hay marcas sin regla, versión o ejecución"
pct = 100.0 * ce06.explicables / ce06.sospechosas if ce06.sospechosas else 100.0
print(f"✔ CE-06: {ce06.sospechosas:,} reseñas sospechosas, {pct:.0f} % explicables")

# Las FK de Unity Catalog son informativas: se comprueban aquí
huerfanas = {
  "salud → dim_negocio":   f"SELECT count(*) c FROM {GLD}.gld_salud_resenas s LEFT ANTI JOIN {GLD}.gld_dim_negocio d USING (business_id)",
  "salud → dim_usuario":   f"SELECT count(*) c FROM {GLD}.gld_salud_resenas s LEFT ANTI JOIN {GLD}.gld_dim_usuario_riesgo d USING (user_id)",
  "salud → dim_tiempo":    f"SELECT count(*) c FROM {GLD}.gld_salud_resenas s LEFT ANTI JOIN {GLD}.gld_dim_tiempo d ON d.fecha = s.fecha_resena",
  "kpi → dim_tiempo":      f"SELECT count(*) c FROM {GLD}.gld_kpi_resenas_sospechosas_negocio k LEFT ANTI JOIN {GLD}.gld_dim_tiempo d ON d.fecha = k.mes",
  "checkins → dim_negocio":f"SELECT count(*) c FROM {GLD}.gld_checkins_diarios k LEFT ANTI JOIN {GLD}.gld_dim_negocio d USING (business_id)",
  "checkins → dim_tiempo": f"SELECT count(*) c FROM {GLD}.gld_checkins_diarios k LEFT ANTI JOIN {GLD}.gld_dim_tiempo d ON d.fecha = k.fecha",
}
malas = {k: spark.sql(q).first().c for k, q in huerfanas.items()}
assert not any(malas.values()), f"❌ Filas sin su dimensión: { {k: v for k, v in malas.items() if v} }"
print("✔ Todas las FK de gold tienen su fila en la dimensión")

# Contrato: las 51 columnas gold existen y tienen COMMENT (lo exige E3 y lo usa E4/02)
cols = spark.sql("""SELECT table_name, column_name, comment FROM workspace.information_schema.columns
                    WHERE table_schema = 'yelp_gold'""")
print(f"✔ Columnas gold: {cols.count()} (el Diccionario define 51) · sin COMMENT: {cols.filter('comment IS NULL').count()}")

display(spark.sql(f"SELECT nivel_riesgo, count(*) AS cuentas FROM {GLD}.gld_dim_usuario_riesgo GROUP BY nivel_riesgo ORDER BY cuentas DESC"))

# Bitácora (sección 8.2)
BITACORA.append(("gold", "(notebook completo)", None, round(time.time() - T0, 1)))
(spark.createDataFrame(BITACORA, "capa STRING, tabla STRING, filas BIGINT, duracion_seg DOUBLE")
      .select(F.current_timestamp().alias("ejecucion_ts"), "capa", "tabla", "filas", "duracion_seg")
      .write.insertInto(f"{GOV}.pipeline_bitacora"))
print(f"✔ Bitácora: {len(BITACORA)} registros · duración total {BITACORA[-1][3]} s")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definition of Done (E7.3)
# MAGIC - [x] Esquema estrella completo: 3 dimensiones (`gld_dim_negocio`, `gld_dim_usuario_riesgo`, `gld_dim_tiempo`), 1 hecho (`gld_salud_resenas`) y 3 hechos agregados de KPIs, más la vista `vw_rating_verificado_negocio`.
# MAGIC - [x] `gld_dim_tiempo` generada con `sequence` + `explode`; SCD tipo 1 en las dimensiones (sección 4.3).
# MAGIC - [x] PK/FK informativas (declaradas en E3) verificadas por código; COMMENT en las 51 columnas, tomados del Diccionario.
# MAGIC - [x] Retención de 30 días en el historial de Delta (ADR-04), declarada en el DDL de E3.
# MAGIC - [x] CE-06 verificado: toda reseña sospechosa tiene reglas, versión y ejecución.
# MAGIC - [x] Tabla stakeholder → producto → pregunta completa (arriba).
# MAGIC - [ ] Re-ejecutar E4/02 (tags) y E6 (permisos de gold) después de la primera corrida.
# MAGIC - [ ] CE-01: captura del Lineage `brz_review → slv_review → gld_salud_resenas` en Catalog Explorer.
# MAGIC - [ ] CE-03: captura de `DESCRIBE HISTORY workspace.yelp_gold.gld_salud_resenas`.

# COMMAND ----------

dbutils.notebook.exit("ok: gold publicado")