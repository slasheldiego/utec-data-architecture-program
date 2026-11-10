# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # E7.4 · Orquestación, monitoreo y FinOps (Grupo 06)
# MAGIC **Cierra con:** M10 DataOps / MLOps / FinOps
# MAGIC
# MAGIC El pipeline deja de ser "notebooks que alguien corre a mano" y se vuelve un **Lakeflow Job** con
# MAGIC dependencias, reintentos y alertas (sección 3.2, bloque *Ejecución automática del proceso*, y sección 8.1).
# MAGIC
# MAGIC ```
# MAGIC 01_bronze_ingesta ──► 02_silver_limpieza ──► 03_gold_productos_datos
# MAGIC                          │ (el gate de calidad vive dentro de silver y ejecuta E5)
# MAGIC                          └─► si una tarea falla → aviso por correo al grupo; gold conserva la versión anterior
# MAGIC ```
# MAGIC
# MAGIC | Decisión (sección 8.1) | Valor | Por qué |
# MAGIC |---|---|---|
# MAGIC | Tareas | 3, dependientes | Si silver o su gate fallan, gold no se publica y los consumidores siguen viendo la versión anterior |
# MAGIC | Reintentos | 1 por tarea, 2 minutos después | Cubre fallos técnicos transitorios sin agotar la cuota |
# MAGIC | Tiempo máximo | 30 min bronze · 40 min silver (incluye E5) · 30 min gold | Un fallo no consume la cuota diaria de Free Edition |
# MAGIC | Aviso | Correo al grupo si falla | Lo revisa el Data Custodian; el steward decide si es calidad |
# MAGIC | Disparador | **Programado, diario a las 22:00 (hora de Lima)** | La muestra es estática: un disparador por llegada de archivos casi nunca correría y no construiría el historial diario que pide CE-04. A las 22:00 no compite por la cuota con el trabajo del equipo |
# MAGIC
# MAGIC > **Quién crea el job importa.** El job corre con la identidad de quien lo crea (*Run as*). Debe crearlo un
# MAGIC > **Data Custodian** (David o Fiorela): E7/03 necesita leer el texto de las reseñas sin máscara.
# MAGIC
# MAGIC > **Trazabilidad —** implementa la sección 8 del Documento Formal (8.1 cadencia y responsables, 8.2 bitácora y FinOps).

# COMMAND ----------

# DBTITLE 1,Parámetros del job
dbutils.widgets.dropdown("crear_job", "no", ["no", "si"], "1. ¿Crear o actualizar el job?")
dbutils.widgets.text("correo_alertas", "ecodata.utec@gmail.com", "2. Correo para avisos de falla")
dbutils.widgets.dropdown("programacion", "UNPAUSED", ["UNPAUSED", "PAUSED"], "3. Programación diaria 22:00")
dbutils.widgets.text("nombre_job", "YelpConnect Grupo 06 — pipeline bronze-silver-gold", "4. Nombre del job")

CREAR = dbutils.widgets.get("crear_job") == "si"
CORREOS = [c.strip() for c in dbutils.widgets.get("correo_alertas").split(",") if c.strip()]
PAUSA = dbutils.widgets.get("programacion")
NOMBRE = dbutils.widgets.get("nombre_job")

# Los notebooks del job están en la misma carpeta que este notebook (Git folder del grupo)
ruta = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
CARPETA = ruta.rsplit("/", 1)[0]
NOTEBOOKS = {   # ✏️ al integrar la versión final a main, quitar el sufijo _exploratorio
    "bronze": f"{CARPETA}/01_bronze_ingesta_exploratorio",
    "silver": f"{CARPETA}/02_silver_limpieza_exploratorio",
    "gold":   f"{CARPETA}/03_gold_productos_datos_exploratorio",
}
for k, v in NOTEBOOKS.items():
    print(f"{k:7s} → {v}")
print(f"\nQuien ejecuta: {spark.sql('SELECT current_user()').first()[0]} (el job correrá con esta identidad)")

# COMMAND ----------

# DBTITLE 1,1. Crear o actualizar el Lakeflow Job (job como código)
# Se usa el SDK de Databricks, que ya viene instalado en los notebooks. Si el job ya existe con ese nombre,
# se actualiza (reset) en lugar de crear un duplicado. Sin cluster: en Free Edition las tareas corren en serverless.
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

def tarea(clave, notebook, minutos, depende_de=None):
    return jobs.Task(
        task_key=clave,
        notebook_task=jobs.NotebookTask(notebook_path=notebook, source=jobs.Source.WORKSPACE),
        depends_on=[jobs.TaskDependency(task_key=depende_de)] if depende_de else None,
        max_retries=1, min_retry_interval_millis=120_000, retry_on_timeout=False,
        timeout_seconds=minutos * 60)

config = dict(
    name=NOMBRE,
    tasks=[tarea("bronze", NOTEBOOKS["bronze"], 30),
           tarea("silver", NOTEBOOKS["silver"], 40, "bronze"),
           tarea("gold",   NOTEBOOKS["gold"],   30, "silver")],
    schedule=jobs.CronSchedule(quartz_cron_expression="0 0 22 * * ?", timezone_id="America/Lima",
                               pause_status=jobs.PauseStatus(PAUSA)),
    email_notifications=jobs.JobEmailNotifications(on_failure=CORREOS),
    max_concurrent_runs=1,          # nunca dos corridas a la vez sobre las mismas tablas
    timeout_seconds=100 * 60,       # tope del job completo
    tags={"proyecto": "yelpconnect", "grupo": "06"},
)

w = WorkspaceClient()
existentes = [j for j in w.jobs.list(name=NOMBRE)]
if not CREAR:
    estado = f"ya existe (job_id {existentes[0].job_id})" if existentes else "todavía no existe"
    print(f"ℹ️ Modo consulta: el job {estado}. Para crearlo o actualizarlo, poner el widget 1 en 'si' y re-ejecutar.")
elif existentes:
    JOB_ID = existentes[0].job_id
    w.jobs.reset(job_id=JOB_ID, new_settings=jobs.JobSettings(**config))
    print(f"✔ Job actualizado: {NOMBRE} (job_id {JOB_ID})")
else:
    JOB_ID = w.jobs.create(**config).job_id
    print(f"✔ Job creado: {NOMBRE} (job_id {JOB_ID})")
if existentes and not CREAR:
    JOB_ID = existentes[0].job_id
print("Abrir en la UI: Jobs & Pipelines → buscar el nombre del job → pestaña Tasks (captura del grafo).")

# COMMAND ----------

# DBTITLE 1,Historial de corridas del job (evidencia: corrida exitosa y corrida detenida por el gate)
import datetime as dt
valor = lambda e: e.value if e is not None else None
if "JOB_ID" in globals():
    corridas = [(r.run_id,
                 valor(r.state.life_cycle_state) if r.state else None,
                 valor(r.state.result_state) if r.state else None,
                 dt.datetime.fromtimestamp(r.start_time / 1000) if r.start_time else None,
                 (r.state.state_message or "")[:120] if r.state else "")
                for r in w.jobs.list_runs(job_id=JOB_ID, limit=10)]
    if corridas:
        display(spark.createDataFrame(corridas, "run_id BIGINT, ciclo STRING, resultado STRING, inicio TIMESTAMP, mensaje STRING"))
    else:
        print("El job aún no tiene corridas: usar 'Run now' en la UI o esperar a las 22:00.")
else:
    print("Primero crear el job (celda anterior).")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evidencia del gate de calidad deteniendo una corrida
# MAGIC Dos formas de obtenerla:
# MAGIC 1. **Real (recomendada):** las excepciones de DQ-011 y DQ-018 (Acta 05) caducan el **12/11/2026**. Si no se
# MAGIC    renuevan, la corrida del 13/11 se detiene en la tarea *silver* por DQ-011 (criticidad alta, 93,3 %), gold no
# MAGIC    se publica y llega el correo de falla. Es exactamente el comportamiento que describe la sección 8.1.
# MAGIC 2. **Forzada:** en E5 `_grupo_06`, cambiar temporalmente el umbral de DQ-001 de `100` a `101`, lanzar *Run now*,
# MAGIC    capturar la falla y **devolver el umbral a 100**. Anotar ambas acciones en la bitácora de 8.2.
# MAGIC
# MAGIC > Ojo: con 1 reintento, una corrida detenida por el gate se intenta dos veces. Es aceptable para la muestra; en
# MAGIC > producción convendría que el gate termine la tarea sin reintento.

# COMMAND ----------

# DBTITLE 1,2. Monitoreo — bitácora de corridas del pipeline
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_gov.pipeline_bitacora (
# MAGIC   ejecucion_ts     TIMESTAMP,
# MAGIC   capa             STRING,
# MAGIC   tabla            STRING,
# MAGIC   filas            BIGINT,
# MAGIC   duracion_seg     DOUBLE
# MAGIC ) COMMENT 'Observabilidad del pipeline: volumetría y duración por corrida';
# MAGIC
# MAGIC -- Los notebooks 01, 02 y 03 (_grupo_06) ya registran aquí sus métricas en cada corrida.
# MAGIC -- Resumen por día y capa (en gold, la duración máxima es la del notebook completo).
# MAGIC SELECT date(ejecucion_ts) AS dia, capa,
# MAGIC        count(DISTINCT ejecucion_ts)        AS corridas,
# MAGIC        sum(filas)                          AS filas_registradas,
# MAGIC        max(duracion_seg)                   AS duracion_max_seg
# MAGIC FROM workspace.yelp_gov.pipeline_bitacora
# MAGIC GROUP BY ALL
# MAGIC ORDER BY dia DESC, capa;

# COMMAND ----------

# DBTITLE 1,DoD: ≥3 corridas en la bitácora y en dq_resultados (CE-04)
corridas_bitacora = spark.sql("""SELECT count(DISTINCT ejecucion_ts) c
                                 FROM workspace.yelp_gov.pipeline_bitacora WHERE capa = 'gold'""").first().c
corridas_dq = spark.sql("SELECT count(DISTINCT ejecucion_ts) c FROM workspace.yelp_gov.dq_resultados").first().c
print(f"{'✔' if corridas_bitacora >= 3 else '…'} Corridas completas (hasta gold) en la bitácora: {corridas_bitacora} (meta ≥3)")
print(f"{'✔' if corridas_dq >= 3 else '…'} Ejecuciones del motor de calidad en dq_resultados: {corridas_dq} (CE-04 pide ≥3)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. FinOps en Free Edition
# MAGIC No hay factura, pero sí hay **cuota diaria de cómputo serverless**: el recurso escaso es la cuota, no el dinero (8.2).
# MAGIC
# MAGIC | # | Decisión de eficiencia | Efecto |
# MAGIC |---|---|---|
# MAGIC | 1 | Muestra de una sola ciudad (Philadelphia) con topes de 400.000 reseñas y 150.000 usuarios | El pipeline completo cabe en la cuota diaria; bronze ocupa ~324 MB. Costo: DQ-011 y DQ-018 fallan por el muestreo (Acta 05) |
# MAGIC | 2 | Auto Loader con `trigger(availableNow=True)` para reseñas | Procesa solo archivos nuevos y se apaga; no queda un stream consumiendo cuota |
# MAGIC | 3 | Silver con `insertInto` sobre tablas ya creadas y sin `cache()` | No se recrean tablas ni se fuerza memoria en serverless (no soportado) |
# MAGIC | 4 | Gold recalcula y sobrescribe; `gld_kpi_calidad_datos` solo agrega ejecuciones nuevas; `gld_dim_tiempo` se genera una vez | Cada corrida hace el mínimo trabajo necesario |
# MAGIC | 5 | El tablero y Genie leen solo gold (ya agregada), nunca silver | Las consultas del tablero escanean miles de filas en lugar de cientos de miles |
# MAGIC | 6 | Retención de 30 días del historial de Delta (ADR-04) | Acota el crecimiento del almacenamiento por versiones antiguas |
# MAGIC | 7 | Job a las 22:00, con tiempo máximo por tarea y 1 solo reintento | Un fallo no agota la cuota del día siguiente ni compite con el desarrollo |
# MAGIC
# MAGIC ### Estimación del costo si corriera en un workspace pagado
# MAGIC `costo mensual ≈ DBU por corrida × 30 corridas × precio del DBU de Jobs Serverless`
# MAGIC - **DBU por corrida:** la celda siguiente intenta leerlo de `system.billing.usage`. Si Free Edition no expone esa tabla,
# MAGIC   se completa a mano en el widget con el consumo observado.
# MAGIC - **Precio del DBU:** tomarlo de la página oficial https://www.databricks.com/product/pricing (Jobs Serverless,
# MAGIC   según nube y región) y anotar la fecha de consulta. **No inventar el valor.**

# COMMAND ----------

# DBTITLE 1,Estimación de costo mensual (completar el precio desde la página oficial)
dbutils.widgets.text("precio_usd_por_dbu", "", "5. Precio USD por DBU (Jobs Serverless, página oficial)")
dbutils.widgets.text("dbu_por_corrida_manual", "", "6. DBU por corrida (solo si no hay system.billing)")

dbu_corrida = None
try:
    if "JOB_ID" in globals():
        uso = spark.sql(f"""
          SELECT usage_metadata.job_run_id AS corrida, sum(usage_quantity) AS dbu
          FROM system.billing.usage
          WHERE usage_metadata.job_id = '{JOB_ID}' AND usage_unit = 'DBU'
          GROUP BY 1""")
        display(uso)
        dbu_corrida = uso.selectExpr("avg(dbu) d").first().d
except Exception as e:
    print(f"ℹ️ system.billing.usage no disponible en este workspace ({str(e).splitlines()[0][:90]}). Usar el widget 6.")

if dbu_corrida is None and dbutils.widgets.get("dbu_por_corrida_manual"):
    dbu_corrida = float(dbutils.widgets.get("dbu_por_corrida_manual"))
precio = dbutils.widgets.get("precio_usd_por_dbu")
if dbu_corrida and precio:
    mensual = dbu_corrida * 30 * float(precio)
    print(f"DBU por corrida ≈ {dbu_corrida:.2f} · precio {precio} USD/DBU → costo mensual ≈ {mensual:,.2f} USD (30 corridas)")
    print("Anotar el resultado y la fecha del precio en la sección 8.2 del Documento Formal.")
else:
    print("… Faltan datos: completar el precio (widget 5) y, si no hay system.billing, los DBU por corrida (widget 6).")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definition of Done (E7.4)
# MAGIC - [ ] Job multi-tarea creado (celda 1) con reintentos, tiempo máximo y aviso por correo: captura del grafo y de la configuración.
# MAGIC - [ ] Evidencia de una corrida exitosa y de una corrida detenida por el gate (celda de historial).
# MAGIC - [ ] Bitácora poblada con ≥3 corridas (celda de DoD).
# MAGIC - [x] Decisiones de eficiencia documentadas (tabla FinOps).
# MAGIC - [ ] Estimación de costo con el precio oficial y su fecha, trasladada a la sección 8.2.