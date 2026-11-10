# Databricks notebook source
# MAGIC %md
# MAGIC # E6 · Controles técnicos de gobierno en Unity Catalog
# MAGIC **Cierra con:** M2 (diseño) + M5 (implementación en plataforma)
# MAGIC
# MAGIC La estrategia de gobierno definida en el Documento Formal de Arquitectura de Datos se **implementa** aquí con 4 controles nativos de UC:
# MAGIC tags de clasificación, permisos (GRANT), enmascaramiento de PII con vistas/funciones,
# MAGIC y evidencia de lineage. En Free Edition los 5 integrantes son usuarios del mismo
# MAGIC workspace → cada integrante "actúa" un rol del caso para probar los permisos.
# MAGIC
# MAGIC > **Trazabilidad —** este entregable **implementa** la sección «Estrategia de gobierno: roles, matriz RACI y políticas de clasificación» del
# MAGIC > **Documento Formal de Arquitectura de Datos**. Aquí se implementan los controles que hacen exigible esa estrategia. Un rol sin GRANT y una clasificación sin tag son gobierno declarativo, no efectivo.

# COMMAND ----------

# MAGIC %md
# MAGIC La estrategia de la sección 5 del Documento Formal se **implementa** aquí con controles nativos de UC:
# MAGIC tags de clasificación, permisos (GRANT) por rol, enmascaramiento de datos personales, la matriz RACI
# MAGIC como tabla consultable y la evidencia de lineage.
# MAGIC
# MAGIC En Free Edition hay un solo workspace y no todos los integrantes tienen cuenta: cada cuenta **actúa** un rol del caso.
# MAGIC
# MAGIC | Rol del caso | Persona del caso | Cuenta que lo actúa | Permisos |
# MAGIC |---|---|---|---|
# MAGIC | Data Custodian | Equipo de Data Platform | David (administrador) y Fiorela | Todos los privilegios en bronze, silver, gold y gov |
# MAGIC | Data Owner | Andrew Collins (Producto) | Leila | Lectura en silver con datos personales enmascarados y en la **gold de consumo general**; en gov solo tablas de gobierno y `vw_dq_reglas_publica` (no `dq_reglas`, no la cuarentena); sin bronze |
# MAGIC | Data Steward | Rafael Nakamura (T&S) | David (también custodian: conserva los permisos operativos) | Lectura en silver y en **toda** gold (incluida la parte Restringida), escritura en gov, ve datos en claro |
# MAGIC | Consumidores | Marketing, Data Science, Op. Comerciales | Juan Carlos y Edgar | Solo lectura en la **gold de consumo general** |
# MAGIC
# MAGIC **Gold restringida a Trust & Safety (sección 4.3 y 5.3):** `gld_salud_resenas` y `gld_dim_usuario_riesgo`.
# MAGIC El resto de gold es de consumo general. Por eso a Owner y Consumidores se les da SELECT **tabla por tabla** y no
# MAGIC sobre todo el schema: un SELECT sobre el schema incluiría también las dos tablas restringidas.
# MAGIC
# MAGIC Cada recuadro de correos acepta **varios correos separados por coma** y puede quedar **vacío** si el rol no tiene cuenta.
# MAGIC
# MAGIC > **Importante.** Quien creó la cuenta de Databricks es administrador y dueño del catálogo: ve todo
# MAGIC > aunque no tenga GRANT. Las pruebas de permisos y máscaras deben hacerse con los **otros** integrantes.
# MAGIC
# MAGIC > **Trazabilidad —** implementa la sección «5. Estrategia de gobierno y matriz RACI» del Documento Formal.
# MAGIC > Un rol sin GRANT y una clasificación sin tag son gobierno declarativo, no efectivo.

# COMMAND ----------

# DBTITLE 1,Correos por rol (varios separados por coma; vacío si el rol no tiene cuenta)
dbutils.widgets.text("email_custodian", "ecodata.utec@gmail.com, fvcastillon@gmail.com", "Data Custodian — David, Fiorela")
dbutils.widgets.text("email_owner", "leilacordova6@gmail.com", "Data Owner — Leila")
dbutils.widgets.text("email_steward", "ecodata.utec@gmail.com", "Data Steward — David")
dbutils.widgets.text("email_consumidores", "jcrevolledo@gmail.com,esuarezq@gmail.com", "Consumidores — Juan Carlos, Edgar")

def correos(widget):
    return [e.strip() for e in dbutils.widgets.get(widget).split(",") if e.strip()]

CUSTODIANS, OWNERS = correos("email_custodian"), correos("email_owner")
STEWARDS, CONSUMIDORES = correos("email_steward"), correos("email_consumidores")
assert CUSTODIANS, "❌ Ingrese al menos un correo de Data Custodian (quien opera el pipeline)"
yo = spark.sql("SELECT current_user()").first()[0]
if yo not in CUSTODIANS:
    print(f"⚠️ Usted ({yo}) no está en Data Custodian. Si no es administrador, perderá acceso para operar el pipeline.")
print(f"Custodian: {CUSTODIANS}\nOwner: {OWNERS}\nSteward: {STEWARDS}\nConsumidores: {CONSUMIDORES}")

def ejecutar(sql):
    """Ejecuta y reporta; no detiene el notebook para poder ver todos los resultados de una vez."""
    try:
        spark.sql(sql); print("✔", " ".join(sql.split())[:110])
        return True
    except Exception as e:
        print("❌", " ".join(sql.split())[:110], "\n   →", str(e).split("\n")[0][:200])
        return False

# COMMAND ----------

# DBTITLE 1,Control 1 — Clasificación con tags (schemas, tablas y columnas)
# Schemas: capa del lakehouse
for capa in ["bronze", "silver", "gold", "gov"]:
    ejecutar(f"ALTER SCHEMA workspace.yelp_{capa} SET TAGS ('capa' = '{capa}', 'dominio' = 'yelpconnect')")

# Tablas silver: dominio, owner y steward (sección 5.2)
TABLAS = {
    "slv_business": "negocio", "slv_user": "usuario", "slv_review": "contenido",
    "slv_tip": "contenido", "slv_check_in": "interaccion",
}
for tabla, dominio in TABLAS.items():
    ejecutar(f"""ALTER TABLE workspace.yelp_silver.{tabla} SET TAGS (
                 'dominio' = '{dominio}', 'data_owner' = 'andrew_collins', 'data_steward' = 'rafael_nakamura')""")

# Columnas: E4/02 ya aplica el tag 'clasificacion' desde el Diccionario. Se reafirman aquí las de datos
# personales (Confidencial) para que el control no dependa del orden de ejecución.
CONFIDENCIALES = [("slv_user", "user_id"), ("slv_user", "name"), ("slv_review", "user_id"),
                  ("slv_review", "text"), ("slv_tip", "user_id"), ("slv_tip", "text")]
for tabla, col in CONFIDENCIALES:
    ejecutar(f"ALTER TABLE workspace.yelp_silver.{tabla} ALTER COLUMN {col} SET TAGS ('clasificacion' = 'confidencial')")

# COMMAND ----------

# DBTITLE 1,Control 1b — Columnas Restringido de gold (re-ejecutar después de E7/03)
# Nombres definitivos de la sección 4.3. Columnas Restringido según el Diccionario de Datos (Grupo06).
# E4/02 ya aplica estos tags desde el Excel; aquí se reafirman para que el control no dependa del orden.
TABLA_HECHO_GOLD = "gld_salud_resenas"
TABLAS_GOLD_RESTRINGIDAS = {TABLA_HECHO_GOLD, "gld_dim_usuario_riesgo"}   # solo Trust & Safety (data_stewards)
RESTRINGIDAS = [(TABLA_HECHO_GOLD, "es_sospechosa"), (TABLA_HECHO_GOLD, "regla_disparada"),
                (TABLA_HECHO_GOLD, "es_verificada"),
                ("gld_dim_usuario_riesgo", "n_senales"), ("gld_dim_usuario_riesgo", "senales"),
                ("gld_dim_usuario_riesgo", "nivel_riesgo")]
for tabla, col in RESTRINGIDAS:
    if spark.catalog.tableExists(f"workspace.yelp_gold.{tabla}"):
        ejecutar(f"ALTER TABLE workspace.yelp_gold.{tabla} ALTER COLUMN {col} SET TAGS ('clasificacion' = 'restringido')")
    else:
        print(f"… workspace.yelp_gold.{tabla} aún no existe: re-ejecutar esta celda después de E7/03")

# COMMAND ----------

# DBTITLE 1,Evidencia de tags
# MAGIC %sql
# MAGIC SELECT schema_name, table_name, column_name, tag_value AS clasificacion
# MAGIC FROM workspace.information_schema.column_tags
# MAGIC WHERE tag_name = 'clasificacion'
# MAGIC ORDER BY CASE tag_value WHEN 'restringido' THEN 1 WHEN 'confidencial' THEN 2 WHEN 'interno' THEN 3 ELSE 4 END,
# MAGIC          schema_name, table_name, column_name;

# COMMAND ----------

# DBTITLE 1,Control 2 — Permisos por rol (principio de mínimo privilegio)
q = lambda e: f"`{e}`"

def revocar(privilegio_sql, email):
    """Nunca se revoca a un custodian: si una persona tiene dos roles (p. ej. steward y custodian),
    conserva los permisos del rol operativo."""
    if email in CUSTODIANS:
        print(f"… se omite '{privilegio_sql}' para {email}: también es Data Custodian")
        return
    ejecutar(f"{privilegio_sql} FROM {q(email)}")

for email in CUSTODIANS + OWNERS + STEWARDS + CONSUMIDORES:
    ejecutar(f"GRANT USE CATALOG ON CATALOG workspace TO {q(email)}")

# Custodian: opera las cuatro capas
for email in CUSTODIANS:
    for capa in ["bronze", "silver", "gold", "gov"]:
        ejecutar(f"GRANT ALL PRIVILEGES ON SCHEMA workspace.yelp_{capa} TO {q(email)}")

# Steward: lee silver y gold; mantiene gov (glosario, diccionario, reglas)
for email in STEWARDS:
    for capa in ["silver", "gold"]:
        ejecutar(f"GRANT USE SCHEMA, SELECT ON SCHEMA workspace.yelp_{capa} TO {q(email)}")
    ejecutar(f"GRANT USE SCHEMA, SELECT, MODIFY ON SCHEMA workspace.yelp_gov TO {q(email)}")
    revocar("REVOKE ALL PRIVILEGES ON SCHEMA workspace.yelp_bronze", email)

# Gold de consumo general: todas las tablas y vistas de yelp_gold salvo las dos restringidas.
# Se lee del catálogo en cada corrida → re-ejecutar esta celda después de E7/03 (cuando gold ya existe).
TABLAS_GOLD = [r.tableName for r in spark.sql("SHOW TABLES IN workspace.yelp_gold").collect()]
GOLD_GENERAL = sorted(t for t in TABLAS_GOLD if t not in TABLAS_GOLD_RESTRINGIDAS)
print(f"Gold de consumo general: {GOLD_GENERAL or '(aún vacía: re-ejecutar después de E7/03)'}")

def acceso_gold_general(email):
    """SELECT tabla por tabla en la gold de consumo general; nada sobre las tablas restringidas."""
    revocar("REVOKE ALL PRIVILEGES ON SCHEMA workspace.yelp_gold", email)   # quita un SELECT de schema previo
    ejecutar(f"GRANT USE SCHEMA ON SCHEMA workspace.yelp_gold TO {q(email)}")
    for t in GOLD_GENERAL:
        ejecutar(f"GRANT SELECT ON TABLE workspace.yelp_gold.{t} TO {q(email)}")
    for t in TABLAS_GOLD_RESTRINGIDAS & set(TABLAS_GOLD):
        revocar(f"REVOKE ALL PRIVILEGES ON TABLE workspace.yelp_gold.{t}", email)

# Owner: lee silver (enmascarado) y la gold de consumo general; en gov solo las tablas de gobierno y la vista
# pública del catálogo de reglas. NO dq_reglas (guarda el parámetro Restringido de DQ-016, sección 5.3),
# NO la cuarentena ni las vistas de evaluación (contienen nombres y textos sin máscara); sin bronze.
TABLAS_GOBIERNO = ["glosario_negocio", "diccionario_datos", "vw_dq_reglas_publica",
                   "dq_resultados", "dq_excepciones", "matriz_raci"]
for email in OWNERS:
    for capa in ["silver", "gov"]:
        revocar(f"REVOKE ALL PRIVILEGES ON SCHEMA workspace.yelp_{capa}", email)
    ejecutar(f"GRANT USE SCHEMA, SELECT ON SCHEMA workspace.yelp_silver TO {q(email)}")
    acceso_gold_general(email)
    ejecutar(f"GRANT USE SCHEMA ON SCHEMA workspace.yelp_gov TO {q(email)}")
    for t in TABLAS_GOBIERNO:
        if spark.catalog.tableExists(f"workspace.yelp_gov.{t}"):
            ejecutar(f"GRANT SELECT ON TABLE workspace.yelp_gov.{t} TO {q(email)}")
    revocar("REVOKE ALL PRIVILEGES ON TABLE workspace.yelp_gov.dq_reglas", email)   # por si quedó de la versión anterior
    revocar("REVOKE ALL PRIVILEGES ON SCHEMA workspace.yelp_bronze", email)

# Consumidores: solo la gold de consumo general
for email in CONSUMIDORES:
    acceso_gold_general(email)
    for capa in ["bronze", "silver", "gov"]:
        revocar(f"REVOKE ALL PRIVILEGES ON SCHEMA workspace.yelp_{capa}", email)

# COMMAND ----------

# DBTITLE 1,Evidencia de permisos
# Si aparece un grupo amplio (por ejemplo 'account users' o 'users') con privilegios sobre el catálogo,
# los REVOKE por usuario no bastan: hay que revocarle a ese grupo, porque los permisos se heredan.
display(spark.sql("SHOW GRANTS ON CATALOG workspace"))
for capa in ["bronze", "silver", "gold", "gov"]:
    print(f"— workspace.yelp_{capa}")
    display(spark.sql(f"SHOW GRANTS ON SCHEMA workspace.yelp_{capa}"))
# Evidencia de CE-07: en las tablas restringidas solo deben aparecer custodians y steward
for t in sorted(TABLAS_GOLD_RESTRINGIDAS & set(TABLAS_GOLD)):
    print(f"— workspace.yelp_gold.{t} (Restringido)")
    display(spark.sql(f"SHOW GRANTS ON TABLE workspace.yelp_gold.{t}"))
if spark.catalog.tableExists("workspace.yelp_gov.dq_reglas"):
    print("— workspace.yelp_gov.dq_reglas (parámetro Restringido de DQ-016)")
    display(spark.sql("SHOW GRANTS ON TABLE workspace.yelp_gov.dq_reglas"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Control 3 — Enmascaramiento de datos personales (Confidencial)
# MAGIC Ven el valor real: quienes estén en Data Custodian o Data Steward (por correo) y los miembros del grupo
# MAGIC `data_stewards`, si existe (*Settings → Identity and access → Groups*). No es obligatorio crear el grupo.
# MAGIC
# MAGIC El custodian también ve el valor real: ejecuta el pipeline y las reglas de calidad (DQ-013, DQ-014 evalúan
# MAGIC nombres y textos), pero no consume los datos. Todos los demás ven el valor enmascarado.

# COMMAND ----------

# DBTITLE 1,Funciones de máscara y aplicación a las columnas
autorizados = ", ".join(f"'{e}'" for e in STEWARDS + CUSTODIANS)

ejecutar(f"""
CREATE OR REPLACE FUNCTION workspace.yelp_gov.mascara_nombre(nombre STRING)
RETURNS STRING
COMMENT 'Sección 5.3 · Confidencial: nombre en claro solo para data_stewards y el custodian del pipeline'
RETURN CASE
  WHEN is_account_group_member('data_stewards') OR current_user() IN ({autorizados}) THEN nombre
  WHEN nombre IS NULL THEN NULL
  ELSE concat(left(nombre, 1), '****')
END""")

ejecutar(f"""
CREATE OR REPLACE FUNCTION workspace.yelp_gov.mascara_texto(texto STRING)
RETURNS STRING
COMMENT 'Sección 5.3 · Confidencial: texto libre en claro solo para data_stewards y el custodian del pipeline'
RETURN CASE
  WHEN is_account_group_member('data_stewards') OR current_user() IN ({autorizados}) THEN texto
  WHEN texto IS NULL THEN NULL
  ELSE '[texto oculto: dato Confidencial]'
END""")

ejecutar("ALTER TABLE workspace.yelp_silver.slv_user   ALTER COLUMN name SET MASK workspace.yelp_gov.mascara_nombre")
ejecutar("ALTER TABLE workspace.yelp_silver.slv_review ALTER COLUMN text SET MASK workspace.yelp_gov.mascara_texto")
ejecutar("ALTER TABLE workspace.yelp_silver.slv_tip    ALTER COLUMN text SET MASK workspace.yelp_gov.mascara_texto")

# COMMAND ----------

# DBTITLE 1,Prueba de la máscara (ejecutar con dos usuarios distintos y capturar ambos resultados)
# MAGIC %sql
# MAGIC SELECT current_user() AS quien_consulta, u.user_id, u.name, left(r.text, 60) AS texto_resena
# MAGIC FROM workspace.yelp_silver.slv_user u
# MAGIC JOIN workspace.yelp_silver.slv_review r ON r.user_id = u.user_id
# MAGIC LIMIT 5;

# COMMAND ----------

# DBTITLE 1,Matriz RACI consultable (CE-02 y KPI de gobierno del dashboard)
# MAGIC %sql
# MAGIC INSERT OVERWRITE workspace.yelp_gov.matriz_raci VALUES
# MAGIC   ('business', 'Negocio',     'Andrew Collins (Producto)', 'Rafael Nakamura (Trust & Safety)', 'Equipo de Data Platform', 'Producto, Marketing, Operaciones Comerciales, Trust & Safety', 'Interno'),
# MAGIC   ('review',   'Contenido',   'Andrew Collins (Producto)', 'Rafael Nakamura (Trust & Safety)', 'Equipo de Data Platform', 'Trust & Safety, Data Science, Producto',                       'Confidencial'),
# MAGIC   ('user',     'Usuario',     'Andrew Collins (Producto)', 'Rafael Nakamura (Trust & Safety)', 'Equipo de Data Platform', 'Trust & Safety, Data Science',                                 'Confidencial'),
# MAGIC   ('checkin',  'Interacción', 'Andrew Collins (Producto)', 'Rafael Nakamura (Trust & Safety)', 'Equipo de Data Platform', 'Operaciones Comerciales, Trust & Safety',                      'Interno'),
# MAGIC   ('tip',      'Contenido',   'Andrew Collins (Producto)', 'Rafael Nakamura (Trust & Safety)', 'Equipo de Data Platform', 'Trust & Safety, Data Science',                                 'Confidencial');
# MAGIC
# MAGIC SELECT * FROM workspace.yelp_gov.matriz_raci ORDER BY entidad;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Control 4 — Evidencia de lineage y auditoría
# MAGIC 1. En **Catalog Explorer**, abrir la tabla gold de reseñas (la definida en 4.3) → pestaña **Lineage** → *See lineage graph*.
# MAGIC    Debe verse `brz_review → slv_review → tabla gold` sin saltos (criterio CE-01). Guardar captura en `img/`.
# MAGIC 2. Repetir para `slv_check_in → gld_checkins_diarios` (Operaciones Comerciales).
# MAGIC 3. Esto responde al dolor de Trust & Safety: "reglas de fraude sin linaje claro" (MOT-01).
# MAGIC
# MAGIC ### Definition of Done (E6)
# MAGIC - [ ] Tags de clasificación aplicados a schemas, tablas y columnas (celda de evidencia).
# MAGIC - [ ] GRANTs ejecutados + captura de Leila (Data Owner) intentando, y fallando, leer bronze.
# MAGIC - [ ] CE-07: captura de Juan Carlos o Edgar intentando, y fallando, leer `gld_dim_usuario_riesgo`, y de David leyéndola.
# MAGIC - [ ] Captura de Leila leyendo `vw_dq_reglas_publica` (DQ-016 aparece como `[Restringido]`) y fallando al leer `dq_reglas`.
# MAGIC - [ ] Máscara funcionando: captura del mismo SELECT con Leila (Data Owner, enmascarado) y con David (Data Steward, en claro).
# MAGIC - [ ] Matriz RACI cargada en `yelp_gov.matriz_raci`.
# MAGIC - [ ] Lineage de `review` documentado con capturas.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Control 4 — Evidencia de lineage y auditoría
# MAGIC > ✏️ TODO — Insertar en `img/` capturas del lineage end-to-end de Catalog Explorer
# MAGIC > (raw → bronze → silver → gold → dashboard) para la entidad `review`, respondiendo
# MAGIC > directamente al dolor de Trust & Safety: "reglas de fraude sin linaje claro".
# MAGIC
# MAGIC ### Definition of Done (E6)
# MAGIC - [ ] Tags de clasificación aplicados a schemas y columnas PII.
# MAGIC - [ ] GRANTs ejecutados + captura de un usuario "Marketing" intentando (y fallando) leer bronze.
# MAGIC - [ ] Máscara PII funcionando: captura del mismo SELECT con dos usuarios distintos.
# MAGIC - [ ] Lineage de `review` documentado con capturas.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Control 4 — Evidencia de lineage y auditoría
# MAGIC > ✏️ TODO — Insertar en `img/` capturas del lineage end-to-end de Catalog Explorer
# MAGIC > (raw → bronze → silver → gold → dashboard) para la entidad `review`, respondiendo
# MAGIC > directamente al dolor de Trust & Safety: "reglas de fraude sin linaje claro".
# MAGIC
# MAGIC ### Definition of Done (E6)
# MAGIC - [ ] Tags de clasificación aplicados a schemas y columnas PII.
# MAGIC - [ ] GRANTs ejecutados + captura de un usuario "Marketing" intentando (y fallando) leer bronze.
# MAGIC - [ ] Máscara PII funcionando: captura del mismo SELECT con dos usuarios distintos.
# MAGIC - [ ] Lineage de `review` documentado con capturas.