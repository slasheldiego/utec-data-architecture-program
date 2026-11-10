# Databricks notebook source
# MAGIC %md
# MAGIC # E3 · Modelo Físico — DDL sobre Delta Lake / Unity Catalog
# MAGIC **Cierra con:** M4 Modelado Avanzado · Se implementa en `yelp_silver` (conformado) y `yelp_gold` (dimensional).
# MAGIC
# MAGIC Reglas del entregable:
# MAGIC 1. **Toda tabla y toda columna llevan `COMMENT`** — ese metadata alimenta E4 (diccionario) automáticamente.
# MAGIC 2. Declarar constraints informativas de UC: `PRIMARY KEY` / `FOREIGN KEY` (no se
# MAGIC    enforcan, pero documentan el modelo y habilitan optimizaciones y lineage).
# MAGIC 3. Constraints sí enforzadas donde aplique: `NOT NULL` y `CHECK`.
# MAGIC
# MAGIC Abajo va UNA tabla completa de ejemplo; el grupo completa el resto según su modelo lógico.
# MAGIC
# MAGIC > **Trazabilidad —** este entregable **implementa** la sección «Modelos conceptual y lógico» del
# MAGIC > **Documento Formal de Arquitectura de Datos**. Este notebook es la materialización física de ese modelo: cada entidad y atributo del modelo lógico debe existir aquí como tabla y columna con su COMMENT.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- =====================================================================================
# MAGIC -- E3 · Modelo físico (capa silver) — Grupo 06 · Caso Trust & Safety
# MAGIC -- Notebook destino: E3_modelo_fisico/01_modelo_fisico_ddl (una celda %sql por tabla)
# MAGIC -- Coherente con: Diccionario_de_Datos_YelpConnect_Grupo06.xlsx (28 columnas),
# MAGIC --                Libro de Gobierno (Clasificación por atributo) y sección 4 del Documento Formal.
# MAGIC -- Notas:
# MAGIC --   * PRIMARY KEY / FOREIGN KEY son informativas en Unity Catalog (no se enforzan).
# MAGIC --   * NOT NULL sí se enforza.
# MAGIC --   * CHECK sí existe en Delta y se agrega sobre la tabla ya creada con ALTER TABLE ... ADD CONSTRAINT.
# MAGIC --     Decisión vigente (Anexo C del Documento Formal): NO se declara CHECK sobre stars; el rango 1-5 lo garantiza
# MAGIC --     DQ-001 enviando a cuarentena. Ver la celda opcional al final del DDL.
# MAGIC --     La validación de rango (p.ej. stars 1-5) se garantiza en el pipeline silver antes de llegar aquí.
# MAGIC --   * Los COMMENT son los que el notebook E4 toma como "Descripción de Negocio".
# MAGIC --   * `date` es palabra reservada en SQL: se escribe siempre entre comillas invertidas.
# MAGIC -- =====================================================================================
# MAGIC
# MAGIC -- ------------------------------- slv_business -------------------------------------
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_silver.slv_business (
# MAGIC   business_id   STRING        NOT NULL COMMENT 'Código único que identifica a cada negocio registrado en YelpConnect.',
# MAGIC   name          STRING                 COMMENT 'Nombre con el que el negocio aparece en la app.',
# MAGIC   address       STRING                 COMMENT 'Dirección física publicada en la ficha del negocio.',
# MAGIC   stars         DECIMAL(2,1)           COMMENT 'Calificación promedio publicada en la ficha del negocio, redondeada a medias estrellas.',
# MAGIC   review_count  INT                    COMMENT 'Número de reseñas que la app muestra para el negocio.',
# MAGIC   is_open       INT                    COMMENT 'Indica si el negocio figura abierto (1) o cerrado (0) en la plataforma.',
# MAGIC   categories    STRING                 COMMENT 'Rubros o tipos de actividad del negocio, separados por comas.',
# MAGIC   CONSTRAINT pk_slv_business PRIMARY KEY (business_id)
# MAGIC )
# MAGIC COMMENT 'Negocios de la muestra de Philadelphia — capa conformada. Data Owner: Producto (Andrew Collins). Steward: Rafael Nakamura (Trust & Safety).'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC -- --------------------------------- slv_user ---------------------------------------
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_silver.slv_user (
# MAGIC   user_id            STRING        NOT NULL COMMENT 'Código único de la cuenta de un usuario de YelpConnect.',
# MAGIC   name               STRING                 COMMENT 'Nombre de pila con el que el usuario aparece en sus contribuciones. Dato personal: se enmascara con mascara_nombre.',
# MAGIC   review_count       INT                    COMMENT 'Total de reseñas que el perfil del usuario declara haber publicado.',
# MAGIC   yelping_since      DATE                   COMMENT 'Fecha en que el usuario creó su cuenta en la plataforma.',
# MAGIC   useful             INT                    COMMENT 'Votos útiles que otros usuarios dieron al conjunto de la actividad del usuario.',
# MAGIC   average_stars      DECIMAL(3,2)           COMMENT 'Promedio de las calificaciones que el usuario ha dado en sus reseñas.',
# MAGIC   compliment_writer  INT                    COMMENT 'Cantidad de reconocimientos que el usuario recibió por la calidad de sus textos.',
# MAGIC   CONSTRAINT pk_slv_user PRIMARY KEY (user_id)
# MAGIC )
# MAGIC COMMENT 'Usuarios que publicaron reseñas o tips en la muestra — capa conformada. Contiene datos personales. Data Owner: Producto (Andrew Collins); accesos aprobados por Trust & Safety (Rafael Nakamura).'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC -- -------------------------------- slv_review --------------------------------------
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_silver.slv_review (
# MAGIC   review_id    STRING  NOT NULL COMMENT 'Código único de cada reseña publicada.',
# MAGIC   user_id      STRING           COMMENT 'Usuario que publicó la reseña.',
# MAGIC   business_id  STRING           COMMENT 'Negocio sobre el que trata la reseña.',
# MAGIC   stars        INT              COMMENT 'Puntuación de 1 a 5 estrellas que el usuario asigna al negocio en su reseña.',
# MAGIC   `date`       DATE             COMMENT 'Día en que se publicó la reseña.',
# MAGIC   text         STRING           COMMENT 'Opinión escrita por el usuario; puede contener datos personales suyos o de terceros.',
# MAGIC   useful       INT              COMMENT 'Cantidad de usuarios que marcaron la reseña como útil.',
# MAGIC   CONSTRAINT pk_slv_review PRIMARY KEY (review_id),
# MAGIC   CONSTRAINT fk_review_user     FOREIGN KEY (user_id)     REFERENCES workspace.yelp_silver.slv_user (user_id),
# MAGIC   CONSTRAINT fk_review_business FOREIGN KEY (business_id) REFERENCES workspace.yelp_silver.slv_business (business_id)
# MAGIC )
# MAGIC COMMENT 'Reseñas de la muestra — capa conformada, sin duplicados de review_id. Data Owner: Producto (Andrew Collins); accesos aprobados por Trust & Safety (Rafael Nakamura).'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC -- --------------------------------- slv_tip ----------------------------------------
# MAGIC -- Sin clave primaria: el origen no entrega identificador de tip. Se documenta como decisión en 4.2.
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_silver.slv_tip (
# MAGIC   user_id           STRING  COMMENT 'Usuario que publicó el tip.',
# MAGIC   business_id       STRING  COMMENT 'Negocio sobre el que trata el tip.',
# MAGIC   text              STRING  COMMENT 'Comentario breve del usuario; puede contener datos personales.',
# MAGIC   `date`            DATE    COMMENT 'Día en que se publicó el tip.',
# MAGIC   compliment_count  INT     COMMENT 'Cantidad de reconocimientos que recibió el tip.',
# MAGIC   CONSTRAINT fk_tip_user     FOREIGN KEY (user_id)     REFERENCES workspace.yelp_silver.slv_user (user_id),
# MAGIC   CONSTRAINT fk_tip_business FOREIGN KEY (business_id) REFERENCES workspace.yelp_silver.slv_business (business_id)
# MAGIC )
# MAGIC COMMENT 'Tips de la muestra — capa conformada. Data Owner: Producto (Andrew Collins); accesos aprobados por Trust & Safety (Rafael Nakamura).'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC -- ------------------------------- slv_check_in -------------------------------------
# MAGIC -- Grano evento: 1 fila = 1 check-in. El origen trae todas las marcas en un solo texto separado por comas;
# MAGIC -- el pipeline silver lo separa (split + explode) y conserva fecha y hora para poder distinguir visitas del mismo día.
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_silver.slv_check_in (
# MAGIC   business_id  STRING     NOT NULL COMMENT 'Negocio en el que se registró la visita.',
# MAGIC   `date`       TIMESTAMP  NOT NULL COMMENT 'Momento en que se registró la visita al negocio.',
# MAGIC   CONSTRAINT pk_slv_check_in PRIMARY KEY (business_id, `date`),
# MAGIC   CONSTRAINT fk_check_in_business FOREIGN KEY (business_id) REFERENCES workspace.yelp_silver.slv_business (business_id)
# MAGIC )
# MAGIC COMMENT 'Check-ins de la muestra a grano evento — capa conformada. Fuente del volumen diario oficial de check-ins. Data Owner: Producto (Andrew Collins).'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# DBTITLE 1,(Opcional, no adoptado) CHECK preventivo sobre slv_review.stars
# MAGIC %sql
# MAGIC -- Hoy el documento dice que el DDL NO declara CHECK (Anexo C). Si el grupo decide agregarlo como
# MAGIC -- calidad preventiva además de DQ-001 (detectiva), quitar los comentarios y actualizar el Anexo C.
# MAGIC -- Va con ALTER TABLE: en Delta el CHECK se agrega sobre la tabla ya creada (y valida las filas que ya tiene).
# MAGIC -- ALTER TABLE workspace.yelp_silver.slv_review DROP CONSTRAINT IF EXISTS chk_review_stars;
# MAGIC -- ALTER TABLE workspace.yelp_silver.slv_review ADD CONSTRAINT chk_review_stars CHECK (stars BETWEEN 1 AND 5);
# MAGIC SELECT 'CHECK no adoptado: el rango de stars se controla con DQ-001 + cuarentena (Anexo C)' AS decision;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Capa GOLD dimensional (sección 4.3 · esquema estrella)
# MAGIC Mismo patrón que silver: aquí se define el molde (tipos, NOT NULL, COMMENT, PK/FK y retención de 30 días, ADR-04)
# MAGIC y **E7/03 solo lo llena** con `insertInto`. Columnas, tipos y COMMENT salen del Diccionario de Datos (Grupo06): 51 en total.
# MAGIC
# MAGIC | Rol en la estrella | Tabla | Grano (1 fila =) | Acceso |
# MAGIC |---|---|---|---|
# MAGIC | Dimensión | `gld_dim_tiempo` | un día | General |
# MAGIC | Dimensión | `gld_dim_negocio` | un negocio | General |
# MAGIC | Dimensión | `gld_dim_usuario_riesgo` | una cuenta | **Restringido** |
# MAGIC | Hecho | `gld_salud_resenas` | una reseña | **Restringido** |
# MAGIC | Hecho agregado | `gld_kpi_resenas_sospechosas_negocio` | un negocio en un mes | General |
# MAGIC | Hecho agregado | `gld_checkins_diarios` | un negocio en un día | General |
# MAGIC | Hecho agregado | `gld_kpi_calidad_datos` | una regla en una ejecución | General |
# MAGIC | Vista | `vw_rating_verificado_negocio` | un negocio en un mes | General |
# MAGIC
# MAGIC Las FK hacia `gld_dim_usuario_riesgo` y `gld_dim_negocio` usan las claves del origen (`user_id`, `business_id`): con SCD tipo 1
# MAGIC no hace falta clave sustituta. `gld_kpi_calidad_datos` no declara FK a `yelp_gov.dq_reglas` porque esa tabla no tiene PK.

# COMMAND ----------

# DBTITLE 1,DDL · gld_dim_tiempo
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_gold.gld_dim_tiempo (
# MAGIC   fecha             DATE    NOT NULL COMMENT 'Día del calendario; una fila por día desde la primera hasta la última fecha de la muestra.',
# MAGIC   anio              INT     NOT NULL COMMENT 'Año calendario de la fecha.',
# MAGIC   mes               INT     NOT NULL COMMENT 'Número de mes de la fecha.',
# MAGIC   dia_semana        INT     NOT NULL COMMENT 'Día de la semana de la fecha (1 = domingo, 7 = sábado).',
# MAGIC   es_fin_de_semana  BOOLEAN NOT NULL COMMENT 'TRUE si la fecha cae sábado o domingo.',
# MAGIC   CONSTRAINT pk_gld_dim_tiempo PRIMARY KEY (fecha)
# MAGIC )
# MAGIC COMMENT 'Calendario de la muestra; una fila por día (estática). Consumo general. Data Owner: Andrew Collins (Producto).'
# MAGIC TBLPROPERTIES ('delta.logRetentionDuration' = 'interval 30 days',
# MAGIC                'delta.deletedFileRetentionDuration' = 'interval 30 days')   -- ADR-04;

# COMMAND ----------

# DBTITLE 1,DDL · gld_dim_negocio
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_gold.gld_dim_negocio (
# MAGIC   business_id  STRING       NOT NULL COMMENT 'Negocio de la muestra; una fila por negocio (SCD tipo 1).',
# MAGIC   name         STRING       NOT NULL COMMENT 'Nombre con el que el negocio aparece en la app.',
# MAGIC   categories   STRING                COMMENT 'Rubros o tipos de actividad del negocio, separados por comas.',
# MAGIC   stars        DECIMAL(2,1)          COMMENT 'Calificación promedio que la app muestra en la ficha del negocio.',
# MAGIC   is_open      INT          NOT NULL COMMENT 'Indica si el negocio figura abierto (1) o cerrado (0).',
# MAGIC   CONSTRAINT pk_gld_dim_negocio PRIMARY KEY (business_id)
# MAGIC )
# MAGIC COMMENT 'Dimensión de negocios; una fila por negocio (SCD tipo 1). Consumo general. Data Owner: Andrew Collins (Producto).'
# MAGIC TBLPROPERTIES ('delta.logRetentionDuration' = 'interval 30 days',
# MAGIC                'delta.deletedFileRetentionDuration' = 'interval 30 days')   -- ADR-04;

# COMMAND ----------

# DBTITLE 1,DDL · gld_dim_usuario_riesgo
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_gold.gld_dim_usuario_riesgo (
# MAGIC   user_id             STRING        NOT NULL COMMENT 'Cuenta evaluada por las señales antifraude; una fila por usuario.',
# MAGIC   yelping_since       DATE                   COMMENT 'Fecha en que el usuario creó su cuenta en la plataforma.',
# MAGIC   n_resenas_cargadas  BIGINT        NOT NULL COMMENT 'Reseñas del usuario que existen en la muestra.',
# MAGIC   n_senales           INT           NOT NULL COMMENT 'Cantidad de señales antifraude distintas que activó la cuenta.',
# MAGIC   senales             ARRAY<STRING> NOT NULL COMMENT 'Lista de las señales antifraude distintas que activó la cuenta en sus reseñas o en su perfil.',
# MAGIC   nivel_riesgo        STRING        NOT NULL COMMENT 'Prioridad de revisión de la cuenta según sus señales; no es una sanción.',
# MAGIC   version_reglas      BIGINT        NOT NULL COMMENT 'Versión del catálogo de reglas con la que se calculó el nivel de riesgo.',
# MAGIC   ejecucion_ts        TIMESTAMP     NOT NULL COMMENT 'Momento de la corrida que calculó el nivel de riesgo.',
# MAGIC   CONSTRAINT pk_gld_dim_usuario_riesgo PRIMARY KEY (user_id)
# MAGIC )
# MAGIC COMMENT 'Nivel de riesgo vigente de cada cuenta según las señales de la sección 6.4 (SCD tipo 1). RESTRINGIDA a Trust & Safety (data_stewards). Es prioridad de revisión, no sanción.'
# MAGIC TBLPROPERTIES ('delta.logRetentionDuration' = 'interval 30 days',
# MAGIC                'delta.deletedFileRetentionDuration' = 'interval 30 days')   -- ADR-04;

# COMMAND ----------

# DBTITLE 1,DDL · gld_salud_resenas
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_gold.gld_salud_resenas (
# MAGIC   review_id        STRING        NOT NULL COMMENT 'Reseña evaluada por las señales antifraude; la tabla tiene una fila por reseña que llegó a gold.',
# MAGIC   business_id      STRING        NOT NULL COMMENT 'Negocio sobre el que trata la reseña.',
# MAGIC   user_id          STRING        NOT NULL COMMENT 'Usuario que publicó la reseña. Solo existe en la parte de gold restringida a Trust & Safety.',
# MAGIC   fecha_resena     DATE          NOT NULL COMMENT 'Día en que se publicó la reseña.',
# MAGIC   stars            INT           NOT NULL COMMENT 'Puntuación de 1 a 5 estrellas que el usuario asigna al negocio.',
# MAGIC   es_sospechosa    BOOLEAN       NOT NULL COMMENT 'Indica si la reseña activó al menos una señal antifraude de la sección 6.4 del Documento Formal. Es una marca para revisión, no una sanción.',
# MAGIC   regla_disparada  ARRAY<STRING> NOT NULL COMMENT 'Lista de todas las reglas antifraude que activó la reseña, no solo la primera.',
# MAGIC   es_verificada    BOOLEAN       NOT NULL COMMENT 'Indica si la reseña no tiene ninguna marca de sospecha. Solo las reseñas verificadas computan en el rating y en las métricas oficiales de Producto y Marketing.',
# MAGIC   version_reglas   BIGINT        NOT NULL COMMENT 'Versión del catálogo de reglas con la que se calcularon las marcas de la fila.',
# MAGIC   ejecucion_ts     TIMESTAMP     NOT NULL COMMENT 'Momento de la corrida del pipeline que calculó la marca; coincide con ejecucion_ts de dq_resultados.',
# MAGIC   CONSTRAINT pk_gld_salud_resenas PRIMARY KEY (review_id),
# MAGIC   CONSTRAINT fk_salud_resenas_business_id FOREIGN KEY (business_id) REFERENCES workspace.yelp_gold.gld_dim_negocio (business_id),
# MAGIC   CONSTRAINT fk_salud_resenas_user_id FOREIGN KEY (user_id) REFERENCES workspace.yelp_gold.gld_dim_usuario_riesgo (user_id),
# MAGIC   CONSTRAINT fk_salud_resenas_fecha_resena FOREIGN KEY (fecha_resena) REFERENCES workspace.yelp_gold.gld_dim_tiempo (fecha)
# MAGIC )
# MAGIC COMMENT 'Hecho principal: una fila por reseña con sus marcas de sospecha, la versión de las reglas y la ejecución que las generó (CE-03, CE-06). RESTRINGIDA a Trust & Safety (data_stewards). Consumidor: Rafael Nakamura.'
# MAGIC TBLPROPERTIES ('delta.logRetentionDuration' = 'interval 30 days',
# MAGIC                'delta.deletedFileRetentionDuration' = 'interval 30 days')   -- ADR-04;

# COMMAND ----------

# DBTITLE 1,DDL · gld_kpi_resenas_sospechosas_negocio
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_gold.gld_kpi_resenas_sospechosas_negocio (
# MAGIC   business_id                   STRING       NOT NULL COMMENT 'Negocio al que corresponde el indicador mensual.',
# MAGIC   mes                           DATE         NOT NULL COMMENT 'Mes del indicador, representado por su primer día.',
# MAGIC   n_resenas                     BIGINT       NOT NULL COMMENT 'Reseñas del negocio publicadas en el mes que llegaron a gold.',
# MAGIC   n_sospechosas                 BIGINT       NOT NULL COMMENT 'Reseñas del negocio en el mes marcadas como sospechosas. Es un conteo agregado que no identifica reseñas ni cuentas.',
# MAGIC   pct_sospechosas               DECIMAL(5,2) NOT NULL COMMENT 'Proporción de reseñas sospechosas sobre el total de reseñas del negocio en el mes.',
# MAGIC   pct_sospechosas_mes_anterior  DECIMAL(5,2)          COMMENT 'Porcentaje de reseñas sospechosas del mismo negocio en el mes anterior; vacío si no tuvo reseñas ese mes.',
# MAGIC   CONSTRAINT pk_gld_kpi_resenas_sospechosas_negocio PRIMARY KEY (business_id, mes),
# MAGIC   CONSTRAINT fk_kpi_resenas_sospechosas_negocio_business_id FOREIGN KEY (business_id) REFERENCES workspace.yelp_gold.gld_dim_negocio (business_id),
# MAGIC   CONSTRAINT fk_kpi_resenas_sospechosas_negocio_mes FOREIGN KEY (mes) REFERENCES workspace.yelp_gold.gld_dim_tiempo (fecha)
# MAGIC )
# MAGIC COMMENT 'Reseñas sospechosas por negocio y mes, solo conteos (no identifica reseñas ni cuentas). Responde REQ-01. Consumo general.'
# MAGIC TBLPROPERTIES ('delta.logRetentionDuration' = 'interval 30 days',
# MAGIC                'delta.deletedFileRetentionDuration' = 'interval 30 days')   -- ADR-04;

# COMMAND ----------

# DBTITLE 1,DDL · gld_checkins_diarios
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_gold.gld_checkins_diarios (
# MAGIC   business_id  STRING NOT NULL COMMENT 'Negocio que recibió las visitas del día.',
# MAGIC   fecha        DATE   NOT NULL COMMENT 'Día al que corresponde el conteo de visitas.',
# MAGIC   n_checkins   BIGINT NOT NULL COMMENT 'Cantidad de check-ins válidos que recibió el negocio en el día.',
# MAGIC   CONSTRAINT pk_gld_checkins_diarios PRIMARY KEY (business_id, fecha),
# MAGIC   CONSTRAINT fk_checkins_diarios_business_id FOREIGN KEY (business_id) REFERENCES workspace.yelp_gold.gld_dim_negocio (business_id),
# MAGIC   CONSTRAINT fk_checkins_diarios_fecha FOREIGN KEY (fecha) REFERENCES workspace.yelp_gold.gld_dim_tiempo (fecha)
# MAGIC )
# MAGIC COMMENT 'Volumen diario oficial de check-ins válidos por negocio. Responde REQ-07 (Carla Romero, Ads Strategy). Consumo general.'
# MAGIC TBLPROPERTIES ('delta.logRetentionDuration' = 'interval 30 days',
# MAGIC                'delta.deletedFileRetentionDuration' = 'interval 30 days')   -- ADR-04;

# COMMAND ----------

# DBTITLE 1,DDL · gld_kpi_calidad_datos
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS workspace.yelp_gold.gld_kpi_calidad_datos (
# MAGIC   ejecucion_ts      TIMESTAMP NOT NULL COMMENT 'Momento de la corrida del motor de calidad.',
# MAGIC   regla_id          STRING    NOT NULL COMMENT 'Identificador de la regla evaluada.',
# MAGIC   dimension         STRING    NOT NULL COMMENT 'Dimensión de calidad que evalúa la regla.',
# MAGIC   criticidad        STRING    NOT NULL COMMENT 'Criticidad de la regla; las de criticidad alta detienen el pipeline si fallan sin excepción vigente.',
# MAGIC   umbral_pct        DOUBLE    NOT NULL COMMENT 'Porcentaje mínimo de registros que deben cumplir la regla. No es el parámetro de una señal antifraude.',
# MAGIC   pct_cumplimiento  DOUBLE    NOT NULL COMMENT 'Porcentaje de registros que cumplieron la regla en la ejecución.',
# MAGIC   paso              BOOLEAN   NOT NULL COMMENT 'TRUE si el porcentaje de cumplimiento alcanza el umbral de la regla.',
# MAGIC   con_excepcion     BOOLEAN   NOT NULL COMMENT 'TRUE si la regla tenía una excepción aprobada y vigente en la fecha de ejecución. La regla sigue figurando como fallida.',
# MAGIC   CONSTRAINT pk_gld_kpi_calidad_datos PRIMARY KEY (ejecucion_ts, regla_id)
# MAGIC )
# MAGIC COMMENT 'Salud de la calidad de datos: una fila por regla y ejecución; solo se agregan filas (CE-04). Responde REQ-04 (Laura Hernández, CDO). Consumo general.'
# MAGIC TBLPROPERTIES ('delta.logRetentionDuration' = 'interval 30 days',
# MAGIC                'delta.deletedFileRetentionDuration' = 'interval 30 days')   -- ADR-04;

# COMMAND ----------

# DBTITLE 1,DDL · vw_rating_verificado_negocio (vista; se llena sola cuando E7/03 carga gld_salud_resenas)
# MAGIC %sql
# MAGIC -- La vista lee gld_salud_resenas (Restringido), pero solo expone conteos y promedios por negocio y mes.
# MAGIC -- En Unity Catalog, quien consulta la vista necesita SELECT sobre la vista, no sobre la tabla de origen.
# MAGIC CREATE OR REPLACE VIEW workspace.yelp_gold.vw_rating_verificado_negocio (
# MAGIC   business_id            COMMENT 'Negocio al que corresponde el rating calculado; una fila por negocio y mes.',
# MAGIC   mes                    COMMENT 'Mes del cálculo, representado por su primer día.',
# MAGIC   n_resenas_verificadas  COMMENT 'Reseñas verificadas del negocio en el mes. Es un conteo agregado que no identifica reseñas ni cuentas.',
# MAGIC   rating_verificado      COMMENT 'Promedio de las calificaciones de las reseñas verificadas del negocio en el mes.',
# MAGIC   rating_publicado       COMMENT 'Calificación promedio que la app muestra en la ficha del negocio.',
# MAGIC   brecha_rating          COMMENT 'Diferencia entre el rating publicado y el rating calculado con reseñas verificadas. Un valor positivo indica que el rating publicado es mayor.'
# MAGIC )
# MAGIC COMMENT 'Rating calculado solo con reseñas verificadas frente al rating publicado, por negocio y mes. Responde REQ-05 (Andrew Collins) y REQ-06 (Jessica Martinez). Consumo general.'
# MAGIC AS
# MAGIC SELECT s.business_id,
# MAGIC        trunc(s.fecha_resena, 'MM')                                                   AS mes,
# MAGIC        count_if(s.es_verificada)                                                     AS n_resenas_verificadas,
# MAGIC        CAST(avg(CASE WHEN s.es_verificada THEN s.stars END) AS DECIMAL(3,2))         AS rating_verificado,
# MAGIC        n.stars                                                                       AS rating_publicado,
# MAGIC        CAST(n.stars - avg(CASE WHEN s.es_verificada THEN s.stars END) AS DECIMAL(3,2)) AS brecha_rating
# MAGIC FROM workspace.yelp_gold.gld_salud_resenas s
# MAGIC JOIN workspace.yelp_gold.gld_dim_negocio n USING (business_id)
# MAGIC GROUP BY s.business_id, trunc(s.fecha_resena, 'MM'), n.stars
# MAGIC HAVING count_if(s.es_verificada) >= 1;

# COMMAND ----------

# DBTITLE 1: Validación automática del entregable (correr antes del PR)
tablas = spark.sql("SHOW TABLES IN workspace.yelp_silver").collect()
assert len(tablas) >= 5, "Deben existir al menos las 5 entidades en silver"
gold = spark.sql("SHOW TABLES IN workspace.yelp_gold").collect()
assert len(gold) >= 8, f"Deben existir las 7 tablas gold y la vista (hay {len(gold)})"

sin_comment = spark.sql("""
  SELECT table_name, column_name
  FROM workspace.information_schema.columns
  WHERE table_schema IN ('yelp_silver','yelp_gold')
    AND (comment IS NULL OR comment = '')
""").collect()
assert not sin_comment, f"❌ Columnas sin COMMENT (rompe E4): {[(r.table_name, r.column_name) for r in sin_comment]}"
print(f"✔ Modelo físico validado: {len(tablas)} tablas silver y {len(gold)} objetos gold, todas las columnas con COMMENT")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definition of Done (E3)
# MAGIC - [x] DDL completo de silver (5 entidades) y gold (esquema estrella: 3 dimensiones, 1 hecho, 3 hechos agregados y 1 vista).
# MAGIC - [x] 100% de columnas con COMMENT (la celda de validación pasa).
# MAGIC - [x] PK/FK declaradas en silver y gold. CHECK sobre `stars` no adoptado: el rango lo garantiza DQ-001 con cuarentena (Anexo C); la celda opcional lo deja listo si el grupo lo decide.
# MAGIC - [ ] Captura del diagrama de lineage/ER de Catalog Explorer en `img/`.