# Plataforma de Datos YelpConnect — Grupo 06 (Ecodata)
### Programa de Arquitectura de Datos · UTEC · Caso Trust & Safety: detección de reseñas falsas

Implementación en Databricks Free Edition del Documento Formal de Arquitectura de Datos del Grupo 06.
Muestra: Philadelphia (14.577 negocios, 400.000 reseñas, 150.000 usuarios).

**Integrantes:** 
- Leila (Arquitecta de datos)
- David (Steward de gobierno y calidad)
- Fiorela (Ingeniera de datos)
- Juan Carlos (Analista de BI y valor)
- Edgar (Modelador de datos)

## Notebooks del Grupo 06

Los notebooks del grupo son los que terminan en **`_grupo_06`** (en E3: `_grupo6`). Los que no tienen sufijo
son el esqueleto original del curso y se conservan solo como referencia.

| Carpeta | Notebook del grupo | Qué deja en el catálogo |
|---|---|---|
| 00_setup | `00_configurar_lakehouse_grupo_06`, `01_verificar_datos_raw_grupo_06` | Schemas, volumes y tablas de control |
| E3 | `01_modelo_fisico_ddl_grupo6` | DDL de silver (5 tablas) y de gold (7 tablas + 1 vista), con COMMENT y PK/FK |
| E4 | `01_glosario_negocio_grupo_06`, `02_diccionario_datos_grupo_06` , `03_golden_record_negocio_grupo_06`| `glosario_negocio` (29 términos), `diccionario_datos` (79 elementos) y tags de clasificación, mdm_negocio_correspondencia y mdm_negocio_golden (golden record de negocios). |
| E5 | `01_reglas_calidad_grupo_06` | `dq_reglas` (19 reglas), `dq_resultados`, `dq_excepciones`, `vw_dq_reglas_publica` |
| E6 | `01_controles_unity_catalog_grupo_06` | Permisos por rol, máscaras de datos personales, `matriz_raci` |
| E7 | `01_bronze_ingesta` → `02_silver_limpieza` → `03_gold_productos_datos` → `04_job_orquestacion` (todos `_grupo_06`) | Bronze, silver con cuarentena, gold, job diario y `pipeline_bitacora` |
| E8 | `01_consultas_base_dashboard_grupo_06` | Consultas del tablero (2 páginas) y del Genie Space |
| EX | `01_analisis_ia_resenas_grupo_06` | `yelp_gov.ex_resenas_enriquecidas` (bonus) |

Las capturas de evidencia están en `img/`.

> **Importante — dónde vive cada entregable.**
> El Capstone tiene **dos soportes complementarios**:
>
> | Soporte | Qué contiene | Naturaleza |
> |---|---|---|
> | **Documento Formal de Arquitectura de Datos** | Visión y SoAW, arquitectura de referencia (ABB/SBB), modelos conceptual y lógico, estrategia de gobierno y matriz RACI, definición de reglas en lenguaje de negocio, requerimientos de información | Diseño, definiciones y conceptual |
> | **Este repositorio (Databricks)** | Modelo físico, glosario y diccionario operativos, motor de calidad, controles técnicos, pipeline, dashboard | Implementación y evidencia ejecutable |
>
> Regla de oro: **el documento define QUÉ y POR QUÉ; la plataforma demuestra QUE SE CUMPLE.**
> Ningún notebook rehace el diseño: cada uno declara en su cabecera qué sección del
> documento formal implementa (bloque **Trazabilidad**).

---

## 1. Setup inicial del grupo (Semana 0 / Onboarding)

1. **Un integrante crea la cuenta del grupo** en [Databricks Free Edition](https://www.databricks.com/learn/free-edition)
   (correo del grupo, no personal). Free Edition permite **un solo workspace y un
   metastore por cuenta**, así que la cuenta es del equipo, no de una persona.
2. Invitar a los 5 integrantes como usuarios del workspace
   (`Settings → Identity and access → Users`).
3. Crear un repositorio **fork/clon de este esqueleto** en GitHub
   (nombre sugerido: `capstone-grupoNN-yelpconnect`).
4. En Databricks: `Workspace → Create → Git folder` y conectar el repo del grupo.
5. Ejecutar `00_setup/00_configurar_lakehouse` — crea schemas, volumes y tablas de control.
6. Preparar la muestra de datos con `datos/preparar_muestra_yelp.py` (se corre
   **en tu laptop**, no en Databricks) y subir los archivos resultantes al Volume.
7. Ejecutar `00_setup/01_verificar_datos_raw` para validar que todo está en su lugar.

## 2. Convenciones del Lakehouse (obligatorias — se evalúan)

| Elemento | Convención |
|---|---|
| Catálogo | `workspace` (el default de Free Edition) |
| Schemas | `yelp_bronze`, `yelp_silver`, `yelp_gold`, `yelp_gov` |
| Volume de datos crudos | `/Volumes/workspace/yelp_bronze/raw/` |
| Volume de artefactos de gobierno | `/Volumes/workspace/yelp_gov/artefactos/` |
| Tablas bronze | `brz_<entidad>` (ej. `brz_business`) |
| Tablas silver | `slv_<entidad>` |
| Tablas gold | `gld_<producto_de_datos>` (ej. `gld_kpi_resenas_negocio`) |
| Vistas | `vw_<nombre>` |
| Reglas de calidad | `DQ-nnn`, correlativas y únicas |
| Idioma de comentarios/metadata | Español (o inglés, pero consistente) |
| Ramas Git | `main` protegida; trabajar en `feature/E<k>-<tema>` y merge por PR |

## 3. Mapa entregable ↔ carpeta ↔ módulo

Los entregables **E1, E2 y las definiciones conceptuales de E3 y E6** no viven aquí:
son secciones del Documento Formal. Lo que sigue es lo que se despliega en la plataforma.

| Entregable ejecutable | Carpeta | Módulo que lo cierra | Sección del Documento Formal que implementa |
|---|---|---|---|
| E3. Modelo físico (DDL sobre Delta / UC) | `E3_modelo_fisico` | M4 Modelado Avanzado | Modelos conceptual y lógico |
| E4. Glosario y Diccionario operativos | `E4_diccionario_glosario` | M3 MDM y Metadata | Anexos de glosario y diccionario |
| E5. Motor de reglas de calidad | `E5_calidad_datos` | M2/M3 (definición) · M6 (ejecución) | Reglas de calidad en lenguaje de negocio |
| E6. Controles técnicos de gobierno | `E6_controles_gobierno` | M2 (diseño) · M5 (implementación) | Estrategia de gobierno, roles y RACI |
| E7. Pipeline de ingesta y habilitación | `E7_pipeline_lakehouse` | M6 Big Data + M7 Integración | Arquitectura de referencia (ABB/SBB) |
| E8. Dashboard BI + Genie | `E8_dashboard_bi` | M9 BI y Visualización | Requerimientos de información y KPIs |
| EX. Bonus: IA sobre reseñas | `EX_bonus_ia` | M8 IA y No Estructurados | Casos de uso analíticos avanzados |
| Operación (job, monitoreo, FinOps) | `E7.../04_job_orquestacion` | M10 DataOps/MLOps/FinOps | Modelo operativo de la plataforma |

## 3.1 Artefactos institucionales de gobierno (E4)

El glosario y el diccionario **no se redactan en formato libre**: se completan en las
plantillas oficiales del bloque de Seguridad, Gobernanza y Compliance, disponibles en
`E4_diccionario_glosario/plantillas/`:

| Plantilla | Hojas | Se carga en |
|---|---|---|
| `Glosario_de_terminos_de_negocio_YelpConnect_PLANTILLA.xlsx` | Definiciones · Glosario · Guía Capstone | `workspace.yelp_gov.glosario_negocio` |
| `Diccionario_de_Datos_YelpConnect_PLANTILLA.xlsx` | Definición · Diccionario · Diccionario resumido · Guía Capstone | `workspace.yelp_gov.diccionario_datos` |

Las tablas Delta tienen **exactamente las mismas columnas que las plantillas** (12 y 25
respectivamente), así que el Excel no se traduce: se carga. Flujo:

1. El grupo completa el Excel (interfaz de negocio, se discute con las "áreas" del caso).
2. Sube el archivo a `/Volumes/workspace/yelp_gov/artefactos/` sin cambiarle el nombre.
3. Ejecuta los notebooks `E4.../01_glosario_negocio` y `E4.../02_diccionario_datos`.
4. Los validadores comprueban cobertura, ownership, clasificación y que toda regla citada
   como `DQ-nnn` exista realmente en `yelp_gov.dq_reglas`.

Los dos Excel completados se **anexan además al Documento Formal**: son el mismo archivo,
no dos versiones distintas. El diccionario cierra el círculo cuando su columna de
clasificación se aplica como **tag real** en Unity Catalog y alimenta los permisos de E6.

## 4. Reglas de juego en Free Edition

- Solo hay **cómputo serverless** con **cuota diaria**. Si la agotan, el cómputo se
  apaga hasta el día siguiente (los datos NO se pierden). Moraleja FinOps: trabajen
  con la **muestra**, no con el dataset completo, y eviten `display()` de millones de filas.
- El dataset oficial es el **Yelp Open Dataset** muestreado a UNA ciudad
  (el script de muestreo lo hace por ustedes). Objetivo: < 300 MB en crudo.
- Todo entregable debe ser **reproducible**: si el profesor clona el repo y ejecuta
  los notebooks en orden, obtiene el mismo resultado.

## 5. Definition of Done global

Un entregable de plataforma está terminado cuando:

1. El notebook corre de inicio a fin sin errores y sus validadores pasan.
2. Las tablas y objetos existen en el schema correcto, con comentarios y tags.
3. La cabecera declara qué sección del Documento Formal implementa, y **no contradice** esa
   sección (si la implementación obligó a cambiar el diseño, se actualiza el documento y se
   deja constancia de la decisión).
4. El markdown del notebook documenta decisiones y supuestos propios de la implementación.
5. Está en `main` vía Pull Request revisado por al menos otro integrante.

## 6. Orden de ejecución recomendado

Primera vez (una sola vez, en este orden):
```
00_setup/00_configurar_lakehouse  →  00_setup/01_verificar_datos_raw
E7/01_bronze_ingesta              (carga los 5 archivos; reseñas con Auto Loader)
E3/01_modelo_fisico_ddl           (crea las tablas silver y gold vacías)
E7/02_silver_limpieza             (llena silver y cuarentena; su gate ejecuta E5)
E7/03_gold_productos_datos        (llena gold y calcula las señales de fraude)
E4/01_glosario  →  E4/02_diccionario  (requiere subir los Excel _Grupo06 a /Volumes/workspace/yelp_gov/artefactos/)
E4/03_golden_record_negocio        (requiere bronze; lo ejecuta un custodian)
E6/01_controles_unity_catalog     (requiere el grupo data_stewards y los correos en los widgets)
E4/02_diccionario                 (otra vez: valida las reglas citadas y etiqueta gold)
E7/04_job_orquestacion            (con el widget crear_job = si)
```
Desde ahí, el job corre solo cada día a las 22:00 (hora de Lima): bronze → silver (+ E5) → gold.
E8 se arma en la interfaz (Dashboards y Genie) con las consultas del notebook. EX es opcional.

## 7. Decisiones de implementación que difieren del esqueleto

| Decisión | Motivo |
|---|---|
| La cuarentena vive en `yelp_gov.cuarentena_<entidad>`, no en silver | Silver solo contiene datos válidos y el Diccionario cubre el 100 % de sus columnas |
| Conversiones con `try_cast` | En modo ANSI un solo valor mal formado detendría el pipeline; con `try_cast` el registro va a cuarentena |
| Silver y gold se llenan con `insertInto` sobre el DDL de E3 | Respeta tipos, COMMENT, constraints y máscaras; las tablas no se recrean en cada corrida |
| El DDL de gold está en E3, no en E7/03 | Mismo patrón que silver: E3 define el molde y E7 lo llena |
| El golden record vive en yelp_gov y no en gold | Los 20 grupos requieren aprobación del steward antes de cambiar gld_dim_negocio
| Las señales de fraude se leen de `dq_reglas` | El umbral Restringido de DQ-016 no aparece en el código y una recalibración se aplica sola |
| Permisos de gold tabla por tabla | `gld_salud_resenas` y `gld_dim_usuario_riesgo` son Restringidas (solo Trust & Safety) |
| El bonus de IA guarda su resultado en `yelp_gov` | Es un experimento no aprobado: no está en el Diccionario ni debe llegar a los consumidores |
| El tablero se publica sin credenciales embebidas | Cada lector consulta con sus propios permisos (CE-07) |

Detalle completo en la sección 9 del Documento Formal.