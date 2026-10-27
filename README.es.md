# DataMart S.A.S. — Pipeline ETL Analítico y Data Warehouse
**Prueba de Desempeño:** Pipeline ETL con Apache Airflow, FastAPI y Data Warehouse en PostgreSQL  
**Autor:** Jayzir Martínez | DataMart Analytics Team  
**Cohorte:** Riwi Cohorte 5 | Octubre 2026  
**Repositorio Oficial:** [`https://github.com/JayzirTech/datamart-analytics-etl-pipeline.git`](https://github.com/JayzirTech/datamart-analytics-etl-pipeline.git)

> 🌐 **Language / Idioma:** [English](README.md) | **Español**

---

## 1. Visión General de la Solución

Solución de ingeniería de datos integral, modular y 100% contenerizada para **DataMart S.A.S.** que automatiza la ingesta masiva de transacciones de retail e-commerce (+1,000,000 de registros), enriquece los productos en tiempo real mediante un microservicio REST en **FastAPI**, estandariza las divisas a USD consumiendo **Frankfurter API**, valida la calidad de datos con políticas de rechazo estrictas y puebla un **Data Warehouse en PostgreSQL** mediante un modelo dimensional en estrella optimizado para consultas analíticas sub-segundo.

```mermaid
graph TD
    subgraph FUENTES["1. Fuentes de Datos Crudos"]
        D1["data.csv (Kaggle)"]
        D2["online_retail_II.csv (Kaggle)"]
    end

    subgraph SERVICIOS["2. Servicios Externos & Microservicios"]
        API_CURR["Frankfurter API<br/><i>(Tasas de cambio a USD en vivo)</i>"]
        FASTAPI["FastAPI Microservicio :8000<br/><i>(Catálogo taxonomía y proveedores)</i>"]
    end

    subgraph AIRFLOW["3. Orquestación Distribuida (Apache Airflow 3.3.2 en Docker)"]
        SCHED["airflow-scheduler"]
        WORKER["airflow-worker (Celery)"]
        RABBIT["RabbitMQ Broker :5672 / :15672"]
        WEB["airflow-api-server :8080 (UI)"]
        PG_META[("PostgreSQL Metadata")]
        
        SCHED -->|Encola tareas| RABBIT
        RABBIT -->|Ejecuta tareas| WORKER
        SCHED --- PG_META
        WEB --- PG_META
    end

    subgraph DWH["4. Almacenamiento Analítico (PostgreSQL DWH Puerto :5433)"]
        DWH_DB[("datamart_dwh<br/>- dim_products<br/>- dim_customers<br/>- dim_exchange_rates<br/>- fact_sales (USD)<br/>- fact_returns (USD)<br/>- fact_daily_product_revenue<br/>- audit_product_descriptions<br/>- rejected_records")]
    end

    subgraph CONSUMO["5. Capa de Consumo & Auditoría Analítica"]
        SQL["sql/queries_negocio.sql<br/>(7 Consultas de Negocio)"]
        NOTEBOOK["notebooks/consultas_negocio.ipynb<br/>(Cuaderno Interactivo Dinámico)"]
        DBDIAG["dbdiagram.io<br/>(Modelo Dimensional en la Nube)"]
    end

    D1 & D2 -->|Lectura idempotente| WORKER
    API_CURR -->|Paridades vigentes| WORKER
    FASTAPI -->|Enriquecimiento REST| WORKER
    WORKER -->|Carga ACID con ON CONFLICT| DWH_DB
    DWH_DB --> SQL
    DWH_DB --> NOTEBOOK
    DWH_DB --> DBDIAG
```

### 1.1 Arquitectura de Datos: Patrón Medallion (Bronce, Plata y Oro)

El procesamiento y refinamiento de los datos implementa el patrón **Medallion Architecture**, garantizando que cada registro transaccional transite por etapas de ingesta inmutable, estandarización atómica conformada y curaduría de negocio analítica:

```mermaid
graph LR
    subgraph BRONZE["🥉 CAPA BRONCE (Raw / Ingesta Inmutable)"]
        B1["data.csv & online_retail_II.csv<br/><i>(Fuentes transaccionales crudas)</i>"]
        B2["Frankfurter API & FastAPI<br/><i>(Respuestas JSON crudas de APIs)</i>"]
        B3["rejected_records<br/><i>(Auditoría JSONB cruda de datos anómalos)</i>"]
    end

    subgraph SILVER["🥈 CAPA PLATA (Cleaned, Enriched & Conformed)"]
        S1["dim_products<br/><i>(Moda canónica + Taxonomía FastAPI)</i>"]
        S2["dim_customers<br/><i>(Clientes limpios + Imputación 'ANONYMOUS')</i>"]
        S3["dim_exchange_rates<br/><i>(Tasas de paridad y normalización a USD)</i>"]
        S4["fact_sales & fact_returns<br/><i>(Hechos atómicos segregados y deduplicados)</i>"]
    end

    subgraph GOLD["🥇 CAPA ORO (Aggregated, Curated & Business-Ready)"]
        G1["fact_daily_product_revenue<br/><i>(Pre-agregación diaria: Net Revenue por SKU)</i>"]
        G2["queries_negocio.sql<br/><i>(7 Consultas analíticas estratégicas)</i>"]
        G3["consultas_negocio.ipynb<br/><i>(Cuaderno interactivo con KPIs ejecutivos)</i>"]
    end

    B1 & B2 -->|Validación, Limpieza y Deduplicación| SILVER
    B1 -->|Violación de Calidad| B3
    SILVER -->|Agregación y Métricas de Negocio| GOLD
```

* **🥉 Capa Bronce (Raw):** Preserva las fuentes en bruto tal como fueron recibidas (`data/raw/*.csv`, respuestas REST originales) y almacena registros anómalos en [`rejected_records`](file:///c:/Users/Coder/Desktop/JayzirTech/data-analytics-performance-test/dwh_init/01_init_dwh.sql#L137) con formato `JSONB` original para auditoría forense sin pérdida de información.
* **🥈 Capa Plata (Cleaned & Conformed):** Aplica validación estricta de calidad, deduplicación por clave natural compuesta priorizando `data.csv`, enriquecimiento con el catálogo FastAPI, estandarización de divisas a USD con Frankfurter API y estructuración en dimensiones conformadas y hechos atómicos segregados (`fact_sales` y `fact_returns`).
* **🥇 Capa Oro (Business / Analytics-Ready):** Almacena las tablas agregadas de alto rendimiento como [`fact_daily_product_revenue`](file:///c:/Users/Coder/Desktop/JayzirTech/data-analytics-performance-test/dwh_init/01_init_dwh.sql#L112) (revenue neto diario por SKU) y expone la capa de consumo analítico ([`sql/queries_negocio.sql`](file:///c:/Users/Coder/Desktop/JayzirTech/data-analytics-performance-test/sql/queries_negocio.sql) y [`notebooks/consultas_negocio.ipynb`](file:///c:/Users/Coder/Desktop/JayzirTech/data-analytics-performance-test/notebooks/consultas_negocio.ipynb)) con respuestas sub-segundo para la gerencia.

### 1.2 Diagrama Entidad-Relación del Data Warehouse (Modelo Dimensional en Estrella de Kimball)

```mermaid
erDiagram
    dim_products ||--o{ fact_sales : "1:N (product_key)"
    dim_products ||--o{ fact_returns : "1:N (product_key)"
    dim_products ||--o{ fact_daily_product_revenue : "1:N (product_key)"
    dim_products ||--o{ audit_product_descriptions : "1:N (stock_code)"
    dim_customers ||--o{ fact_sales : "1:N (customer_key)"
    dim_customers ||--o{ fact_returns : "1:N (customer_key)"
    dim_exchange_rates ||--o{ fact_sales : "1:N (currency_code)"
    dim_exchange_rates ||--o{ fact_returns : "1:N (currency_code)"

    dim_products {
        int product_key PK
        varchar stock_code UK
        varchar canonical_description
        varchar category_name
        varchar supplier_country
        boolean is_active
    }

    dim_customers {
        int customer_key PK
        varchar customer_id UK
        varchar customer_name
        varchar customer_type
        varchar country
    }

    dim_exchange_rates {
        varchar currency_code PK
        varchar currency_name
        numeric rate_to_usd
    }

    fact_sales {
        bigint sale_key PK
        varchar invoice_no
        date invoice_date
        timestamptz invoice_timestamp_utc
        int product_key FK
        int customer_key FK
        int quantity
        numeric unit_price
        numeric gross_revenue
    }

    fact_returns {
        bigint return_key PK
        varchar invoice_no
        date invoice_date
        timestamptz invoice_timestamp_utc
        int product_key FK
        int customer_key FK
        int quantity_returned
        numeric unit_price
        numeric return_amount
    }

    fact_daily_product_revenue {
        bigint daily_summary_key PK
        date invoice_date
        int product_key FK
        numeric gross_revenue
        numeric return_amount
        numeric net_revenue
        int total_sold_qty
        int total_returned_qty
    }

    audit_product_descriptions {
        varchar stock_code PK
        varchar raw_description PK
        int frequency
    }

    rejected_records {
        bigserial record_id PK
        varchar source_dataset
        jsonb raw_record
        varchar rejection_reason
        timestamptz rejected_at_utc
    }
```

> 📌 **Modelo en dbdiagram.io:**
> * 🌐 **Visualización Interactiva Pública (Web):** [DataMart S.A.S. — Data Warehouse & Analytics Data Model](https://dbdiagram.io/d/DataMart-S-A-S-—-Data-Warehouse-and-Analytics-Data-Model-6ae0c592ed134e407e204467)
> * 📄 **Archivo Fuente DBML:** [`docs/diagrama_modelo_datos.dbml`](file:///c:/Users/Coder/Desktop/JayzirTech/data-analytics-performance-test/docs/diagrama_modelo_datos.dbml)
> * 📑 **Detalle en Decisiones Técnicas:** [Sección 5 de DECISIONES.md](file:///c:/Users/Coder/Desktop/JayzirTech/data-analytics-performance-test/DECISIONES.md#5-diagrama-del-modelo-de-datos-dbdiagramio)

---

## 2. Pila Tecnológica

| Componente | Tecnología | Versión / Puerto | Función |
| :--- | :--- | :--- | :--- |
| **Orquestador** | Apache Airflow | `3.3.2` (`:8080`) | Planificación, reintentos y orquestación del DAG diario |
| **Broker de Mensajes** | RabbitMQ | `3-management-alpine` (`:15672` / `:5672`) | Broker distribuido para CeleryExecutor |
| **Microservicio Catálogo** | FastAPI + Uvicorn | Python 3.11 (`:8000`) | Taxonomía, proveedor y estado activo/descontinuado |
| **API de Divisas** | Frankfurter API | REST (`api.frankfurter.dev`) | Tasas de cambio en vivo hacia USD con fallback semilla local |
| **Data Warehouse** | PostgreSQL | `15-alpine` (`:5433`) | Almacén analítico dimensional y hechos segregados |
| **Metastore Airflow** | PostgreSQL | `15-alpine` (Interno `5432`) | Base de datos de estado y metadatos de Airflow |
| **Contenedores** | Docker & Docker Compose | v2+ | Despliegue automatizado sin intervención manual |
| **Calidad y ETL** | Python / Pandas / Psycopg2 | 3.13 | Clean Architecture, SOLID e Idempotencia con UPSERTs |

---

## 3. Guía de Despliegue Rápido (< 10 Minutos)

El entorno cuenta con **autoconfiguración total (*Zero-Manual Intervention*)**: al levantar los contenedores con Docker Compose, el servicio `airflow-init` crea automáticamente la base de datos, ejecuta las migraciones, registra al usuario administrador, configura la conexión de PostgreSQL DWH y establece todas las variables operativas en Airflow.

### Paso 1: Clonar el Repositorio
```bash
git clone https://github.com/JayzirTech/datamart-analytics-etl-pipeline.git
cd datamart-analytics-etl-pipeline
```

### Paso 2: Crear el Archivo de Variables de Entorno
Copia la plantilla preconfigurada `.env.example` a `.env`:
```bash
# En Windows PowerShell:
Copy-Item .env.example .env

# En Linux / macOS / Bash:
cp .env.example .env
```

*(Las credenciales y puertos por defecto ya vienen listos para funcionar sin requerir modificaciones adicionales).*

### Paso 3: Levantar los Contenedores
Ejecuta Docker Compose para compilar e iniciar los 8 servicios en segundo plano:
```bash
docker compose up -d
```

### Paso 4: Verificar el Estado de los Servicios
Verifica que todos los contenedores estén activos y saludables:
```bash
docker compose ps
```

Deberás observar los siguientes contenedores activos:
* `datamart_airflow_webserver` — `Up` (`http://localhost:8080`)
* `datamart_airflow_scheduler` — `Up`
* `datamart_airflow_worker` — `Up`
* `datamart_airflow_dag_processor` — `Up`
* `datamart_fastapi_products` — `Up (healthy)` (`http://localhost:8000`)
* `datamart_postgres_dwh` — `Up (healthy)` (Puerto `5433`)
* `datamart_postgres_airflow` — `Up (healthy)`
* `datamart_rabbitmq` — `Up (healthy)` (`http://localhost:15672`)
* `datamart_airflow_init` — `Exited (0)` *(normal, finalizó exitosamente la inicialización)*

---

## 4. Validación de Connections y Variables en Airflow

Para certificar que la autoconfiguración del pipeline se ejecutó correctamente sin intervención humana, ejecuta las siguientes comprobaciones:

### 4.1 Validar la Conexión hacia el Data Warehouse (`postgres_dwh_conn`)
```bash
docker exec datamart_airflow_scheduler airflow connections get postgres_dwh_conn
```
> **Salida esperada:**
> ```text
> conn_type: postgres
> host: postgres_dwh
> port: 5432
> schema: datamart_dwh
> login: dwh_user
> ```

### 4.2 Validar las Variables Operativas de Airflow
```bash
docker exec datamart_airflow_scheduler airflow variables list
```
> **Salida esperada:**
> ```text
> key                  
> ====================
> PIPELINE_ENVIRONMENT
> RAW_DATA_PATH       
> FRANKFURTER_API_URL 
> FASTAPI_BASE_URL    
> ```

---

## 5. Accesos y Credenciales de Servicios

| Servicio | URL Local | Usuario | Contraseña |
| :--- | :--- | :--- | :--- |
| **Airflow Web UI** | [http://localhost:8080](http://localhost:8080) | `admin` | `admin` |
| **FastAPI Swagger Docs** | [http://localhost:8000/docs](http://localhost:8000/docs) | N/A | N/A |
| **RabbitMQ Management** | [http://localhost:15672](http://localhost:15672) | `airflow_broker` | `broker_secret_password` |
| **PostgreSQL Data Warehouse** | `localhost:5433` / DB: `datamart_dwh` | `dwh_user` | `dwh_secret_password` |

---

## 6. Ejecución del Pipeline ETL en Airflow

El DAG **`datamart_etl_daily_pipeline`** está configurado con schedule diario y reintentos automáticos (`retries=2, retry_delay=60s`).

### Disparar el Pipeline Manualmente:
1. **Desde la Web UI:** Ingresa a [http://localhost:8080](http://localhost:8080), localiza `datamart_etl_daily_pipeline` y pulsa el botón **Trigger DAG (▶)**.
2. **Desde la Consola CLI:**
   ```bash
   docker exec datamart_airflow_scheduler airflow dags trigger datamart_etl_daily_pipeline
   ```

### Fases de Ejecución del DAG:
```mermaid
graph LR
    A[task_verify_sources] --> B[task_extract_currency_rates]
    B --> C[task_run_full_etl]
    C --> D[task_audit_data_quality]
```

1. **`task_verify_sources`:** Valida la presencia de `data.csv` y `online_retail_II.csv` en `data/raw/`.
2. **`task_extract_currency_rates`:** Consulta en vivo **Frankfurter API** (`https://api.frankfurter.dev/v1/latest?base=USD`) y sincroniza 29 tasas de cambio vigentes en `dim_exchange_rates`.
3. **`task_run_full_etl`:**
   * Aplica reglas de limpieza y validación de calidad.
   * Asigna `'ANONYMOUS'` a clientes nulos (ADR-0002).
   * Deduplica por clave natural compuesta con prioridad a `data.csv` (ADR-0004).
   * Calcula la descripción canónica por moda estadística (ADR-0003).
   * Consulta el catálogo del microservicio FastAPI (`/products/bulk`) y enriquece categorías (ADR-0001).
   * Convierte todos los precios a USD.
   * Segrega ventas en `fact_sales` y devoluciones en `fact_returns` (ADR-0008).
   * Calcula y persiste el Revenue Neto diario en `fact_daily_product_revenue` (ADR-0008).
4. **`task_audit_data_quality`:** Audita conteos, sumas de control y tasas de rechazo.

---

## 7. Verificación de Carga y Consultas de Negocio

### 7.1 Comprobar Conteo de Filas en PostgreSQL DWH
Ejecuta una consulta rápida para validar los datos en el Data Warehouse:
```bash
docker exec datamart_postgres_dwh psql -U dwh_user -d datamart_dwh -c "
SELECT 'fact_sales' AS tabla, count(*) AS total_filas FROM fact_sales
UNION ALL SELECT 'fact_returns', count(*) FROM fact_returns
UNION ALL SELECT 'fact_daily_product_revenue', count(*) FROM fact_daily_product_revenue
UNION ALL SELECT 'dim_products', count(*) FROM dim_products
UNION ALL SELECT 'dim_customers', count(*) FROM dim_customers
UNION ALL SELECT 'dim_exchange_rates', count(*) FROM dim_exchange_rates
UNION ALL SELECT 'rejected_records', count(*) FROM rejected_records;
"
```

> **Volúmenes Esperados en DWH:**
> * `fact_sales`: **524,796 filas**
> * `fact_returns`: **9,177 filas**
> * `fact_daily_product_revenue`: **278,011 filas**
> * `dim_products`: **3,828 productos**
> * `dim_customers`: **4,372 clientes**
> * `rejected_records`: **5,034 filas auditadas**

### 7.2 Ejecutar las 7 Consultas de Negocio (Sección 7)
El archivo [`sql/queries_negocio.sql`](file:///c:/Users/Coder/Desktop/JayzirTech/data-analytics-performance-test/sql/queries_negocio.sql) contiene las consultas que resuelven formalmente cada una de las 7 preguntas del enunciado:

```bash
# En Windows PowerShell:
Get-Content "sql/queries_negocio.sql" | docker exec -i datamart_postgres_dwh psql -U dwh_user -d datamart_dwh

# En Linux / macOS / Bash:
docker exec -i datamart_postgres_dwh psql -U dwh_user -d datamart_dwh < sql/queries_negocio.sql
```

### 7.3 Exploración Interactiva en Cuaderno Jupyter
Para una experiencia visual interactiva donde todos los cálculos, conclusiones y porcentajes son calculados dinámicamente por código Python:
* Abre el archivo [`notebooks/consultas_negocio.ipynb`](file:///c:/Users/Coder/Desktop/JayzirTech/data-analytics-performance-test/notebooks/consultas_negocio.ipynb) directamente en VS Code o JupyterLab.
* Contiene tablas tabuladas en HTML y métricas calculadas en tiempo real a partir del Data Warehouse.

---

## 8. Principios SOLID y Clean Architecture en el Código (`src/`)

El código fue estructurado bajo una estricta separación de responsabilidades para garantizar mantenibilidad, testabilidad desacoplada y extensibilidad:

```
src/
├── interfaces/             # Contratos abstractos (Interface Segregation / Dependency Inversion)
│   ├── extractor.py        # BaseExtractor
│   ├── validator.py        # BaseValidator
│   ├── transformer.py      # BaseTransformer
│   └── loader.py           # BaseLoader
├── extractors/             # Implementaciones de extracción desacopladas
│   ├── kaggle_extractor.py # Descarga y lectura de datasets crudos
│   ├── currency_extractor.py # Consulta de Frankfurter API con fallback semilla
│   └── api_extractor.py    # Cliente REST HTTP para microservicio FastAPI
├── validators/             # Reglas de calidad y segregación de rechazos
│   └── data_validator.py   # Validación de tipos, precios <= 0 y registros anómalos
├── transformers/           # Transformaciones puras de negocio
│   └── datamart_transformer.py # Clave compuesta, moda canónica, conversión USD
├── loaders/                # Cargas idempotentes a base de datos
│   └── postgres_loader.py  # Inserciones por lotes (execute_batch) con ON CONFLICT
└── config/                 # Semillas de respaldo y configuraciones operativas
    ├── exchange_rates_seed.json # Paridades offline para fallback de divisas
    └── settings.py
```

* **Single Responsibility Principle (SRP):** Cada módulo tiene un único motivo de cambio (el extractor solo extrae, el validador solo audita calidad, el cargador solo ejecuta sentencias en base de datos).
* **Open/Closed Principle (OCP):** Nuevas fuentes de datos (ej. S3, BigQuery) pueden incorporarse extendiendo `BaseExtractor` sin tocar el motor de transformación.
* **Liskov Substitution Principle (LSP):** Cualquier extractor o cargador implementa la interfaz base y es intercambiable en el pipeline.
* **Interface Segregation Principle (ISP):** Interfaces pequeñas y específicas orientadas a la tarea ETL concreta.
* **Dependency Inversion Principle (DIP):** Las tareas de Airflow dependen de abstracciones (`BaseExtractor`, `BaseLoader`), no de implementaciones concretas fijas.

---

## 9. Documentos de Entrega y Referencias Técnicas

1. **Documento Consolidado de Decisiones Técnicas:** [`DECISIONES.md`](file:///c:/Users/Coder/Desktop/JayzirTech/data-analytics-performance-test/DECISIONES.md) (Justificación DWH, resolución de ambigüedades y garantía de idempotencia).
2. **Diagrama Interactivo en dbdiagram.io:** [Visualización Pública Web](https://dbdiagram.io/d/DataMart-S-A-S-—-Data-Warehouse-and-Analytics-Data-Model-6ae0c592ed134e407e204467) | [Archivo Fuente DBML](file:///c:/Users/Coder/Desktop/JayzirTech/data-analytics-performance-test/docs/diagrama_modelo_datos.dbml).
3. **Exploración de Datos (EDA):** [`docs/EXPLORACION_DATOS.md`](file:///c:/Users/Coder/Desktop/JayzirTech/data-analytics-performance-test/docs/EXPLORACION_DATOS.md).
4. **Registro de Decisiones de Arquitectura (14 ADRs):** [`docs/adr/`](file:///c:/Users/Coder/Desktop/JayzirTech/data-analytics-performance-test/docs/adr/README.md).
5. **Consultas SQL de Negocio:** [`sql/queries_negocio.sql`](file:///c:/Users/Coder/Desktop/JayzirTech/data-analytics-performance-test/sql/queries_negocio.sql).
6. **Cuaderno Analítico Interactivo:** [`notebooks/consultas_negocio.ipynb`](file:///c:/Users/Coder/Desktop/JayzirTech/data-analytics-performance-test/notebooks/consultas_negocio.ipynb).
7. **DDL del Data Warehouse:** [`dwh_init/01_init_dwh.sql`](file:///c:/Users/Coder/Desktop/JayzirTech/data-analytics-performance-test/dwh_init/01_init_dwh.sql).

---

## 10. Video Demostración del Pipeline (End-to-End)

Como evidencia del funcionamiento integral del sistema y cumplimiento de la rúbrica de evaluación oficial (20/20 pts en documentación y decisiones), se incluye la demostración en video del pipeline ejecutándose de extremo a extremo:

* 🎥 **Enlace al Video Demo:** [Ver Video Demostración en YouTube (https://youtu.be/M46kV_vK2ys)](https://youtu.be/M46kV_vK2ys)

### Puntos clave cubiertos en la demostración:
1. **Despliegue Automatizado:** Ejecución de `docker compose up -d` y comprobación de los 8 contenedores en estado `healthy`.
2. **Microservicio FastAPI:** Verificación de los endpoints `/health` y `/products` en `http://localhost:8000/docs`.
3. **Apache Airflow:** Interfaz en `http://localhost:8080`, revisión de la conexión `postgres_dwh_conn` y variables operativas (`FRANKFURTER_API_URL`, `RAW_DATA_PATH`, etc.).
4. **Ejecución y Monitoreo del DAG:** Disparo del DAG `datamart_etl_daily_pipeline` y ejecución exitosa de sus 4 tareas (`success`).
5. **Data Warehouse en PostgreSQL:** Validación de filas en puerto `5433`, ejecución de [`sql/queries_negocio.sql`](file:///c:/Users/Coder/Desktop/JayzirTech/data-analytics-performance-test/sql/queries_negocio.sql) y cuaderno interactivo [`notebooks/consultas_negocio.ipynb`](file:///c:/Users/Coder/Desktop/JayzirTech/data-analytics-performance-test/notebooks/consultas_negocio.ipynb).
6. **Modelo de Datos:** Visualización interactiva en [dbdiagram.io](https://dbdiagram.io/d/DataMart-S-A-S-—-Data-Warehouse-and-Analytics-Data-Model-6ae0c592ed134e407e204467).
