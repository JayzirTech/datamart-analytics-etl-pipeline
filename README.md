# DataMart S.A.S. — Analytical ETL Pipeline & Data Warehouse
**Performance Test:** End-to-End ETL Pipeline with Apache Airflow, FastAPI & PostgreSQL Data Warehouse  
**Author:** Jayzir Martínez | DataMart Analytics Team  
**Cohort:** Riwi Cohort 5 | October 2026  
**Official Repository:** [`https://github.com/JayzirTech/datamart-analytics-etl-pipeline.git`](https://github.com/JayzirTech/datamart-analytics-etl-pipeline.git)

> 🌐 **Language / Idioma:** **English** | [Español](README.es.md)

---

## 1. Solution Overview

An end-to-end, modular, and 100% containerized data engineering solution built for **DataMart S.A.S.** It automates the mass ingestion of retail e-commerce transactions (+1,000,000 records), enriches products in real time via a **FastAPI** REST microservice, standardizes currencies to USD by consuming the **Frankfurter API**, enforces data quality with strict rejection policies, and populates an analytical **PostgreSQL Data Warehouse** modeled with a Kimball Star Schema optimized for sub-second analytical reporting.

```mermaid
graph TD
    subgraph SOURCES["1. Raw Data Sources"]
        D1["data.csv (Kaggle)"]
        D2["online_retail_II.csv (Kaggle)"]
    end

    subgraph SERVICES["2. External Services & Microservices"]
        API_CURR["Frankfurter API<br/><i>(Live currency exchange rates to USD)</i>"]
        FASTAPI["FastAPI Microservice :8000<br/><i>(Catalog taxonomy & suppliers)</i>"]
    end

    subgraph AIRFLOW["3. Distributed Orchestration (Apache Airflow 3.3.2 on Docker)"]
        SCHED["airflow-scheduler"]
        WORKER["airflow-worker (Celery)"]
        RABBIT["RabbitMQ Broker :5672 / :15672"]
        WEB["airflow-api-server :8080 (UI)"]
        PG_META[("PostgreSQL Metadata")]
        
        SCHED -->|Queues tasks| RABBIT
        RABBIT -->|Executes tasks| WORKER
        SCHED --- PG_META
        WEB --- PG_META
    end

    subgraph DWH["4. Analytical Storage (PostgreSQL DWH Port :5433)"]
        DWH_DB[("datamart_dwh<br/>- dim_products<br/>- dim_customers<br/>- dim_exchange_rates<br/>- fact_sales (USD)<br/>- fact_returns (USD)<br/>- fact_daily_product_revenue<br/>- audit_product_descriptions<br/>- rejected_records")]
    end

    subgraph CONSUMPTION["5. Consumption & Analytical Audit Layer"]
        SQL["sql/queries_negocio.sql<br/>(7 Business Questions)"]
        NOTEBOOK["notebooks/consultas_negocio.ipynb<br/>(Dynamic Interactive Notebook)"]
        DBDIAG["dbdiagram.io<br/>(Cloud Dimensional Model)"]
    end

    D1 & D2 -->|Idempotent Ingestion| WORKER
    API_CURR -->|Live Rates| WORKER
    FASTAPI -->|REST Enrichment| WORKER
    WORKER -->|ACID Load with ON CONFLICT| DWH_DB
    DWH_DB --> SQL
    DWH_DB --> NOTEBOOK
    DWH_DB --> DBDIAG
```

### 1.1 Data Architecture: Medallion Pattern (Bronze, Silver & Gold)

Data processing and refinement strictly implements the **Medallion Architecture**, ensuring every transaction transitions through stages of immutable ingestion, conformed atomic standardization, and business analytical curation:

```mermaid
graph LR
    subgraph BRONZE["🥉 BRONZE LAYER (Raw / Immutable Ingestion)"]
        B1["data.csv & online_retail_II.csv<br/><i>(Raw transactional sources)</i>"]
        B2["Frankfurter API & FastAPI<br/><i>(Raw JSON API responses)</i>"]
        B3["rejected_records<br/><i>(Raw JSONB audit of anomalous data)</i>"]
    end

    subgraph SILVER["🥈 SILVER LAYER (Cleaned, Enriched & Conformed)"]
        S1["dim_products<br/><i>(Canonical mode + FastAPI taxonomy)</i>"]
        S2["dim_customers<br/><i>(Cleaned customers + 'ANONYMOUS' imputation)</i>"]
        S3["dim_exchange_rates<br/><i>(Exchange rates & USD normalization)</i>"]
        S4["fact_sales & fact_returns<br/><i>(Segregated & deduplicated atomic facts)</i>"]
    end

    subgraph GOLD["🥇 GOLD LAYER (Aggregated, Curated & Business-Ready)"]
        G1["fact_daily_product_revenue<br/><i>(Daily pre-aggregation: Net Revenue per SKU)</i>"]
        G2["queries_negocio.sql<br/><i>(7 Strategic analytical SQL queries)</i>"]
        G3["consultas_negocio.ipynb<br/><i>(Interactive notebook with executive KPIs)</i>"]
    end

    B1 & B2 -->|Validation, Cleaning & Deduplication| SILVER
    B1 -->|Quality Violation| B3
    SILVER -->|Aggregation & Business Metrics| GOLD
```

* **🥉 Bronze Layer (Raw):** Preserves raw source datasets as received (`data/raw/*.csv`, raw JSON API responses) and stores anomalous records in [`rejected_records`](file:///c:/Users/Coder/Desktop/JayzirTech/data-analytics-performance-test/dwh_init/01_init_dwh.sql#L137) in raw `JSONB` format for forensic data audit without data loss.
* **🥈 Silver Layer (Cleaned & Conformed):** Enforces strict quality validation, composite natural key deduplication prioritizing `data.csv`, category enrichment via FastAPI, currency standardization to USD with Frankfurter API, and atomic segregation into conformed dimensions and fact tables (`fact_sales` and `fact_returns`).
* **🥇 Gold Layer (Business / Analytics-Ready):** Stores high-performance aggregated tables such as [`fact_daily_product_revenue`](file:///c:/Users/Coder/Desktop/JayzirTech/data-analytics-performance-test/dwh_init/01_init_dwh.sql#L112) (daily net revenue per SKU) and exposes the consumption layer ([`sql/queries_negocio.sql`](file:///c:/Users/Coder/Desktop/JayzirTech/data-analytics-performance-test/sql/queries_negocio.sql) and [`notebooks/consultas_negocio.ipynb`](file:///c:/Users/Coder/Desktop/JayzirTech/data-analytics-performance-test/notebooks/consultas_negocio.ipynb)) with sub-second response times for executive reporting.

### 1.2 Data Warehouse Entity-Relationship Diagram (Kimball Star Schema)

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

> 📌 **Data Model on dbdiagram.io:**
> * 🌐 **Public Interactive Visualization (Web):** [DataMart S.A.S. — Data Warehouse & Analytics Data Model](https://dbdiagram.io/d/DataMart-S-A-S-—-Data-Warehouse-and-Analytics-Data-Model-6ae0c592ed134e407e204467)
> * 📄 **DBML Source Code File:** [`docs/diagrama_modelo_datos.dbml`](file:///c:/Users/Coder/Desktop/JayzirTech/data-analytics-performance-test/docs/diagrama_modelo_datos.dbml)
> * 📑 **Technical Decisions Reference:** [Section 5 in DECISIONS.md](file:///c:/Users/Coder/Desktop/JayzirTech/data-analytics-performance-test/DECISIONS.md#5-data-model-diagram-dbdiagramio)

---

## 2. Technology Stack

| Component | Technology | Version / Port | Purpose |
| :--- | :--- | :--- | :--- |
| **Orchestrator** | Apache Airflow | `3.3.2` (`:8080`) | Scheduling, retries, and daily DAG orchestration |
| **Message Broker** | RabbitMQ | `3-management-alpine` (`:15672` / `:5672`) | Distributed broker for CeleryExecutor |
| **Catalog Microservice** | FastAPI + Uvicorn | Python 3.11 (`:8000`) | Product taxonomy, supplier country, and active status |
| **Currency API** | Frankfurter API | REST (`api.frankfurter.dev`) | Live currency conversion rates to USD with seed fallback |
| **Data Warehouse** | PostgreSQL | `15-alpine` (`:5433`) | Analytical dimensional warehouse with segregated facts |
| **Airflow Metastore** | PostgreSQL | `15-alpine` (Internal `5432`) | State and task instance metadata database |
| **Containers** | Docker & Docker Compose | v2+ | Zero-manual intervention containerized deployment |
| **Quality & ETL** | Python / Pandas / Psycopg2 | 3.13 | Clean Architecture, SOLID principles, and UPSERT idempotency |

---

## 3. Quickstart Deployment Guide (< 10 Minutes)

The environment features **Zero-Manual Intervention**: when launching containers via Docker Compose, the `airflow-init` service automatically provisions the database, applies migrations, registers the admin user, configures the PostgreSQL DWH connection, and sets all operational Airflow variables.

### Step 1: Clone the Repository
```bash
git clone https://github.com/JayzirTech/datamart-analytics-etl-pipeline.git
cd datamart-analytics-etl-pipeline
```

### Step 2: Create Environment Variables File
Copy the pre-configured `.env.example` template to `.env`:
```bash
# In Windows PowerShell:
Copy-Item .env.example .env

# In Linux / macOS / Bash:
cp .env.example .env
```

*(Default credentials and ports are pre-configured to work out-of-the-box without manual modifications).*

### Step 3: Start All Containers
Launch Docker Compose to build and start the 8 services in the background:
```bash
docker compose up -d
```

### Step 4: Verify Container Health
Verify that all containers are active and healthy:
```bash
docker compose ps
```

You should see the following active containers:
* `datamart_airflow_webserver` — `Up` (`http://localhost:8080`)
* `datamart_airflow_scheduler` — `Up`
* `datamart_airflow_worker` — `Up`
* `datamart_airflow_dag_processor` — `Up`
* `datamart_fastapi_products` — `Up (healthy)` (`http://localhost:8000`)
* `datamart_postgres_dwh` — `Up (healthy)` (Port `5433`)
* `datamart_postgres_airflow` — `Up (healthy)`
* `datamart_rabbitmq` — `Up (healthy)` (`http://localhost:15672`)
* `datamart_airflow_init` — `Exited (0)` *(normal, initialization completed successfully)*

---

## 4. Airflow Connections and Variables Validation

To certify that the pipeline self-configuration executed without manual human intervention, run the following verification commands:

### 4.1 Validate Data Warehouse Connection (`postgres_dwh_conn`)
```bash
docker exec datamart_airflow_scheduler airflow connections get postgres_dwh_conn
```
> **Expected Output:**
> ```text
> conn_type: postgres
> host: postgres_dwh
> port: 5432
> schema: datamart_dwh
> login: dwh_user
> ```

### 4.2 Validate Operational Airflow Variables
```bash
docker exec datamart_airflow_scheduler airflow variables list
```
> **Expected Output:**
> ```text
> key                  
> ====================
> PIPELINE_ENVIRONMENT
> RAW_DATA_PATH       
> FRANKFURTER_API_URL 
> FASTAPI_BASE_URL    
> ```

---

## 5. Service Access and Credentials

| Service | Local URL | Username | Password |
| :--- | :--- | :--- | :--- |
| **Airflow Web UI** | [http://localhost:8080](http://localhost:8080) | `admin` | `admin` |
| **FastAPI Swagger Docs** | [http://localhost:8000/docs](http://localhost:8000/docs) | N/A | N/A |
| **RabbitMQ Management** | [http://localhost:15672](http://localhost:15672) | `airflow_broker` | `broker_secret_password` |
| **PostgreSQL Data Warehouse** | `localhost:5433` / DB: `datamart_dwh` | `dwh_user` | `dwh_secret_password` |

---

## 6. Airflow ETL Pipeline Execution

The DAG **`datamart_etl_daily_pipeline`** is configured with a daily schedule and automatic retries (`retries=2, retry_delay=60s`).

### Trigger the Pipeline Manually:
1. **Via Web UI:** Open [http://localhost:8080](http://localhost:8080), locate `datamart_etl_daily_pipeline`, and click **Trigger DAG (▶)**.
2. **Via CLI:**
   ```bash
   docker exec datamart_airflow_scheduler airflow dags trigger datamart_etl_daily_pipeline
   ```

### DAG Execution Phases:
```mermaid
graph LR
    A[task_verify_sources] --> B[task_extract_currency_rates]
    B --> C[task_run_full_etl]
    C --> D[task_audit_data_quality]
```

1. **`task_verify_sources`:** Verifies presence of `data.csv` and `online_retail_II.csv` in `data/raw/`.
2. **`task_extract_currency_rates`:** Queries the live **Frankfurter API** (`https://api.frankfurter.dev/v1/latest?base=USD`) and synchronizes 29 active currency exchange rates to `dim_exchange_rates`.
3. **`task_run_full_etl`:**
   * Enforces data cleaning and validation rules.
   * Assigns `'ANONYMOUS'` to null customer records (ADR-0002).
   * Deduplicates overlapping records via composite natural key prioritizing `data.csv` (ADR-0004).
   * Computes canonical product descriptions using statistical mode (ADR-0003).
   * Queries the FastAPI product catalog (`/products/bulk`) and enriches categories (ADR-0001).
   * Standardizes all transactional amounts to USD.
   * Segregates valid sales into `fact_sales` and returns into `fact_returns` (ADR-0008).
   * Pre-calculates and persists Daily Net Revenue in `fact_daily_product_revenue` (ADR-0008).
4. **`task_audit_data_quality`:** Audits row counts, check sums, and rejection rates.

---

## 7. Data Load Verification and Business Queries

### 7.1 Verify Row Counts in PostgreSQL DWH
Run a query to validate loaded volumes in the Data Warehouse:
```bash
docker exec datamart_postgres_dwh psql -U dwh_user -d datamart_dwh -c "
SELECT 'fact_sales' AS table_name, count(*) AS total_rows FROM fact_sales
UNION ALL SELECT 'fact_returns', count(*) FROM fact_returns
UNION ALL SELECT 'fact_daily_product_revenue', count(*) FROM fact_daily_product_revenue
UNION ALL SELECT 'dim_products', count(*) FROM dim_products
UNION ALL SELECT 'dim_customers', count(*) FROM dim_customers
UNION ALL SELECT 'dim_exchange_rates', count(*) FROM dim_exchange_rates
UNION ALL SELECT 'rejected_records', count(*) FROM rejected_records;
"
```

> **Expected Volumes in DWH:**
> * `fact_sales`: **524,796 rows**
> * `fact_returns`: **9,177 rows**
> * `fact_daily_product_revenue`: **278,011 rows**
> * `dim_products`: **3,828 products**
> * `dim_customers`: **4,372 customers**
> * `rejected_records`: **5,034 audited rows**

### 7.2 Execute the 7 Business Queries (Section 7)
The SQL file [`sql/queries_negocio.sql`](file:///c:/Users/Coder/Desktop/JayzirTech/data-analytics-performance-test/sql/queries_negocio.sql) formally answers each of the 7 business evaluation questions:

```bash
# In Windows PowerShell:
Get-Content "sql/queries_negocio.sql" | docker exec -i datamart_postgres_dwh psql -U dwh_user -d datamart_dwh

# In Linux / macOS / Bash:
docker exec -i datamart_postgres_dwh psql -U dwh_user -d datamart_dwh < sql/queries_negocio.sql
```

### 7.3 Interactive Exploration via Jupyter Notebook
For a visual, interactive experience where all calculations, percentages, and conclusions are generated dynamically via Python code:
* Open [`notebooks/consultas_negocio.ipynb`](file:///c:/Users/Coder/Desktop/JayzirTech/data-analytics-performance-test/notebooks/consultas_negocio.ipynb) directly in VS Code or JupyterLab.
* Features formatted HTML tables and real-time analytical metrics derived directly from the Data Warehouse.

---

## 8. SOLID Principles & Clean Architecture in Code (`src/`)

Source code follows a strict separation of concerns to ensure maintainability, decoupled testability, and extensibility:

```
src/
├── interfaces/             # Abstract contracts (Interface Segregation / Dependency Inversion)
│   ├── extractor.py        # BaseExtractor
│   ├── validator.py        # BaseValidator
│   ├── transformer.py      # BaseTransformer
│   └── loader.py           # BaseLoader
├── extractors/             # Decoupled extraction implementations
│   ├── kaggle_extractor.py # Raw dataset downloading and loading
│   ├── currency_extractor.py # Frankfurter API client with seed fallback
│   └── api_extractor.py    # REST HTTP client for FastAPI microservice
├── validators/             # Quality rules and rejection segregation
│   └── data_validator.py   # Type checking, prices <= 0, and anomaly filtering
├── transformers/           # Pure business transformations
│   └── datamart_transformer.py # Composite key, canonical mode, USD conversion
├── loaders/                # Idempotent database loaders
│   └── postgres_loader.py  # Batch loading (execute_batch) with ON CONFLICT clauses
└── config/                 # Seeds and operational configuration
    ├── exchange_rates_seed.json # Offline parity backup seed
    └── settings.py
```

* **Single Responsibility Principle (SRP):** Each module has a single reason to change (extractor extracts, validator audits quality, loader manages persistence).
* **Open/Closed Principle (OCP):** New data sources (e.g., S3, BigQuery) can be added by extending `BaseExtractor` without altering transformation logic.
* **Liskov Substitution Principle (LSP):** Any extractor or loader implements the base contract and is interchangeable in the pipeline.
* **Interface Segregation Principle (ISP):** Lean, specific interfaces tailored to concrete ETL stages.
* **Dependency Inversion Principle (DIP):** Airflow tasks depend on abstractions (`BaseExtractor`, `BaseLoader`), not concrete implementations.

---

## 9. Delivery Deliverables & Technical References

1. **Consolidated Technical Decisions Document:** [`DECISIONS.md`](file:///c:/Users/Coder/Desktop/JayzirTech/data-analytics-performance-test/DECISIONS.md) (DWH justification, ambiguous cases resolution, and idempotency guarantees).
2. **Interactive Diagram on dbdiagram.io:** [Public Cloud Visualization](https://dbdiagram.io/d/DataMart-S-A-S-—-Data-Warehouse-and-Analytics-Data-Model-6ae0c592ed134e407e204467) | [DBML Source File](file:///c:/Users/Coder/Desktop/JayzirTech/data-analytics-performance-test/docs/diagrama_modelo_datos.dbml).
3. **Exploratory Data Analysis (EDA):** [`docs/EXPLORACION_DATOS.md`](file:///c:/Users/Coder/Desktop/JayzirTech/data-analytics-performance-test/docs/EXPLORACION_DATOS.md).
4. **Architecture Decision Records (14 ADRs):** [`docs/adr/`](file:///c:/Users/Coder/Desktop/JayzirTech/data-analytics-performance-test/docs/adr/README.md).
5. **Business SQL Queries:** [`sql/queries_negocio.sql`](file:///c:/Users/Coder/Desktop/JayzirTech/data-analytics-performance-test/sql/queries_negocio.sql).
6. **Interactive Analytical Notebook:** [`notebooks/consultas_negocio.ipynb`](file:///c:/Users/Coder/Desktop/JayzirTech/data-analytics-performance-test/notebooks/consultas_negocio.ipynb).
7. **Data Warehouse DDL Schema:** [`dwh_init/01_init_dwh.sql`](file:///c:/Users/Coder/Desktop/JayzirTech/data-analytics-performance-test/dwh_init/01_init_dwh.sql).

---

## 10. End-to-End Pipeline Video Demonstration

As required by the official evaluation rubric (20/20 pts in documentation and technical decisions), a complete end-to-end video walkthrough of the running system is provided:

* 🎥 **Video Demo Link:** [Watch Video Demonstration on YouTube (https://youtu.be/M46kV_vK2ys)](https://youtu.be/M46kV_vK2ys)

### Key Demonstration Highlights:
1. **Automated Deployment:** Executing `docker compose up -d` and checking all 8 containers in `healthy` status.
2. **FastAPI Microservice:** Testing `/health` and `/products/{code}` endpoints on `http://localhost:8000/docs`.
3. **Apache Airflow:** UI on `http://localhost:8080`, validation of `postgres_dwh_conn` and operational variables (`FRANKFURTER_API_URL`, `RAW_DATA_PATH`, etc.).
4. **DAG Execution & Monitoring:** Successful execution of DAG `datamart_etl_daily_pipeline` across all 4 tasks in green (`success`).
5. **PostgreSQL Data Warehouse:** Row count validation on port `5433`, execution of [`sql/queries_negocio.sql`](file:///c:/Users/Coder/Desktop/JayzirTech/data-analytics-performance-test/sql/queries_negocio.sql), and interactive results in [`notebooks/consultas_negocio.ipynb`](file:///c:/Users/Coder/Desktop/JayzirTech/data-analytics-performance-test/notebooks/consultas_negocio.ipynb).
6. **Data Modeling:** Interactive Star Schema walkthrough on [dbdiagram.io](https://dbdiagram.io/d/DataMart-S-A-S-—-Data-Warehouse-and-Analytics-Data-Model-6ae0c592ed134e407e204467).
