"""
==============================================================================
DATAMART S.A.S. - DAG DE ORQUESTACIÓN ETL DIARIA (AIRFLOW 3.3.2)
==============================================================================
Pipeline analítico de extremo a extremo que procesa transacciones,
aisla inconsistencias contables a rejected_records, enriquece con la API
FastAPI de catálogo, estandariza divisas a USD con Frankfurter API y carga
de forma idempotente en PostgreSQL Data Warehouse.

Conforme a los Registros de Decisiones de Arquitectura (ADR-001 al ADR-0014).
==============================================================================
"""

import os
import sys
from datetime import datetime, timedelta

# Asegurar visibilidad de src/ dentro del contenedor de Airflow
sys.path.insert(0, "/opt/airflow/src")

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.hooks.base import BaseHook

# Importación de clases bajo Principios SOLID
from extractors.csv_extractor import CsvExtractor
from extractors.api_extractor import ApiExtractor
from extractors.currency_extractor import CurrencyExtractor
from validators.quality_validator import QualityValidator
from transformers.datamart_transformer import DatamartTransformer
from loaders.postgres_loader import PostgresLoader


# Parámetros por defecto del DAG (Reintentos automáticos ante fallos)
DEFAULT_ARGS = {
    "owner": "Jayzir Martinez",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=20),
}

# Instancia del DAG con schedule diario e idempotencia
dag = DAG(
    dag_id="datamart_etl_daily_pipeline",
    default_args=DEFAULT_ARGS,
    description="Pipeline ETL diario de DataMart S.A.S. con CeleryExecutor y PostgreSQL DWH",
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["datamart", "etl", "dwh", "celery", "solid"],
)


def get_dwh_connection_uri() -> str:
    """Lee y construye la URI de la conexión oficial de Airflow (postgres_dwh_conn)."""
    try:
        conn = BaseHook.get_connection("postgres_dwh_conn")
        login = conn.login or "dwh_user"
        pwd = conn.password or "dwh_secret_password"
        host = conn.host or "postgres_dwh"
        port = conn.port or 5432
        schema = conn.schema or "datamart_dwh"
        return f"postgresql://{login}:{pwd}@{host}:{port}/{schema}"
    except Exception as e:
        print(f"[DAG] Advertencia al leer connection 'postgres_dwh_conn': {e}. Usando URI por defecto.")
        return os.getenv(
            "AIRFLOW_CONN_POSTGRES_DWH_CONN",
            "postgresql://dwh_user:dwh_secret_password@postgres_dwh:5432/datamart_dwh"
        )


def verify_sources_fn(**kwargs):
    """
    Tarea 1: Verifica la disponibilidad de los archivos y lee Airflow Variables.
    """
    raw_path = Variable.get("RAW_DATA_PATH", default_var="/opt/airflow/data/raw")
    api_url = Variable.get("FASTAPI_BASE_URL", default_var="http://fastapi_products:8000")
    frankfurter_url = Variable.get("FRANKFURTER_API_URL", default_var="https://api.frankfurter.dev/v1/latest?base=USD")
    print(f">> [DAG] Variable RAW_DATA_PATH: {raw_path}")
    print(f">> [DAG] Variable FASTAPI_BASE_URL: {api_url}")
    print(f">> [DAG] Variable FRANKFURTER_API_URL: {frankfurter_url}")

    if not os.path.exists(raw_path):
        os.makedirs(raw_path, exist_ok=True)
    
    files = os.listdir(raw_path)
    print(f">> [DAG] Archivos en carpeta de datos: {files}")
    return {"raw_path": raw_path, "api_url": api_url, "frankfurter_url": frankfurter_url, "files_found": files}


def extract_currency_rates_fn(**kwargs):
    """
    Tarea 2: Consulta Frankfurter API y persiste paridades hacia USD en dim_exchange_rates.
    """
    dwh_uri = get_dwh_connection_uri()
    frankfurter_url = Variable.get("FRANKFURTER_API_URL", default_var="https://api.frankfurter.dev/v1/latest?base=USD")
    curr_extractor = CurrencyExtractor(api_url=frankfurter_url)
    rates = curr_extractor.extract()

    loader = PostgresLoader(conn_uri=dwh_uri)
    loader.load_exchange_rates(rates, source_label="FRANKFURTER_API")
    print(f">> [DAG] Tasas hacia USD sincronizadas: {len(rates)} divisas registradas.")


def run_full_etl_fn(**kwargs):
    """
    Tarea 3: Ejecuta la extracción, validación de calidad, transformación, enriquecimiento
    y carga idempotente en PostgreSQL Data Warehouse bajo Principios SOLID.
    """
    raw_path = Variable.get("RAW_DATA_PATH", default_var="/opt/airflow/data/raw")
    api_url = Variable.get("FASTAPI_BASE_URL", default_var="http://fastapi_products:8000")
    dwh_uri = get_dwh_connection_uri()

    # 1. Extracción de fuentes CSV (CsvExtractor)
    csv_extractor = CsvExtractor(raw_data_path=raw_path)
    raw_df = csv_extractor.extract()
    print(f">> [DAG Extracción] Filas brutas leídas: {len(raw_df)}")

    # 2. Validación de Calidad y Detección de Rechazos (QualityValidator - ADR-009)
    validator = QualityValidator()
    valid_df, rejected_df = validator.validate(raw_df)

    # 3. Transformación Analítica y Reglas de Negocio (DatamartTransformer)
    curr_extractor = CurrencyExtractor()
    transformer = DatamartTransformer(currency_extractor=curr_extractor)
    transformed = transformer.transform(valid_df)

    # 4. Enriquecimiento de Catálogo con API FastAPI (ApiExtractor - ADR-001, ADR-012)
    api_extractor = ApiExtractor(base_url=api_url)
    unique_products_df = transformed["dim_products"]
    products_payload = [
        {"stock_code": row["stock_code"], "description": row["canonical_description"]}
        for _, row in unique_products_df.iterrows()
    ]
    
    print(f">> [DAG Enriquecimiento] Consultando API FastAPI para {len(products_payload)} productos...")
    enriched_map = api_extractor.bulk_enrich(products_payload)
    enriched_products_list = list(enriched_map.values())

    # 5. Carga Idempotente al Data Warehouse (PostgresLoader - ADR-006, ADR-008)
    loader = PostgresLoader(conn_uri=dwh_uri)
    
    # 5.1 Clientes
    customer_map = loader.load_dim_customers(transformed["dim_customers"])
    # 5.2 Productos
    product_map = loader.load_dim_products(enriched_products_list)
    # 5.3 Ventas
    sales_loaded = loader.load_fact_sales(transformed["fact_sales"], product_map, customer_map)
    # 5.4 Devoluciones
    returns_loaded = loader.load_fact_returns(transformed["fact_returns"], product_map, customer_map)
    # 5.5 Revenue diario consolidado
    daily_loaded = loader.load_daily_product_revenue(transformed["fact_daily_product_revenue"], product_map)
    # 5.6 Log de rechazos
    rejected_loaded = loader.load_rejected_records(rejected_df)

    print("=" * 70)
    print(">> [DAG RESUMEN DE CARGA EXITOSA]")
    print(f"   - Clientes registrados: {len(customer_map)}")
    print(f"   - Referencias de producto: {len(product_map)}")
    print(f"   - Ventas cargadas (fact_sales): {sales_loaded}")
    print(f"   - Devoluciones cargadas (fact_returns): {returns_loaded}")
    print(f"   - Agregados diarios persistidos: {daily_loaded}")
    print(f"   - Transacciones rechazadas auditadas: {rejected_loaded}")
    print("=" * 70)


def audit_quality_fn(**kwargs):
    """
    Tarea 4: Auditoría final de integridad en el DWH.
    """
    import psycopg2
    dwh_uri = get_dwh_connection_uri()
    with psycopg2.connect(dwh_uri) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM fact_sales WHERE gross_revenue <= 0;")
            invalid_sales = cur.fetchone()[0]
            if invalid_sales > 0:
                raise ValueError(f"[Auditoría Calidad] Se encontraron {invalid_sales} ventas con revenue <= 0 en fact_sales.")

            cur.execute("SELECT COUNT(*) FROM fact_sales;")
            total_sales = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM fact_returns;")
            total_returns = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM rejected_records;")
            total_rejected = cur.fetchone()[0]

            print(f">> [Auditoría OK] Total Ventas: {total_sales} | Total Devoluciones: {total_returns} | Rechazos auditados: {total_rejected}")


# Definición de tareas del DAG
task_verify_sources = PythonOperator(
    task_id="task_verify_sources",
    python_callable=verify_sources_fn,
    dag=dag,
)

task_extract_currency = PythonOperator(
    task_id="task_extract_currency_rates",
    python_callable=extract_currency_rates_fn,
    dag=dag,
)

task_run_etl = PythonOperator(
    task_id="task_run_full_etl",
    python_callable=run_full_etl_fn,
    dag=dag,
)

task_audit = PythonOperator(
    task_id="task_audit_data_quality",
    python_callable=audit_quality_fn,
    dag=dag,
)

# Dependencias explícitas
task_verify_sources >> task_extract_currency >> task_run_etl >> task_audit
