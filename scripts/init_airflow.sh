#!/usr/bin/env bash
# ==============================================================================
# DATAMART S.A.S. - SCRIPT DE INICIALIZACIÓN DE AIRFLOW (ADR-005, ADR-013)
# ==============================================================================
set -e

echo "========================================================================"
echo ">> [1/5] Verificando y descargando datasets requeridos (kagglehub)..."
echo "========================================================================"
python /opt/airflow/scripts/download_datasets.py || echo "[AVISO] Descarga automática omitida o continuada con datos existentes."

echo "========================================================================"
echo ">> [2/5] Aplicando migraciones de base de datos de Airflow (db migrate)..."
echo "========================================================================"
airflow db migrate

echo "========================================================================"
echo ">> [3/5] Configurando credenciales de usuario administrador de Airflow..."
echo "========================================================================"
python -c "import json; open('/opt/airflow/simple_auth_manager_passwords.json.generated', 'w').write(json.dumps({'${_AIRFLOW_WWW_USER_USERNAME:-admin}': '${_AIRFLOW_WWW_USER_PASSWORD:-admin}'}))"
airflow users create \
    --username "${_AIRFLOW_WWW_USER_USERNAME:-admin}" \
    --password "${_AIRFLOW_WWW_USER_PASSWORD:-admin}" \
    --firstname "Admin" \
    --lastname "DataMart" \
    --role "${_AIRFLOW_WWW_USER_ROLE:-Admin}" \
    --email "${_AIRFLOW_WWW_USER_EMAIL:-admin@datamart.com}" 2>/dev/null || true
echo "[INFO] Credenciales de admin configuradas exitosamente."


echo "========================================================================"
echo ">> [4/5] Configurando Airflow Connection hacia el repositorio analítico..."
echo "========================================================================"
airflow connections delete 'postgres_dwh_conn' 2>/dev/null || true

airflow connections add 'postgres_dwh_conn' \
    --conn-type 'postgres' \
    --conn-host 'postgres_dwh' \
    --conn-login "${DWH_DB_USER:-dwh_user}" \
    --conn-password "${DWH_DB_PASSWORD:-dwh_secret_password}" \
    --conn-schema "${DWH_DB_NAME:-datamart_dwh}" \
    --conn-port 5432

echo "========================================================================"
echo ">> [5/5] Inicializando Airflow Variables operativas del pipeline..."
echo "========================================================================"
airflow variables set RAW_DATA_PATH "${RAW_DATA_PATH:-/opt/airflow/data/raw}"
airflow variables set FASTAPI_BASE_URL "${FASTAPI_BASE_URL:-http://fastapi_products:8000}"
airflow variables set FRANKFURTER_API_URL "${FRANKFURTER_API_URL:-https://api.frankfurter.dev/v1/latest?base=USD}"
airflow variables set PIPELINE_ENVIRONMENT "${PIPELINE_ENVIRONMENT:-development}"

echo "========================================================================"
echo ">> [LISTO] Inicialización de Airflow completada exitosamente."
echo "========================================================================"
