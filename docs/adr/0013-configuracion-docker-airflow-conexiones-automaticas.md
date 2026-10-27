# 13. Configuración de Entorno Docker Airflow y Conexiones Automáticas

* **Estado:** Aceptado
* **Fecha:** 2026-10-27

## Contexto
El evaluador exige que la solución arranque con un único comando (`docker-compose up`) sin pasos manuales adicionales, y que las conexiones (`Airflow Connections`) y variables operativas (`Airflow Variables`) queden inicializadas desde el arranque sin intervención en la interfaz web. Asimismo, se requiere garantizar rapidez en el arranque de los contenedores distribuidos con Celery.

## Decisión
1. **Imagen Personalizada (`Dockerfile.airflow`):**
   - Construir una imagen extendida a partir de `apache/airflow:3.3.2` que instale previamente: `pandas`, `psycopg2-binary`, `apache-airflow-providers-postgres`, `requests` y `kagglehub`.
   - Se evitan descargas repetitivas en cada encendido de contenedor asociadas a `_PIP_ADDITIONAL_REQUIREMENTS`.
2. **Doble Garantía de Configuración Automática:**
   - **Vía variables de entorno nativas de Airflow:** Se definen `AIRFLOW_CONN_POSTGRES_DWH_CONN` y las variables `AIRFLOW_VAR_*` en el entorno común de los contenedores.
   - **Vía contenedor `airflow-init`:** Se ejecuta un script de inicialización (`scripts/init_airflow.sh`) que aplica migraciones de base de datos (`airflow db migrate`), crea el usuario administrador (`admin`/`admin`), y registra explícitamente la conexión y variables en el catálogo de metadatos para visibilidad directa en la UI de Airflow (*Admin -> Connections* y *Admin -> Variables*).

## Consecuencias
- **Positivas:** Cumplimiento riguroso del criterio "cero intervención manual", arranque rápido y consistente de los workers, y transparencia total para el evaluador.
