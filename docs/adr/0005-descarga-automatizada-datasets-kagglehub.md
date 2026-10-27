# 5. Descarga Automatizada de Datasets al Iniciar Docker (kagglehub)

* **Estado:** Aceptado
* **Fecha:** 2026-10-27

## Contexto
El objetivo de la prueba es permitir que cualquier evaluador levante el entorno con un único comando (`docker-compose up`) y ejecute el pipeline en menos de 10 minutos sin necesidad de descargar y ubicar manualmente archivos CSV voluminosos.

## Decisión
Se decide implementar un script/servicio de verificación al arranque de Docker (`scripts/download_datasets.py`) que comprueba la existencia de los archivos en `data/raw/`:
1. `data.csv` (fuente `carrie1/ecommerce-data`)
2. `online_retail_II.csv` (fuente `thedevastator/online-retail-transaction-dataset`)

Si los archivos no están presentes, se descargan automáticamente mediante la librería oficial `kagglehub` de forma transparente. Si ya existen, la operación se omite (idempotencia).

## Consecuencias
- **Positivas:** Experiencia "cero configuración" (out-of-the-box) para el evaluador. Cumplimiento riguroso del criterio de levantamiento rápido y automatizado.
- **Respaldo:** Soporte para ejecución offline en caso de que los archivos ya hayan sido descargados previamente en el volumen compartido.
