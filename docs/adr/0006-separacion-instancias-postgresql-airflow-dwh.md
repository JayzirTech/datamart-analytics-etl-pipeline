# 6. Separación de Instancias PostgreSQL (Metadatos de Airflow vs DWH Analítico)

* **Estado:** Aceptado
* **Fecha:** 2026-10-27

## Contexto
El enunciado técnico especifica textualmente:
> *"Una base de datos PostgreSQL dedicada a los metadatos internos de Airflow (no debe ser la misma que el repositorio analítico). Una base de datos PostgreSQL separada que funciona como repositorio analítico destino del pipeline, con las tablas ya creadas al iniciar."*

## Decisión
Se decide configurar dos contenedores de PostgreSQL totalmente independientes en `docker-compose.yml`:
1. `postgres_airflow`: Base de datos para el estado y metadatos de Airflow (red interna Docker).
2. `postgres_dwh`: Base de datos para el Data Warehouse analítico, mapeada al puerto del host `5433` (para evitar colisiones con instancias locales preexistentes en el puerto `5432`), con scripts DDL de inicialización automática en `/docker-entrypoint-initdb.d/`.

## Consecuencias
- **Positivas:** Aislamiento absoluto entre la carga operacional/orquestación y la carga analítica de datos. Cumplimiento estricto del requerimiento evaluativo.
- **Seguridad:** Credenciales y conexiones desacopladas mediante variables de entorno en `.env`.
