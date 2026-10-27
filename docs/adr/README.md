# Registro de Decisiones de Arquitectura (ADR)
## Proyecto: DataMart S.A.S. — Pipeline ETL con Apache Airflow y Data Warehouse

Este repositorio mantiene los Registros de Decisiones de Arquitectura (Architecture Decision Records) de forma individual para garantizar trazabilidad, modularidad y claridad técnica.

| ID | Título | Estado | Fecha | Archivo |
| :--- | :--- | :---: | :---: | :--- |
| **ADR-0001** | Creación de API Interna de Productos con FastAPI | `Aceptado` | 2026-10-27 | [0001-creacion-api-interna-productos-fastapi.md](0001-creacion-api-interna-productos-fastapi.md) |
| **ADR-0002** | Tratamiento de Transacciones sin Identificador de Cliente (`CustomerID` Nulo) | `Aceptado` | 2026-10-27 | [0002-tratamiento-transacciones-sin-customer-id.md](0002-tratamiento-transacciones-sin-customer-id.md) |
| **ADR-0003** | Resolución de Variaciones de Descripción de Producto (Nombre Canónico por Moda) | `Aceptado` | 2026-10-27 | [0003-resolucion-variaciones-descripcion-producto-moda.md](0003-resolucion-variaciones-descripcion-producto-moda.md) |
| **ADR-0004** | Deduplicación y Unificación de Datasets Solapados | `Aceptado` | 2026-10-27 | [0004-deduplicacion-unificacion-datasets-solapados.md](0004-deduplicacion-unificacion-datasets-solapados.md) |
| **ADR-0005** | Descarga Automatizada de Datasets al Iniciar Docker (`kagglehub`) | `Aceptado` | 2026-10-27 | [0005-descarga-automatizada-datasets-kagglehub.md](0005-descarga-automatizada-datasets-kagglehub.md) |
| **ADR-0006** | Separación de Instancias PostgreSQL (Metadatos vs DWH Analítico en Puerto 5433) | `Aceptado` | 2026-10-27 | [0006-separacion-instancias-postgresql-airflow-dwh.md](0006-separacion-instancias-postgresql-airflow-dwh.md) |
| **ADR-0007** | Eliminación de la Dimensión de Fecha (`dim_dates`) y Uso de Tipos Nativos | `Aceptado` | 2026-10-27 | [0007-eliminacion-dimension-fecha-uso-tipos-nativos.md](0007-eliminacion-dimension-fecha-uso-tipos-nativos.md) |
| **ADR-0008** | Estructura de Hechos con Dos Tablas Separadas (`fact_sales` y `fact_returns`) y Tabla Agregada | `Aceptado` | 2026-10-27 | [0008-estructura-hechos-dos-tablas-separadas-y-agregada.md](0008-estructura-hechos-dos-tablas-separadas-y-agregada.md) |
| **ADR-0009** | Criterios Estrictos de Rechazo a `rejected_records` | `Aceptado` | 2026-10-27 | [0009-criterios-estrictos-rechazo-rejected-records.md](0009-criterios-estrictos-rechazo-rejected-records.md) |
| **ADR-0010** | Versión de Apache Airflow (3.3.2) y Ejecutor Celery con RabbitMQ | `Aceptado` | 2026-10-27 | [0010-version-apache-airflow-3-3-2-celery-rabbitmq.md](0010-version-apache-airflow-3-3-2-celery-rabbitmq.md) |
| **ADR-0011** | Aplicación de Arquitectura Limpia y Principios SOLID en el Pipeline | `Aceptado` | 2026-10-27 | [0011-arquitectura-limpia-y-principios-solid.md](0011-arquitectura-limpia-y-principios-solid.md) |
| **ADR-0012** | Criterios para el Catálogo de Productos en la API FastAPI | `Aceptado` | 2026-10-27 | [0012-criterios-catalogo-productos-fastapi.md](0012-criterios-catalogo-productos-fastapi.md) |
| **ADR-0013** | Configuración de Entorno Docker Airflow y Conexiones Automáticas | `Aceptado` | 2026-10-27 | [0013-configuracion-docker-airflow-conexiones-automaticas.md](0013-configuracion-docker-airflow-conexiones-automaticas.md) |
| **ADR-0014** | Estandarización de Divisas a USD mediante API Pública con Fallback a Semilla | `Aceptado` | 2026-10-27 | [0014-conversion-divisas-usd-api-con-fallback-semilla.md](0014-conversion-divisas-usd-api-con-fallback-semilla.md) |


