# 10. Versión de Apache Airflow (3.3.2) y Ejecutor Celery con RabbitMQ

* **Estado:** Aceptado
* **Fecha:** 2026-10-27

## Contexto
El entorno orquestador debe ser escalable y estar preparado para una eventual migración productiva a la nube (AWS con RabbitMQ y Kafka). Se requiere utilizar la versión más reciente disponible de Apache Airflow.

## Decisión
Se decide utilizar la imagen oficial `apache/airflow:3.3.2` configurada con **`CeleryExecutor`**.

### Componentes de Infraestructura:
- **Message Broker:** RabbitMQ (`rabbitmq:management`) para la distribución asíncrona de tareas hacia los workers de Celery.
- **Airflow Webserver:** Interfaz gráfica accesible en `http://localhost:8080`.
- **Airflow Scheduler:** Planificador y supervisor de ejecuciones de DAGs.
- **Airflow Worker:** Contenedor dedicado a la ejecución distribuida de tareas Celery.
- **Postgres Airflow:** Almacén de metadatos de Airflow y result backend.

## Consecuencias
- **Positivas:** Desacoplamiento total entre componentes, alta resiliencia y preparación directa para despliegues distribuidos en AWS (Amazon MQ).
- **Negativas / Mitigaciones:** Mayor consumo de recursos que `LocalExecutor`; se compensa con límites ligeros de memoria en `docker-compose.yml`.
