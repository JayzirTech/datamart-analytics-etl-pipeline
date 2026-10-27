# 7. Eliminación de la Dimensión de Fecha (dim_dates) y Uso de Tipos Nativos

* **Estado:** Aceptado
* **Fecha:** 2026-10-27

## Contexto
En el modelado tradicional de data warehouses se suele incorporar una tabla dimensional `dim_dates`. Sin embargo, en bases de datos relacionales modernas de alto desempeño como PostgreSQL, el manejo nativo de fechas ofrece gran soporte analítico.

## Decisión
Se decide prescindir de la tabla `dim_dates` y almacenar directamente en las tablas de hechos:
- `invoice_timestamp_utc`: Tipo `TIMESTAMP WITH TIME ZONE` para trazabilidad exacta en UTC.
- `invoice_date`: Tipo `DATE` para particionamiento lógico y agrupaciones diarias directas.

## Consecuencias
- **Positivas:**
  - Modelo dimensional más limpio y directo.
  - Se eliminan joins innecesarios en las consultas analíticas.
  - Facilita el uso de funciones analíticas nativas de PostgreSQL como `DATE_TRUNC('month', invoice_timestamp_utc)` o `EXTRACT(year FROM invoice_timestamp_utc)`.
  - Simplifica el proceso de carga en el pipeline ETL.
