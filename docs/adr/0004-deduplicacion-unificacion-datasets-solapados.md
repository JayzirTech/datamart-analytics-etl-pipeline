# 4. Deduplicación y Unificación de Datasets Solapados

* **Estado:** Aceptado
* **Fecha:** 2026-10-27

## Contexto
El pipeline integra dos fuentes obligatorias de Kaggle (`data.csv` y `online_retail_II.csv`) que presentan solapamiento temporal en varios periodos y convenciones de columnas ligeramente distintas.

## Decisión
Se decide aplicar una clave natural compuesta formada por:
`[InvoiceNo, StockCode, InvoiceDate, Quantity]`

En caso de colisión exacta de registros entre ambas fuentes, se otorga prioridad al registro proveniente del volcado transaccional diario (`data.csv`), por constituir la fuente operacional más reciente y con mayor granularidad.

## Consecuencias
- **Positivas:** Se previene la sobrestimación de ventas, ingresos y devoluciones por duplicidad entre fuentes históricas y diarias.
- **Trazabilidad:** Se documenta la regla de prioridad explícitamente en el pipeline de transformación.
