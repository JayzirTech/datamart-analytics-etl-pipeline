# 3. Resolución de Variaciones de Descripción de Producto (Nombre Canónico)

* **Estado:** Aceptado
* **Fecha:** 2026-10-27

## Contexto
En los registros operacionales, un mismo código de producto (`StockCode`) presenta variaciones de redacción, mayúsculas, minúsculas o erratas (por ejemplo: `CANDLE HOLDER WHITE`, `Candle Holder White`, `candle holder white`).

## Decisión
Se decide determinar el nombre canónico de cada producto mediante la **moda estadística** (la descripción más frecuente normalizada) o la provista por el catálogo maestro de la API.

## Consecuencias
- **Positivas:** Se garantiza uniformidad tipográfica y consistencia analítica en los reportes de ventas y rankings de productos, eliminando fragmentación generada por diferencias de digitación.
- **Negativas / Mitigaciones:** Requiere una etapa de agregación previa en la transformación; se implementa eficientemente con pandas/SQL en la etapa de transformación del ETL.
