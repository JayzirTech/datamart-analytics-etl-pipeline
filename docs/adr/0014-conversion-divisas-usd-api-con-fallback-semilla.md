# 14. Estandarización de Divisas a USD mediante API Pública con Fallback a Semilla

* **Estado:** Aceptado
* **Fecha:** 2026-10-27

## Contexto
El enunciado identifica como problema de negocio crítico:
> *"La dirección no puede comparar el desempeño entre países porque las monedas y los formatos de fecha son distintos en cada fuente."*

Además, la Pregunta de Negocio #4 requiere analizar el ticket promedio entre países. Dado que los datasets contienen transacciones en distintas monedas (GBP en Reino Unido, EUR en países europeos, COP en Colombia, MXN en México, PEN en Perú), se requiere una estandarización monetaria para permitir comparabilidad financiera real.

## Decisión
1. **Moneda Base Corporativa:** Estandarizar todos los importes analíticos a **USD** (Dólar estadounidense).
2. **Estrategia Resiliente de Obtención de Tasas de Cambio:**
   - **Opción Primaria:** Consultar la API oficial de **Frankfurter API** (`https://api.frankfurter.dev/v1/latest?base=USD`) para obtener las paridades de divisas hacia USD.
   - **Mecanismo de Resiliencia:** Si tras 3 reintentos con timeout la API externa no responde o no hay acceso a internet, o para monedas regionales no rastreadas por el BCE (ej. COP, PEN), el pipeline utiliza automáticamente el catálogo semilla local respaldado (`exchange_rates_seed.json`).

3. **Persistencia Histórica:**
   - Las tasas obtenidas se persisten en la tabla `dim_exchange_rates` dentro del Data Warehouse PostgreSQL.
   - Dado que PostgreSQL utiliza un volumen administrado de Docker (`postgres_dwh_data`), los registros históricos permanecen guardados permanentemente, garantizando idempotencia en re-ejecuciones.
4. **Campos en el Modelo Dimensional:**
   - `currency_original`: Código ISO de la moneda local del país emisor (ej. GBP, EUR, COP).
   - `unit_price_original`: Precio en moneda local de origen.
   - `exchange_rate_to_usd`: Factor de conversión aplicado a USD.
   - `unit_price`: Precio unitario estandarizado en USD.
   - `gross_revenue`: Importe bruto estandarizado en USD (`quantity * unit_price`).

## Consecuencias
- **Positivas:** Resuelve directamente el dolor gerencial de consolidación multinacional, permite responder con precisión el ticket promedio por país, y combina actualización dinámica con alta resiliencia ante cortes de conectividad.
