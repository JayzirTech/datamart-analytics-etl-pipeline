-- ==============================================================================
-- DATAMART S.A.S. - PRUEBA DE DESEMPEÑO: PIPELINE ANALÍTICO Y DATA WAREHOUSE
-- ARCHIVO DE CONSULTAS SQL DE NEGOCIO (SECCIÓN 7 DEL ENUNCIADO)
-- Base de Datos Destino: PostgreSQL (datamart_dwh / Puerto Host: 5433)
-- Autor: Jayzir Martínez | DataMart Analytics Team
-- ==============================================================================
-- Instrucciones de Ejecución:
-- 1. Desde terminal:
--    docker exec -i datamart_postgres_dwh psql -U dwh_user -d datamart_dwh < sql/queries_negocio.sql
-- 2. Desde cualquier cliente SQL (DBeaver, DataGrip, pgAdmin):
--    Host: localhost, Puerto: 5433, Base de Datos: datamart_dwh, Usuario: dwh_user, Password: dwh_secret_password
-- ==============================================================================


-- ==============================================================================
-- PREGUNTA 1: EVOLUCIÓN MENSUAL DE VENTAS NETAS
-- "¿Cuál fue la evolución mensual de las ventas netas (descontando devoluciones) 
--  durante el periodo cubierto por los datos?"
-- ==============================================================================
-- Justificación Técnica:
-- Se consulta la tabla de hechos agregada 'fact_daily_product_revenue' (ADR-0008),
-- la cual contiene el cálculo idempotente pre-agregado por fecha:
-- Net Revenue = Gross Revenue (Ventas) - Return Amount (Devoluciones).
-- Tiempos de respuesta sub-segundo gracias a los índices por fecha.
-- ==============================================================================

SELECT 
    TO_CHAR(invoice_date, 'YYYY-MM') AS mes,
    ROUND(SUM(gross_revenue)::numeric, 2) AS ventas_brutas_usd,
    ROUND(SUM(return_amount)::numeric, 2) AS devoluciones_usd,
    ROUND(SUM(net_revenue)::numeric, 2) AS ventas_netas_usd,
    SUM(total_sold_qty) AS unidades_vendidas,
    SUM(total_returned_qty) AS unidades_devueltas,
    ROUND(((SUM(return_amount) / NULLIF(SUM(gross_revenue), 0)) * 100)::numeric, 2) AS pct_tasa_devolucion
FROM fact_daily_product_revenue
GROUP BY TO_CHAR(invoice_date, 'YYYY-MM')
ORDER BY mes ASC;


-- ==============================================================================
-- PREGUNTA 2: CATEGORÍAS DE PRODUCTO (REVENUE VS DEVOLUCIONES)
-- "¿Qué categorías de producto generaron más revenue bruto y cuáles tuvieron 
--  mayor proporción de devoluciones?"
-- ==============================================================================
-- Justificación Técnica:
-- Cruce entre 'fact_daily_product_revenue' y la dimensión 'dim_products' enriquecida
-- por el microservicio FastAPI (ADR-0001, ADR-0012).
-- Se calcula la tasa porcentual de devolución: (Monto Devoluciones / Revenue Bruto) * 100.
-- ==============================================================================

SELECT 
    p.category_name AS categoria,
    ROUND(SUM(fd.gross_revenue)::numeric, 2) AS gross_revenue_usd,
    ROUND(SUM(fd.return_amount)::numeric, 2) AS return_amount_usd,
    ROUND(SUM(fd.net_revenue)::numeric, 2) AS net_revenue_usd,
    SUM(fd.total_sold_qty) AS unidades_vendidas,
    SUM(fd.total_returned_qty) AS unidades_devueltas,
    ROUND(((SUM(fd.return_amount) / NULLIF(SUM(fd.gross_revenue), 0)) * 100)::numeric, 2) AS tasa_devolucion_pct
FROM fact_daily_product_revenue fd
JOIN dim_products p ON fd.product_key = p.product_key
GROUP BY p.category_name
ORDER BY gross_revenue_usd DESC;


-- ==============================================================================
-- PREGUNTA 3: TOP 10 PRODUCTOS POR REVENUE NETO Y TOP 10 POR TASA DE DEVOLUCIÓN
-- "¿Cuáles son los 10 productos con mayor revenue neto y cuáles son los 10 con 
--  mayor tasa de devolución?"
-- ==============================================================================
-- Justificación Técnica:
-- Se divide en dos consultas complementarias:
-- 3A: Productos estrella por mayor aporte monetario neto a la compañía.
-- 3B: Productos problemáticos por mayor tasa de devolución porcentual.
-- Para evitar sesgos estadísticos (ej. un producto vendido 1 vez y devuelto 1 vez = 100%),
-- se aplica una regla de representatividad estadística (HAVING total_sold_qty >= 50).
-- ==============================================================================

-- 3A. Top 10 Productos con Mayor Revenue Neto (USD)
SELECT 
    p.stock_code,
    p.canonical_description AS descripcion_canonica,
    p.category_name AS categoria,
    ROUND(SUM(fd.net_revenue)::numeric, 2) AS net_revenue_usd,
    SUM(fd.total_sold_qty) AS unidades_vendidas,
    SUM(fd.total_returned_qty) AS unidades_devueltas,
    ROUND(((SUM(fd.return_amount) / NULLIF(SUM(fd.gross_revenue), 0)) * 100)::numeric, 2) AS tasa_devolucion_pct
FROM fact_daily_product_revenue fd
JOIN dim_products p ON fd.product_key = p.product_key
GROUP BY p.stock_code, p.canonical_description, p.category_name
ORDER BY net_revenue_usd DESC
LIMIT 10;

-- 3B. Top 10 Productos con Mayor Tasa de Devolución (% sobre unidades y monto)
SELECT 
    p.stock_code,
    p.canonical_description AS descripcion_canonica,
    p.category_name AS categoria,
    ROUND(SUM(fd.gross_revenue)::numeric, 2) AS gross_revenue_usd,
    ROUND(SUM(fd.return_amount)::numeric, 2) AS return_amount_usd,
    SUM(fd.total_sold_qty) AS unidades_vendidas,
    SUM(fd.total_returned_qty) AS unidades_devueltas,
    ROUND(((SUM(fd.return_amount) / NULLIF(SUM(fd.gross_revenue), 0)) * 100)::numeric, 2) AS tasa_devolucion_pct
FROM fact_daily_product_revenue fd
JOIN dim_products p ON fd.product_key = p.product_key
GROUP BY p.stock_code, p.canonical_description, p.category_name
HAVING SUM(fd.total_sold_qty) >= 50
ORDER BY tasa_devolucion_pct DESC
LIMIT 10;


-- ==============================================================================
-- PREGUNTA 4: CONCENTRACIÓN POR PAÍS Y TICKET PROMEDIO EN USD
-- "¿Qué países concentran la mayor parte de las transacciones y cómo varía 
--  el ticket promedio entre ellos?"
-- ==============================================================================
-- Justificación Técnica:
-- Se agregan las ventas a nivel de factura única ('invoice_no') sobre 'fact_sales',
-- garantizando que el ticket promedio refleje el valor real por pedido completo
-- en dólares estandarizados vía Frankfurter API (ADR-0014).
-- Se calcula el porcentaje de concentración mediante Window Functions (OVER()).
-- ==============================================================================

WITH invoice_totals AS (
    SELECT 
        country,
        invoice_no,
        SUM(gross_revenue) AS total_factura_usd,
        SUM(quantity) AS total_articulos_factura
    FROM fact_sales
    GROUP BY country, invoice_no
)
SELECT 
    country AS pais,
    COUNT(invoice_no) AS total_pedidos,
    ROUND((COUNT(invoice_no) * 100.0 / SUM(COUNT(invoice_no)) OVER())::numeric, 2) AS pct_concentracion_pedidos,
    ROUND(SUM(total_factura_usd)::numeric, 2) AS revenue_total_usd,
    ROUND(AVG(total_factura_usd)::numeric, 2) AS ticket_promedio_usd,
    ROUND(AVG(total_articulos_factura)::numeric, 1) AS articulos_promedio_por_pedido
FROM invoice_totals
GROUP BY country
ORDER BY total_pedidos DESC
LIMIT 15;


-- ==============================================================================
-- PREGUNTA 5: COMPORTAMIENTO CLIENTES IDENTIFICADOS VS TRANSACCIONES SIN CLIENTE
-- "¿Existe alguna diferencia en el comportamiento de compra entre clientes 
--  identificados y transacciones sin customer ID?"
-- ==============================================================================
-- Justificación Técnica:
-- Conforme al ADR-0002, las transacciones sin CustomerID no fueron descartadas
-- sino asignadas al cliente canónico 'ANONYMOUS'.
-- Esta consulta compara el volumen transaccional, ticket promedio, revenue global
-- y profundidad del carrito entre clientes registrados y clientes invitados.
-- ==============================================================================

WITH invoice_summary AS (
    SELECT 
        s.invoice_no,
        c.customer_type,
        SUM(s.gross_revenue) AS total_factura_usd,
        SUM(s.quantity) AS total_articulos_factura,
        COUNT(s.sale_key) AS lineas_por_factura
    FROM fact_sales s
    JOIN dim_customers c ON s.customer_key = c.customer_key
    GROUP BY s.invoice_no, c.customer_type
)
SELECT 
    CASE 
        WHEN customer_type = 'ANONIMO' THEN 'Consumidor Anónimo / Sin Registro'
        ELSE 'Cliente Identificado / Registrado'
    END AS segmento_cliente,
    COUNT(invoice_no) AS total_transacciones,
    ROUND((COUNT(invoice_no) * 100.0 / SUM(COUNT(invoice_no)) OVER())::numeric, 2) AS pct_transacciones,
    ROUND(SUM(total_factura_usd)::numeric, 2) AS revenue_total_usd,
    ROUND((SUM(total_factura_usd) * 100.0 / SUM(SUM(total_factura_usd)) OVER())::numeric, 2) AS pct_revenue_total,
    ROUND(AVG(total_factura_usd)::numeric, 2) AS ticket_promedio_usd,
    ROUND(AVG(total_articulos_factura)::numeric, 1) AS articulos_promedio_por_pedido,
    ROUND(AVG(lineas_por_factura)::numeric, 1) AS variedad_items_por_pedido
FROM invoice_summary
GROUP BY customer_type;


-- ==============================================================================
-- PREGUNTA 6: INCONSISTENCIA DE DESCRIPCIONES Y TOTAL DE CÓDIGOS ÚNICOS
-- "¿Qué productos aparecen en las transacciones pero no tienen descripción consistente?
--  ¿Cuántos códigos únicos de producto existen en total?"
-- ==============================================================================
-- Justificación Técnica:
-- Parte 1: Conteo total de códigos de producto normalizados en 'dim_products'.
-- Parte 2: Detección y auditoría de los 647 productos que presentaron 2 o más
-- variaciones de texto en los datasets fuente, contrastados contra la descripción
-- canónica seleccionada por la moda estadística (ADR-0003).
-- ==============================================================================

-- 6A. Total de Códigos Únicos de Producto Registrados en el Catálogo
SELECT 
    COUNT(DISTINCT stock_code) AS total_codigos_unicos_catalogo,
    COUNT(CASE WHEN is_active THEN 1 END) AS productos_activos,
    COUNT(CASE WHEN NOT is_active THEN 1 END) AS productos_descontinuados
FROM dim_products;

-- 6B. Productos con Descripciones Inconsistentes en Transacciones
SELECT 
    a.stock_code,
    p.canonical_description AS descripcion_canonica_adoptada,
    p.category_name AS categoria,
    COUNT(DISTINCT a.raw_description) AS total_variaciones_encontradas,
    STRING_AGG(a.raw_description, ' || ' ORDER BY a.frequency DESC) AS variaciones_detectadas
FROM audit_product_descriptions a
JOIN dim_products p ON a.stock_code = p.stock_code
GROUP BY a.stock_code, p.canonical_description, p.category_name
HAVING COUNT(DISTINCT a.raw_description) > 1
ORDER BY total_variaciones_encontradas DESC, a.stock_code ASC
LIMIT 20;


-- ==============================================================================
-- PREGUNTA 7: RECOMENDACIÓN ESTRATÉGICA PARA EL EQUIPO DE PRODUCTO
-- "Con base en los datos, ¿qué recomendación concreta y específica le harías al 
--  equipo de producto de DataMart? La recomendación debe estar respaldada por números 
--  del análisis, no ser genérica."
-- ==============================================================================
-- RECOMENDACIONES ESTRATÉGICAS BASADAS EN DATOS:
--
-- 1. CONTROL DE CALIDAD Y SUSPENSIÓN INMEDIATA DE SKUs PROBLEMÁTICOS:
--    El análisis revela que apenas 2 productos concentran más de $328,000 USD en devoluciones:
--    - SKU '23843' ('PAPER CRAFT , LITTLE BIRDIE'): 80,995 unidades vendidas y el 100% DEVUELTAS ($224,866.42 USD).
--    - SKU '23166' ('MEDIUM CERAMIC TOP STORAGE JAR'): 78,033 unidades vendidas y 74,494 DEVUELTAS (95.23% tasa, $103,403.66 USD).
--    ACCIÓN: Pausar inmediatamente la comercialización de estos SKUs e iniciar investigación con
--    el proveedor de Papelería/Hogar por posible defecto masivo de empaque o fabricación.
--
-- 2. PROGRAMA DE CAPTACIÓN B2B PARA CLIENTES ANÓNIMOS:
--    Los compradores sin registro generan el 17.00% de la facturación total ($2,317,458.74 USD)
--    con un ticket promedio de $1,622.87 USD (2.66 veces mayor que el cliente identificado promedio: $610.54 USD)
--    y compran en promedio 92.5 artículos por factura (compras corporativas/mayoristas).
--    ACCIÓN: Implementar un checkout con registro rápido incentivado (ej. 2% de descuento corporativo
--    en la primera orden registrada) para capturar los datos de contacto y fidelizar este segmento de alto valor.
-- ==============================================================================

-- Consulta Diagnóstica de Respaldo para la Recomendación:
SELECT 
    'SKU Problemático Crítico' AS hallazgo,
    p.stock_code AS codigo_referencia,
    p.canonical_description AS descripcion,
    p.category_name AS categoria,
    ROUND(SUM(fd.gross_revenue)::numeric, 2) AS ventas_brutas_usd,
    ROUND(SUM(fd.return_amount)::numeric, 2) AS devoluciones_usd,
    ROUND(((SUM(fd.return_amount) / NULLIF(SUM(fd.gross_revenue), 0)) * 100)::numeric, 2) AS tasa_devolucion_pct
FROM fact_daily_product_revenue fd
JOIN dim_products p ON fd.product_key = p.product_key
WHERE p.stock_code IN ('23843', '23166', 'M')
GROUP BY p.stock_code, p.canonical_description, p.category_name
ORDER BY devoluciones_usd DESC;
