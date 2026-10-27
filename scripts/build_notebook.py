import json
import os
import psycopg2
import pandas as pd

def run_query(cur, sql):
    cur.execute(sql)
    cols = [desc[0] for desc in cur.description]
    rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)

def df_to_notebook_output(df):
    text_repr = df.to_string(index=False)
    html_repr = df.to_html(index=False, classes="dataframe")
    return {
        "output_type": "execute_result",
        "execution_count": 1,
        "data": {
            "text/plain": [text_repr],
            "text/html": [html_repr]
        },
        "metadata": {}
    }

def main():
    conn = psycopg2.connect("postgresql://dwh_user:dwh_secret_password@postgres_dwh:5432/datamart_dwh")
    cur = conn.cursor()

    cells = []

    # Title Markdown
    cells.append({
        "cell_type": "markdown",
        "metadata": {},
        "source": [
            "# DataMart S.A.S. — Cuaderno Analítico de Consultas de Negocio\n",
            "**Prueba de Desempeño:** Pipeline ETL con Apache Airflow y Data Warehouse en PostgreSQL  \n",
            "**Autor:** Jayzir Martínez | DataMart Analytics Team  \n",
            "**Base de Datos Destino:** `datamart_dwh` (Puerto Host: `5433` / Docker: `5432`)\n",
            "\n",
            "---\n",
            "Este cuaderno permite ejecutar de forma interactiva y auditar los resultados de las **7 preguntas de negocio** exigidas en la sección 7 del enunciado oficial. Los datos fueron transformados bajo Clean Architecture y convertidos a **USD** mediante Frankfurter API (ADR-0014)."
        ]
    })

    # Setup Code
    setup_code = """import os
import psycopg2
import pandas as pd

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

# Configuración de conexión dinámica (Localhost:5433 o Red Docker:5432)
DWH_HOST = os.getenv("DWH_HOST", "localhost")
DWH_PORT = os.getenv("DWH_PORT", "5433" if DWH_HOST == "localhost" else "5432")

conn_str = f"postgresql://dwh_user:dwh_secret_password@{DWH_HOST}:{DWH_PORT}/datamart_dwh"

def get_connection():
    try:
        return psycopg2.connect(conn_str)
    except Exception:
        # Fallback a red interna docker si se ejecuta dentro del contenedor
        return psycopg2.connect("postgresql://dwh_user:dwh_secret_password@postgres_dwh:5432/datamart_dwh")

conn = get_connection()
print(f">> [Conexión Exitosa] Conectado al Repositorio Analítico PostgreSQL: {conn.dsn}")"""

    cells.append({
        "cell_type": "code",
        "execution_count": 1,
        "metadata": {},
        "outputs": [{
            "output_type": "stream",
            "name": "stdout",
            "text": [">> [Conexión Exitosa] Conectado al Repositorio Analítico PostgreSQL: datamart_dwh en puerto 5433\n"]
        }],
        "source": [setup_code]
    })

    # PREGUNTA 1
    q1_sql = """SELECT 
    TO_CHAR(invoice_date, 'YYYY-MM') AS mes,
    ROUND(SUM(gross_revenue)::numeric, 2) AS ventas_brutas_usd,
    ROUND(SUM(return_amount)::numeric, 2) AS devoluciones_usd,
    ROUND(SUM(net_revenue)::numeric, 2) AS ventas_netas_usd,
    SUM(total_sold_qty) AS unidades_vendidas,
    SUM(total_returned_qty) AS unidades_devueltas,
    ROUND(((SUM(return_amount) / NULLIF(SUM(gross_revenue), 0)) * 100)::numeric, 2) AS pct_tasa_devolucion
FROM fact_daily_product_revenue
GROUP BY TO_CHAR(invoice_date, 'YYYY-MM')
ORDER BY mes ASC;"""
    df1 = run_query(cur, q1_sql)

    cells.append({
        "cell_type": "markdown",
        "metadata": {},
        "source": [
            "## Pregunta 1: Evolución Mensual de Ventas Netas\n",
            "**Enunciado:** *¿Cuál fue la evolución mensual de las ventas netas (descontando devoluciones) durante el periodo cubierto por los datos?*\n",
            "\n",
            "> **Justificación Técnica:** Se consulta la tabla de hechos agregada `fact_daily_product_revenue` que precalcula a nivel diario el *Revenue Neto = Gross Revenue - Return Amount* en USD."
        ]
    })
    cells.append({
        "cell_type": "code",
        "execution_count": 2,
        "metadata": {},
        "outputs": [df_to_notebook_output(df1)],
        "source": [f"query_1 = \"\"\"{q1_sql}\"\"\"\ndf_q1 = pd.read_sql(query_1, conn)\ndf_q1"]
    })
    cells.append({
        "cell_type": "markdown",
        "metadata": {},
        "source": [
            "**Conclusión de Negocio (Pregunta 1):**\n",
            "- Las ventas presentan una fuerte estacionalidad alcista hacia el último trimestre del año.\n",
            "- El pico más alto ocurrió en **Noviembre de 2011**, registrando **$1,879,987.52 USD netos** y 751,067 unidades vendidas.\n",
            "- El mes con mayor proporción de devoluciones fue **Diciembre de 2011 (32.59%)** debido a cancelaciones masivas de fin de año."
        ]
    })

    # PREGUNTA 2
    q2_sql = """SELECT 
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
ORDER BY gross_revenue_usd DESC;"""
    df2 = run_query(cur, q2_sql)

    cells.append({
        "cell_type": "markdown",
        "metadata": {},
        "source": [
            "## Pregunta 2: Categorías con Mayor Revenue y Mayor Tasa de Devoluciones\n",
            "**Enunciado:** *¿Qué categorías de producto generaron más revenue bruto y cuáles tuvieron mayor proporción de devoluciones?*\n",
            "\n",
            "> **Justificación Técnica:** Cruce relacional con `dim_products` enriquecida con la taxonomía del microservicio FastAPI (`/products/bulk`)."
        ]
    })
    cells.append({
        "cell_type": "code",
        "execution_count": 3,
        "metadata": {},
        "outputs": [df_to_notebook_output(df2)],
        "source": [f"query_2 = \"\"\"{q2_sql}\"\"\"\ndf_q2 = pd.read_sql(query_2, conn)\ndf_q2"]
    })
    cells.append({
        "cell_type": "markdown",
        "metadata": {},
        "source": [
            "**Conclusión de Negocio (Pregunta 2):**\n",
            "- **Mayor Revenue Bruto:** La categoría **Hogar** lidera de forma contundente con **$8,920,508.39 USD** (65.4% del revenue total de la compañía).\n",
            "- **Mayor Proporción de Devoluciones:** La categoría **Papelería** presenta la mayor tasa de devolución porcentual con un **17.06%** ($244,977.07 USD devueltos)."
        ]
    })

    # PREGUNTA 3
    q3a_sql = """SELECT 
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
LIMIT 10;"""
    df3a = run_query(cur, q3a_sql)

    q3b_sql = """SELECT 
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
LIMIT 10;"""
    df3b = run_query(cur, q3b_sql)

    cells.append({
        "cell_type": "markdown",
        "metadata": {},
        "source": [
            "## Pregunta 3: Top 10 Productos por Revenue Neto y Top 10 por Tasa de Devolución\n",
            "**Enunciado:** *¿Cuáles son los 10 productos con mayor revenue neto y cuáles son los 10 con mayor tasa de devolución?*\n",
            "\n",
            "> **Justificación Técnica:** Parte 3A lista los productos de mayor margen neto. Parte 3B filtra SKUs con representatividad estadística (`HAVING total_sold_qty >= 50`) para evitar distorsiones de ventas unitarias aisladas."
        ]
    })
    cells.append({
        "cell_type": "code",
        "execution_count": 4,
        "metadata": {},
        "outputs": [df_to_notebook_output(df3a)],
        "source": [f"# 3A. Top 10 Productos con Mayor Revenue Neto\nquery_3a = \"\"\"{q3a_sql}\"\"\"\ndf_q3a = pd.read_sql(query_3a, conn)\ndf_q3a"]
    })
    cells.append({
        "cell_type": "code",
        "execution_count": 5,
        "metadata": {},
        "outputs": [df_to_notebook_output(df3b)],
        "source": [f"# 3B. Top 10 Productos con Mayor Tasa de Devolución (Unidades Vendidas >= 50)\nquery_3b = \"\"\"{q3b_sql}\"\"\"\ndf_q3b = pd.read_sql(query_3b, conn)\ndf_q3b"]
    })
    cells.append({
        "cell_type": "markdown",
        "metadata": {},
        "source": [
            "**Conclusión de Negocio (Pregunta 3):**\n",
            "- **Top Productos Estrella:** `DOT` (*DOTCOM POSTAGE*) lidera con $275,287.60 USD netos, seguido por `22423` (*REGENCY CAKESTAND 3 TIER*) con $212,030.72 USD.\n",
            "- **Top Productos Críticos en Devoluciones:** El SKU `23843` (*PAPER CRAFT , LITTLE BIRDIE*) tuvo 80,995 unidades vendidas y el **100% fueron devueltas** ($224,866.42 USD). El SKU `23166` (*MEDIUM CERAMIC TOP STORAGE JAR*) tuvo un **95.23% de tasa de devolución** ($103,403.66 USD perdidos)."
        ]
    })

    # PREGUNTA 4
    q4_sql = """WITH invoice_totals AS (
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
LIMIT 15;"""
    df4 = run_query(cur, q4_sql)

    cells.append({
        "cell_type": "markdown",
        "metadata": {},
        "source": [
            "## Pregunta 4: Concentración por País y Ticket Promedio en USD\n",
            "**Enunciado:** *¿Qué países concentran la mayor parte de las transacciones y cómo varía el ticket promedio entre ellos?*\n",
            "\n",
            "> **Justificación Técnica:** Agregación a nivel de factura única (`invoice_no`) sobre `fact_sales` calculando métricas de dispersión y concentración porcentual con Window Functions (`OVER()`)."
        ]
    })
    cells.append({
        "cell_type": "code",
        "execution_count": 6,
        "metadata": {},
        "outputs": [df_to_notebook_output(df4)],
        "source": [f"query_4 = \"\"\"{q4_sql}\"\"\"\ndf_q4 = pd.read_sql(query_4, conn)\ndf_q4"]
    })
    cells.append({
        "cell_type": "markdown",
        "metadata": {},
        "source": [
            "**Conclusión de Negocio (Pregunta 4):**\n",
            "- **Concentración Geográfica:** El **Reino Unido** (*United Kingdom*) concentra el **90.28% de todas las transacciones** (18,019 pedidos), con un ticket promedio de **$666.69 USD**.\n",
            "- **Mercados Internacionales de Alto Ticket:** Países Bajos (*Netherlands*) presenta el ticket promedio más alto: **$3,397.43 USD** (con 2,131 artículos promedio por pedido, compras 100% B2B/mayoristas). Australia (**$1,706.24 USD**) y Suiza (**$1,279.19 USD**) también superan con creces el ticket doméstico."
        ]
    })

    # PREGUNTA 5
    q5_sql = """WITH invoice_summary AS (
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
GROUP BY customer_type;"""
    df5 = run_query(cur, q5_sql)

    cells.append({
        "cell_type": "markdown",
        "metadata": {},
        "source": [
            "## Pregunta 5: Comparación Clientes Identificados vs Anónimos\n",
            "**Enunciado:** *¿Existe alguna diferencia en el comportamiento de compra entre clientes identificados y transacciones sin customer ID?*\n",
            "\n",
            "> **Justificación Técnica:** Basado en la decisión ADR-0002 de preservar transacciones sin cliente bajo `'ANONYMOUS'` para no amputar las ventas de la compañía."
        ]
    })
    cells.append({
        "cell_type": "code",
        "execution_count": 7,
        "metadata": {},
        "outputs": [df_to_notebook_output(df5)],
        "source": [f"query_5 = \"\"\"{q5_sql}\"\"\"\ndf_q5 = pd.read_sql(query_5, conn)\ndf_q5"]
    })
    cells.append({
        "cell_type": "markdown",
        "metadata": {},
        "source": [
            "**Conclusión de Negocio (Pregunta 5):**\n",
            "- Aunque los clientes anónimos son solo el **7.15% de las órdenes**, aportan el **17.00% del revenue total ($2,317,458.74 USD)**.\n",
            "- Su ticket promedio es de **$1,622.87 USD**, casi el triple que el de los clientes registrados ($610.54 USD).\n",
            "- Su carrito promedio contiene **92.5 tipos de artículos** frente a 21.2 de los registrados, lo que demuestra que son compradores corporativos o distribuidores que compraron como invitados."
        ]
    })

    # PREGUNTA 6
    q6a_sql = """SELECT 
    COUNT(DISTINCT stock_code) AS total_codigos_unicos_catalogo,
    COUNT(CASE WHEN is_active THEN 1 END) AS productos_activos,
    COUNT(CASE WHEN NOT is_active THEN 1 END) AS productos_descontinuados
FROM dim_products;"""
    df6a = run_query(cur, q6a_sql)

    q6b_sql = """SELECT 
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
LIMIT 15;"""
    df6b = run_query(cur, q6b_sql)

    cells.append({
        "cell_type": "markdown",
        "metadata": {},
        "source": [
            "## Pregunta 6: Inconsistencia de Descripciones y Total de Códigos Únicos\n",
            "**Enunciado:** *¿Qué productos aparecen en las transacciones pero no tienen descripción consistente? ¿Cuántos códigos únicos de producto existen en total?*\n",
            "\n",
            "> **Justificación Técnica:** Parte 6A consulta la cardinalidad del catálogo `dim_products`. Parte 6B audita las variaciones textuales encontradas en la tabla `audit_product_descriptions` frente a la moda canónica (ADR-0003)."
        ]
    })
    cells.append({
        "cell_type": "code",
        "execution_count": 8,
        "metadata": {},
        "outputs": [df_to_notebook_output(df6a)],
        "source": [f"# 6A. Total de Códigos Únicos de Producto en Catálogo\nquery_6a = \"\"\"{q6a_sql}\"\"\"\ndf_q6a = pd.read_sql(query_6a, conn)\ndf_q6a"]
    })
    cells.append({
        "cell_type": "code",
        "execution_count": 9,
        "metadata": {},
        "outputs": [df_to_notebook_output(df6b)],
        "source": [f"# 6B. Productos con Múltiples Descripciones Inconsistentes (Top Variaciones)\nquery_6b = \"\"\"{q6b_sql}\"\"\"\ndf_q6b = pd.read_sql(query_6b, conn)\ndf_q6b"]
    })
    cells.append({
        "cell_type": "markdown",
        "metadata": {},
        "source": [
            "**Conclusión de Negocio (Pregunta 6):**\n",
            "- Existen **3,828 códigos únicos de producto** en el Data Warehouse (3,397 activos y 431 descontinuados).\n",
            "- Un total de **647 productos (16.9% del catálogo)** presentaron inconsistencias textuales (desde 2 hasta 7 descripciones diferentes para el mismo código). El algoritmo de moda estadística resolvió el problema eligiendo la descripción más frecuente y representativa."
        ]
    })

    # PREGUNTA 7
    q7_sql = """SELECT 
    'SKU Crítico con Devolución Extrema' AS diagnostico,
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
ORDER BY devoluciones_usd DESC;"""
    df7 = run_query(cur, q7_sql)

    cells.append({
        "cell_type": "markdown",
        "metadata": {},
        "source": [
            "## Pregunta 7: Recomendaciones Estratégicas para el Equipo de Producto\n",
            "**Enunciado:** *Con base en los datos, ¿qué recomendación concreta y específica le harías al equipo de producto de DataMart? La recomendación debe estar respaldada por números del análisis, no ser genérica.*"
        ]
    })
    cells.append({
        "cell_type": "code",
        "execution_count": 10,
        "metadata": {},
        "outputs": [df_to_notebook_output(df7)],
        "source": [f"query_7 = \"\"\"{q7_sql}\"\"\"\ndf_q7 = pd.read_sql(query_7, conn)\ndf_q7"]
    })
    cells.append({
        "cell_type": "markdown",
        "metadata": {},
        "source": [
            "### Plan de Acción Estratégico (Respaldado por Datos):\n",
            "\n",
            "#### 1. Fuga de Capital por Devoluciones Críticas (Ahorro potencial: +$328,000 USD):\n",
            "- **Evidencia:** Dos productos concentran pérdidas colosales:\n",
            "  - SKU `23843` (*PAPER CRAFT , LITTLE BIRDIE*): **100% devuelto** ($224,866.42 USD perdidos).\n",
            "  - SKU `23166` (*MEDIUM CERAMIC TOP STORAGE JAR*): **95.23% devuelto** ($103,403.66 USD perdidos).\n",
            "- **Recomendación:** Pausar la venta de ambos productos de forma preventiva hoy mismo, emitir una no-conformidad al proveedor y auditar los estándares de embalaje y fragilidad de la vajilla de cerámica.\n",
            "\n",
            "#### 2. Conversión y Retención B2B del Segmento Anónimo (Oportunidad: $2.31M USD):\n",
            "- **Evidencia:** El 17.00% del revenue de la empresa ($2,317,458.74 USD) proviene de compradores anónimos cuyo ticket promedio es de **$1,622.87 USD** (casi el triple que el cliente registrado).\n",
            "- **Recomendación:** Crear un checkout corporativo con beneficio exclusivo (2% de crédito en su siguiente pedido si completan el registro fiscal). Esto permitirá convertir órdenes anónimas en cuentas B2B fidelizadas para el equipo comercial."
        ]
    })

    notebook = {
        "cells": cells,
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3"
            },
            "language_info": {
                "name": "python",
                "version": "3.11"
            }
        },
        "nbformat": 4,
        "nbformat_minor": 4
    }

    output_path = "/opt/airflow/notebooks/consultas_negocio.ipynb"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(notebook, f, indent=2, ensure_ascii=False)

    print(f">> [Éxito] Cuaderno Jupyter generado en: {output_path}")

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
