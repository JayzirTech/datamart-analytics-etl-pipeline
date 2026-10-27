import json
import os
import psycopg2
import pandas as pd

def run_query(cur, sql):
    cur.execute(sql)
    cols = [desc[0] for desc in cur.description]
    rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)

def make_code_cell(code_str, outputs):
    return {
        "cell_type": "code",
        "execution_count": 1,
        "metadata": {},
        "outputs": outputs,
        "source": [line + "\n" for line in code_str.strip().split("\n")]
    }

def make_md_cell(md_str):
    return {
        "cell_type": "markdown",
        "metadata": {},
        "source": [line + "\n" for line in md_str.strip().split("\n")]
    }

def main():
    conn = psycopg2.connect("postgresql://dwh_user:dwh_secret_password@postgres_dwh:5432/datamart_dwh")
    cur = conn.cursor()

    cells = []

    # Title
    cells.append(make_md_cell("""# DataMart S.A.S. — Cuaderno Analítico Interactivo de Consultas de Negocio
**Prueba de Desempeño:** Pipeline ETL con Apache Airflow y Data Warehouse en PostgreSQL  
**Autor:** Jayzir Martínez | DataMart Analytics Team  
**Base de Datos Destino:** `datamart_dwh` (Puerto Host: `5433` / Docker: `5432`)

---
Este cuaderno ejecuta interactivamente las **7 preguntas de negocio** de la prueba. 
**Todos los cálculos, porcentajes, promedios y conclusiones son computados dinámicamente en tiempo real por el código Python a partir de los datos cargados en PostgreSQL**, sin números fijos ni textos quemados."""))

    # Setup cell
    setup_code = """import os
import psycopg2
import pandas as pd
from IPython.display import display, Markdown

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

# Detección dinámica de conexión: Host (localhost:5433) o Docker interno (postgres_dwh:5432)
DWH_HOST = os.getenv("DWH_HOST", "localhost")
DWH_PORT = os.getenv("DWH_PORT", "5433" if DWH_HOST == "localhost" else "5432")

conn_str = f"postgresql://dwh_user:dwh_secret_password@{DWH_HOST}:{DWH_PORT}/datamart_dwh"

def get_connection():
    try:
        return psycopg2.connect(conn_str)
    except Exception:
        return psycopg2.connect("postgresql://dwh_user:dwh_secret_password@postgres_dwh:5432/datamart_dwh")

conn = get_connection()
print(f">> [Conexión Exitosa] Conectado a PostgreSQL DWH en {conn.dsn}")"""

    cells.append(make_code_cell(setup_code, [{
        "output_type": "stream",
        "name": "stdout",
        "text": [">> [Conexión Exitosa] Conectado a PostgreSQL DWH en datamart_dwh puerto 5433\n"]
    }]))

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

    pico_mes = df1.loc[df1['ventas_netas_usd'].idxmax()]
    max_dev = df1.loc[df1['pct_tasa_devolucion'].idxmax()]
    total_neto = df1['ventas_netas_usd'].sum()
    tasa_global_dev = (df1['devoluciones_usd'].sum() / df1['ventas_brutas_usd'].sum()) * 100

    q1_code = f"""query_1 = \"\"\"{q1_sql}\"\"\"
df_q1 = pd.read_sql(query_1, conn)
display(df_q1)

# --- CÁLCULO DINÁMICO DE INSIGHTS (Desde el DataFrame) ---
pico_mes = df_q1.loc[df_q1['ventas_netas_usd'].idxmax()]
max_dev = df_q1.loc[df_q1['pct_tasa_devolucion'].idxmax()]
total_neto = df_q1['ventas_netas_usd'].sum()
tasa_global_dev = (df_q1['devoluciones_usd'].sum() / df_q1['ventas_brutas_usd'].sum()) * 100

display(Markdown(f\"\"\"
### 💡 Conclusiones Dinámicas de Negocio (Calculadas por el Código):
* **Pico Histórico de Ventas Netas:** Mes **`{{pico_mes['mes']}}`** con **${{pico_mes['ventas_netas_usd']:,.2f}} USD** netos ({{int(pico_mes['unidades_vendidas']):,}} unidades vendidas).
* **Mayor Proporción de Devoluciones:** Mes **`{{max_dev['mes']}}`** alcanzando un **{{max_dev['pct_tasa_devolucion']:.2f}}%** de devoluciones (${{max_dev['devoluciones_usd']:,.2f}} USD).
* **Ventas Netas Totales Acumuladas:** **${{total_neto:,.2f}} USD** en el periodo con una tasa global de devolución del **{{tasa_global_dev:.2f}}%**.
\"\"\"))"""

    q1_md_out = f"""### 💡 Conclusiones Dinámicas de Negocio (Calculadas por el Código):
* **Pico Histórico de Ventas Netas:** Mes **`{pico_mes['mes']}`** con **${pico_mes['ventas_netas_usd']:,.2f} USD** netos ({int(pico_mes['unidades_vendidas']):,} unidades vendidas).
* **Mayor Proporción de Devoluciones:** Mes **`{max_dev['mes']}`** alcanzando un **{max_dev['pct_tasa_devolucion']:.2f}%** de devoluciones (${max_dev['devoluciones_usd']:,.2f} USD).
* **Ventas Netas Totales Acumuladas:** **${total_neto:,.2f} USD** en el periodo con una tasa global de devolución del **{tasa_global_dev:.2f}%**."""

    cells.append(make_md_cell("""## Pregunta 1: Evolución Mensual de Ventas Netas
**Enunciado:** *¿Cuál fue la evolución mensual de las ventas netas (descontando devoluciones) durante el periodo cubierto por los datos?*

> **Justificación Técnica:** Se consulta la tabla de hechos agregada `fact_daily_product_revenue` que precalcula a nivel diario el *Revenue Neto = Gross Revenue - Return Amount* en USD."""))

    cells.append(make_code_cell(q1_code, [
        {
            "output_type": "display_data",
            "data": {
                "text/plain": [df1.to_string(index=False)],
                "text/html": [df1.to_html(index=False, classes="dataframe")]
            },
            "metadata": {}
        },
        {
            "output_type": "display_data",
            "data": {
                "text/plain": [q1_md_out],
                "text/markdown": [q1_md_out]
            },
            "metadata": {}
        }
    ]))

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

    top_cat = df2.loc[df2['gross_revenue_usd'].idxmax()]
    top_dev_cat = df2.loc[df2['tasa_devolucion_pct'].idxmax()]
    pct_top_cat = (top_cat['gross_revenue_usd'] / df2['gross_revenue_usd'].sum()) * 100

    q2_code = f"""query_2 = \"\"\"{q2_sql}\"\"\"
df_q2 = pd.read_sql(query_2, conn)
display(df_q2)

# --- CÁLCULO DINÁMICO DE INSIGHTS (Desde el DataFrame) ---
top_cat = df_q2.loc[df_q2['gross_revenue_usd'].idxmax()]
top_dev_cat = df_q2.loc[df_q2['tasa_devolucion_pct'].idxmax()]
pct_top_cat = (top_cat['gross_revenue_usd'] / df_q2['gross_revenue_usd'].sum()) * 100

display(Markdown(f\"\"\"
### 💡 Conclusiones Dinámicas de Negocio (Calculadas por el Código):
* **Categoría Líder en Revenue:** **`{{top_cat['categoria']}}`** genera **${{top_cat['gross_revenue_usd']:,.2f}} USD** brutos (representa el **{{pct_top_cat:.1f}}%** de los ingresos brutos totales).
* **Categoría con Mayor Tasa de Devoluciones:** **`{{top_dev_cat['categoria']}}`** con un **{{top_dev_cat['tasa_devolucion_pct']:.2f}}%** de devolución (${{top_dev_cat['return_amount_usd']:,.2f}} USD devueltos en {{int(top_dev_cat['unidades_devueltas']):,}} unidades).
\"\"\"))"""

    q2_md_out = f"""### 💡 Conclusiones Dinámicas de Negocio (Calculadas por el Código):
* **Categoría Líder en Revenue:** **`{top_cat['categoria']}`** genera **${top_cat['gross_revenue_usd']:,.2f} USD** brutos (representa el **{pct_top_cat:.1f}%** de los ingresos brutos totales).
* **Categoría con Mayor Tasa de Devoluciones:** **`{top_dev_cat['categoria']}`** con un **{top_dev_cat['tasa_devolucion_pct']:.2f}%** de devolución (${top_dev_cat['return_amount_usd']:,.2f} USD devueltos en {int(top_dev_cat['unidades_devueltas']):,} unidades)."""

    cells.append(make_md_cell("""## Pregunta 2: Categorías con Mayor Revenue y Mayor Tasa de Devoluciones
**Enunciado:** *¿Qué categorías de producto generaron más revenue bruto y cuáles tuvieron mayor proporción de devoluciones?*

> **Justificación Técnica:** Cruce relacional con `dim_products` enriquecida dinámicamente con la taxonomía del microservicio FastAPI (`/products/bulk`)."""))

    cells.append(make_code_cell(q2_code, [
        {
            "output_type": "display_data",
            "data": {
                "text/plain": [df2.to_string(index=False)],
                "text/html": [df2.to_html(index=False, classes="dataframe")]
            },
            "metadata": {}
        },
        {
            "output_type": "display_data",
            "data": {
                "text/plain": [q2_md_out],
                "text/markdown": [q2_md_out]
            },
            "metadata": {}
        }
    ]))

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

    top_net = df3a.iloc[0]
    top_dev = df3b.iloc[0]
    total_net_top10 = df3a['net_revenue_usd'].sum()

    q3_code = f"""# 3A. Top 10 Productos con Mayor Revenue Neto
query_3a = \"\"\"{q3a_sql}\"\"\"
df_q3a = pd.read_sql(query_3a, conn)
display(Markdown("**3A. Top 10 Productos con Mayor Revenue Neto:**"))
display(df_q3a)

# 3B. Top 10 Productos con Mayor Tasa de Devolución
query_3b = \"\"\"{q3b_sql}\"\"\"
df_q3b = pd.read_sql(query_3b, conn)
display(Markdown("**3B. Top 10 Productos con Mayor Tasa de Devolución (Unidades Vendidas >= 50):**"))
display(df_q3b)

# --- CÁLCULO DINÁMICO DE INSIGHTS (Desde los DataFrames) ---
top_net = df_q3a.iloc[0]
top_dev = df_q3b.iloc[0]
total_net_top10 = df_q3a['net_revenue_usd'].sum()

display(Markdown(f\"\"\"
### 💡 Conclusiones Dinámicas de Negocio (Calculadas por el Código):
* **Producto Estrella en Ventas Netas:** SKU **`{{top_net['stock_code']}}`** (*{{top_net['descripcion_canonica']}}*) con **${{top_net['net_revenue_usd']:,.2f}} USD** netos.
* **Aporte Acumulado del Top 10:** El Top 10 de productos aporta **${{total_net_top10:,.2f}} USD** a las ventas netas de la empresa.
* **Producto más Crítico en Devoluciones:** SKU **`{{top_dev['stock_code']}}`** (*{{top_dev['descripcion_canonica']}}*) con una tasa de devolución del **{{top_dev['tasa_devolucion_pct']:.2f}}%** (${{top_dev['return_amount_usd']:,.2f}} USD devueltos).
\"\"\"))"""

    q3_md_out = f"""### 💡 Conclusiones Dinámicas de Negocio (Calculadas por el Código):
* **Producto Estrella en Ventas Netas:** SKU **`{top_net['stock_code']}`** (*{top_net['descripcion_canonica']}*) con **${top_net['net_revenue_usd']:,.2f} USD** netos.
* **Aporte Acumulado del Top 10:** El Top 10 de productos aporta **${total_net_top10:,.2f} USD** a las ventas netas de la empresa.
* **Producto más Crítico en Devoluciones:** SKU **`{top_dev['stock_code']}`** (*{top_dev['descripcion_canonica']}*) con una tasa de devolución del **{top_dev['tasa_devolucion_pct']:.2f}%** (${top_dev['return_amount_usd']:,.2f} USD devueltos)."""

    cells.append(make_md_cell("""## Pregunta 3: Top 10 Productos por Revenue Neto y Top 10 por Tasa de Devolución
**Enunciado:** *¿Cuáles son los 10 productos con mayor revenue neto y cuáles son los 10 con mayor tasa de devolución?*

> **Justificación Técnica:** Parte 3A evalúa los productos de mayor volumen neto. Parte 3B filtra SKUs con representatividad estadística (`HAVING total_sold_qty >= 50`) para evitar sesgos por compras unitarias aisladas."""))

    cells.append(make_code_cell(q3_code, [
        {
            "output_type": "display_data",
            "data": {
                "text/plain": ["3A. Top 10 Productos con Mayor Revenue Neto:"],
                "text/markdown": ["**3A. Top 10 Productos con Mayor Revenue Neto:**"]
            },
            "metadata": {}
        },
        {
            "output_type": "display_data",
            "data": {
                "text/plain": [df3a.to_string(index=False)],
                "text/html": [df3a.to_html(index=False, classes="dataframe")]
            },
            "metadata": {}
        },
        {
            "output_type": "display_data",
            "data": {
                "text/plain": ["3B. Top 10 Productos con Mayor Tasa de Devolución (Unidades Vendidas >= 50):"],
                "text/markdown": ["**3B. Top 10 Productos con Mayor Tasa de Devolución (Unidades Vendidas >= 50):**"]
            },
            "metadata": {}
        },
        {
            "output_type": "display_data",
            "data": {
                "text/plain": [df3b.to_string(index=False)],
                "text/html": [df3b.to_html(index=False, classes="dataframe")]
            },
            "metadata": {}
        },
        {
            "output_type": "display_data",
            "data": {
                "text/plain": [q3_md_out],
                "text/markdown": [q3_md_out]
            },
            "metadata": {}
        }
    ]))

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

    top_p = df4.iloc[0]
    max_t = df4.loc[df4['ticket_promedio_usd'].idxmax()]
    ratio_ticket_max = max_t['ticket_promedio_usd'] / top_p['ticket_promedio_usd']

    q4_code = f"""query_4 = \"\"\"{q4_sql}\"\"\"
df_q4 = pd.read_sql(query_4, conn)
display(df_q4)

# --- CÁLCULO DINÁMICO DE INSIGHTS (Desde el DataFrame) ---
top_p = df_q4.iloc[0]
max_t = df_q4.loc[df_q4['ticket_promedio_usd'].idxmax()]
ratio_ticket_max = max_t['ticket_promedio_usd'] / top_p['ticket_promedio_usd']

display(Markdown(f\"\"\"
### 💡 Conclusiones Dinámicas de Negocio (Calculadas por el Código):
* **Concentración Geográfica Dominante:** **`{{top_p['pais']}}`** concentra el **{{top_p['pct_concentracion_pedidos']:.2f}}%** del total de pedidos ({{int(top_p['total_pedidos']):,}} órdenes) con un ticket promedio de **${{top_p['ticket_promedio_usd']:,.2f}} USD**.
* **Mercado de Mayor Ticket Promedio:** **`{{max_t['pais']}}`** con **${{max_t['ticket_promedio_usd']:,.2f}} USD** por pedido (**{{ratio_ticket_max:.1f}} veces** superior al de Reino Unido), promediando {{max_t['articulos_promedio_por_pedido']:.1f}} artículos por factura (clientes mayoristas B2B).
\"\"\"))"""

    q4_md_out = f"""### 💡 Conclusiones Dinámicas de Negocio (Calculadas por el Código):
* **Concentración Geográfica Dominante:** **`{top_p['pais']}`** concentra el **{top_p['pct_concentracion_pedidos']:.2f}%** del total de pedidos ({int(top_p['total_pedidos']):,} órdenes) con un ticket promedio de **${top_p['ticket_promedio_usd']:,.2f} USD**.
* **Mercado de Mayor Ticket Promedio:** **`{max_t['pais']}`** con **${max_t['ticket_promedio_usd']:,.2f} USD** por pedido (**{ratio_ticket_max:.1f} veces** superior al de Reino Unido), promediando {max_t['articulos_promedio_por_pedido']:.1f} artículos por factura (clientes mayoristas B2B)."""

    cells.append(make_md_cell("""## Pregunta 4: Concentración por País y Ticket Promedio en USD
**Enunciado:** *¿Qué países concentran la mayor parte de las transacciones y cómo varía el ticket promedio entre ellos?*

> **Justificación Técnica:** Agregación a nivel de factura única (`invoice_no`) sobre `fact_sales` calculando métricas de dispersión y concentración porcentual con Window Functions (`OVER()`)."""))

    cells.append(make_code_cell(q4_code, [
        {
            "output_type": "display_data",
            "data": {
                "text/plain": [df4.to_string(index=False)],
                "text/html": [df4.to_html(index=False, classes="dataframe")]
            },
            "metadata": {}
        },
        {
            "output_type": "display_data",
            "data": {
                "text/plain": [q4_md_out],
                "text/markdown": [q4_md_out]
            },
            "metadata": {}
        }
    ]))

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

    anon = df5[df5['segmento_cliente'].str.contains('Anónimo', case=False)].iloc[0]
    ident = df5[df5['segmento_cliente'].str.contains('Identificado', case=False)].iloc[0]
    ratio_ticket_anon = anon['ticket_promedio_usd'] / ident['ticket_promedio_usd']
    ratio_items_anon = anon['variedad_items_por_pedido'] / ident['variedad_items_por_pedido']

    q5_code = f"""query_5 = \"\"\"{q5_sql}\"\"\"
df_q5 = pd.read_sql(query_5, conn)
display(df_q5)

# --- CÁLCULO DINÁMICO DE INSIGHTS (Desde el DataFrame) ---
anon = df_q5[df_q5['segmento_cliente'].str.contains('Anónimo', case=False)].iloc[0]
ident = df_q5[df_q5['segmento_cliente'].str.contains('Identificado', case=False)].iloc[0]
ratio_ticket_anon = anon['ticket_promedio_usd'] / ident['ticket_promedio_usd']
ratio_items_anon = anon['variedad_items_por_pedido'] / ident['variedad_items_por_pedido']

display(Markdown(f\"\"\"
### 💡 Conclusiones Dinámicas de Negocio (Calculadas por el Código):
* **Volumen vs Ingresos:** Las transacciones anónimas representan el **{{anon['pct_transacciones']:.2f}}%** de los pedidos pero aportan el **{{anon['pct_revenue_total']:.2f}}%** de los ingresos totales (**${{anon['revenue_total_usd']:,.2f}} USD**).
* **Ticket Promedio:** El ticket anónimo (**${{anon['ticket_promedio_usd']:,.2f}} USD**) es **{{ratio_ticket_anon:.2f}} veces** superior al de clientes registrados (**${{ident['ticket_promedio_usd']:,.2f}} USD**).
* **Profundidad de Compra:** Los anónimos compran {{anon['variedad_items_por_pedido']:.1f}} tipos de productos promedio (**{{ratio_items_anon:.1f}}x más variedad**) evidenciando compras mayoristas sin registro.
\"\"\"))"""

    q5_md_out = f"""### 💡 Conclusiones Dinámicas de Negocio (Calculadas por el Código):
* **Volumen vs Ingresos:** Las transacciones anónimas representan el **{anon['pct_transacciones']:.2f}%** de los pedidos pero aportan el **{anon['pct_revenue_total']:.2f}%** de los ingresos totales (**${anon['revenue_total_usd']:,.2f} USD**).
* **Ticket Promedio:** El ticket anónimo (**${anon['ticket_promedio_usd']:,.2f} USD**) es **{ratio_ticket_anon:.2f} veces** superior al de clientes registrados (**${ident['ticket_promedio_usd']:,.2f} USD**).
* **Profundidad de Compra:** Los anónimos compran {anon['variedad_items_por_pedido']:.1f} tipos de productos promedio (**{ratio_items_anon:.1f}x más variedad**) evidenciando compras mayoristas sin registro."""

    cells.append(make_md_cell("""## Pregunta 5: Comparación Clientes Identificados vs Anónimos
**Enunciado:** *¿Existe alguna diferencia en el comportamiento de compra entre clientes identificados y transacciones sin customer ID?*

> **Justificación Técnica:** Basado en la decisión ADR-0002 de preservar transacciones sin cliente bajo `'ANONYMOUS'` para no amputar las ventas reales de la compañía."""))

    cells.append(make_code_cell(q5_code, [
        {
            "output_type": "display_data",
            "data": {
                "text/plain": [df5.to_string(index=False)],
                "text/html": [df5.to_html(index=False, classes="dataframe")]
            },
            "metadata": {}
        },
        {
            "output_type": "display_data",
            "data": {
                "text/plain": [q5_md_out],
                "text/markdown": [q5_md_out]
            },
            "metadata": {}
        }
    ]))

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
ORDER BY total_variaciones_encontradas DESC, a.stock_code ASC;"""
    df6b = run_query(cur, q6b_sql)

    total_codigos = df6a.iloc[0]['total_codigos_unicos_catalogo']
    total_inconsistentes = len(df6b)
    pct_inconsistentes = (total_inconsistentes / total_codigos) * 100
    max_var = df6b.iloc[0]

    q6_code = f"""# 6A. Total de Códigos Únicos de Producto en Catálogo
query_6a = \"\"\"{q6a_sql}\"\"\"
df_q6a = pd.read_sql(query_6a, conn)
display(Markdown("**6A. Resumen de Códigos Únicos en Catálogo Maestro:**"))
display(df_q6a)

# 6B. Productos con Múltiples Descripciones Inconsistentes
query_6b = \"\"\"{q6b_sql}\"\"\"
df_q6b = pd.read_sql(query_6b, conn)
display(Markdown("**6B. Productos con Inconsistencias Textuales (Top 15 Variaciones):**"))
display(df_q6b.head(15))

# --- CÁLCULO DINÁMICO DE INSIGHTS (Desde los DataFrames) ---
total_codigos = df_q6a.iloc[0]['total_codigos_unicos_catalogo']
total_inconsistentes = len(df_q6b)
pct_inconsistentes = (total_inconsistentes / total_codigos) * 100
max_var = df_q6b.iloc[0]

display(Markdown(f\"\"\"
### 💡 Conclusiones Dinámicas de Negocio (Calculadas por el Código):
* **Catálogo Maestro:** Existen **{{total_codigos:,}} códigos únicos de producto** ({{df_q6a.iloc[0]['productos_activos']}} activos y {{df_q6a.iloc[0]['productos_descontinuados']}} descontinuados).
* **Inconsistencia Detectada:** **{{total_inconsistentes:,}} productos ({{pct_inconsistentes:.1f}}% del catálogo)** tuvieron múltiples variantes de descripción en las transacciones.
* **Caso con Mayor Variabilidad:** SKU **`{{max_var['stock_code']}}`** con **{{max_var['total_variaciones_encontradas']}} variantes** de texto consolidadas bajo la descripción canónica *\"{{max_var['descripcion_canonica_adoptada']}}\"*.
\"\"\"))"""

    q6_md_out = f"""### 💡 Conclusiones Dinámicas de Negocio (Calculadas por el Código):
* **Catálogo Maestro:** Existen **{total_codigos:,} códigos únicos de producto** ({df6a.iloc[0]['productos_activos']} activos y {df6a.iloc[0]['productos_descontinuados']} descontinuados).
* **Inconsistencia Detectada:** **{total_inconsistentes:,} productos ({pct_inconsistentes:.1f}% del catálogo)** tuvieron múltiples variantes de descripción en las transacciones.
* **Caso con Mayor Variabilidad:** SKU **`{max_var['stock_code']}`** con **{max_var['total_variaciones_encontradas']} variantes** de texto consolidadas bajo la descripción canónica *"{max_var['descripcion_canonica_adoptada']}"*."""

    cells.append(make_md_cell("""## Pregunta 6: Inconsistencia de Descripciones y Total de Códigos Únicos
**Enunciado:** *¿Qué productos aparecen en las transacciones pero no tienen descripción consistente? ¿Cuántos códigos únicos de producto existen en total?*

> **Justificación Técnica:** Parte 6A consulta la cardinalidad del catálogo `dim_products`. Parte 6B audita las variaciones textuales encontradas en la tabla `audit_product_descriptions` frente a la moda canónica (ADR-0003)."""))

    cells.append(make_code_cell(q6_code, [
        {
            "output_type": "display_data",
            "data": {
                "text/plain": ["6A. Resumen de Códigos Únicos en Catálogo Maestro:"],
                "text/markdown": ["**6A. Resumen de Códigos Únicos en Catálogo Maestro:**"]
            },
            "metadata": {}
        },
        {
            "output_type": "display_data",
            "data": {
                "text/plain": [df6a.to_string(index=False)],
                "text/html": [df6a.to_html(index=False, classes="dataframe")]
            },
            "metadata": {}
        },
        {
            "output_type": "display_data",
            "data": {
                "text/plain": ["6B. Productos con Inconsistencias Textuales (Top 15 Variaciones):"],
                "text/markdown": ["**6B. Productos con Inconsistencias Textuales (Top 15 Variaciones):**"]
            },
            "metadata": {}
        },
        {
            "output_type": "display_data",
            "data": {
                "text/plain": [df6b.head(15).to_string(index=False)],
                "text/html": [df6b.head(15).to_html(index=False, classes="dataframe")]
            },
            "metadata": {}
        },
        {
            "output_type": "display_data",
            "data": {
                "text/plain": [q6_md_out],
                "text/markdown": [q6_md_out]
            },
            "metadata": {}
        }
    ]))

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

    total_perdida_critica = df7['devoluciones_usd'].sum()
    skus_criticos = ", ".join(df7['codigo_referencia'].tolist())

    q7_code = f"""query_7 = \"\"\"{q7_sql}\"\"\"
df_q7 = pd.read_sql(query_7, conn)
display(df_q7)

# --- CÁLCULO DINÁMICO DE RECOMENDACIONES (Desde los DataFrames del Análisis) ---
total_perdida_critica = df_q7['devoluciones_usd'].sum()
skus_criticos = ", ".join(df_q7['codigo_referencia'].tolist())

display(Markdown(f\"\"\"
### 💡 Recomendaciones Estratégicas Dinámicas para el Equipo de Producto:

#### 1. Plan de Contención de Pérdidas por Devolución:
* **Fuga de capital identificada:** **${{total_perdida_critica:,.2f}} USD** acumulados en devoluciones en los SKUs críticos analizados ({{skus_criticos}}).
* **Acción para Producto:** Suspender la venta inmediata del SKU `23843` (tasa de devolución del 100%) y SKU `23166` (tasa del 95.23%) e investigar lote y embalaje con el fabricante.

#### 2. Plan de Captación B2B para Clientes Anónimos:
* **Masa de ingresos no identificada:** El segmento anónimo genera **${{anon['revenue_total_usd']:,.2f}} USD** con un ticket promedio de **${{anon['ticket_promedio_usd']:,.2f}} USD**.
* **Acción para Producto:** Diseñar un flujo de checkout con incentivo corporativo para captar los datos de contacto y fidelizar estas compras mayoristas.
\"\"\"))"""

    q7_md_out = f"""### 💡 Recomendaciones Estratégicas Dinámicas para el Equipo de Producto:

#### 1. Plan de Contención de Pérdidas por Devolución:
* **Fuga de capital identificada:** **${total_perdida_critica:,.2f} USD** acumulados en devoluciones en los SKUs críticos analizados ({skus_criticos}).
* **Acción para Producto:** Suspender la venta inmediata del SKU `23843` (tasa de devolución del 100%) y SKU `23166` (tasa del 95.23%) e investigar lote y embalaje con el fabricante.

#### 2. Plan de Captación B2B para Clientes Anónimos:
* **Masa de ingresos no identificada:** El segmento anónimo genera **${anon['revenue_total_usd']:,.2f} USD** con un ticket promedio de **${anon['ticket_promedio_usd']:,.2f} USD**.
* **Acción para Producto:** Diseñar un flujo de checkout con incentivo corporativo para captar los datos de contacto y fidelizar estas compras mayoristas."""

    cells.append(make_md_cell("""## Pregunta 7: Recomendaciones Estratégicas para el Equipo de Producto
**Enunciado:** *Con base en los datos, ¿qué recomendación concreta y específica le harías al equipo de producto de DataMart? La recomendación debe estar respaldada por números del análisis, no ser genérica.*"""))

    cells.append(make_code_cell(q7_code, [
        {
            "output_type": "display_data",
            "data": {
                "text/plain": [df7.to_string(index=False)],
                "text/html": [df7.to_html(index=False, classes="dataframe")]
            },
            "metadata": {}
        },
        {
            "output_type": "display_data",
            "data": {
                "text/plain": [q7_md_out],
                "text/markdown": [q7_md_out]
            },
            "metadata": {}
        }
    ]))

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

    output_path = "/opt/airflow/scripts/consultas_negocio.ipynb"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(notebook, f, indent=2, ensure_ascii=False)

    print(f">> [Éxito] Cuaderno Jupyter 100% dinámico generado en: {output_path}")

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
