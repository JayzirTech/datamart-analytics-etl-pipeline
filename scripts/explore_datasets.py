"""
Script de Exploración y Análisis de Calidad de Datos (EDA)
DataMart S.A.S. - Evaluación de fuentes data.csv y online_retail_II.csv
"""

import os
import pandas as pd
import numpy as np

RAW_DIR = "/app/data/raw"

def explore_file(filename, encoding="ISO-8859-1"):
    filepath = os.path.join(RAW_DIR, filename)
    print("=" * 80)
    print(f"EXPLORANDO: {filename}")
    print("=" * 80)
    
    df = pd.read_csv(filepath, encoding=encoding, low_memory=False)
    print(f"Dimensiones: {df.shape[0]} filas x {df.shape[1]} columnas")
    print(f"Columnas: {list(df.columns)}")
    print("\nTipos de datos y Nulos:")
    null_summary = pd.DataFrame({
        "Tipo": df.dtypes,
        "Nulos": df.isnull().sum(),
        "% Nulos": (df.isnull().sum() / len(df) * 100).round(2)
    })
    print(null_summary)

    # Identificar nombres de columnas comunes
    col_map = {c.lower().replace(" ", "").replace("_", ""): c for c in df.columns}
    qty_col = col_map.get("quantity")
    price_col = col_map.get("unitprice") or col_map.get("price")
    cust_col = col_map.get("customerid") or col_map.get("customer")
    date_col = col_map.get("invoicedate")
    code_col = col_map.get("stockcode")
    desc_col = col_map.get("description")
    country_col = col_map.get("country")

    if qty_col:
        qty_num = pd.to_numeric(df[qty_col], errors="coerce")
        print("\nDistribución de Quantity:")
        print(f"  - Positivos (> 0): {(qty_num > 0).sum()} ({(qty_num > 0).sum() / len(df) * 100:.2f}%)")
        print(f"  - Ceros (== 0): {(qty_num == 0).sum()}")
        print(f"  - Negativos (< 0 - Devoluciones): {(qty_num < 0).sum()} ({(qty_num < 0).sum() / len(df) * 100:.2f}%)")
        print(f"  - Rango: [{qty_num.min()} .. {qty_num.max()}]")

    if price_col:
        price_num = pd.to_numeric(df[price_col], errors="coerce")
        print("\nDistribución de UnitPrice / Price:")
        print(f"  - Positivos (> 0): {(price_num > 0).sum()} ({(price_num > 0).sum() / len(df) * 100:.2f}%)")
        print(f"  - Ceros (== 0): {(price_num == 0).sum()}")
        print(f"  - Negativos (< 0): {(price_num < 0).sum()}")
        print(f"  - Rango: [{price_num.min()} .. {price_num.max()}]")

    # Casos de rechazo
    if qty_col and price_col:
        qty_num = pd.to_numeric(df[qty_col], errors="coerce")
        price_num = pd.to_numeric(df[price_col], errors="coerce")
        
        bad_praxis = (qty_num <= 0) & (price_num <= 0)
        invalid_sales = (qty_num > 0) & (price_num <= 0)
        print("\nAnálisis de Casos Anómalos (Reglas de Rechazo ADR-009):")
        print(f"  - Mala praxis (Qty <= 0 Y Price <= 0): {bad_praxis.sum()} filas")
        print(f"  - Ventas inválidas (Qty > 0 Y Price <= 0): {invalid_sales.sum()} filas")

    if cust_col:
        null_cust = df[cust_col].isnull().sum()
        print(f"\nTransacciones sin Customer ID: {null_cust} filas ({null_cust / len(df) * 100:.2f}%)")

    if date_col:
        dates = pd.to_datetime(df[date_col], errors="coerce")
        print(f"\nRango de Fechas: {dates.min()} hasta {dates.max()}")

    if country_col:
        print(f"\nTop 5 Países por volumen de transacciones:")
        print(df[country_col].value_counts().head(5))

    if code_col and desc_col:
        clean_code = df[code_col].astype(str).str.strip().str.upper()
        unique_codes = clean_code.nunique()
        print(f"\nCódigos únicos de producto: {unique_codes}")
        
        # Variaciones de descripción
        desc_counts = df.groupby(clean_code)[desc_col].nunique()
        var_desc_codes = (desc_counts > 1).sum()
        print(f"Productos con más de 1 descripción diferente: {var_desc_codes} ({var_desc_codes / unique_codes * 100:.2f}%)")

    return df

print("=" * 80)
print("INICIANDO EXPLORACIÓN DE LOS DATASETS DESCARGADOS")
print("=" * 80)

df_daily = explore_file("data.csv", encoding="ISO-8859-1")
df_hist = explore_file("online_retail_II.csv", encoding="ISO-8859-1")

print("\n" + "=" * 80)
print("ANÁLISIS DE SOLAPAMIENTO ENTRE FUENTES:")
print("=" * 80)
d1 = pd.to_datetime(df_daily["InvoiceDate"], errors="coerce")
date_col2 = "InvoiceDate" if "InvoiceDate" in df_hist.columns else "invoicedate"
d2 = pd.to_datetime(df_hist[date_col2], errors="coerce")

print(f"data.csv periodo:             {d1.min()} a {d1.max()}")
print(f"online_retail_II.csv periodo: {d2.min()} a {d2.max()}")

# Comparar duplicidad exacta entre datasets
merged_overlap = pd.merge(
    df_daily[["InvoiceNo", "StockCode", "Quantity"]],
    df_hist[["InvoiceNo", "StockCode", "Quantity"]],
    on=["InvoiceNo", "StockCode", "Quantity"],
    how="inner"
)
print(f"Total registros duplicados/solapados entre ambos datasets: {len(merged_overlap)} filas")
