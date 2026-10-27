import pandas as pd
import numpy as np
from typing import Dict, Any, Tuple
from interfaces.transformer import BaseTransformer
from extractors.currency_extractor import CurrencyExtractor


class DatamartTransformer(BaseTransformer):
    """
    Transformador de reglas de negocio analíticas (ADR-002, ADR-003, ADR-004, ADR-008, ADR-014).
    Aplica:
    - Normalización de códigos de producto (Upper, Trim).
    - Asignación de 'ANONYMOUS' a clientes nulos.
    - Deduplicación por clave natural compuesta priorizando data.csv.
    - Cálculo de la descripción canónica por la moda estadística.
    - Conversión monetaria a USD mediante CurrencyExtractor.
    - Segregación estricta de fact_sales y fact_returns.
    - Cálculo y agregación de Revenue Neto diario por producto.
    """

    def __init__(self, currency_extractor: CurrencyExtractor = None):
        self.currency_extractor = currency_extractor or CurrencyExtractor()

    def transform(self, valid_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        print(f"[DatamartTransformer] Iniciando transformaciones de negocio sobre {len(valid_df)} filas...")
        df = valid_df.copy()

        # 1. Normalización de códigos de producto
        df["stock_code"] = df["stock_code"].astype(str).str.strip().str.upper()

        # 2. Manejo de clientes nulos -> 'ANONYMOUS' (ADR-002)
        def clean_customer_id(val):
            if pd.isna(val) or str(val).strip().lower() in ["", "nan", "none", "null", "0"]:
                return "ANONYMOUS"
            # Si viene como float (ej. 12345.0), convertir a entero y luego string
            try:
                fval = float(val)
                if fval.is_integer():
                    return str(int(fval))
            except Exception:
                pass
            return str(val).strip()

        df["customer_id"] = df["customer_id"].apply(clean_customer_id)
        df["customer_type"] = np.where(df["customer_id"] == "ANONYMOUS", "ANONIMO", "IDENTIFICADO")
        df["customer_name"] = np.where(
            df["customer_id"] == "ANONYMOUS",
            "Consumidor Final / Anónimo",
            "Cliente #" + df["customer_id"]
        )

        # 3. Deduplicación por clave compuesta con prioridad a data.csv (ADR-004)
        df["priority"] = np.where(df["source_file"] == "data.csv", 1, 2)
        df.sort_values(by=["priority", "invoice_date"], ascending=[True, True], inplace=True)
        dedup_keys = ["invoice_no", "stock_code", "invoice_timestamp_utc", "quantity"]
        initial_len = len(df)
        df.drop_duplicates(subset=dedup_keys, keep="first", inplace=True)
        print(f"[DatamartTransformer] Registros únicos tras deduplicación: {len(df)} (Filas solapadas descartadas: {initial_len - len(df)})")
        df.drop(columns=["priority"], inplace=True)

        # 4. Cálculo de descripción canónica por la MODA estadística (ADR-003)
        valid_descriptions = df[df["description"].notna() & (df["description"].str.strip() != "")].copy()
        valid_descriptions["clean_desc"] = valid_descriptions["description"].str.strip().str.upper()
        
        # Calcular moda por stock_code
        modes = valid_descriptions.groupby("stock_code")["clean_desc"].agg(
            lambda x: x.mode().iloc[0] if not x.mode().empty else "DESCRIPCION GENERAL"
        ).to_dict()

        df["canonical_description"] = df["stock_code"].map(modes).fillna(df["description"].fillna("DESCRIPCION GENERAL").str.strip().str.upper())

        # 5. Conversión de divisas a USD (ADR-014)
        rates = self.currency_extractor.extract()
        country_to_curr = self.currency_extractor.country_to_currency

        def get_rate_and_currency(country_name):
            country_clean = str(country_name).strip() if pd.notnull(country_name) else "United Kingdom"
            curr = country_to_curr.get(country_clean, "GBP")
            rate = rates.get(curr, 1.0)
            return curr, rate

        curr_rates = [get_rate_and_currency(c) for c in df["country"]]
        df["currency_original"] = [c[0] for c in curr_rates]
        df["exchange_rate_to_usd"] = [c[1] for c in curr_rates]

        # Precios estandarizados a USD
        df["unit_price"] = (df["unit_price_original"] * df["exchange_rate_to_usd"]).round(4)

        # 6. Separación de hechos: fact_sales y fact_returns (ADR-008)
        sales_mask = df["quantity"] > 0
        sales_df = df[sales_mask].copy()
        returns_df = df[~sales_mask].copy()

        # Métricas de ventas
        sales_df["gross_revenue"] = (sales_df["quantity"] * sales_df["unit_price"]).round(4)

        # Métricas de devoluciones
        returns_df["quantity_returned"] = returns_df["quantity"].abs().astype(int)
        returns_df["return_amount"] = (returns_df["quantity_returned"] * returns_df["unit_price"]).round(4)

        # 7. Resumen de Revenue Diario por Producto (ADR-008)
        # Ventas agregadas
        sales_agg = sales_df.groupby(["invoice_date", "stock_code"]).agg(
            gross_revenue=("gross_revenue", "sum"),
            total_sold_qty=("quantity", "sum")
        ).reset_index()

        # Devoluciones agregadas
        returns_agg = returns_df.groupby(["invoice_date", "stock_code"]).agg(
            return_amount=("return_amount", "sum"),
            total_returned_qty=("quantity_returned", "sum")
        ).reset_index()

        daily_revenue_df = pd.merge(sales_agg, returns_agg, on=["invoice_date", "stock_code"], how="outer").fillna(0)
        daily_revenue_df["net_revenue"] = (daily_revenue_df["gross_revenue"] - daily_revenue_df["return_amount"]).round(4)

        # Tablas dimensionales preliminares
        unique_customers = df[["customer_id", "customer_name", "customer_type", "country"]].drop_duplicates(subset=["customer_id"])
        unique_products = df[["stock_code", "canonical_description"]].drop_duplicates(subset=["stock_code"])

        print(f"[DatamartTransformer] Hechos generados -> Ventas: {len(sales_df)} | Devoluciones: {len(returns_df)} | Agregados diarios: {len(daily_revenue_df)}")

        return {
            "dim_customers": unique_customers,
            "dim_products": unique_products,
            "fact_sales": sales_df,
            "fact_returns": returns_df,
            "fact_daily_product_revenue": daily_revenue_df
        }
