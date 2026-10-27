import os
import psycopg2
from psycopg2.extras import execute_batch, execute_values
from typing import Dict, Any, List
import pandas as pd

from interfaces.loader import BaseLoader


class PostgresLoader(BaseLoader):
    """
    Cargador de datos analíticos hacia PostgreSQL Data Warehouse (ADR-006, ADR-008).
    Garantiza IDEMPOTENCIA mediante sentencias ON CONFLICT en todas las tablas dimensionales y de hechos.
    """

    def __init__(self, conn_uri: str = None):
        if not conn_uri:
            # Intentar leer desde variable de entorno o construir desde credenciales
            conn_uri = os.getenv(
                "AIRFLOW_CONN_POSTGRES_DWH_CONN",
                f"postgresql://{os.getenv('DWH_DB_USER', 'dwh_user')}:{os.getenv('DWH_DB_PASSWORD', 'dwh_secret_password')}@{os.getenv('DWH_HOST', 'postgres_dwh')}:{os.getenv('DWH_PORT', '5432')}/{os.getenv('DWH_DB_NAME', 'datamart_dwh')}"
            )
        self.conn_uri = conn_uri

    def _get_connection(self):
        return psycopg2.connect(self.conn_uri)

    def load_exchange_rates(self, rates: Dict[str, float], source_label: str = "FRANKFURTER_API"):
        """Actualiza la tabla dim_exchange_rates con las paridades hacia USD."""
        query = """
            INSERT INTO dim_exchange_rates (currency_code, currency_name, country_default, rate_to_usd, source, updated_at_utc)
            VALUES (%s, %s, %s, %s, %s, NOW())
            ON CONFLICT (currency_code) DO UPDATE
            SET rate_to_usd = EXCLUDED.rate_to_usd,
                source = EXCLUDED.source,
                updated_at_utc = NOW();
        """
        rows = []
        for code, rate in rates.items():
            rows.append((code, f"Moneda {code}", "Global", rate, source_label))

        with self._get_connection() as conn:
            with conn.cursor() as cur:
                execute_batch(cur, query, rows)
                conn.commit()
        print(f"[PostgresLoader] dim_exchange_rates actualizada con {len(rows)} divisas.")

    def load_dim_customers(self, df_customers: pd.DataFrame) -> Dict[str, int]:
        """Carga clientes nuevos de forma idempotente y retorna mapeo customer_id -> customer_key."""
        insert_query = """
            INSERT INTO dim_customers (customer_id, customer_name, customer_type, country)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (customer_id) DO UPDATE
            SET country = COALESCE(EXCLUDED.country, dim_customers.country);
        """
        rows = [
            (row["customer_id"], row["customer_name"], row["customer_type"], row.get("country", "No Especificado"))
            for _, row in df_customers.iterrows()
        ]

        with self._get_connection() as conn:
            with conn.cursor() as cur:
                execute_batch(cur, insert_query, rows, page_size=1000)
                conn.commit()
                
                # Obtener catálogo de claves
                cur.execute("SELECT customer_id, customer_key FROM dim_customers;")
                customer_map = dict(cur.fetchall())
        
        print(f"[PostgresLoader] dim_customers sincronizada. Total registrados: {len(customer_map)}")
        return customer_map

    def load_dim_products(self, products_enriched: List[Dict[str, Any]]) -> Dict[str, int]:
        """Carga o actualiza catálogo de productos enriquecidos y retorna mapeo stock_code -> product_key."""
        insert_query = """
            INSERT INTO dim_products (stock_code, canonical_description, category_name, supplier_country, is_active, updated_at_utc)
            VALUES (%s, %s, %s, %s, %s, NOW())
            ON CONFLICT (stock_code) DO UPDATE
            SET canonical_description = EXCLUDED.canonical_description,
                category_name = EXCLUDED.category_name,
                supplier_country = EXCLUDED.supplier_country,
                is_active = EXCLUDED.is_active,
                updated_at_utc = NOW();
        """
        rows = [
            (
                p["stock_code"],
                p.get("canonical_name", p.get("canonical_description", "PRODUCTO GENERAL")),
                p.get("category", p.get("category_name", "Hogar")),
                p.get("supplier_country", "Colombia"),
                p.get("is_active", True)
            )
            for p in products_enriched
        ]

        with self._get_connection() as conn:
            with conn.cursor() as cur:
                execute_batch(cur, insert_query, rows, page_size=1000)
                conn.commit()
                
                # Obtener catálogo de claves
                cur.execute("SELECT stock_code, product_key FROM dim_products;")
                product_map = dict(cur.fetchall())

        print(f"[PostgresLoader] dim_products sincronizada. Total referencias: {len(product_map)}")
        return product_map

    def load_fact_sales(self, df_sales: pd.DataFrame, product_map: Dict[str, int], customer_map: Dict[str, int]) -> int:
        """Carga transacciones de venta válidas a fact_sales de forma idempotente."""
        query = """
            INSERT INTO fact_sales (
                invoice_no, invoice_date, invoice_timestamp_utc, product_key, customer_key,
                country, quantity, currency_original, unit_price_original, exchange_rate_to_usd,
                unit_price, gross_revenue, source_file, batch_id
            )
            VALUES %s
            ON CONFLICT (invoice_no, product_key, invoice_timestamp_utc, quantity) DO NOTHING;
        """
        anon_key = customer_map.get("ANONYMOUS", 1)
        rows = []

        for _, row in df_sales.iterrows():
            prod_key = product_map.get(row["stock_code"])
            cust_key = customer_map.get(row["customer_id"], anon_key)
            if not prod_key:
                continue

            rows.append((
                str(row["invoice_no"]),
                row["invoice_date"],
                row["invoice_timestamp_utc"],
                prod_key,
                cust_key,
                str(row["country"]),
                int(row["quantity"]),
                str(row.get("currency_original", "GBP")),
                float(row["unit_price_original"]),
                float(row.get("exchange_rate_to_usd", 1.0)),
                float(row["unit_price"]),
                float(row["gross_revenue"]),
                str(row.get("source_file", "data.csv")),
                str(row.get("batch_id", "DIARIO"))
            ))

        with self._get_connection() as conn:
            with conn.cursor() as cur:
                execute_values(cur, query, rows, page_size=2000)
                conn.commit()

        print(f"[PostgresLoader] fact_sales cargada con éxito ({len(rows)} filas procesadas).")
        return len(rows)

    def load_fact_returns(self, df_returns: pd.DataFrame, product_map: Dict[str, int], customer_map: Dict[str, int]) -> int:
        """Carga transacciones de devolución a fact_returns de forma idempotente."""
        query = """
            INSERT INTO fact_returns (
                invoice_no, invoice_date, invoice_timestamp_utc, product_key, customer_key,
                country, quantity_returned, currency_original, unit_price_original, exchange_rate_to_usd,
                unit_price, return_amount, source_file, batch_id
            )
            VALUES %s
            ON CONFLICT (invoice_no, product_key, invoice_timestamp_utc, quantity_returned) DO NOTHING;
        """
        anon_key = customer_map.get("ANONYMOUS", 1)
        rows = []

        for _, row in df_returns.iterrows():
            prod_key = product_map.get(row["stock_code"])
            cust_key = customer_map.get(row["customer_id"], anon_key)
            if not prod_key:
                continue

            rows.append((
                str(row["invoice_no"]),
                row["invoice_date"],
                row["invoice_timestamp_utc"],
                prod_key,
                cust_key,
                str(row["country"]),
                int(row["quantity_returned"]),
                str(row.get("currency_original", "GBP")),
                float(row["unit_price_original"]),
                float(row.get("exchange_rate_to_usd", 1.0)),
                float(row["unit_price"]),
                float(row["return_amount"]),
                str(row.get("source_file", "data.csv")),
                str(row.get("batch_id", "DIARIO"))
            ))

        with self._get_connection() as conn:
            with conn.cursor() as cur:
                execute_values(cur, query, rows, page_size=2000)
                conn.commit()

        print(f"[PostgresLoader] fact_returns cargada con éxito ({len(rows)} filas procesadas).")
        return len(rows)

    def load_daily_product_revenue(self, df_daily: pd.DataFrame, product_map: Dict[str, int]) -> int:
        """Carga y actualiza fact_daily_product_revenue con el cálculo de revenue neto diario."""
        query = """
            INSERT INTO fact_daily_product_revenue (
                invoice_date, product_key, gross_revenue, return_amount, net_revenue,
                total_sold_qty, total_returned_qty, calculated_at_utc
            )
            VALUES %s
            ON CONFLICT (invoice_date, product_key) DO UPDATE
            SET gross_revenue = EXCLUDED.gross_revenue,
                return_amount = EXCLUDED.return_amount,
                net_revenue = EXCLUDED.net_revenue,
                total_sold_qty = EXCLUDED.total_sold_qty,
                total_returned_qty = EXCLUDED.total_returned_qty,
                calculated_at_utc = NOW();
        """
        rows = []
        for _, row in df_daily.iterrows():
            prod_key = product_map.get(row["stock_code"])
            if not prod_key:
                continue
            rows.append((
                row["invoice_date"],
                prod_key,
                float(row["gross_revenue"]),
                float(row["return_amount"]),
                float(row["net_revenue"]),
                int(row["total_sold_qty"]),
                int(row["total_returned_qty"])
            ))

        with self._get_connection() as conn:
            with conn.cursor() as cur:
                execute_values(cur, query, rows, page_size=2000)
                conn.commit()

        print(f"[PostgresLoader] fact_daily_product_revenue consolidada ({len(rows)} filas diarias agregadas).")
        return len(rows)

    def load_rejected_records(self, df_rejected: pd.DataFrame) -> int:
        """Carga el log de rechazos a la tabla rejected_records."""
        if df_rejected.empty:
            return 0

        query = """
            INSERT INTO rejected_records (source_file, raw_record, rejection_reason, batch_id, rejected_at_utc)
            VALUES (%s, %s, %s, %s, NOW());
        """
        rows = [
            (
                row["source_file"],
                row["raw_record"],
                row["rejection_reason"],
                "DIARIO"
            )
            for _, row in df_rejected.iterrows()
        ]

        with self._get_connection() as conn:
            with conn.cursor() as cur:
                execute_batch(cur, query, rows, page_size=1000)
                conn.commit()

        print(f"[PostgresLoader] rejected_records insertados: {len(rows)} anomalías auditadas.")
        return len(rows)

    def load(self, data: Dict[str, Any]) -> Dict[str, int]:
        """Orquestador de carga idempotente completa."""
        pass
