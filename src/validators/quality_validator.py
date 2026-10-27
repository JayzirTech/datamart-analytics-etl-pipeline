import json
import pandas as pd
from typing import Tuple, Dict, Any, List
from interfaces.validator import BaseValidator


class QualityValidator(BaseValidator):
    """
    Validador de calidad y segregador de anomalías contables (ADR-009).
    Aplica las reglas de negocio estrictas:
    1. Quantity <= 0 AND UnitPrice <= 0 -> Rechazo: 'CANTIDAD_Y_PRECIO_INVALIDOS_CORRECCION_ERRONEA'
    2. Quantity > 0 AND UnitPrice <= 0  -> Rechazo: 'PRECIO_UNITARIO_INVALIDO_LEQ_0'
    3. Fechas o claves corruptas       -> Rechazo: 'ERROR_FORMATO_DATOS'
    """

    def validate(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Segrega el DataFrame en:
        - valid_df: Filas financieramente válidas (ventas con precio > 0 y devoluciones con precio > 0).
        - rejected_df: Filas rechazadas preparadas para la tabla rejected_records.
        """
        print(f"[QualityValidator] Iniciando validación sobre {len(df)} registros...")
        
        # Copia para no alterar el original
        working_df = df.copy()

        # Coerción numérica segura
        working_df["quantity_clean"] = pd.to_numeric(working_df["quantity"], errors="coerce")
        working_df["unit_price_clean"] = pd.to_numeric(working_df["unit_price_raw"], errors="coerce")
        working_df["invoice_date_dt"] = pd.to_datetime(working_df["invoice_date_raw"], errors="coerce")

        rejection_reasons = []
        is_rejected = []

        for idx, row in working_df.iterrows():
            qty = row["quantity_clean"]
            price = row["unit_price_clean"]
            date_val = row["invoice_date_dt"]
            inv_no = str(row["invoice_no"]).strip() if pd.notnull(row["invoice_no"]) else ""
            stock = str(row["stock_code"]).strip() if pd.notnull(row["stock_code"]) else ""

            # Caso 1: Error estructural irrecuperable
            if pd.isna(qty) or pd.isna(price) or pd.isna(date_val) or not inv_no or not stock:
                is_rejected.append(True)
                rejection_reasons.append("ERROR_FORMATO_DATOS")
            # Caso 2: Mala práctica contable (cantidad <= 0 Y precio <= 0)
            elif qty <= 0 and price <= 0:
                is_rejected.append(True)
                rejection_reasons.append("CANTIDAD_Y_PRECIO_INVALIDOS_CORRECCION_ERRONEA")
            # Caso 3: Venta con precio no positivo (Regla obligatoria de la prueba)
            elif qty > 0 and price <= 0:
                is_rejected.append(True)
                rejection_reasons.append("PRECIO_UNITARIO_INVALIDO_LEQ_0")
            else:
                is_rejected.append(False)
                rejection_reasons.append(None)

        working_df["is_rejected"] = is_rejected
        working_df["rejection_reason"] = rejection_reasons

        # Separar
        rejected_mask = working_df["is_rejected"] == True
        rejected_subset = working_df[rejected_mask].copy()
        valid_subset = working_df[~rejected_mask].copy()

        # Construir estructura para rejected_records
        records_to_reject = []
        for _, row in rejected_subset.iterrows():
            raw_dict = {
                "invoice_no": str(row.get("invoice_no", "")),
                "stock_code": str(row.get("stock_code", "")),
                "description": str(row.get("description", "")),
                "quantity": str(row.get("quantity", "")),
                "unit_price": str(row.get("unit_price_raw", "")),
                "invoice_date": str(row.get("invoice_date_raw", "")),
                "customer_id": str(row.get("customer_id", "")),
                "country": str(row.get("country", ""))
            }
            records_to_reject.append({
                "source_file": row.get("source_file", "desconocido"),
                "raw_record": json.dumps(raw_dict, ensure_ascii=False),
                "rejection_reason": row["rejection_reason"]
            })

        rejected_df = pd.DataFrame(records_to_reject) if records_to_reject else pd.DataFrame(columns=["source_file", "raw_record", "rejection_reason"])

        # Limpiar columnas auxiliares en valid_subset
        valid_subset["quantity"] = valid_subset["quantity_clean"].astype(int)
        valid_subset["unit_price_original"] = valid_subset["unit_price_clean"].astype(float)
        valid_subset["invoice_timestamp_utc"] = valid_subset["invoice_date_dt"].dt.tz_localize(None).dt.tz_localize("UTC")
        valid_subset["invoice_date"] = valid_subset["invoice_timestamp_utc"].dt.date
        valid_subset.drop(columns=["quantity_clean", "unit_price_clean", "invoice_date_dt", "is_rejected", "rejection_reason"], inplace=True)

        print(f"[QualityValidator] Registros válidos: {len(valid_subset)} | Registros rechazados: {len(rejected_df)}")
        return valid_subset, rejected_df
