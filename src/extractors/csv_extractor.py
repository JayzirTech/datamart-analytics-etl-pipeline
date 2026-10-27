import os
import glob
import pandas as pd
from typing import Dict, List, Optional
from interfaces.extractor import BaseExtractor


class CsvExtractor(BaseExtractor):
    """
    Extractor de fuentes tabulares CSV (ADR-004, ADR-011).
    Lee data.csv y online_retail_II.csv manejando diferentes encodings y unificando nombres de columnas.
    """

    COLUMN_MAPPINGS = {
        # Mapeos para data.csv
        "invoiceno": "invoice_no",
        "stockcode": "stock_code",
        "description": "description",
        "quantity": "quantity",
        "invoicedate": "invoice_date_raw",
        "unitprice": "unit_price_raw",
        "customerid": "customer_id",
        "country": "country",
        # Variantes comunes en online_retail_II.csv
        "invoice": "invoice_no",
        "price": "unit_price_raw",
        "customer id": "customer_id",
    }

    def __init__(self, raw_data_path: str):
        self.raw_data_path = os.path.abspath(raw_data_path)

    def extract(self, filename: Optional[str] = None) -> pd.DataFrame:
        """
        Extrae un archivo CSV específico o todos los archivos CSV presentes en raw_data_path.
        """
        if filename:
            file_path = os.path.join(self.raw_data_path, filename)
            return self._read_and_standardize_csv(file_path, filename)

        # Si no se especifica archivo, buscar data.csv y online_retail_II.csv
        frames = []
        target_files = ["data.csv", "online_retail_II.csv"]
        
        for fname in target_files:
            matches = glob.glob(os.path.join(self.raw_data_path, "**", fname), recursive=True)
            if matches:
                df = self._read_and_standardize_csv(matches[0], fname)
                frames.append(df)
            else:
                print(f"[CsvExtractor] Archivo {fname} no encontrado en {self.raw_data_path}")

        if not frames:
            raise FileNotFoundError(f"No se encontraron archivos CSV válidos en: {self.raw_data_path}")

        return pd.concat(frames, ignore_index=True)

    def _read_and_standardize_csv(self, file_path: str, filename: str) -> pd.DataFrame:
        print(f"[CsvExtractor] Leyendo: {file_path}...")
        df = None
        encodings = ["utf-8", "ISO-8859-1", "latin1", "cp1252"]

        for enc in encodings:
            try:
                df = pd.read_csv(file_path, encoding=enc, low_memory=False)
                print(f"[CsvExtractor] Leído exitosamente con codificación: {enc} ({len(df)} filas)")
                break
            except (UnicodeDecodeError, Exception) as e:
                continue

        if df is None:
            raise ValueError(f"No fue posible leer {file_path} con ninguna codificación compatible.")

        # Normalizar nombres de columnas a minúsculas y mapear
        normalized_cols = {}
        for col in df.columns:
            clean_col = str(col).strip().lower()
            target_col = self.COLUMN_MAPPINGS.get(clean_col, clean_col)
            normalized_cols[col] = target_col

        df = df.rename(columns=normalized_cols)
        df["source_file"] = filename

        # Columnas esenciales mínimas esperadas
        expected = ["invoice_no", "stock_code", "description", "quantity", "invoice_date_raw", "unit_price_raw", "country"]
        for exp in expected:
            if exp not in df.columns:
                df[exp] = None

        if "customer_id" not in df.columns:
            df["customer_id"] = None

        return df
