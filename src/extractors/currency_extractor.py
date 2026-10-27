import json
import os
import time
import requests
from typing import Dict, Any

from interfaces.extractor import BaseExtractor


class CurrencyExtractor(BaseExtractor):
    """
    Extractor de tasas de cambio con política de resiliencia (ADR-0014).
    1. Consulta Frankfurter API (https://api.frankfurter.dev/v1/latest?base=USD).
    2. En caso de fallo tras 3 reintentos, activa el fallback local (exchange_rates_seed.json).
    """

    DEFAULT_API_URL = "https://api.frankfurter.dev/v1/latest?base=USD"
    MAX_RETRIES = 3
    TIMEOUT_SECONDS = 5

    def __init__(self, seed_path: str = None, api_url: str = None):
        if not seed_path:
            seed_path = os.path.join(
                os.path.dirname(__file__), "..", "config", "exchange_rates_seed.json"
            )
        self.seed_path = os.path.abspath(seed_path)
        self.api_url = (
            api_url
            or os.getenv("FRANKFURTER_API_URL")
            or os.getenv("AIRFLOW_VAR_FRANKFURTER_API_URL")
            or self.DEFAULT_API_URL
        )
        self.seed_data = self._load_seed()
        self.rates_to_usd: Dict[str, float] = {}
        self.country_to_currency = self.seed_data.get("country_to_currency", {})

    def _load_seed(self) -> Dict[str, Any]:
        if os.path.exists(self.seed_path):
            try:
                with open(self.seed_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                print(f"[CurrencyExtractor] Advertencia al leer seed: {e}")
        return {"rates": {"USD": 1.0, "GBP": 1.30, "EUR": 1.08, "COP": 0.00025, "MXN": 0.055, "PEN": 0.27}}

    def extract(self) -> Dict[str, float]:
        """Extrae tasas de cambio vigentes hacia USD."""
        rates_from_api = self._fetch_from_frankfurter()
        
        # Iniciar con el catálogo semilla base
        final_rates = dict(self.seed_data.get("rates", {}))

        # Enriquecer con los datos en vivo de Frankfurter si la llamada fue exitosa
        if rates_from_api:
            # En Frankfurter con base=USD: 1 USD = X moneda_extranjera.
            # Por tanto: 1 moneda_extranjera = (1.0 / X) USD
            for curr, rate_val in rates_from_api.items():
                if rate_val and rate_val > 0:
                    final_rates[curr.upper()] = round(1.0 / float(rate_val), 6)
            print(f"[CurrencyExtractor] Tasas actualizadas exitosamente vía Frankfurter API ({len(rates_from_api)} divisas).")
        else:
            print("[CurrencyExtractor] Utilizando tasas de cambio del catálogo semilla local (Fallback activo).")

        final_rates["USD"] = 1.000000
        self.rates_to_usd = final_rates
        return self.rates_to_usd

    def _fetch_from_frankfurter(self) -> Dict[str, float]:
        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                response = requests.get(self.api_url, timeout=self.TIMEOUT_SECONDS)
                if response.status_code == 200:
                    data = response.json()
                    return data.get("rates", {})
            except Exception as e:
                print(f"[CurrencyExtractor] Intento {attempt}/{self.MAX_RETRIES} fallido con Frankfurter: {e}")
                time.sleep(1)
        return {}

    def get_rate(self, country: str, default_currency: str = "GBP") -> float:
        """Determina la tasa de cambio a USD dado el país de la transacción."""
        if not self.rates_to_usd:
            self.extract()
        
        currency = self.country_to_currency.get(country, default_currency).upper()
        return self.rates_to_usd.get(currency, 1.0)
