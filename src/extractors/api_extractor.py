import requests
from typing import Dict, List, Any, Optional
from interfaces.extractor import BaseExtractor


class ApiExtractor(BaseExtractor):
    """
    Cliente REST para consultar el microservicio de Catálogo de Productos en FastAPI (ADR-001, ADR-012).
    Permite enriquecer las referencias con Categoría oficial, País de Proveedor y Estado.
    """

    def __init__(self, base_url: str = "http://fastapi_products:8000"):
        self.base_url = base_url.rstrip("/")

    def check_health(self) -> bool:
        """Verifica disponibilidad del servicio en /health."""
        try:
            r = requests.get(f"{self.base_url}/health", timeout=3)
            return r.status_code == 200 and r.json().get("status") == "healthy"
        except Exception:
            return False

    def extract(self, stock_code: str, description_hint: str = "") -> Dict[str, Any]:
        """Consulta el detalle de un producto individual."""
        try:
            params = {}
            if description_hint:
                params["description_hint"] = description_hint
            r = requests.get(f"{self.base_url}/products/{stock_code}", params=params, timeout=5)
            if r.status_code == 200:
                return r.json()
        except Exception as e:
            print(f"[ApiExtractor] Error al consultar {stock_code}: {e}")
        
        # Fallback determinista si la API no responde
        return {
            "stock_code": stock_code.strip().upper(),
            "canonical_name": description_hint or f"PRODUCT {stock_code}",
            "category": "Hogar",
            "supplier_country": "Colombia",
            "is_active": True
        }

    def bulk_enrich(self, products: List[Dict[str, str]]) -> Dict[str, Dict[str, Any]]:
        """
        Enriquece masivamente una lista de productos [{'stock_code': '...', 'description': '...'}]
        utilizando el endpoint /products/bulk de FastAPI.
        """
        results_map = {}
        try:
            r = requests.post(
                f"{self.base_url}/products/bulk",
                json={"products": products},
                timeout=30
            )
            if r.status_code == 200:
                items = r.json()
                for item in items:
                    results_map[item["stock_code"]] = item
                return results_map
        except Exception as e:
            print(f"[ApiExtractor] Error en bulk enrich: {e}. Activando fallback local.")

        # Fallback si falla el bulk
        for p in products:
            code = p.get("stock_code", "").strip().upper()
            if code not in results_map:
                results_map[code] = {
                    "stock_code": code,
                    "canonical_name": p.get("description", f"PRODUCT {code}"),
                    "category": "Hogar",
                    "supplier_country": "Colombia",
                    "is_active": True
                }
        return results_map
