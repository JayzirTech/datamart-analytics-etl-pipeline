import json
import os
import hashlib
from datetime import datetime, timezone
from typing import Dict, List, Optional
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

from models import Product, PaginatedProductsResponse, HealthCheckResponse

app = FastAPI(
    title="DataMart S.A.S. - Catálogo de Productos (Legacy System API)",
    description="API REST que expone el catálogo de productos legado para enriquecimiento analítico del pipeline ETL.",
    version="1.0.0",
)

# Catálogo en memoria
CATALOG_DB: Dict[str, Product] = {}

# Palabras clave por categoría oficial (ADR-012)
KEYWORD_CATEGORY_RULES = {
    "Electrónica": [
        "LIGHT", "LED", "ALARM", "CLOCK", "RADIO", "BATTERY", "USB", "ELECTRONIC",
        "LAMP", "TORCH", "FAN", "CALCULATOR", "SPEAKER", "HEADPHONE", "DIGITAL", "NIGHT LIGHT"
    ],
    "Ropa": [
        "BAG", "T-SHIRT", "SHIRT", "DRESS", "SCARF", "HAT", "APRON", "SOCKS",
        "SHOES", "GLOVES", "COAT", "JACKET", "POCKET", "PURSE", "WALLET", "TIE", "BACKPACK", "SHOPPER"
    ],
    "Papelería": [
        "NOTEBOOK", "PEN", "PENCIL", "PAPER", "CARD", "STICKER", "DIARY", "CALENDAR",
        "ERASER", "RULER", "ENVELOPE", "STAMP", "TAPE", "BOOK", "MARKER", "CHALKBOARD", "NAPKINS", "DOILIES"
    ],
    "Deportes": [
        "BOTTLE", "FITNESS", "BALL", "BICYCLE", "TRAVEL", "OUTDOOR", "GYM", "SPORT",
        "SWIM", "YOGA", "RACKET", "TOWEL", "CAMPING", "JOGGING", "PLASTERS"
    ],
}

def classify_product(stock_code: str, description_hint: str = "") -> Product:
    """Clasifica determinísticamente un producto no pre-cargado bajo las reglas de ADR-012."""
    code_clean = stock_code.strip().upper()
    text = f"{code_clean} {description_hint}".upper()
    
    # 1. Categoría
    category = "Hogar"  # Categoría por defecto según perfil comercial de DataMart
    for cat, keywords in KEYWORD_CATEGORY_RULES.items():
        if any(kw in text for kw in keywords):
            category = cat
            break

    # 2. País de proveedor según vocación regional (ADR-012 Enfoque A)
    hash_val = int(hashlib.md5(code_clean.encode()).hexdigest(), 16)
    if category == "Ropa":
        supplier_country = "Colombia" if (hash_val % 10) < 6 else "Perú"
    elif category == "Electrónica":
        supplier_country = "México" if (hash_val % 10) < 7 else "China"
    elif category == "Deportes":
        supplier_country = "México" if (hash_val % 10) < 5 else "Colombia"
    else:  # Hogar y Papelería
        bucket = hash_val % 10
        if bucket < 4:
            supplier_country = "Colombia"
        elif bucket < 7:
            supplier_country = "México"
        else:
            supplier_country = "Perú"

    # 3. Estado Activo (ADR-012 Enfoque 1: ~90% activos, 10% descontinuados históricos)
    is_active = (hash_val % 100) >= 12

    name = description_hint.strip() if description_hint else f"PRODUCT REFERENCE {code_clean}"

    return Product(
        stock_code=code_clean,
        canonical_name=name,
        category=category,
        supplier_country=supplier_country,
        is_active=is_active
    )


@app.on_event("startup")
def load_catalog_seed():
    """Carga los productos desde el archivo semilla catalog_seed.json si existe."""
    seed_path = os.path.join(os.path.dirname(__file__), "catalog_seed.json")
    if os.path.exists(seed_path):
        try:
            with open(seed_path, "r", encoding="utf-8") as f:
                items = json.load(f)
                for item in items:
                    p = Product(**item)
                    CATALOG_DB[p.stock_code.upper()] = p
            print(f"[FastAPI] Catálogo semilla cargado exitosamente: {len(CATALOG_DB)} productos.")
        except Exception as e:
            print(f"[FastAPI] Error al cargar seed JSON: {e}")


@app.get("/health", response_model=HealthCheckResponse, tags=["Monitoreo"])
def health_check():
    """Consulta de salud del microservicio (Health Check) exigida por el negocio."""
    return HealthCheckResponse(
        status="healthy",
        service="datamart-products-catalog-api",
        version="1.0.0",
        total_products_loaded=len(CATALOG_DB),
        timestamp_utc=datetime.now(timezone.utc).isoformat()
    )


@app.get("/products", response_model=PaginatedProductsResponse, tags=["Catálogo"])
def get_products(
    page: int = Query(1, ge=1, description="Número de página"),
    limit: int = Query(50, ge=1, le=200, description="Cantidad por página"),
    category: Optional[str] = Query(None, description="Filtrar por categoría oficial"),
    is_active: Optional[bool] = Query(None, description="Filtrar por estado activo/descontinuado")
):
    """Devuelve el listado de productos con soporte para paginación y filtros."""
    all_products = list(CATALOG_DB.values())

    if category:
        all_products = [p for p in all_products if p.category.lower() == category.lower()]
    if is_active is not None:
        all_products = [p for p in all_products if p.is_active == is_active]

    total = len(all_products)
    total_pages = (total + limit - 1) // limit if total > 0 else 1
    start = (page - 1) * limit
    end = start + limit
    items = all_products[start:end]

    return PaginatedProductsResponse(
        total=total,
        page=page,
        limit=limit,
        total_pages=total_pages,
        items=items
    )


@app.get("/products/{stock_code}", response_model=Product, tags=["Catálogo"])
def get_product_by_code(stock_code: str, description_hint: Optional[str] = None):
    """Consulta un producto específico por su código único de referencia."""
    code_clean = stock_code.strip().upper()
    if code_clean in CATALOG_DB:
        return CATALOG_DB[code_clean]

    # Clasificación bajo ADR-012 y persistencia en memoria
    product = classify_product(code_clean, description_hint or "")
    CATALOG_DB[code_clean] = product
    return product


@app.post("/products/bulk", response_model=List[Product], tags=["Catálogo"])
def bulk_enrich_products(payload: Dict[str, List[Dict[str, str]]]):
    """Permite resolver múltiples códigos de producto en una sola petición para optimizar el ETL."""
    items_to_resolve = payload.get("products", [])
    results = []
    for item in items_to_resolve:
        code = item.get("stock_code", "").strip().upper()
        hint = item.get("description", "")
        if not code:
            continue
        if code in CATALOG_DB:
            results.append(CATALOG_DB[code])
        else:
            product = classify_product(code, hint)
            CATALOG_DB[code] = product
            results.append(product)
    return results
