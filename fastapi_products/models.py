from typing import List, Optional
from pydantic import BaseModel, Field

class Product(BaseModel):
    stock_code: str = Field(..., description="Código de referencia único del producto en mayúsculas")
    canonical_name: str = Field(..., description="Nombre canónico y normalizado del producto")
    category: str = Field(..., description="Categoría de negocio: Electrónica, Hogar, Ropa, Deportes, Papelería")
    supplier_country: str = Field(..., description="País de origen del proveedor (Colombia, México, Perú, etc.)")
    is_active: bool = Field(True, description="Estado operativo en catálogo: True=Activo, False=Descontinuado")

class PaginatedProductsResponse(BaseModel):
    total: int = Field(..., description="Total de productos registrados")
    page: int = Field(..., description="Página actual")
    limit: int = Field(..., description="Límite de elementos por página")
    total_pages: int = Field(..., description="Total de páginas disponibles")
    items: List[Product] = Field(..., description="Lista de productos en la página actual")

class HealthCheckResponse(BaseModel):
    status: str
    service: str
    version: str
    total_products_loaded: int
    timestamp_utc: str
