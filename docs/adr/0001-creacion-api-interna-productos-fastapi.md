# 1. Creación de API Interna de Productos con FastAPI

* **Estado:** Aceptado
* **Fecha:** 2026-10-27

## Contexto
DataMart mantiene su catálogo de productos en un sistema legado. El enunciado de la prueba técnica plantea como una mejora opcional de alto valor construir una API REST interna para enriquecer las transacciones de ventas con información de categorías y metadatos de proveedores.

## Decisión
Se decide implementar un microservicio en FastAPI como un contenedor independiente dentro del entorno `docker-compose`.

### Especificaciones Técnicas:
1. **Endpoint de Salud:** `GET /health` para monitoreo de disponibilidad del servicio.
2. **Endpoint Paginado:** `GET /products?page=1&limit=50` para navegación de catálogo.
3. **Endpoint Individual:** `GET /products/{stock_code}` para consulta puntual y enriquecimiento en el pipeline.
4. **Campos expuestos:**
   - `stock_code`: Código único de referencia del producto.
   - `canonical_name`: Nombre oficial normalizado.
   - `category`: Una de las 5 categorías oficiales (*Electrónica, Hogar, Ropa, Deportes, Papelería*).
   - `supplier_country`: País de origen del proveedor (*Colombia, México, Perú, etc.*).
   - `is_active`: Estado operativo del producto en el catálogo (*True / False*).

## Consecuencias
- **Positivas:** Máximo puntaje técnico en la prueba, desacoplamiento arquitectónico simulando un entorno empresarial real, y capacidad de enriquecer los datos analíticos de forma controlada.
- **Negativas / Mitigaciones:** Requiere un servicio adicional en Docker; se mitiga utilizando una imagen ligera de Python (`python:3.11-slim`) con bajo consumo de memoria.
