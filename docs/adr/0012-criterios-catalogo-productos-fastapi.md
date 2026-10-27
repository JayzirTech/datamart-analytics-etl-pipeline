# 12. Criterios para el Catálogo de Productos en la API FastAPI

* **Estado:** Aceptado
* **Fecha:** 2026-10-27

## Contexto
Los datos transaccionales de Kaggle carecen de información sobre categorías de negocio, proveedores y estado de ciclo de vida del producto. La API FastAPI requiere exponer estos atributos conforme al modelo de negocio de DataMart S.A.S.

## Decisión
Se decide aplicar los siguientes criterios para la conformación del catálogo semilla de la API FastAPI:

1. **Categorización Semántica:**
   - Mapeo por palabras clave desde las descripciones de los productos hacia las 5 categorías oficiales de DataMart:
     - *Hogar:* Artículos de decoración, cocina, menaje, iluminación y descanso.
     - *Ropa:* Prendas de vestir, bolsos, accesorios textiles y calzado.
     - *Electrónica:* Relojes digitales, baterías, accesorios USB y lámparas LED.
     - *Papelería:* Cuadernos, bolígrafos, tarjetas, papel de regalo y agendas.
     - *Deportes:* Botellas de hidratación, artículos de viaje y accesorios de fitness.
2. **País de Origen del Proveedor (`supplier_country`):**
   - Enfoque A (Por vocación industrial y comercial regional):
     - Ropa: Colombia y Perú.
     - Electrónica: México y China.
     - Hogar y Papelería: Colombia, México y Perú.
     - Deportes: México y Colombia.
3. **Estado de Actividad del Producto (`is_active`):**
   - Enfoque 1 (Criterio temporal transaccional):
     - `True` (Activo): Si el producto registra ventas en el periodo reciente del histórico transaccional.
     - `False` (Descontinuado): Si el producto únicamente tuvo movimientos en el periodo histórico antiguo y dejó de comercializarse.

## Consecuencias
- **Positivas:** Coherencia de negocio realista para las 7 preguntas analíticas y enriquecimiento transparente consumible vía REST.
