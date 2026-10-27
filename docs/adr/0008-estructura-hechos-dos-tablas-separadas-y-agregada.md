# 8. Estructura de Hechos con Dos Tablas Separadas (fact_sales y fact_returns) y Tabla Agregada

* **Estado:** Aceptado
* **Fecha:** 2026-10-27

## Contexto
El negocio exige que toda transacción con cantidad menor o igual a cero se trate como devolución o ajuste, no como venta, y que el pipeline permita calcular el balance neto (ventas menos devoluciones). Además, se especifica que el pipeline debe calcular y almacenar el revenue bruto y el revenue neto por producto en el mismo periodo diario.

## Decisión
Se decide implementar dos tablas de hechos transaccionales separadas y una tabla de hechos agregada diaria:
1. `fact_sales`: Almacena exclusivamente transacciones con cantidad $> 0$.
2. `fact_returns`: Almacena exclusivamente transacciones con cantidad $\le 0$.
3. `fact_daily_product_revenue`: Almacena el cálculo consolidado diario por producto:
   - `invoice_date` (DATE)
   - `product_key` (INT)
   - `gross_revenue` (NUMERIC)
   - `return_amount` (NUMERIC)
   - `net_revenue` (NUMERIC: `gross_revenue - return_amount`)
   - `total_sold_qty` (INT)
   - `total_returned_qty` (INT)

## Consecuencias
- **Positivas:**
  - Cumplimiento estricto del requerimiento de separación de ventas y devoluciones.
  - Almacenamiento persistido del revenue neto diario según la regla del negocio.
  - Tiempos de consulta subsegundo en las preguntas de negocio sobre evolución mensual y top de productos.
