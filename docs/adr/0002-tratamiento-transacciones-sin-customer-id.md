# 2. Tratamiento de Transacciones sin Identificador de Cliente (CustomerID Nulo)

* **Estado:** Aceptado
* **Fecha:** 2026-10-27

## Contexto
El dataset transaccional contiene un volumen significativo de registros con la columna `CustomerID` vacía o nula. El negocio no cuenta con una directriz oficial previa sobre cómo manejarlos. Además, la pregunta de negocio #5 de la prueba requiere evaluar si existe diferencia en el comportamiento de compra entre clientes identificados y transacciones sin cliente.

## Decisión
Se decide conservar estas transacciones en el Data Warehouse, asignándoles un cliente canónico con nombre explícito `'ANONYMOUS'` e identificador sustituto (`-1` o `'CUSTOMER_ANONYMOUS'`).

## Consecuencias
- **Positivas:**
  - Se previene la pérdida artificial de volumen de ventas e ingresos en el cálculo del revenue global.
  - Se habilita directamente la respuesta a la pregunta de negocio #5 (*comparativa de ticket promedio y frecuencia entre clientes registrados y anónimos*).
- **Impacto documentado:** Las ventas anónimas se consideran ventas en mostrador o ventas web sin sesión iniciada (consumidor final).
