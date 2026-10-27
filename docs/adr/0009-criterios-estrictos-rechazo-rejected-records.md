# 9. Criterios Estrictos de Rechazo a rejected_records

* **Estado:** Aceptado
* **Fecha:** 2026-10-27

## Contexto
El documento técnico establece que:
> *"El precio unitario no puede ser cero ni negativo en una venta válida. Los registros que incumplan esta condición deben registrarse en el log de rechazos con el motivo."*

Adicionalmente, se detectan registros en los datasets donde tanto la cantidad como el precio unitario son menores o iguales a cero, lo cual responde a malas prácticas operacionales que no representan transacciones financieras válidas ni devoluciones comerciales reales.

## Decisión
Se decide canalizar a la tabla `rejected_records` los siguientes casos:
1. `Quantity <= 0` AND `UnitPrice <= 0`: Registro erróneo / mala práctica contable (`CANTIDAD_Y_PRECIO_INVALIDOS_CORRECCION_ERRONEA`).
2. `Quantity > 0` AND `UnitPrice <= 0`: Venta con precio no positivo (`PRECIO_UNITARIO_INVALIDO_LEQ_0`).
3. Registros con campos de fecha no parseables o estructura irrecuperable (`ERROR_FORMATO_DATOS`).

## Consecuencias
- **Positivas:**
  - Se mantiene la integridad analítica de `fact_sales` y `fact_returns`.
  - Se preserva la trazabilidad total mediante auditoría en `rejected_records` (`source_file`, `raw_record`, `rejection_reason`, `rejected_at_utc`).
