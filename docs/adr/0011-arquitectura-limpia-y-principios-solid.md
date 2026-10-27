# 11. Aplicación de Arquitectura Limpia y Principios SOLID en el Pipeline

* **Estado:** Aceptado
* **Fecha:** 2026-10-27

## Contexto
El código del pipeline debe cumplir estándares rigurosos de ingeniería de software, evitando scripts monolíticos y facilitando su mantenimiento, prueba y extensión.

## Decisión
Se decide estructurar el código del ETL bajo los principios de **Clean Architecture** y **SOLID**:

1. **S - Single Responsibility Principle:**
   - Módulos especializados e independientes: `extractors/`, `validators/`, `transformers/` y `loaders/`.
2. **O - Open/Closed Principle:**
   - Clases base e interfaces (`BaseExtractor`, `BaseTransformer`, `BaseLoader`) que permiten incorporar nuevas fuentes o destinos sin modificar el flujo orquestador central.
3. **L - Liskov Substitution Principle:**
   - Las implementaciones concretas pueden sustituirse sin romper la lógica del pipeline.
4. **I - Interface Segregation Principle:**
   - Interfaces pequeñas y con propósitos específicos.
5. **D - Dependency Inversion Principle:**
   - Los servicios dependen de abstracciones (interfaces) y configuraciones inyectadas, desacoplándose de la infraestructura concreta.

## Consecuencias
- **Positivas:** Código altamente legible, modular, fácilmente testeable y mantenible.
