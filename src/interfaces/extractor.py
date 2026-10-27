from abc import ABC, abstractmethod
from typing import Any

class BaseExtractor(ABC):
    """
    Contrato abstracto para extracción de datos (Principio de Inversión de Dependencias - DIP).
    Permite desacoplar el origen físico de los datos (CSV, REST API, DB) del orquestador.
    """

    @abstractmethod
    def extract(self, *args, **kwargs) -> Any:
        """Extrae y retorna los datos desde la fuente configurada."""
        pass
