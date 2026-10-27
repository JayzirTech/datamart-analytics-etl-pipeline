from abc import ABC, abstractmethod
from typing import Any, Dict

class BaseLoader(ABC):
    """
    Contrato abstracto para persistencia idempotente en almacenes de datos analíticos (DWH).
    """

    @abstractmethod
    def load(self, data: Any, *args, **kwargs) -> Dict[str, int]:
        """
        Carga los datos en el repositorio destino de forma idempotente.
        Retorna un resumen de filas afectadas por tabla.
        """
        pass
