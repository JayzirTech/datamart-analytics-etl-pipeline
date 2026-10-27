from abc import ABC, abstractmethod
from typing import Tuple, Any

class BaseValidator(ABC):
    """
    Contrato abstracto para validación de calidad y separación de rechazos (Principio de Responsabilidad Única - SRP).
    """

    @abstractmethod
    def validate(self, data: Any, *args, **kwargs) -> Tuple[Any, Any]:
        """
        Valida el conjunto de datos y retorna:
        (registros_validos, registros_rechazados)
        """
        pass
