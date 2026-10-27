from abc import ABC, abstractmethod
from typing import Any

class BaseTransformer(ABC):
    """
    Contrato abstracto para transformación, estandarización y cálculo de métricas de negocio.
    """

    @abstractmethod
    def transform(self, data: Any, *args, **kwargs) -> Any:
        """Aplica transformaciones de negocio sobre los datos."""
        pass
