from abc import ABC, abstractmethod
from typing import List, Any


class Repository(ABC):
    @abstractmethod
    def get_objects(self) -> List[Any]:
        raise NotImplementedError

    @abstractmethod
    def save_objects(sellf) -> None:
        raise NotImplementedError
