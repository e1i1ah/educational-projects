from abc import ABC, abstractmethod
from typing import List, Any


class Repository(ABC):
    @abstractmethod
    def get_objects(self):
        raise NotImplementedError

    @abstractmethod
    def save_objects(sellf):
        raise NotImplementedError
