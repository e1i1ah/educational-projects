from abc import ABC, abstractmethod
from typing import List, Any


class Connector(ABC):
    @abstractmethod
    def download_dataset(self) -> List[Any]:
        raise NotImplementedError
