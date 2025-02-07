from abc import ABC, abstractmethod
from typing import Dict

class DBConnection(ABC):
    @abstractmethod
    def fetch(self, query: str) -> Dict[str, tuple]:
        pass

