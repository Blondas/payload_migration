from abc import ABC, abstractmethod
from typing import Dict, Optional


class DBConnection(ABC):
    @abstractmethod
    def fetch_all(self, query: str) -> Dict[str, tuple]:
        pass
    
    @abstractmethod
    def fetch_one(self, query: str) -> Optional[tuple]:
        pass
    
    @abstractmethod
    def update(self, query: str) -> None:
        pass

