from abc import ABC, abstractmethod
from pathlib import Path
  
class UnitOfWorkProcessor(ABC):
    @abstractmethod
    def process(
        self, 
        tape_name: str,
        tape_location: Path
    ) -> None:
        pass