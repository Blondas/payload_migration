from abc import ABC, abstractmethod
from pathlib import Path


class TapeImportConfirmer(ABC):
    @abstractmethod
    def wait_for_confirmation(self, tape_name: str, tape_location: Path) -> None:
        pass
    
    @abstractmethod
    def get_tape_confirmation_file(self, tape_name: str, tape_location: Path) -> Path:
        pass