from abc import ABC, abstractmethod
from pathlib import Path


class TapeImportConfirmer(ABC):
    @abstractmethod
    def wait_for_confirmation(self, tape_name: str, tape_location: Path) -> None:
        pass