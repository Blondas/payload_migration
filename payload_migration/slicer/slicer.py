from abc import ABC, abstractmethod
from pathlib import Path

class Slicer(ABC):
    @abstractmethod
    def execute(
        self, 
        tape_location: Path,
        output_directory: Path,
        log_location: Path
    ) -> None:
        pass