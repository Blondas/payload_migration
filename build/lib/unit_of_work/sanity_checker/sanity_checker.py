from abc import ABC, abstractmethod
from pathlib import Path

class SanityChecker(ABC):
    @abstractmethod
    def execute(
        self, 
        tape_name: str,
        slicer_log: Path,
        slicer_output_directory: Path,
        sanity_checker_log: Path
    ) -> None:
        pass