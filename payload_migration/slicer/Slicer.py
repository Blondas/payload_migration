from abc import ABC, abstractmethod
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

class Slicer(ABC):
    @abstractmethod
    def execute(
        self, 
        tape_location: Path, 
        collection_name: str, 
        output_directory: Path,
        log_location: Path
    ) -> None:
        pass