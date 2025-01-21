from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict


class SymlinkCreator(ABC):
    @abstractmethod
    def create_symlinks(self) -> Dict[Path, Exception]:
        pass