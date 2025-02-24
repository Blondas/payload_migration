from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict


class LinkCreator(ABC):
    @abstractmethod
    def create_links(self) -> Dict[Path, Exception]:
        pass