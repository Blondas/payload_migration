from abc import ABC, abstractmethod
from pathlib import Path


class PathTransformer(ABC):
    @abstractmethod
    def transform(self, path: Path, target_base_dir: Path) -> Path:
        pass