from abc import ABC, abstractmethod
from pathlib import Path

class HcpUploader(ABC):
    @abstractmethod

    def upload_dir(
        self,
        directory: Path
    ) -> None:
        pass