from abc import ABC, abstractmethod
from pathlib import Path


class HcpUploader(ABC):
    @abstractmethod
    def upload_file(
        self,
        file_path: Path,
        bucket: str,
        object_name: str
    ) -> None:
        pass

    @abstractmethod
    def upload_files(
        self,
        file_paths: list[Path],
        bucket: str,
        max_workers: int
    ) -> None:
        pass