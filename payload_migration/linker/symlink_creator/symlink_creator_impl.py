from pathlib import Path
from typing import Dict, Optional, List
import logging

from payload_migration.linker.path_transformer import PathTransformer
from payload_migration.linker.symlink_creator import SymlinkConfig, SymlinkCreator

logger = logging.getLogger(__name__)

class SymlinkCreatorImpl(SymlinkCreator):
    def __init__(
        self,
        config: SymlinkConfig,
        path_transformer: PathTransformer,
    ):
        self._config: SymlinkConfig = config
        self._path_transformer: PathTransformer  = path_transformer

    def create_symlinks(self) -> Dict[Path, Optional[Exception]]:
        """
        Create symlinks for all matching files in the source directory.

        Returns:
            Dict mapping source paths to exceptions if errors occurred
        """
        results: Dict[Path, Optional[Exception]] = {}
        for source_file in self._get_source_files():
            try:
                target_path = self._path_transformer.transform(source_file, self._config.target_base_dir)
                SymlinkCreatorImpl._create_symlink(source_file, target_path)
                results[source_file] = None
                logging.debug(
                    "Created symlink",
                    extra={
                        "source_file": source_file,
                        "target_path": target_path
                    }
                 )
            except Exception as e:
                results[source_file] = e
                logger.error(
                    "Failed to create symlink",
                    extra={
                        "source_file": source_file
                    },
                    exc_info=e
                )

        return results

    def _get_source_files(self) -> List[Path]:
        return list(self._config.source_dir.glob(self._config.file_pattern))

    @staticmethod
    def _create_symlink(source_file: Path, target_path: Path) -> None:
        target_path.parent.mkdir(parents=True, exist_ok=True)
        if target_path.exists():
            raise FileExistsError(f"Target path already exists: {target_path}")
        target_path.symlink_to(source_file)