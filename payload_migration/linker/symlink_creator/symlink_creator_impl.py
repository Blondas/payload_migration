from pathlib import Path
from typing import Dict, Optional, List
import logging

from payload_migration.linker.path_transformer import PathTransformer
from payload_migration.linker.symlink_creator.symlink_creator import SymlinkCreator

logger = logging.getLogger(__name__)

class SymlinkCreatorImpl(SymlinkCreator):
    def __init__(
        self,
        source_dir: Path,
        target_base_dir: Path,
        file_pattern: str,
        path_transformer: PathTransformer
    ):
        self._source_dir: Path = source_dir
        self._target_base_dir: Path = target_base_dir
        self._file_pattern: str = file_pattern
        self._path_transformer: PathTransformer  = path_transformer

    def create_symlinks(self) -> Dict[Path, Optional[Exception]]:
        results: Dict[Path, Optional[Exception]] = {}
        for source_file in self._get_source_files():
            try:
                target_path = self._path_transformer.transform(source_file, self._target_base_dir)
                SymlinkCreatorImpl._create_symlink(source_file, target_path)
                results[source_file] = None
                logging.debug(f"Created symlink, source_file={source_file}, target_path{target_path}")
            except Exception as e:
                results[source_file] = e
                logger.error(
                    f"Failed to create symlink, source_file={source_file}, error_message={str(e)}",
                    exc_info=True
                )

        return results

    def _get_source_files(self) -> List[Path]:
        src_files = list(self._source_dir.glob(self._file_pattern))
        logger.debug(f"Found {len(src_files)} source files")
        return src_files

    @staticmethod
    def _create_symlink(source_file: Path, target_path: Path) -> None:
        """Raises:
            FileExistsError: If target_path already exists
            OSError: If symlink creation fails (permissions, etc.)
        """
        target_path.parent.mkdir(parents=True, exist_ok=True)
        if target_path.exists():
            raise FileExistsError(f"Target path already exists: {target_path}")
        target_path.link_to(source_file)