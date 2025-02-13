from pathlib import Path
from typing import Dict, Optional, List
import logging

from payload_migration.linker.path_transformer.path_transformer import PathTransformer
from payload_migration.linker.link_creator.link_creator import LinkCreator

logger = logging.getLogger(__name__)

class LinkCreatorImpl(LinkCreator):
    def __init__(
        self,
        source_dir: Path,
        output_directory: Path,
        file_patterns: list[str],
        path_transformer: PathTransformer
    ):
        self._source_dir: Path = source_dir
        self._output_directory: Path = output_directory
        self._file_patterns: list[str]= file_patterns
        self._path_transformer: PathTransformer  = path_transformer

    def create_links(self) -> Dict[Path, Optional[Exception]]:
        results: Dict[Path, Optional[Exception]] = {}
        for source_file in self._get_source_files():
            try:
                target_path = self._path_transformer.transform(source_file, self._output_directory)                
                LinkCreatorImpl._create_link(source_file, target_path)
                results[source_file] = None
                logging.debug(f"Created symlink, source_file={source_file}, target_path{target_path}")
            except Exception as e:
                results[source_file] = e
                logger.error(
                    f"Failed to create symlink, source_file={source_file}, error_message={str(e)}",
                    exc_info=True
                )

        logger.info(f"Link creation completed: {len([v for v in results.values() if v is None])} successful, {len([v for v in results.values() if v is not None])} failed")
        
        return results

    def _get_source_files(self) -> List[Path]:
        src_files = list(set(
            src_file
            for pattern in self._file_patterns
            for src_file in self._source_dir.glob(pattern)
        ))
        logger.debug(f"Found {len(src_files)} source files")
        return src_files

    @staticmethod
    def _create_link(source_file: Path, target_path: Path) -> None:
        """Raises:
            FileExistsError: If target_path already exists
            OSError: If hardlink creation fails (permissions, etc.)
        """
        target_path.parent.mkdir(parents=True, exist_ok=True)
        if target_path.exists():
            raise FileExistsError(f"Target path already exists: {target_path}")
        source_file.link_to(target_path)