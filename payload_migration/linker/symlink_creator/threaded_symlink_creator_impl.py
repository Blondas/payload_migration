from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, List
import logging
from threading import Lock

from payload_migration.linker.path_transformer import PathTransformer
from payload_migration.linker.symlink_creator import SymlinkCreator, SymlinkConfig

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LinkTask:
    source_file: Path
    target_path: Path


class ThreadedSymlinkCreatorImpl(SymlinkCreator):
    def __init__(
        self,
        config: SymlinkConfig,
        path_transformer: PathTransformer,
        max_workers: int = 32  # Empirically good default for I/O bound tasks
    ):
        self._config: SymlinkConfig = config
        self._path_transformer: PathTransformer = path_transformer
        self._max_workers: int = max_workers
        self._mkdir_lock: Lock = Lock()  # Prevent race conditions in mkdir

    def create_symlinks(self) -> Dict[Path, Optional[Exception]]:
        """
        Create symlinks for all matching files in the source directory using a thread pool.

        Returns:
            Dict mapping source paths to exceptions if errors occurred
        """
        results: Dict[Path, Optional[Exception]] = {}
        tasks: List[LinkTask] = self._prepare_tasks()

        with ThreadPoolExecutor(max_workers=self._max_workers) as executor:
            future_to_task = {
                executor.submit(self._create_single_symlink, task): task
                for task in tasks
            }

            for future in as_completed(future_to_task):
                task = future_to_task[future]
                try:
                    error = future.result()
                    results[task.source_file] = error
                    if error is None:
                        logger.debug(
                            "Created symlink",
                            extra={
                                "source_file": task.source_file,
                                "target_path": task.target_path
                            }
                        )
                except Exception as e:
                    results[task.source_file] = e
                    logger.error(
                        "Failed to create symlink",
                        extra={"source_file": task.source_file},
                        exc_info=e
                    )

        return results

    def _prepare_tasks(self) -> List[LinkTask]:
        """Prepare all symlink tasks before execution."""
        return [
            LinkTask(
                source_file=source_file,
                target_path=self._path_transformer.transform(
                    source_file,
                    self._config.target_base_dir
                )
            )
            for source_file in self._get_source_files()
        ]

    def _get_source_files(self) -> List[Path]:
        return list(self._config.source_dir.glob(self._config.file_pattern))

    @staticmethod
    def _create_symlink(self, task: LinkTask) -> Optional[Exception]:
        """Raises:
            FileExistsError: If target path already exists (e.g. pre-existing file/link)
            OSError: If symlink creation fails (permissions, etc.)

        Returns:
            Optional[Exception]: None if successful, caught exception if failed

        Note:
            This implementation assumes that each task's target_path is unique.
            No thread synchronization is needed since each thread operates on a different path.
        """
        try:
            task.target_path.parent.mkdir(parents=True, exist_ok=True)
            if task.target_path.exists():
                return FileExistsError(f"Target path already exists: {task.target_path}")

            task.target_path.symlink_to(task.source_file)
            return None
        except Exception as e:
            return e