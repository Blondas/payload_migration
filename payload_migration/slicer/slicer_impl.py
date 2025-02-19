import os
import subprocess
from pathlib import Path
from payload_migration.slicer.slicer import Slicer
import logging

logger = logging.getLogger(__name__)


class SlicerImpl(Slicer):
    def __init__(
        self, 
        slicer_path: Path,
    ):
        self._slicer_path: Path = slicer_path
        if not os.access(self._slicer_path, os.X_OK):
                    raise ValueError(f"Slicer path {self._slicer_path} is not executable")
    

    def execute(
        self, 
        tape_location: Path,
        output_directory: Path,
        log_file: Path
    ) -> None:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        output_directory.mkdir(parents=True, exist_ok=True)

        try:
            cmd = [
                str(self._slicer_path),
                str(tape_location),
                str(output_directory),
                str(log_file)
            ]

            logger.info(f"Executing slicer command: {' '.join(map(str, cmd))}")

            result = subprocess.run(
                cmd,
                check=True,
                text=True,
                cwd=output_directory
            )

            if result.stderr:
                logger.warning(f"Command executed with warnings: {result.stderr}")

            logger.info("Command executed successfully")

        except subprocess.CalledProcessError as e:
            error_msg = f"Slicer command failed: {e.stderr}"
            logger.error(error_msg)
            raise Exception(error_msg) from e
        except Exception as e:
            error_msg = f"Unexpected error during Slicer command execution: {str(e)}"
            logger.error(error_msg)
            raise Exception(error_msg) from e
        
    @staticmethod
    def _get_tape_name(tape_location: Path) -> str:
        return tape_location.name