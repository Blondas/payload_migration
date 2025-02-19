import os
import subprocess
from pathlib import Path

from payload_migration.sanity_checker.sanity_checker import SanityChecker
import logging

logger = logging.getLogger(__name__)


class SanityCheckerImpl(SanityChecker):
    def __init__(
        self, 
        sanity_checker_path: Path,
    ):
        self._sanity_checker_path: Path = sanity_checker_path
        if not os.access(self._sanity_checker_path, os.X_OK):
            raise ValueError(f"Sanity checker path {self._sanity_checker_path} is not executable")

    def execute(
        self,
        tape_name: str,
        slicer_log: Path,
        slicer_output_directory: Path,
        sanity_checker_log: Path
    ) -> None:
        sanity_checker_log.parent.mkdir(parents=True, exist_ok=True)

        if not slicer_log.is_file():
            error_message: f"Slicer log file not found: {slicer_log}"
            logger.error(f"")
            raise FileNotFoundError(f"Slicer log file not found: {slicer_log}")
              
        try:
            cmd = [
                str(self._sanity_checker_path),
                str(tape_name),
                str(slicer_log),
                str(slicer_output_directory)
            ]

            logger.info(f"Executing sanity checker command: {' '.join(map(str, cmd))}")

            result = subprocess.run(
                cmd,
                check=True,
                text=True,
                capture_output=True,
                cwd=slicer_output_directory
            )

            if result.stdout:
                logger.info(f"Command output: {result.stdout}")
            if result.stderr:
                logger.error(f"Command stderr: {result.stderr}")

            logger.info("Command executed successfully")

        except subprocess.CalledProcessError as e:
            error_msg = f"Sanity checker command failed: {e.stderr}"
            logger.error(error_msg)
            raise Exception(error_msg) from e
        except Exception as e:
            error_msg = f"Unexpected error during Sanity checker command execution: {str(e)}"
            logger.error(error_msg)
            raise Exception(error_msg) from e
        
    @staticmethod
    def _get_tape_name(tape_location: Path) -> str:
        return tape_location.name