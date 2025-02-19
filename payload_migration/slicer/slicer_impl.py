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
            cmd = f'"{self._slicer_path}" "{tape_location}" 2>"{log_file}"'
            logger.info(f"Executing slicer command: {cmd}")
            
            
            
            
            cmd = [
                str(self._slicer_path),
                str(tape_location),
                str(log_file)
            ]

            logger.info(f"Executing slicer command: {' '.join(map(str, cmd))}")

            result = subprocess.run(
                cmd,
                check=True,
                shell=True,  # Required for redirection
                text=True,
                cwd=output_directory
            )

            logger.info("Command executed successfully")
            
            if result.returncode not in [0,1]:
                error_msg = f"Slicer command failed with exit code {e.returncode},  {e.stderr}"
                logger.error(error_msg)
                raise Exception(error_msg)
                
        except Exception as e:
            error_msg = f"Unexpected error during Slicer command execution: {str(e)}"
            logger.error(error_msg)
            raise Exception(error_msg) from e
        
    @staticmethod
    def _get_tape_name(tape_location: Path) -> str:
        return tape_location.name