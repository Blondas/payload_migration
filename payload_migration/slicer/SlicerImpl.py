import os
import subprocess
from pathlib import Path
from payload_migration.slicer.Slicer import Slicer
import logging

from payload_migration.slicer.collection_name_lookup.collection_name_lookup import CollectionNameLookup
from payload_migration.slicer.collection_name_lookup.collection_name_lookup_error import CollectionNameLookupError


logger = logging.getLogger(__name__)


class SlicerImpl(Slicer):
    def __init__(
        self, 
        slicer_path: Path,
        collection_name_lookup: CollectionNameLookup
    ):
        self._slicer_path: Path = slicer_path
        if not os.access(self._slicer_path, os.X_OK):
                    raise ValueError(f"Slicer path {self._slicer_path} is not executable")
        
        self._collection_name_lookup = collection_name_lookup
    
    def execute(
        self, 
        tape_location: Path,
        output_directory: Path,
        log_location: Path
    ) -> None:

        try:
            collection_name = self._collection_name_lookup.collection_name(self._get_tape_name(tape_location))

            cmd = [
                str(self._slicer_path),
                str(tape_location),
                collection_name,
                str(output_directory)
            ]

            logger.info(f"Executing slicer command: {' '.join(map(str, cmd))}")

            result = subprocess.run(
                cmd,
                check=True,
                text=True
            )

            if result.stderr:
                logger.warning(f"Command executed with warnings: {result.stderr}")

            logger.info("Command executed successfully")

        except CollectionNameLookupError as e:
            raise e
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