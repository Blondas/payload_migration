import logging
import time
from pathlib import Path

from payload_migration.db2.db_connection import DBConnection
from payload_migration.linker.link_creator.link_creator import LinkCreator
from payload_migration.slicer.slicer import Slicer
from payload_migration.uploader.hcp_uploader import HcpUploader
from payload_migration.utils.delete_path import delete_path
from payload_migration.tape_processor.tape_processor import TapeProcessor

logger = logging.getLogger(__name__)

class TapeProcessorImpl(TapeProcessor):
    def __init__(
        self,
        slicer_log_dir: Path,
        slicer_log_name: str,
        tape_location: Path,
        slicer_output_directory: Path,
        linker_target_base_dir: Path,
        db_connection: DBConnection,
        slicer: Slicer,
        link_creator: LinkCreator,
        hcp_uploader: HcpUploader
    ) -> None:
        self._slicer_log_dir: Path = slicer_log_dir
        self._slicer_log_name: str = slicer_log_name
        self._tape_location: Path = tape_location
        self._slicer_output_directory: Path = slicer_output_directory
        self._linker_target_base_dir: Path = linker_target_base_dir
        
        self._db_connection: DBConnection = db_connection
        self._slicer: Slicer = slicer
        self._link_creator: LinkCreator = link_creator
        self._hcp_uploader: HcpUploader = hcp_uploader

    def process_tape(self, perform_deletion: bool) -> None:
        try:
            logger.info(f"Starting tape processing for tape {self._tape_location} ...")

    
            logger.info("Slicer starting ...")
            start_time = time.time()
            slicer_log: Path = self._slicer_log_dir / self._slicer_log_name
            self._slicer.execute(
                self._tape_location,
                self._slicer_output_directory,
                slicer_log
            )
            slicer_duration = time.time() - start_time


            logger.info("Linker starting ...")
            start_time = time.time()
            self._link_creator.create_links()
            linker_duration = time.time() - start_time


            logger.info("Uploader starting ...")
            start_time = time.time()
            self._hcp_uploader.upload_dir(self._linker_target_base_dir)
            uploader_duration = time.time() - start_time


            logger.info("Uploader finished ...")

            deletion_duration = None
            if perform_deletion:
                logger.info("Deletion started ...")
                start_time = time.time()
                delete_path(self._slicer_output_directory.parent.parent)
                deletion_duration = time.time() - start_time
            else:
                logger.info("Deletion skipped.")

            logger.info(f"Processing tape {self._tape_location} finished. "
                f"Slicer duration: {slicer_duration}, "
                f"Linker duration: {linker_duration}, "
                f"Uploader duration: {uploader_duration}, "
                 f"Deletion duration: {deletion_duration if perform_deletion else 'N/A'}. "
            )
        
        except Exception as e:
            logger.error(f"An error occurred during tape processing: {e}", exc_info=True)