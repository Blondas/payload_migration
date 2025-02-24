import logging
from pathlib import Path
import time
from unit_of_work.linker.link_creator.link_creator import LinkCreator
from unit_of_work.processor.unit_of_work_processor import UnitOfWorkProcessor
from unit_of_work.sanity_checker.sanity_checker import SanityChecker
from unit_of_work.slicer.slicer import Slicer
from unit_of_work.tape_import_confirmer.tape_import_confirmer import TapeImportConfirmer
from unit_of_work.tape_register.tape_register import TapeRegister
from unit_of_work.uploader.hcp_uploader import HcpUploader
from unit_of_work.utils.delete_path import delete_path

logger = logging.getLogger(__name__)

class UnitOfWorkProcessorImpl(UnitOfWorkProcessor):
    def __init__(
        self,
        tape_import_confirmer: TapeImportConfirmer,
        tape_register: TapeRegister,
        slicer: Slicer,
        sanity_checker: SanityChecker,
        link_creator: LinkCreator,
        hcp_uploader: HcpUploader,
        slicer_output_directory: Path,
        slicer_log: Path,
        sanity_checker_log: Path,
        linked_output_directory: Path
    ):
        self._tape_register: TapeRegister = tape_register
        self._tape_import_confirmer: TapeImportConfirmer = tape_import_confirmer
        self._slicer: Slicer = slicer
        self._sanity_checker: SanityChecker = sanity_checker
        self._link_creator: LinkCreator = link_creator
        self._hcp_uploader: HcpUploader = hcp_uploader
        self._slicer_output_directory: Path = slicer_output_directory
        self._slicer_log: Path = slicer_log
        self._sanity_checker_log: Path = sanity_checker_log
        self._linker_output_directory = linked_output_directory
    def process(
        self, 
        tape_name: str,
        tape_location: Path
    ) -> None:
        try:
            tape_confirmer_waiting_time: float = self._run_tape_import_confirmer(tape_name, tape_location)
            slicer_duration: float = self._run_slicer(tape_name, tape_location)
            sanity_checker_duration: float = self._run_sanity_checker(tape_name)
            linker_duration: float = self._run_linker(tape_name)
            uploader_duration: float = self._run_uploader(tape_name)
            self._clean_working_dir()
            self._clean_tape_and_tape_confirmation_file(
                tape_location, 
                self._tape_import_confirmer.get_tape_confirmation_file(tape_name, tape_location)
            )

            logger.info(f"Uploader finished, working directory deleted, tape name: {tape_name}")
            logger.info(
                f"Statistics for unit of work: "
                f"[tape={tape_name}] "
                f"[location={tape_location}] "
                f"[confirmer_wait={tape_confirmer_waiting_time}] "
                f"[slicer={slicer_duration}] "
                f"[sanity_checker={sanity_checker_duration}] "
                f"[linker={linker_duration}] "
                f"[uploader={uploader_duration}]"
            )

        except Exception as e:
            logger.error(f"Unit of work failed, tape name: {tape_name}, {str(e)}")
            self._tape_register.set_status_failed(tape_name)
    
    def _run_tape_import_confirmer(self, tape_name: str, tape_location: Path) -> float:
        try:
            logger.info(f"Tape record confirmer starting, tape name: {tape_name}")
            start_time = time.time()
            self._tape_import_confirmer.wait_for_confirmation(tape_name, tape_location)
            self._tape_register.set_status_exported(tape_name)
            return time.time() - start_time
        except Exception as e:
            logger.error(f"Tape record confirmer wait failed, tape name: {tape_name}, tape location: {tape_location}, {str(e)}")
            raise
        
    def _run_slicer(self, tape_name: str, tape_location: Path) -> float:
        try:
            logger.info(f"Slicer starting, tape name: {tape_name}")
            start_time = time.time()
            self._slicer.execute(
                tape_location=tape_location,
                output_directory=self._slicer_output_directory,
                log_file=self._slicer_log
            )
            self._tape_register.set_status_sliced(tape_name)
            return time.time() - start_time
        except Exception as e:
            logger.error(f"Slicer failed, tape name: {tape_name}, {str(e)}")
            raise
        
    def _run_sanity_checker(self, tape_name: str) -> float:
        try:
            logger.info(f"Sanity checker starting, tape name: {tape_name}")
            start_time = time.time()
            self._sanity_checker.execute(
                tape_name=tape_name,
                slicer_log=self._slicer_log,
                slicer_output_directory=self._slicer_output_directory,
                sanity_checker_log=self._sanity_checker_log
            )
            self._tape_register.set_status_sanitized(tape_name)
            return time.time() - start_time
        except Exception as e:
            logger.error(f"Sanity checker failed, tape name: {tape_name}, {str(e)}")
            raise

    def _run_linker(self, tape_name: str) -> float:
        try:
            logger.info(f"Linker starting, tape name: {tape_name}")
            start_time = time.time()
            self._link_creator.create_links()
            self._tape_register.set_status_linked(tape_name)
            duration = time.time() - start_time
            delete_path(self._slicer_output_directory, False)
            return duration
        except Exception as e:
            logger.error(f"Linker failed, tape name: {tape_name} {str(e)}")
            raise

    def _run_uploader(self, tape_name: str) -> float:
        try:
            logger.info(f"Uploader started, tape name: {tape_name}")
            start_time = time.time()
            self._hcp_uploader.upload_dir(self._linker_output_directory)
            self._tape_register.set_status_finished(tape_name)
            duration = time.time() - start_time
            delete_path(self._linker_output_directory, False)
            return duration
        except Exception as e:
            logger.error(f"Uploader failed, tape name: {tape_name} {str(e)}")
            raise

    def _clean_working_dir(self) -> None:
        for working_dir in [
            self._slicer_output_directory,
            self._linker_output_directory
        ]:
            delete_path(working_dir, True)
        
    def _clean_tape_and_tape_confirmation_file(self, tape: Path, tape_confirmation_file: Path) -> None:
        for working_dir in [
            tape,
            tape_confirmation_file
        ]:
            delete_path(working_dir, True)