import concurrent.futures
from pathlib import Path

from payload_migration.executor.executor import Executor
from payload_migration.tape_processor.tape_processor_factory import TapeProcessorFactory
import logging

logger = logging.getLogger(__name__)

class ParallelTapeExecutor(Executor):
    def __init__(
        self, 
        input_directory: Path, 
        num_threads: int, 
        tape_processor_factory: TapeProcessorFactory, 
    ):
        self._input_directory: Path = input_directory
        self._num_threads: int = num_threads
        self._tape_processor_factory: TapeProcessorFactory = tape_processor_factory

    def _process_tape(self, tape_name: str):
        processor = self._tape_processor_factory.create_tape_processor(tape_name)
        processor.process_tape()

    def run(self):
        tape_names = [file.name for file in self._input_directory.glob('*') if file.is_file()]
        logger.info(f"Processing tapes: {tape_names}")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self._num_threads) as executor:
            executor.map(self._process_tape, tape_names)