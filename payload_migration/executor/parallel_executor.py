from pathlib import Path

from payload_migration.config.payload_migration_config import PayloadMigrationConfig
from payload_migration.executor.executor import Executor
from payload_migration.logging import logging_setup
from payload_migration.tape_processor.tape_processor_factory import TapeProcessorFactory
from multiprocessing import Pool


class ParallelTapeExecutor(Executor):
    def __init__(
        self, 
        input_directory: Path, 
        num_threads: int, 
        tape_processor_factory: TapeProcessorFactory,
        payload_migration_config: PayloadMigrationConfig
    ):
        self._input_directory: Path = input_directory
        self._num_threads: int = num_threads
        self._tape_processor_factory: TapeProcessorFactory = tape_processor_factory
        self._payload_migration_config: PayloadMigrationConfig = payload_migration_config

    def _process_tape(self, args: tuple[str, PayloadMigrationConfig]):
        tape_name, config = args
        logging_setup.setup_logging(config.output_base_dir, config.logging_config.log_subdir)
        processor = self._tape_processor_factory.create_tape_processor(tape_name)
        processor.process_tape()
        
        processor = self._tape_processor_factory.create_tape_processor(tape_name)
        processor.process_tape()

    def run(self):
        tape_names = [file.name for file in self._input_directory.glob('*') if file.is_file()]
        process_args = [(name, self._payload_migration_config) for name in tape_names]
        with Pool(processes=self._num_threads) as pool:
            pool.map(self._process_tape, process_args)