import logging
from payload_migration.config.payload_migration_config import PayloadMigrationConfig, load_config
from payload_migration.executor.parallel_executor import ParallelTapeExecutor
from payload_migration.executor.executor import Executor

from payload_migration.logging import logging_setup
from payload_migration.tape_processor.tape_processor_factory import TapeProcessorFactory
from payload_migration.tape_processor.tape_processor_factory_impl import TapeProcessorFactoryImpl

logger = logging.getLogger(__name__)

if __name__ == '__main__':
    payload_migration_config: PayloadMigrationConfig = load_config("./payload_migration/resources/payload_migration_config.yaml")

    logging_setup.setup_logging(payload_migration_config.output_base_dir, payload_migration_config.logging_config.log_subdir)
    logger.info("Starting migration")
    
    tape_processor_factory: TapeProcessorFactory = TapeProcessorFactoryImpl(payload_migration_config)
    
    tape_executor: Executor = ParallelTapeExecutor(
        input_directory=payload_migration_config.input_dir,
        num_threads=10,
        tape_processor_factory=tape_processor_factory
    )
    
    tape_executor.run()
    
    logger.info("Migration complete")
    