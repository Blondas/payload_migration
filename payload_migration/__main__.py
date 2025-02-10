import logging
from payload_migration.config.payload_migration_config import PayloadMigrationConfig, load_config
from payload_migration.db2.db2_connection_impl import DB2ConnectionImpl
from payload_migration.db2.db_connection import DBConnection
from payload_migration.executor.parallel_executor import ParallelTapeExecutor
from payload_migration.executor.executor import Executor
from payload_migration.linker.agid_name_lookup.agid_name_lookup import AgidNameLookup
from payload_migration.linker.agid_name_lookup.agid_name_lookup_impl import AgidNameLookupImpl
from payload_migration.linker.path_transformer.path_transformer import PathTransformer
from payload_migration.linker.path_transformer.path_transformer_impl import PathTransformerImpl
from payload_migration.logging import logging_setup
from payload_migration.slicer.slicer import Slicer
from payload_migration.slicer.slicer_impl import SlicerImpl
from payload_migration.slicer.collection_name_lookup.collection_name_lookup import CollectionNameLookup
from payload_migration.slicer.collection_name_lookup.collection_name_lookup_impl import CollectionNameLookupImpl
from payload_migration.tape_processor.tape_processor_factory import TapeProcessorFactory
from payload_migration.uploader.hcp_uploader import HcpUploader
from payload_migration.uploader.hcp_uploader_aws_cli import HcpUploaderAwsCliImpl
from payload_migration.tape_processor.tape_processor_factory_impl import TapeProcessorFactoryImpl

logger = logging.getLogger(__name__)

if __name__ == '__main__':
    payload_migration_config: PayloadMigrationConfig = load_config("./payload_migration/resources/payload_migration_config.yaml")

    logging_setup.setup_logging(payload_migration_config.logging_config)
    logger.info("Starting migration")
    
    db2_connection: DBConnection = DB2ConnectionImpl(
        payload_migration_config.db_config.database_config,
        payload_migration_config.db_config.user,
        payload_migration_config.db_config.password
    )

    collection_name_lookup: CollectionNameLookup = CollectionNameLookupImpl(db2_connection)
    slicer: Slicer = SlicerImpl(
        payload_migration_config.slicer_config.slicer_path,
        collection_name_lookup
    )
    
    agid_name_lookup: AgidNameLookup = AgidNameLookupImpl(db2_connection)
    path_transformer: PathTransformer = PathTransformerImpl(agid_name_lookup)
    
    hcp_uploader: HcpUploader = HcpUploaderAwsCliImpl(
        payload_migration_config.uploader_config.s3_bucket,
        payload_migration_config.uploader_config.s3_prefix,
        payload_migration_config.uploader_config.verify_ssl
    )
    
    tape_processor_factory: TapeProcessorFactory = TapeProcessorFactoryImpl(
        db_connection=db2_connection,
        slicer=slicer,
        path_transformer=path_transformer,
        hcp_uploader=hcp_uploader,
        linker_file_patterns=payload_migration_config.linker_config.file_patterns,
        input_dir=payload_migration_config.input_dir,
        output_base_dir=payload_migration_config.output_base_dir,
        delete_output_tape_dir=payload_migration_config.delete_output_tape_dir,
        log_subdir=payload_migration_config.logging_config.log_subdir,
        slicer_output_subdir=payload_migration_config.slicer_config.output_subdir,
        linker_output_subdir=payload_migration_config.linker_config.output_subdir
    )
    
    tape_executor: Executor = ParallelTapeExecutor(
        input_directory=payload_migration_config.input_dir,
        num_threads=10,
        tape_processor_factory=tape_processor_factory
    )
    
    tape_executor.run()
    
    logger.info("Migration complete")
    