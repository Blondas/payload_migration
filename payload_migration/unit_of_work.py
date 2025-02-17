import logging
from logging import Logger
from pathlib import Path

from payload_migration.config.payload_migration_config import PayloadMigrationConfig, load_config
from payload_migration.db2.db2_connection_impl import DB2ConnectionImpl
from payload_migration.db2.db_connection import DBConnection
from payload_migration.linker.agid_name_lookup.agid_name_lookup import AgidNameLookup
from payload_migration.linker.agid_name_lookup.agid_name_lookup_impl import AgidNameLookupImpl
from payload_migration.linker.link_creator.link_creator import LinkCreator
from payload_migration.linker.link_creator.link_creator_impl import LinkCreatorImpl
from payload_migration.linker.path_transformer.path_transformer import PathTransformer
from payload_migration.linker.path_transformer.path_transformer_impl import PathTransformerImpl
from payload_migration.logging import logging_setup
from payload_migration.processor.unit_of_work_processor_impl import UnitOfWorkProcessorImpl
from payload_migration.processor.unit_of_work_processor import UnitOfWorkProcessor
from payload_migration.slicer.slicer import Slicer
from payload_migration.slicer.slicer_impl import SlicerImpl
from payload_migration.tape_import_confirmer.tape_import_confirmer import TapeImportConfirmer
from payload_migration.tape_import_confirmer.tape_import_confirmer_impl import TapeImportConfirmerImpl
from payload_migration.tape_register.tape_register import TapeRegister
from payload_migration.tape_register.tape_register_impl import TapeRegisterImpl
from payload_migration.uploader.hcp_uploader import HcpUploader
from payload_migration.uploader.hcp_uploader_aws_cli import HcpUploaderAwsCliImpl
import argparse

logger: Logger = logging.getLogger(__name__)

def parse_args():
    parser = argparse.ArgumentParser(description='Unit of work of payload migration. It process one tape in multiple steps: tape mainframe import, slice, verify sliced, lin, HCP upload')
    parser.add_argument('--tape-name', type=str, required=True, help='Tape Name (vTapeFile)')
    parser.add_argument('--tape-location', type=Path, required=True, help='Tape file location')
    return parser.parse_args()
    

if __name__ == '__main__':
    args = parse_args()
    payload_migration_config: PayloadMigrationConfig = load_config("./payload_migration/resources/payload_migration_config.yaml")

    logging_setup.setup_logging(payload_migration_config.logging_config)
    logger.info(f"Starting unit of work, tape name: {args.tape_name}, tape location: {args.tape_location}")
    
    db2_connection: DBConnection = DB2ConnectionImpl(
        database = payload_migration_config.db_config.database,
        user = payload_migration_config.db_config.user,
        password = payload_migration_config.db_config.password
    )

    tape_register: TapeRegister = TapeRegisterImpl(db2_connection)
    
    tape_import_confirmer: TapeImportConfirmer = TapeImportConfirmerImpl(
        ready_extension=payload_migration_config.tape_import_confirmer_config.ready_extension,
        timeout=payload_migration_config.tape_import_confirmer_config.timeout,
        check_interval=payload_migration_config.tape_import_confirmer_config.check_interval
    )

    slicer_log: Path = payload_migration_config.slicer_config.log_file
    slicer: Slicer = SlicerImpl(
        slicer_path = payload_migration_config.slicer_config.slicer_path
    )
    agid_name_lookup: AgidNameLookup = AgidNameLookupImpl(db2_connection)
    path_transformer: PathTransformer = PathTransformerImpl(agid_name_lookup)
    link_creator: LinkCreator = LinkCreatorImpl(
        source_dir = payload_migration_config.slicer_config.output_directory,
        output_directory= payload_migration_config.linker_config.output_directory,
        file_patterns = payload_migration_config.linker_config.file_patterns,
        path_transformer = path_transformer
    )
    
    hcp_uploader: HcpUploader = HcpUploaderAwsCliImpl(
        s3_bucket = payload_migration_config.uploader_config.s3_bucket,
        s3_prefix = payload_migration_config.uploader_config.s3_prefix,
        verify_ssl = payload_migration_config.uploader_config.verify_ssl
    )
    
    processor: UnitOfWorkProcessor = UnitOfWorkProcessorImpl(
        config = payload_migration_config,
        tape_import_confirmer = tape_import_confirmer,
        tape_register = tape_register,
        slicer = slicer,
        link_creator = link_creator,
        hcp_uploader = hcp_uploader
    )
    
    processor.process(args.tape_name, args.tape_location)
    