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
    return parser.parse_args()
    

if __name__ == '__main__':
    args = parse_args()
    payload_migration_config: PayloadMigrationConfig = load_config("./payload_migration/resources/payload_migration_config.yaml")
    
    tape_name: str = args.tape_name
    
    working_directory: Path = payload_migration_config.output_working_directory / tape_name
    slicer_output_directory: Path = working_directory / 'slicer'
    linker_output_directory: Path = working_directory / 'linker'
    unit_our_work_log: Path = working_directory / 'log' / f'unit_of_work_{tape_name}.log'
    slicer_log: Path = working_directory / 'log' / f'slicer_{tape_name}.log'

    logging_setup.setup_logging(unit_our_work_log)
    tape_location: Path = payload_migration_config.tape_import_confirmer_config.tape_directory / tape_name
    logger.info(f"Starting unit of work, tape name: {tape_name}, tape location: {tape_location}")
    
    
    
    db2_connection: DBConnection = DB2ConnectionImpl(
        database = payload_migration_config.db_config.database,
        user = payload_migration_config.db_config.user,
        password = payload_migration_config.db_config.password
    )
    tape_register: TapeRegister = TapeRegisterImpl(db2_connection, payload_migration_config.tape_register_table)
    tape_import_confirmer: TapeImportConfirmer = TapeImportConfirmerImpl(
        ready_extension=payload_migration_config.tape_import_confirmer_config.ready_extension,
        timeout=payload_migration_config.tape_import_confirmer_config.timeout,
        check_interval=payload_migration_config.tape_import_confirmer_config.check_interval
    )
    slicer: Slicer = SlicerImpl(
        slicer_path = payload_migration_config.slicer_config.slicer_path
    )
    agid_name_lookup: AgidNameLookup = AgidNameLookupImpl(db2_connection)
    path_transformer: PathTransformer = PathTransformerImpl(agid_name_lookup)
    link_creator: LinkCreator = LinkCreatorImpl(
        source_dir = slicer_output_directory,
        output_directory= linker_output_directory,
        file_patterns = payload_migration_config.linker_config.file_patterns,
        path_transformer = path_transformer
    )
    
    hcp_uploader: HcpUploader = HcpUploaderAwsCliImpl(
        s3_bucket = payload_migration_config.uploader_config.s3_bucket,
        s3_prefix = payload_migration_config.uploader_config.s3_prefix,
        verify_ssl = payload_migration_config.uploader_config.verify_ssl
    )
    
    processor: UnitOfWorkProcessor = UnitOfWorkProcessorImpl(
        tape_import_confirmer = tape_import_confirmer,
        tape_register = tape_register,
        slicer = slicer,
        link_creator = link_creator,
        hcp_uploader = hcp_uploader,
        slicer_output_directory=slicer_output_directory,
        slicer_log=slicer_log,
        linked_output_directory=linker_output_directory
    )
    
    
    processor.process(tape_name, tape_location)
    