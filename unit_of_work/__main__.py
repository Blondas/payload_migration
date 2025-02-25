import logging
from logging import Logger
from pathlib import Path

from unit_of_work.config.payload_migration_config import PayloadMigrationConfig, load_config
from unit_of_work.linker.agid_name_lookup.agid_name_lookup import AgidNameLookup
from unit_of_work.linker.agid_name_lookup.agid_name_lookup_impl import AgidNameLookupImpl
from unit_of_work.linker.link_creator.link_creator import LinkCreator
from unit_of_work.linker.link_creator.link_creator_impl import LinkCreatorImpl
from unit_of_work.linker.path_transformer.path_transformer import PathTransformer
from unit_of_work.linker.path_transformer.path_transformer_impl import PathTransformerImpl
from unit_of_work.logging import logging_setup
from unit_of_work.processor.unit_of_work_processor_impl import UnitOfWorkProcessorImpl
from unit_of_work.processor.unit_of_work_processor import UnitOfWorkProcessor
from unit_of_work.sanity_checker.sanity_checker import SanityChecker
from unit_of_work.sanity_checker.sanity_checker_impl import SanityCheckerImpl
from unit_of_work.slicer.slicer import Slicer
from unit_of_work.slicer.slicer_impl import SlicerImpl
from unit_of_work.tape_import_confirmer.tape_import_confirmer import TapeImportConfirmer
from unit_of_work.tape_import_confirmer.tape_import_confirmer_impl import TapeImportConfirmerImpl
from unit_of_work.tape_register.tape_register import TapeRegister
from unit_of_work.tape_register.tape_register_impl import TapeRegisterImpl
# from unit_of_work.uploader.hcp_uploader import HcpUploader
# from unit_of_work.uploader.hcp_uploader_aws_cli import HcpUploaderAwsCliImpl
import argparse
from unit_of_work.db2.db2_connection_impl import DB2ConnectionImpl
from unit_of_work.db2.db_connection import DBConnection

logger: Logger = logging.getLogger(__name__)

def parse_args():
    parser = argparse.ArgumentParser(description='An ETL (Extract, Transform, Load) Python package for payload migration processing, '
                                                 'named "Unit of Work." It extracts by waiting for and confirming the existence of an imported tape, '
                                                 'transforms by slicing the tape, performing sanity checks, and creating a future directory structure via symbolic links, '
                                                 'and loads by uploading the processed data to S3.')
    parser.add_argument('--tape-name', type=str, required=True, help='Tape Name (vTapeFile)')
    return parser.parse_args()
    
    
def main():
    args = parse_args()
    payload_migration_config: PayloadMigrationConfig = load_config("./unit_of_work/resources/payload_migration_config.yaml")
    
    tape_name: str = args.tape_name
    
    working_directory: Path = payload_migration_config.output_working_directory / tape_name
    slicer_output_directory: Path = working_directory / 'slicer'
    linker_output_directory: Path = working_directory / 'linker'
    unit_our_work_log: Path = working_directory / 'log' / f'unit_of_work_{tape_name}.log'
    slicer_log: Path = working_directory / 'log' / f'slicer_{tape_name}.log'
    sanity_checker_log: Path = working_directory / 'log' / f'sanity_checker_{tape_name}.log'

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
        timeout=payload_migration_config.tape_import_confirmer_config.timeout,
        check_interval=payload_migration_config.tape_import_confirmer_config.check_interval
    )
    slicer: Slicer = SlicerImpl(
        slicer_path = payload_migration_config.slicer_config.slicer_path
    )
    sanity_checker: SanityChecker = SanityCheckerImpl(
        sanity_checker_path = payload_migration_config.sanity_checker_config.sanity_checker_path
    )
    agid_name_lookup: AgidNameLookup = AgidNameLookupImpl(db2_connection)
    path_transformer: PathTransformer = PathTransformerImpl(agid_name_lookup)
    link_creator: LinkCreator = LinkCreatorImpl(
        source_dir = slicer_output_directory,
        output_directory= linker_output_directory,
        file_patterns = payload_migration_config.linker_config.file_patterns,
        path_transformer = path_transformer
    )
    
    # hcp_uploader: HcpUploader = HcpUploaderAwsCliImpl(
    #     s3_bucket = payload_migration_config.uploader_config.s3_bucket,
    #     s3_prefix = payload_migration_config.uploader_config.s3_prefix,
    #     verify_ssl = payload_migration_config.uploader_config.verify_ssl
    # )
    
    processor: UnitOfWorkProcessor = UnitOfWorkProcessorImpl(
        tape_import_confirmer = tape_import_confirmer,
        tape_register = tape_register,
        slicer = slicer,
        sanity_checker = sanity_checker,
        link_creator = link_creator,
        slicer_output_directory=slicer_output_directory,
        slicer_log=slicer_log,
        sanity_checker_log=sanity_checker_log,
        linked_output_directory=linker_output_directory
    )
    
    
    processor.process(tape_name, tape_location)
    
if __name__ == '__main__':
    main()