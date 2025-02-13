import logging
from pathlib import Path
import time

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
from payload_migration.slicer.slicer import Slicer
from payload_migration.slicer.slicer_impl import SlicerImpl
from payload_migration.uploader.hcp_uploader import HcpUploader
from payload_migration.uploader.hcp_uploader_aws_cli import HcpUploaderAwsCliImpl
from payload_migration.utils.delete_path import delete_path
import argparse

logger = logging.getLogger(__name__)

def parse_args():
    parser = argparse.ArgumentParser(description='Unit of work of payload migration. It process one tape in multiple steps: tape mainframe import, slice, verify sliced, lin, HCP upload')
    parser.add_argument('--tape-name', type=Path, required=True, help='Tape Name (vTapeFile)')
    parser.add_argument('--tape-location', type=str, required=True, help='Tape file location')
    parser.add_argument('--tape-name', type=str, required=True, help='Tape file location')
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
    
    
    try:
        logger.info(f"Slicer starting, tape name: {args.tape_name}")
        start_time = time.time()
        slicer.execute(
            args.tape_name,
            payload_migration_config.slicer_config.output_directory,
            slicer_log
        )
        slicer_duration = time.time() - start_time
    except Exception as e: 
        logger.error(f"Slicer failed, tape name: {args.tape_name}, {str(e)}")
        raise
    
    try:
        logger.info(f"Linker starting, tape name: {args.tape_name}")
        start_time = time.time()
        link_creator.create_links()
        linker_duration = time.time() - start_time
        delete_path(payload_migration_config.slicer_config.output_directory, False)
    except Exception as e: 
        logger.error(f"Linker failed, tape name: {args.tape_name} {str(e)}")
        raise

    try:
        logger.info(f"Uploader started, tape name: {args.tape_name}")
        start_time = time.time()
        hcp_uploader.upload_dir(payload_migration_config.linker_config.output_directory)
        uploader_duration = time.time() - start_time
        delete_path(payload_migration_config.linker_config.output_directory, False)
    except Exception as e: 
        logger.error(f"Uploader failed, tape name: {args.tape_name} {str(e)}")
        raise
    
    # Ensuring working directory is deleted
    for working_dir in [
        payload_migration_config.slicer_config.output_directory, 
        payload_migration_config.linker_config.output_directory
    ]:
        delete_path(working_dir, True)
    
    logger.info(f"Uploader finished, working directory deleted, tape name: {args.tape_name}")
    logger.info(f"Statistics for unit of work, tape name: {args.tape_name}, tape location: {args.tape_location}, "
                f"Slicer duration: {slicer_duration}, "
                f"Linker duration: {linker_duration}, "
                f"Uploader duration: {uploader_duration}"
    )
    
    
