import logging
from pathlib import Path

import boto3
from mypy_boto3_s3.service_resource import S3ServiceResource

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
from payload_migration.uploader.hcp_uploader import HcpUploader
from payload_migration.uploader.hcp_uploader_impl import HcpUploaderImpl

logger = logging.getLogger(__name__)

if __name__ == '__main__':
    logging_setup.setup_logging(Path('./log'), 'A12793')
    logger.info("Starting migration")

    config: PayloadMigrationConfig = load_config("./payload_migration/resources/payload_migration_config.yaml")
    db2_connection: DBConnection = DB2ConnectionImpl(
        config.db_config.database_config,
        config.db_config.user,
        config.db_config.password
    )
    agid_name_lookup: AgidNameLookup = AgidNameLookupImpl(db2_connection)
    path_transformer: PathTransformer = PathTransformerImpl(agid_name_lookup)
    link_creator: LinkCreator = LinkCreatorImpl(
        config.linker_config.source_dir,
        config.linker_config.target_base_dir,
        config.linker_config.file_patterns,
        path_transformer
    )

    s3: S3ServiceResource = boto3.resource('s3')
    hcp_uploader: HcpUploader = HcpUploaderImpl(
        s3, 
        config.uploader_config.max_workers, 
        config.uploader_config.bucket
    )

    link_creator.create_links()
    hcp_uploader.upload_dir(config.linker_config.target_base_dir)
