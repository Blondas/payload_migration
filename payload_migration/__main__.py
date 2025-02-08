import logging

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
from payload_migration.uploader.hcp_uploader_aws_cli import HcpUploaderAwsCliImpl

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
    agid_name_lookup: AgidNameLookup = AgidNameLookupImpl(db2_connection)
    path_transformer: PathTransformer = PathTransformerImpl(agid_name_lookup)
    link_creator: LinkCreator = LinkCreatorImpl(
        payload_migration_config.linker_config.source_dir,
        payload_migration_config.linker_config.target_base_dir,
        payload_migration_config.linker_config.file_patterns,
        path_transformer
    )
    
    hcp_uploader: HcpUploader = HcpUploaderAwsCliImpl(
        payload_migration_config.uploader_config.s3_bucket,
        payload_migration_config.uploader_config.s3_prefix,
        payload_migration_config.uploader_config.verify_ssl
    )

    logger.info("Linker starting ...")
    link_creator.create_links()

    logger.info("Uploader started ...")
    hcp_uploader.upload_dir(payload_migration_config.linker_config.target_base_dir)
    
    logger.info("Uploader finished ...")
