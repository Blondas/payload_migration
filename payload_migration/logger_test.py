import logging

from payload_migration.config.payload_migration_config import PayloadMigrationConfig, load_config
from payload_migration.logging import logging_setup

logger = logging.getLogger(__name__)

if __name__ == '__main__':
    config: PayloadMigrationConfig = load_config("./payload_migration/resources/payload_migration_config_SAMPLE.yaml")

    logging_setup.setup_logging(config.logging_config)
    logger.info("Starting migration")
    exit(1)