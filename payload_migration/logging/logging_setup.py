import logging
from datetime import datetime

from payload_migration.config.payload_migration_config import LoggingConfig


def setup_logging(logging_config: LoggingConfig) -> None:
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = logging_config.log_dir / f'payload_migration_{logging_config.log_label}_{timestamp}.log'
    log_file.parent.mkdir(parents=True, exist_ok=True)

    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    # File handler
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)

    # Root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)