import logging
from datetime import datetime
from pathlib import Path


def setup_logging(output_base_dir: Path, log_subdir: str) -> None:
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = output_base_dir / log_subdir / f'payload_migration_{timestamp}.log'
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