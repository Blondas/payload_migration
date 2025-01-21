import logging
from pathlib import Path
from typing import Optional
import yaml

from payload_migration.linker.symlink_creator import SymlinkCreator, SymlinkCreatorImpl, SymlinkConfig

logger = logging.getLogger(__name__)

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

def load_config(config_path: Optional[str] = None) -> SymlinkConfig:
    if config_path is None:
        config_path = str(Path(__file__).parent / 'application_config.yaml')

    with open(config_path) as f:
        yaml_config = yaml.safe_load(f)

    return SymlinkConfig(
        source_dir=Path(yaml_config['linker']['source_dir']),
        target_base_dir=Path(yaml_config['linker']['target_base_dir']),
        file_pattern=yaml_config['linker']['file_pattern']
    )

if __name__ == '__main__':
    setup_logging()
    logger.info("main method")

    symlink_config: SymlinkConfig = SymlinkConfig(

    )

    symlink_creator: SymlinkCreator = SymlinkCreatorImpl(

    )