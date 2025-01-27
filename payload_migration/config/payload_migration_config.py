from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import yaml

@dataclass
class LoggingConfig:
    log_dir: Path
    log_label: str

@dataclass
class DbConfig:
    database_config: str
    user: str
    password: str

@dataclass
class LinkerConfig:
    source_dir: Path
    target_base_dir: Path
    agid_name_lookup_table: str
    file_pattern: str

@dataclass
class PayloadMigrationConfig:
    logging_config: LoggingConfig
    db_config: DbConfig
    linker_config: LinkerConfig


def load_config(config_path: Optional[str] = None) -> PayloadMigrationConfig:
    if config_path is None:
        config_path = str(Path(__file__).parent / 'resources' / 'payload_migration_config.yaml')

    with open(config_path) as f:
        yaml_config = yaml.safe_load(f)

    return PayloadMigrationConfig(
        logging_config=LoggingConfig(
            log_dir=Path(yaml_config['logging_config']['log_dir']),
            log_label=yaml_config['logging_config']['log_label'],
        ),
        db_config=DbConfig(
          database_config=yaml_config['db_config']['database'],
          user=yaml_config['db_config']['user'],
          password=yaml_config['db_config']['password']
        ),
        linker_config=LinkerConfig(
            source_dir=Path(yaml_config['linker_config']['source_dir']),
            target_base_dir=Path(yaml_config['linker_config']['target_base_dir']),
            agid_name_lookup_table=yaml_config['linker_config']['agid_name_lookup_table'],
            file_pattern=yaml_config['linker_config']['file_pattern']
        )
    )