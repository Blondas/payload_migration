from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import yaml

@dataclass
class DbConfig:
    database: str
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
    db_config: DbConfig
    linker_config: LinkerConfig


def load_config(config_path: Optional[str] = None) -> PayloadMigrationConfig:
    if config_path is None:
        config_path = str(Path(__file__).parent / 'resources' / 'payload_migration_config.yaml')

    with open(config_path) as f:
        yaml_config = yaml.safe_load(f)

    return PayloadMigrationConfig(
        db_config=DbConfig(
          database=yaml_config['db_config']['database'],
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