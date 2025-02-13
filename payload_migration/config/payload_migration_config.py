from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import yaml

@dataclass
class LoggingConfig:
    log_file: Path

@dataclass
class DbConfig:
    database_config: str
    user: str
    password: str

@dataclass
class SlicerConfig:
    slicer_path: Path
    tape_location: Path 
    output_directory: Path 
    log_file: Path
     
@dataclass
class LinkerConfig:
    output_directory: Path
    agid_name_lookup_table: str
    file_patterns: [str]
    
@dataclass
class UploaderConfig:
    verify_ssl: bool
    s3_bucket: str
    s3_prefix: str

@dataclass
class PayloadMigrationConfig:
    logging_config: LoggingConfig
    db_config: DbConfig
    slicer_config: SlicerConfig
    linker_config: LinkerConfig
    uploader_config: UploaderConfig


def load_config(config_path: Optional[str] = None) -> PayloadMigrationConfig:
    if config_path is None:
        config_path = str(Path(__file__).parent / 'resources' / 'payload_migration_config.yaml')

    with open(config_path) as f:
        yaml_config = yaml.safe_load(f)

    return PayloadMigrationConfig(
        logging_config=LoggingConfig(
            log_file=Path(yaml_config['logging_config']['log_file'])
        ),
        db_config=DbConfig(
          database_config=yaml_config['db_config']['database'],
          user=yaml_config['db_config']['user'],
          password=yaml_config['db_config']['password']
        ),
        slicer_config=SlicerConfig(
            slicer_path=Path(yaml_config['slicer_config']['slicer_path']),
            tape_location=Path(yaml_config['slicer_config']['tape_location']),
            output_directory=Path(yaml_config['slicer_config']['output_directory']),
            log_file=Path(yaml_config['slicer_config']['log_file'])
        ),
        linker_config=LinkerConfig(
            output_directory=Path(yaml_config['linker_config']['output_directory']),
            agid_name_lookup_table=yaml_config['linker_config']['agid_name_lookup_table'],
            file_patterns=yaml_config['linker_config']['file_patterns']
        ),
        uploader_config=UploaderConfig(
            verify_ssl=yaml_config['uploader_config']['verify_ssl'],
            s3_bucket=yaml_config['uploader_config']['s3_bucket'],
            s3_prefix=yaml_config['uploader_config']['s3_prefix'],
        )
    )