from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import yaml

@dataclass
class LoggingConfig:
    log_subdir: str

@dataclass
class DbConfig:
    database_config: str
    user: str
    password: str

@dataclass
class SlicerConfig:
    slicer_path: Path
    output_subdir: str 
    
@dataclass
class LinkerConfig:
    agid_name_lookup_table: str
    file_patterns: [str]
    output_subdir: str
    
@dataclass
class UploaderConfig:
    verify_ssl: bool
    s3_bucket: str
    s3_prefix: str

@dataclass
class PayloadMigrationConfig:
    input_dir: Path
    output_base_dir: Path
    delete_output_tape_dir: bool
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
        input_dir=Path(yaml_config['input_dir']),
        output_base_dir=Path(yaml_config['output_base_dir']),
        delete_output_tape_dir=yaml_config['delete_output_tape_dir'],
        logging_config=LoggingConfig(
            log_dubdir=yaml_config['logging_config']['log_subdir']
        ),
        db_config=DbConfig(
          database_config=yaml_config['db_config']['database'],
          user=yaml_config['db_config']['user'],
          password=yaml_config['db_config']['password']
        ),
        slicer_config=SlicerConfig(
            slicer_path=Path(yaml_config['slicer_config']['slicer_path']),
            output_subdir=yaml_config['slicer_config']['output_subdir']
        ),
        linker_config=LinkerConfig(
            agid_name_lookup_table=yaml_config['linker_config']['agid_name_lookup_table'],
            file_patterns=yaml_config['linker_config']['file_patterns'],
            output_subdir=yaml_config['linker_config']['output_subdir']
        ),
        uploader_config=UploaderConfig(
            verify_ssl=yaml_config['uploader_config']['verify_ssl'],
            s3_bucket=yaml_config['uploader_config']['s3_bucket'],
            s3_prefix=yaml_config['uploader_config']['s3_prefix'],
        )
    )