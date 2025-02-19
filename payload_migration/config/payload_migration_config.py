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
class TapeImportConfirmerConfig:
    tape_directory: Path
    ready_extension: str
    timeout: int
    check_interval: int

@dataclass
class SlicerConfig:
    slicer_path: Path
    
@dataclass
class SanityCheckerConfig:
    sanity_checker_path: Path
     
@dataclass
class LinkerConfig:
    # output_directory: Path
    agid_name_lookup_table: str
    file_patterns: [str]
    
@dataclass
class UploaderConfig:
    verify_ssl: bool
    s3_bucket: str
    s3_prefix: str

@dataclass
class PayloadMigrationConfig:
    tape_register_table: str
    output_working_directory: Path
    db_config: DbConfig
    tape_import_confirmer_config: TapeImportConfirmerConfig
    slicer_config: SlicerConfig
    sanity_checker_config: SanityCheckerConfig
    linker_config: LinkerConfig
    uploader_config: UploaderConfig


def load_config(config_path: Optional[str] = None) -> PayloadMigrationConfig:
    if config_path is None:
        config_path = str(Path(__file__).parent / 'resources' / 'payload_migration_config.yaml')

    with open(config_path) as f:
        yaml_config = yaml.safe_load(f)

    return PayloadMigrationConfig(
        tape_register_table=yaml_config['tape_register_table'],
        output_working_directory=Path(yaml_config['output_working_directory']),
        db_config=DbConfig(
          database=yaml_config['db_config']['database'],
          user=yaml_config['db_config']['user'],
          password=yaml_config['db_config']['password']
        ),
        tape_import_confirmer_config=TapeImportConfirmerConfig(
            tape_directory=Path(yaml_config['tape_import_confirmer_config']['tape_directory']),
            ready_extension=yaml_config['tape_import_confirmer_config']['ready_extension'],
            timeout=yaml_config['tape_import_confirmer_config']['timeout'],
            check_interval=yaml_config['tape_import_confirmer_config']['check_interval']
        ),
        slicer_config=SlicerConfig(
            slicer_path=Path(yaml_config['slicer_config']['slicer_path']),
        ),
        sanity_checker_config=SanityCheckerConfig(
            sanity_checker_path=Path(yaml_config['sanity_checker_config']['sanity_checker_path'])
        ),
        linker_config=LinkerConfig(
            agid_name_lookup_table=yaml_config['linker_config']['agid_name_lookup_table'],
            file_patterns=yaml_config['linker_config']['file_patterns']
        ),
        uploader_config=UploaderConfig(
            verify_ssl=yaml_config['uploader_config']['verify_ssl'],
            s3_bucket=yaml_config['uploader_config']['s3_bucket'],
            s3_prefix=yaml_config['uploader_config']['s3_prefix'],
        )
    )