from payload_migration.config.payload_migration_config import load_config
from pathlib import Path

def test_load_config():
    # Get the project root directory (assuming tests are in a standard location)
    project_root = Path(__file__).parent.parent.parent.parent
    
    config_path = project_root / 'payload_migration/resources/payload_migration_config_SAMPLE.yaml'
    print(config_path)
    assert Path(config_path).exists(), f"Config file not found: {Path(config_path).absolute()}"
    config = load_config(str(config_path))
    print(config)
    assert config is not None
