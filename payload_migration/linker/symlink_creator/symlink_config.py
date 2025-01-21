from dataclasses import dataclass
from pathlib import Path

@dataclass
class SymlinkConfig:
    """Configuration for symlink creation process."""
    source_dir: Path
    target_base_dir: Path
    file_pattern: str = "*"