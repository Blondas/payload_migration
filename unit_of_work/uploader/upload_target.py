from dataclasses import dataclass
from pathlib import Path


@dataclass
class UploadTarget:
    local_path: Path
    s3_key: str