from enum import Enum, unique

@unique
class TapeStatus(Enum):
    NEW: str = "new"
    REQUESTED: str = "requested"
    SLICED: str = "sliced"
    LINKED: str = "linked"
    EXPORTED: str = "exported"
    FAILED: str = "failed"

    def __str__(self) -> str:
        return self.value