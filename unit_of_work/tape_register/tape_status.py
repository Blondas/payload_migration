from enum import Enum, unique

@unique
class TapeStatus(Enum):
    EXPORTED: str = "exported"
    NEW: str = "new"
    REQUESTED: str = "requested"
    SLICED: str = "sliced"
    SANITIZED: str = "sanitized"
    LINKED: str = "linked"
    FINISHED: str = "finished"
    FAILED: str = "failed"
    IN_PROGRESS: str = "in progress"

    def __str__(self) -> str:
        return self.value