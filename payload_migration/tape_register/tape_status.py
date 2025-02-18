from enum import Enum, unique

@unique
class TapeStatus(Enum):
    EXPORTED: str = "exported"
    NEW: str = "new"
    REQUESTED: str = "requested"
    SLICED: str = "sliced"
    LINKED: str = "linked"
    FINISHED: str = "finished"
    FAILED: str = "failed"

    def __str__(self) -> str:
        return self.value