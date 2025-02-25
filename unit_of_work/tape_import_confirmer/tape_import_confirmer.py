from abc import ABC, abstractmethod
from pathlib import Path
from unit_of_work.tape_register.tape_status import TapeStatus


class TapeImportConfirmer(ABC):
    @abstractmethod
    def wait_for_confirmation(self, tape_name: str, tape_location: Path, tape_status: TapeStatus) -> None:
        pass
    
    @abstractmethod
    def get_tape_confirmation_file(self, tape_name: str, tape_location: Path, tape_status: TapeStatus) -> Path:
        pass