from unit_of_work.tape_import_confirmer.tape_import_confirmer import TapeImportConfirmer
from pathlib import Path
import time

class TapeImportConfirmerImpl(TapeImportConfirmer):
    def __init__(
        self,
        ready_extension: str,
        timeout: int,
        check_interval: int
    ) -> None:
        self._ready_extension: str = ready_extension
        self.timeout: int = timeout
        self.check_interval: int = check_interval

    def wait_for_confirmation(self, tape_name: str, tape_location: Path) -> None:
        confirmation_file: Path = self.get_tape_confirmation_file(tape_name, tape_location)
        start_time = time.time()

        while True:
            if tape_location.exists() and confirmation_file.exists():
                return
            if time.time() - start_time > self.timeout:
                raise TimeoutError(f"Timeout waiting for {tape_location} and {confirmation_file}")
            time.sleep(self.check_interval)
            
    def get_tape_confirmation_file(self, tape_name: str, tape_location: Path) -> Path:
        return tape_location.parent / f"{tape_name}{self._ready_extension}"