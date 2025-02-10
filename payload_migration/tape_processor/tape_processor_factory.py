from abc import ABC, abstractmethod
from payload_migration.tape_processor.tape_processor import TapeProcessor

class TapeProcessorFactory(ABC):
    @abstractmethod
    def create_tape_processor(self, tape_name: str) -> TapeProcessor:
        pass