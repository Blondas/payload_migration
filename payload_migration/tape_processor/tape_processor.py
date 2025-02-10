from abc import ABC, abstractmethod

class TapeProcessor(ABC):
    @abstractmethod
    def process_tape(self, perform_deletion: bool):
        pass