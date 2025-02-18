from abc import ABC, abstractmethod

class TapeRegister(ABC):
    @abstractmethod
    def set_status_failed(self, tape_name: str) -> None:
        pass

    @abstractmethod
    def set_status_sliced(self, tape_name: str) -> None:
        pass

    @abstractmethod
    def set_status_linked(self, tape_name: str) -> None:
        pass

    @abstractmethod
    def set_status_exported(self, tape_name: str) -> None:
        pass

    @abstractmethod
    def set_status_finished(self, tape_name: str) -> None:
        pass