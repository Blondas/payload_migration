from abc import abstractmethod, ABC

class Executor(ABC):
    @abstractmethod
    def run(
        self
    ) -> None:
        pass