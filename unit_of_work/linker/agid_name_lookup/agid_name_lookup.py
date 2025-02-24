from abc import ABC, abstractmethod

class AgidNameLookup(ABC):
    @abstractmethod
    def dest_agid_name(self, src_agid_name: str) -> str:
        pass