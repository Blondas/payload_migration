from abc import ABC, abstractmethod

class CollectionNameLookup(ABC):
    @abstractmethod
    def collection_name(self, tape_name: str) -> str:
        pass