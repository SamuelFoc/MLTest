from abc import ABC, abstractmethod
from MLTest.interfaces.Typing import DF


class Component(ABC):
    def __init__(self, data):
        pass
    
    @abstractmethod
    def use(self) -> DF:
        pass

class FlowComponent(ABC):
    @abstractmethod
    def __init__(self, data: DF):
        pass
    
    @abstractmethod
    def use(self) -> DF:
        pass


class DataImportComponent(ABC):
    @abstractmethod
    def __init__(self, src: str):
        pass

    @abstractmethod
    def use(self) -> DF:
        pass

class DataExportComponent(ABC):
    @abstractmethod
    def __init__(self, save_to: str):
        pass

    @abstractmethod
    def use(self) -> None:
        pass