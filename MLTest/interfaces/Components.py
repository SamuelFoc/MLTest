from abc import ABC, abstractmethod
from MLTest.interfaces.Typing import DF

class Component(ABC):
    """
    Basic component interface. All components must implement a `use` method.
    """
    @abstractmethod
    def use(self):
        """
        Execute the component's primary function.
        The exact input and output types will depend on the specific component type.
        """
        pass


class ImportComponent(Component):
    """
    Import component that accepts any input type but must return a Polars DataFrame.
    Requires a `save_to` parameter during initialization.
    """
    @abstractmethod
    def __init__(self, src: str):
        """
        Initialize the ImportComponent with a src destination.
        
        Args:
            src (str): The destination where the imported data are stored.
        """
        self.src = src

    @abstractmethod
    def use(self) -> DF:
        """
        Import data from the specified source and return it as a Polars DataFrame.
        
        Args:
            src: The source of the data, which can be any type depending on the implementation.
        
        Returns:
            DF: A Polars DataFrame.
        """
        pass


class FlowComponent(Component):
    """
    Flow component that accepts a Polars DataFrame as input and returns a processed Polars DataFrame.
    """
    @abstractmethod
    def use(self, data: DF) -> DF:
        """
        Process the provided Polars DataFrame and return the modified Polars DataFrame.
        
        Args:
            data (DF): The Polars DataFrame to be processed.
        
        Returns:
            DF: The processed Polars DataFrame.
        """
        pass
    

class ExportComponent(Component):
    """
    Export component that accepts a Polars DataFrame and can export it to any specified format.
    Requires a `export_to` parameter during initialization.
    """
    @abstractmethod
    def __init__(self, export_to: str):
        """
        Initialize the ExportComponent with an export_to parameter.
        
        Args:
            export_to (str): The source or format to which the data will be exported.
        """
        self.export_to = export_to

    @abstractmethod
    def use(self, data: DF) -> None:
        """
        Export the provided Polars DataFrame to the desired destination or format.
        
        Args:
            data (DF): The Polars DataFrame to be exported.
        """
        pass