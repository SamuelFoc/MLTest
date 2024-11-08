from MLTest.interfaces.Typing import DF
from MLTest.core.Logger import LoggerSingleton
from abc import ABC, abstractmethod
from typing import List

"""
Component Interface Definitions and Guidelines for Extending Components

This file provides base classes for creating various types of components 
in the pipeline. These base classes include centralized logging functionality 
and enforce a common structure for implementing specific types of components, 
such as Import, Flow, Aggregator, and Export components.

### Creating a New Component

1. **Choose the Appropriate Base Class**:
    - Decide which base class (`ImportComponent`, `FlowComponent`, `AggregatorComponent`, 
    `ExportComponent`, or `MultiExportComponent`) fits the functionality of your new component.
    - If none fit, consider extending `Component` directly.

2. **Implement the `use` Method**:
    - All components must implement the `use` method, which defines the component's primary function.

3. **Handle the `__init__` Method**:
    - If your component requires additional parameters or initialization, override the `__init__` method.
    - Always call `super().__init__(...)` in the overridden `__init__` method to ensure the parent class 
      is properly initialized.
    - Pass the `log` parameter explicitly to the parent class's `__init__` if logging functionality is needed.

4. **Logging in Components**:
    - Enable logging by passing `log=True` when creating the component instance.
    - Use the `self.log(message, level)` method to log messages within your component.
    - Ensure the `log` parameter is included in the `__init__` method if it’s overridden.

### Example: Creating a New Component

**Case 1: Without Custom Initialization**:
If no additional initialization is needed, you don’t need to override the `__init__` method:

class MyFlowComponent(FlowComponent):
    def use(self, data: DF) -> DF:
        self.log("Processing data in MyFlowComponent.", level="INFO")
        # Your processing logic here
        return data

**Case 2: With Custom Initialization**:
If the component requires additional parameters, override the `__init__` method and call `super().__init__`:

class MyImportComponent(ImportComponent):
    def __init__(self, src: str, log: bool = False):
        super().__init__(src, log)  # Ensure parent class is properly initialized
        self.custom_param = "Example"

    def use(self) -> DF:
        self.log(f"Loading data from {self.src} using MyImportComponent.", level="INFO")
        # Your data loading logic here
        return polars.read_csv(self.src)

By following these guidelines, you can create modular, reusable components that seamlessly integrate 
into the pipeline while leveraging centralized logging functionality.
"""

class Component(ABC):
    """
    Basic component interface with integrated logging functionality.
    All components must implement a `use` method.
    """
    def __init__(self, log: bool = False):
        """
        Initializes the component with optional logging.

        Parameters:
        - log (bool): Whether to enable logging for the component.
        """
        self.log_enabled = log
        self.logger = self._get_logger()

    def _get_logger(self):
        """
        Retrieves the centralized singleton logger instance for the component.

        Returns:
        - LoggerSingleton: The singleton logger instance.
        """
        return LoggerSingleton().logger

    def log(self, message: str, level: str = "INFO"):
        """
        Logs a message if logging is enabled.

        Parameters:
        - message (str): The message to log.
        - level (str): The logging level ('INFO', 'DEBUG', 'WARNING', 'ERROR', 'CRITICAL').
        """
        if self.log_enabled:
            log_method = getattr(self.logger, level.lower(), self.logger.info)
            log_method(f"[{self.__class__.__name__}] {message}")

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
    def __init__(self, src: str, log: bool = False):
        """
        Initialize the ImportComponent with a src destination.
        
        Args:
            src (str): The destination where the imported data are stored.
        """
        super().__init__(log)
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
    def __init__(self, log: bool = False):
        super().__init__(log)

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


class AggregatorComponent(Component):
    """
    Interface for aggregator components that accept a list of results (DFs) and return a Polars DataFrame.
    """
    def __init__(self, log: bool = False):
        super().__init__(log)

    @abstractmethod
    def use(self, results: List[DF]) -> DF:
        """
        Aggregate a list of results and return a single Polars DataFrame.

        Args:
            results (List[DF]): A list of results in DataFrames to be aggregated into a single DataFrame.
        
        Returns:
            DF: A Polars DataFrame representing the aggregated data.
        """
        pass


class ExportComponent(Component):
    """
    Export component that accepts a Polars DataFrame and can export it to any specified format.
    Requires a `export_to` parameter during initialization.
    """
    def __init__(self, export_to: str, log: bool = False):
        """
        Initialize the ExportComponent with an export_to parameter.
        
        Args:
            export_to (str): The source or format to which the data will be exported.
        """
        super().__init__(log)
        self.export_to = export_to

    @abstractmethod
    def use(self, data: DF) -> None:
        """
        Export the provided Polars DataFrame to the desired destination or format.
        
        Args:
            data (DF): The Polars DataFrame to be exported.
        """
        pass


class MultiExportComponent(Component):
    """
    Export component that accepts a Polars DataFrame and can export it to any specified format.
    Requires a `export_to` parameter during initialization.
    """
    def __init__(self, export_to: str, log: bool = False):
        """
        Initialize the ExportComponent with an export_to parameter.
        
        Args:
            export_to (str): The source or format to which the data will be exported.
        """
        super().__init__(log)
        self.export_to = export_to

    @abstractmethod
    def use(self, data: List[DF]) -> None:
        """
        Export the provided Polars DataFrame to the desired destination or format.
        
        Args:
            data (List[DF]): A List of the Polars DataFrames to be exported.
        """
        pass



        

