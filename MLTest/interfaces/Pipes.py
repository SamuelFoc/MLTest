from abc import ABC, abstractmethod
from MLTest.interfaces.Components import Component
from MLTest.interfaces.Typing import DF
from typing import List


class Pipeline(ABC):
    """
    Base interface for all pipelines.
    """
    def __init__(self, components: List[Component]):
        """
        Initializes the pipeline with a list of components.

        Parameters:
        - components (List[Component]): A list of components to execute in the pipeline.
        """
        self.components = components

    @abstractmethod
    def run(self, *args) -> DF:
        """
        Executes the pipeline.

        Parameters:
        - args: The input arguments for the pipeline.

        Returns:
        - DF: The result of the pipeline (if applicable).
        """
        pass