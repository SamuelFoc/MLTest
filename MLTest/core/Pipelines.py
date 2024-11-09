from MLTest.interfaces.Pipelines import Pipeline
from MLTest.interfaces.Components import Component, FlowComponent, AggregatorComponent, ExportComponent
from MLTest.interfaces.Typing import DF
from typing import List
import importlib.util
import os


class FlowThroughPipe(Pipeline):
    """
    A pipeline that accepts a DataFrame, processes it through all components, and returns a DataFrame.
    Ensures the first component accepts a DataFrame and the last component returns a DataFrame.
    """
    def __init__(self, components: List[FlowComponent]):
        super().__init__(components)
        self._validate_components()

    def _validate_components(self):
        """
        Validates that the first and last components meet the input-output requirements.
        """
        if not isinstance(self.components[0], FlowComponent):
            raise TypeError("The first component of FlowThroughPipe must accept a DataFrame.")
        if not isinstance(self.components[-1], FlowComponent):
            raise TypeError("The last component of FlowThroughPipe must return a DataFrame.")

    def run(self, data: DF) -> DF:
        """
        Passes a DataFrame through all components in the pipeline.

        Parameters:
        - data (DF): The input DataFrame.

        Returns:
        - DF: The processed DataFrame.
        """
        for component in self.components:
            data = component.use(data)
        return data


class LoadingPipe(Pipeline):
    """
    A pipeline that does not accept any input.
    The first component must be of type input: None, output: DF.
    The last component must return a DataFrame.
    """
    def __init__(self, components: List[Component]):
        super().__init__(components)
        self._validate_components()

    def _validate_components(self):
        """
        Validates that the first component is an ImportComponent and the last component returns a DataFrame.
        """
        if not isinstance(self.components[0], Component):
            raise TypeError("The first component of LoadingPipe must be an instance of Component.")
        if not isinstance(self.components[-1], AggregatorComponent):
            raise TypeError("The last component of LoadingPipe must return a DataFrame.")

    def run(self) -> DF:
        """
        Executes the pipeline, starting with the first component.

        Returns:
        - DF: The processed DataFrame.
        """
        data = None
        for component in self.components:
            data = component.use(data) if data else component.use()
        return data


class ExportPipe(Pipeline):
    """
    A pipeline that accepts a DataFrame and does not return any output.
    The first component must accept a DataFrame, and the last component must output None.
    """
    def __init__(self, components: List[Component]):
        super().__init__(components)
        self._validate_components()

    def _validate_components(self):
        """
        Validates that the first component accepts a DataFrame and the last component is an ExportComponent.
        """
        if not isinstance(self.components[0], FlowComponent):
            raise TypeError("The first component of ExportPipe must be a FlowComponent.")
        if not isinstance(self.components[-1], ExportComponent):
            raise TypeError("The last component of ExportPipe must be an instance of ExportComponent.")


    def run(self, data: DF) -> None:
        """
        Passes a DataFrame through all components in the pipeline.

        Parameters:
        - data (DF): The input DataFrame.
        """
        for component in self.components:
            data = component.use(data)


class PipeLoader:
    def __init__(self, folder_path="pipes"):
        self.folder_path = folder_path

    def load_pipeline(self, filename):
        """Dynamically loads a pipeline from a file and creates a runnable class."""
        file_path = os.path.join(self.folder_path, filename + ".py")
        module_name = os.path.splitext(filename)[0]

        spec = importlib.util.spec_from_file_location(module_name, file_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        # Expect the module to have a 'PIPELINE' attribute defined as a FlowPipe instance
        if hasattr(module, "PIPELINE"):
            pipeline = module.PIPELINE

            # Define a new class dynamically
            class DynamicPipeline:
                @classmethod
                def run(cls, input=None):
                    return pipeline.run(input)
            
            return DynamicPipeline
        else:
            raise ValueError(f"{filename} does not contain a valid 'PIPELINE' definition.")
