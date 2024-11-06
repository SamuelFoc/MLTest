from MLTest.interfaces.Pipes import Pipe
from MLTest.interfaces.Typing import DF
from typing import List, Any
import importlib.util
import os

class FlowPipe(Pipe):
    """
    A class that accepts an array of components and sequentially pipes the output
    of one component as the input to the next.
    """
    def __init__(self, components: List = []):
        # Ensure all elements in `components` have a `use` method
        if not all(hasattr(component, 'use') and callable(getattr(component, 'use')) for component in components):
            raise TypeError("All components must have a callable `use` method.")
        
        self.components = components

    def run(self, input: DF = None) -> Any:
        """
        Sequentially calls each component's `use` method, passing the output of one as the input to the next.

        Returns:
        - The output of the last component in the sequence.
        """
        result = input  # Start with `input` if provided; otherwise, let the first component initialize it
        for component in self.components:
            if result is None:
                # If `result` is None, assume the first component initializes the pipeline
                result = component.use()
            else:
                # For subsequent components, pass the output of the previous component
                result = component.use(result)
        return result
    

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
