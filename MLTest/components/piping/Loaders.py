import importlib.util
import os

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
                def run(cls):
                    return pipeline.run()
            
            return DynamicPipeline
        else:
            raise ValueError(f"{filename} does not contain a valid 'PIPELINE' definition.")
