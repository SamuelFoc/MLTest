import importlib.util
import os


class Sequence:
    def __init__(self, pipelines):
        """
        Initialize with a list of pipeline instances or classes.
        Each pipeline should have a `run` method.
        """
        self.pipelines = pipelines

    def run(self, data=None):
        """
        Run each pipeline in sequence, passing the output of one as the input to the next.
        :param data: Optional initial data to pass to the first pipeline.
        :return: Final output after all pipelines have been run.
        """
        for pipeline in self.pipelines:
            if data is not None:
                data = pipeline.run(data)
            else:
                data = pipeline.run()
        return data
    
class SequenceLoader:
    def __init__(self, folder_path="sequences"):
        self.folder_path = folder_path

    def load_sequence(self, filename):
        """Dynamically loads a pipeline from a file and creates a runnable class."""
        file_path = os.path.join(self.folder_path, filename + ".py")
        module_name = os.path.splitext(filename)[0]

        spec = importlib.util.spec_from_file_location(module_name, file_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        # Expect the module to have a 'PIPELINE' attribute defined as a FlowPipe instance
        if hasattr(module, "SEQUENCE"):
            sequence = module.SEQUENCE

            # Define a new class dynamically
            class DynamicSequence:
                @classmethod
                def run(cls, input=None):
                    return sequence.run(input)
            
            return DynamicSequence
        else:
            raise ValueError(f"{filename} does not contain a valid 'PIPELINE' definition.")