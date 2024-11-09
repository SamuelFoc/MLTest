from MLTest.core.Pipelines import LoadingPipe, FlowThroughPipe, ExportPipe
from typing import List, Any


class Sequence:
    def __init__(self, name: str, pipelines: List[Any], args: List[dict], log: bool = False):
        """
        Initializes the Sequence.

        Parameters:
        - name: Name of the sequence.
        - pipelines: List of pipeline classes.
        - args: List of dictionaries containing arguments for each pipeline.
        - log: If True, enable logging for all pipelines (default: False).
        """
        if len(pipelines) != len(args):
            raise ValueError(
                "The number of pipelines must match the number of argument dictionaries."
            )

        self.name = name
        self.pipelines = [
            self._instantiate_pipeline(pipeline_class, pipeline_args, log)
            for pipeline_class, pipeline_args in zip(pipelines, args)
        ]

    def _instantiate_pipeline(self, pipeline_class: Any, pipeline_args: dict, log: bool):
        """
        Instantiate a pipeline with logging settings applied.

        Parameters:
        - pipeline_class: The pipeline class to instantiate.
        - pipeline_args: Arguments to pass to the pipeline class.
        - log: Global logging setting.

        Returns:
        - An instantiated pipeline object.
        """
        # Add 'log' to arguments if it's not already specified
        if "log" not in pipeline_args:
            pipeline_args["log"] = log
        return pipeline_class(**pipeline_args)

    def run(self, data=None):
        """
        Execute the sequence of pipelines.

        Parameters:
        - data: Initial data for the sequence (if required by the first pipeline).

        Returns:
        - Final processed data or None, depending on the pipeline type.
        """
        current_data = data
        for pipeline in self.pipelines:
            # Determine pipeline type and run appropriately
            if isinstance(pipeline, FlowThroughPipe):
                if current_data is None:
                    raise ValueError("FlowThroughPipe requires input data, but none was provided.")
                current_data = pipeline.run(current_data)
            elif isinstance(pipeline, LoadingPipe):
                current_data = pipeline.run()  # No input required
            elif isinstance(pipeline, ExportPipe):
                if current_data is None:
                    raise ValueError("ExportPipe requires input data, but none was provided.")
                pipeline.run(current_data)  # No output expected
                current_data = None  # Reset current_data after export
            else:
                raise TypeError(f"Unknown pipeline type: {type(pipeline)}")

        return current_data