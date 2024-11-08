from MLTest.interfaces.Components import FlowComponent, AggregatorComponent
from MLTest.interfaces.Typing import DF
from typing import List

 
class UseFloatingStorage(FlowComponent):
    """
    Component that accepts a list of FlowComponents and an aggregator component.
    It applies each component's `use` method, collects the results, and passes 
    them to the aggregator component to produce a combined DataFrame.
    """
    def __init__(self, components: List[FlowComponent], aggregator: AggregatorComponent, log: bool = False):
        """
        Initialize the UseFloatingStorage component with a list of components 
        and an aggregator.

        Args:
            components (List[FlowComponent]): A list of FlowComponents to be executed.
            aggregator (AggregatorComponent): A component that accepts a list of 
                                              results and returns a combined DataFrame.
        """ 
        super().__init__(log)
        self.components = components
        self.aggregator = aggregator
        self.storage = []

    def use(self, data: DF) -> DF:
        """
        Execute the `use` method on each component in `components`, store the results,
        and pass the collected results to the aggregator's `use` method. Returns 
        the final aggregated DataFrame.

        Args:
            data (DF): Input DataFrame passed to each component in the list.

        Returns:
            DF: Aggregated DataFrame from the results of all components.
        """
        self.log("Starting UseFloatingStorage execution.", level="INFO")
        results = []

        for i, component in enumerate(self.components):
            self.log(f"Executing component {i+1}/{len(self.components)}: {component.__class__.__name__}.", level="INFO")
            try:
                result = component.use(data)
                results.append(result)
                self.log(f"Component {i+1}/{len(self.components)} executed successfully.", level="INFO")
            except Exception as e:
                self.log(f"Component {i+1}/{len(self.components)} failed with error: {e}.", level="ERROR")
                raise

        self.storage = results
        self.log("All components executed. Passing results to the aggregator.", level="INFO")

        try:
            aggregated_result = self.aggregator.use(results)
            self.log("Aggregator executed successfully.", level="INFO")
        except Exception as e:
            self.log(f"Aggregator failed with error: {e}.", level="ERROR")
            raise

        self.log("UseFloatingStorage execution completed.", level="INFO")
        return aggregated_result
