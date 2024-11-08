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
        results = [component.use(data) for component in self.components]
        self.storage = results

        return self.aggregator.use(results)
