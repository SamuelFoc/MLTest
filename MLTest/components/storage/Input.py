from MLTest.interfaces.Components import Component, AggregatorComponent, ImportComponent
from MLTest.interfaces.Typing import DF
from typing import List


class StoreInputs(Component):
    """
    Component that accepts a list of Components and returns a list of results
    after applying each component's `use` method.
    """
    def __init__(self, components: List[ImportComponent], log: bool = False):
        """
        Initialize with a list of components to store.

        Args:
            components (List[ImportComponent]): A list of import components to be stored and executed.
        """
        super().__init__(log)
        self.components = components
        self.storage = []

    def use(self) -> List[DF]:
        """
        Execute the `use` method on each stored component and collect the results.

        Returns:
            List: A list of DataFrames from each component's `use` method.
        """
        self.log("Starting StoreInputs execution.", level="INFO")
        for i, component in enumerate(self.components):
            self.log(f"Executing component {i+1}/{len(self.components)}: {component.__class__.__name__}.", level="INFO")
            try:
                result = component.use()
                self.storage.append(result)
                self.log(f"Component {i+1}/{len(self.components)} executed successfully.", level="INFO")
            except Exception as e:
                self.log(f"Component {i+1}/{len(self.components)} failed with error: {e}.", level="ERROR")
                raise
        self.log("StoreInputs execution completed. Results stored.", level="INFO")
        return self.storage
    

class StoreAndAggregateInputs(Component):
    """
    Component that accepts a list of Components and aggregator component.
    It applies each component's `use` method, passes the collected results
    to the aggregator, and returns a DataFrame.
    """
    def __init__(self, components: List[Component], aggregator: AggregatorComponent, log: bool = False):
        """
        Initialize with a list of components to store and aggregator component.

        Args:
            components (List[Component]): A list of components to be executed.
            aggregator (Component): A component that accepts a list of results
                                    and returns a DataFrame.
        """
        super().__init__(log)
        self.components = components
        self.aggregator = aggregator
        self.storage = None

    def use(self) -> DF:
        """
        Execute the `use` method on each stored component, collect the results,
        and pass the results to the aggregator's `use` method and return the 
        resulting DataFrame.

        Returns:
            DF: Aggregated DataFrame.
        """
        self.log("Starting StoreAndAggregateInputs execution.", level="INFO")
        results = []
        for i, component in enumerate(self.components):
            self.log(f"Executing component {i+1}/{len(self.components)}: {component.__class__.__name__}.", level="INFO")
            try:
                result = component.use()
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

        self.log("StoreAndAggregateInputs execution completed.", level="INFO")
        return aggregated_result