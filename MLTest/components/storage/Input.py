from MLTest.interfaces.Components import Component, AggregatorComponent, ImportComponent
from MLTest.interfaces.Typing import DF
from typing import List


class StoreInputs(Component):
    """
    Component that accepts a list of Components and returns a list of results
    after applying each component's `use` method.
    """
    def __init__(self, components: List[ImportComponent]):
        """
        Initialize with a list of components to store.

        Args:
            components (List[ImportComponent]): A list of import components to be stored and executed.
        """
        self.components = components
        self.storage = []

    def use(self) -> List[DF]:
        """
        Execute the `use` method on each stored component and collect the results.

        Returns:
            List: A list of Dataframes from each component's `use` method.
        """
        for component in self.components:
            result = component.use()
            self.storage.append(result)
        return self.storage
    

class StoreAndAggregateInputs(Component):
    """
    Component that accepts a list of Components and aggregator component.
    It applies each component's `use` method, passes the collected results
    to the aggregator, and returns a DataFrame.
    """
    def __init__(self, components: List[Component], aggregator: AggregatorComponent):
        """
        Initialize with a list of components to store and aggregator component.

        Args:
            components (List[Component]): A list of components to be executed.
            aggregator (Component): A component that accepts a list of results
                                    and returns a DataFrame.
        """
        self.components = components
        self.aggregator = aggregator
        self.storage = None

    def use(self) -> DF:
        """
        Execute the `use` method on each stored component, collect the results,
        and pass the results to the aggregator's `use` method and return the 
        resulting DataFrame.

        Returns:
            DF: DataFrame
        """
        results = [component.use() for component in self.components]
        self.storage = results

        return self.aggregator.use(results)