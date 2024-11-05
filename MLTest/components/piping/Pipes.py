from MLTest.interfaces.Pipes import Pipe
from typing import List, Any

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

    def run(self, input=None) -> Any:
        """
        Sequentially calls each component's `use` method, passing the output of one as the input to the next.

        Returns:
        - The output of the last component in the sequence.
        """
        result = None  # No initial input; let the first component produce the initial output
        for i, component in enumerate(self.components):
            if input:
                result = component.use(result) if i > 0 else component.use(input)
            else:
                result = component.use(result) if i > 0 else component.use()  # Only pass result starting from the second component
        return result