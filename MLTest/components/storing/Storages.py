from MLTest.interfaces.Storage import Storage
from typing import List

class A_Storage(Storage):
    """
    A class that accepts an array of components and applies the `use` method
    on each component, storing the results in an internal list.
    """
    def __init__(self, components: List):
        # Ensure all elements in `components` have a `use` method
        if not all(hasattr(component, 'use') and callable(getattr(component, 'use')) for component in components):
            raise TypeError("All components must have a callable `use` method.")
        
        self.components = components
        self.results = []

    def use(self):
        """
        Calls the `use` method on each component and stores the results in an array.
        """
        self.results = [component.use() for component in self.components]
        return self.results