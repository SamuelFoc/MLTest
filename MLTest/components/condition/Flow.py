from MLTest.interfaces.Components import FlowComponent, Component
from MLTest.interfaces.Typing import DF
from typing import Callable


class UseConditionalFlow(FlowComponent):
    """
    A component that evaluates a condition and chooses a path based on the result.
    Holds references to components for both true and false outcomes.
    """
    def __init__(self, condition: Callable[[DF], bool], true_component: FlowComponent, false_component: FlowComponent):
        """
        Initialize the ConditionalComponent with a condition and two components for conditional execution.

        Args:
            condition (Callable[[DF], bool]): A lambda function that takes a Polars DataFrame and returns a boolean.
            true_component (Component): The component to execute if the condition is True.
            false_component (Component): The component to execute if the condition is False.
        """
        self.condition = condition
        self.true_component = true_component
        self.false_component = false_component

    def use(self, data: DF) -> DF:
        """
        Evaluate the condition and run the appropriate component based on the result.

        Args:
            data (DF): The input Polars DataFrame to be passed to the selected component.
        
        Returns:
            DF: The result of the true_component or false_component based on the condition.
        """
        if self.condition(data):
            return self.true_component.use(data)
        else:
            return self.false_component.use(data)