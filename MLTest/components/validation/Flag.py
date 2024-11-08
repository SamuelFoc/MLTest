from MLTest.interfaces.Components import FlowComponent
from MLTest.interfaces.Typing import DF
from typing import Callable


class ValidateOrFlag(FlowComponent):
    """
    A FlowComponent that passes data unchanged but checks for specific conditions.
    If a condition is met, it logs a message or raises an exception.
    """
    def __init__(self, condition: Callable[[DF], bool], message: str, raise_exception: bool = False, log: bool = True):
        """
        Initializes the ValidationComponent.

        Parameters:
        - condition (Callable[[DF], bool]): A lambda function to evaluate the condition.
        - message (str): A message to log or include in the exception.
        - raise_exception (bool): Whether to raise an exception if the condition is met.
        - log (bool): Whether to log the message if the condition is met.
        """
        super().__init__(log)
        self.condition = condition
        self.message = message
        self.raise_exception = raise_exception

    def use(self, data: DF) -> DF:
        """
        Passes the DataFrame unchanged and checks the condition.

        Parameters:
        - data (DF): The Polars DataFrame to validate.

        Returns:
        - DF: The same Polars DataFrame, unchanged.

        Raises:
        - ValueError: If the condition is met and raise_exception is True.
        """
        self.log("Starting validation check.", level="INFO")
        
        if self.condition(data):
            self.log(f"Condition met: {self.message}", level="WARNING")
            if self.raise_exception:
                self.log("Raising an exception due to condition being met.", level="ERROR")
                raise ValueError(self.message)
        else:
            self.log("Condition not met. Validation passed.", level="INFO")
        
        return data