from MLTest.interfaces.Components import FlowComponent
from MLTest.interfaces.Typing import DF
import polars as pl

class DataCondition(FlowComponent):
    def __init__(self, condition: pl.Expr, result_if_met: any = None, result_if_not_met: any = None):
        """
        Initializes the DataCondition component.
        
        Parameters:
        - condition (pl.Expr): A Polars expression that defines the condition to check on the data.
        - result_if_met (any): The value to return if the condition is met.
        - result_if_not_met (any): The value to return if the condition is not met.
        """
        self.condition = condition
        self.result_if_met = result_if_met
        self.result_if_not_met = result_if_not_met

    def use(self, dataframe: DF) -> any:
        """
        Evaluates the condition on the DataFrame and returns specified values based on the result.
        
        Parameters:
        - dataframe (DF): The Polars DataFrame to evaluate the condition on.
        
        Returns:
        - any: The value specified in `result_if_met` if the condition is met, otherwise `result_if_not_met`.
        """
        # Evaluate the condition on the DataFrame
        condition_met = dataframe.select(self.condition).to_numpy()[0][0]
        
        # Return based on condition evaluation
        return self.result_if_met if condition_met else self.result_if_not_met
