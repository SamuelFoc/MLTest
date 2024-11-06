from MLTest.interfaces.Components import FlowComponent
from MLTest.interfaces.Typing import DF
import polars as pl

class BinaryReplace(FlowComponent):
    def __init__(self, columns: list[str], option1: str, replace1: int, option2: str, replace2: int):
        """
        Initializes the BinaryReplace component.
        
        Parameters:
        - columns (list[str]): List of column names to apply the binary replacement.
        - option1 (str): The first possible option in the column (e.g., "Yes").
        - replace1 (int): The replacement value for the first option (e.g., 1).
        - option2 (str): The second possible option in the column (e.g., "No").
        - replace2 (int): The replacement value for the second option (e.g., 0).
        """
        self.columns = columns
        self.option1 = option1
        self.replace1 = replace1
        self.option2 = option2
        self.replace2 = replace2

    def use(self, dataframe: DF) -> DF:
        """
        Replaces two possible options in specified columns with given replacement values.
        
        Parameters:
        - dataframe (DF): The Polars DataFrame to process.
        
        Returns:
        - DF: The modified DataFrame with binary replacements applied.
        """
        transformations = []
        for column in self.columns:
            transformations.append(
                pl.when(pl.col(column) == self.option1)
                .then(self.replace1)
                .when(pl.col(column) == self.option2)
                .then(self.replace2)
                .otherwise(None)  # Optional: Handle unexpected values as None
                .alias(column)
            )
        
        # Apply transformations to the DataFrame
        dataframe = dataframe.with_columns(transformations)
        
        return dataframe
