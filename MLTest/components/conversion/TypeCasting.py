from MLTest.interfaces.Components import FlowComponent
from MLTest.interfaces.Typing import DF
import polars as pl

class TypeCasting(FlowComponent):
    def __init__(self, columns_and_types: dict[str, pl.DataType]):
        """
        Initializes the TypeCasting component.
        
        Parameters:
        - columns_and_types (dict[str, pl.DataType]): A dictionary where keys are column names,
          and values are the target Polars data types (e.g., pl.Utf8, pl.Float64, pl.Int64).
        """
        self.columns_and_types = columns_and_types

    def use(self, dataframe: DF) -> DF:
        """
        Casts specified columns to their respective data types.
        
        Parameters:
        - dataframe (DF): The Polars DataFrame to process.
        
        Returns:
        - DF: The modified DataFrame with columns cast to specified types.
        """
        transformations = [
            pl.col(column).cast(dtype).alias(column)
            for column, dtype in self.columns_and_types.items()
        ]
        
        # Apply transformations to the DataFrame
        dataframe = dataframe.with_columns(transformations)
        
        return dataframe
