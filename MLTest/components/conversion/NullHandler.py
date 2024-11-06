from MLTest.interfaces.Components import FlowComponent
from MLTest.interfaces.Typing import DF
import polars as pl

class NullHandler(FlowComponent):
    def __init__(self, fill_values: dict[pl.DataType, any] = None, return_null_columns: bool = False):
        """
        Initializes the NullHandler component.
        
        Parameters:
        - fill_values (dict[pl.DataType, any]): A dictionary where keys are Polars data types
          (e.g., pl.Int64, pl.Float64, pl.Utf8), and values are the default values to replace nulls.
        - return_null_columns (bool): If True, returns the names of columns containing null values.
        """
        self.fill_values = fill_values or {}
        self.return_null_columns = return_null_columns

    def use(self, dataframe: DF) -> DF | list[str]:
        """
        Checks for null values in the specified columns and optionally replaces them.
        
        Parameters:
        - dataframe (DF): The Polars DataFrame to process.
        
        Returns:
        - DF or list[str]: The modified DataFrame with nulls replaced if return_null_columns is False;
                           otherwise, a list of column names with null values.
        """
        # Get columns with null values
        null_columns = [col for col in dataframe.columns if dataframe.select(pl.col(col).is_null().any()).to_numpy()[0][0]]
        
        if self.return_null_columns:
            return null_columns
        
        # Replace null values based on the specified fill_values
        transformations = [
            pl.col(column).fill_null(self.fill_values.get(dataframe.schema[column], None)).alias(column)
            for column in dataframe.columns
            if column in null_columns and dataframe.schema[column] in self.fill_values
        ]
        
        # Apply transformations to the DataFrame
        return dataframe.with_columns(transformations) if transformations else dataframe
