from MLTest.interfaces.Components import FlowComponent
from MLTest.interfaces.Typing import DF
import polars as pl

class NullFiller(FlowComponent):
    def __init__(self, default_fill: dict[pl.DataType, any] = None, column_specific_fill: dict[frozenset, any] = None):
        """
        Initializes the NullFiller component.
        
        Parameters:
        - default_fill (dict[pl.DataType, any]): A dictionary where keys are Polars data types
          (e.g., pl.Int64, pl.Float64, pl.Utf8) and values are the default values to replace nulls for each type.
        - column_specific_fill (dict[frozenset, any]): A dictionary where each key is a frozenset of column names
          and the value is the fill value for those specific columns.
        """
        self.default_fill = default_fill or {}
        self.column_specific_fill = column_specific_fill or {}

    def use(self, dataframe: DF) -> DF:
        """
        Fills null values in specified columns based on default fills or specific column fill values.
        
        Parameters:
        - dataframe (DF): The Polars DataFrame to process.
        
        Returns:
        - DF: The modified DataFrame with nulls filled according to specified rules.
        """
        # Determine the default fill value for each column based on its data type
        transformations = []
        
        # Apply column-specific fills
        for columns, fill_value in self.column_specific_fill.items():
            for column in columns:
                if column in dataframe.columns:
                    transformations.append(
                        pl.col(column).fill_null(fill_value).alias(column)
                    )
        
        # Apply default type-based fills for remaining columns with nulls
        for column in dataframe.columns:
            if column not in [col for cols in self.column_specific_fill for col in cols]:  # Skip columns already handled
                dtype = dataframe.schema[column]
                if dtype in self.default_fill:
                    transformations.append(
                        pl.col(column).fill_null(self.default_fill[dtype]).alias(column)
                    )

        # Apply transformations to the DataFrame
        dataframe = dataframe.with_columns(transformations) if transformations else dataframe
        
        return dataframe
