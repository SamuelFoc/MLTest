from MLTest.interfaces.Components import FlowComponent
from MLTest.interfaces.Typing import DF
import polars as pl


class CastTypes(FlowComponent):
    def __init__(self, columns_and_types: dict[str, pl.DataType], log: bool = False):
        """
        Initializes the TypeCasting component.
        
        Parameters:
        - columns_and_types (dict[str, pl.DataType]): A dictionary where keys are column names,
          and values are the target Polars data types (e.g., pl.Utf8, pl.Float64, pl.Int64).
        """
        super().__init__(log)
        self.columns_and_types = columns_and_types

    def use(self, data: DF) -> DF:
        """
        Casts specified columns to their respective data types.
        
        Parameters:
        - data (DF): The Polars DataFrame to process.
        
        Returns:
        - DF: The modified DataFrame with columns cast to specified types.
        """
        transformations = [
            pl.col(column).cast(dtype).alias(column)
            for column, dtype in self.columns_and_types.items()
        ]
        
        # Apply transformations to the DataFrame
        data = data.with_columns(transformations)
        
        return data
    

class HandleNullValues(FlowComponent):
    def __init__(self, fill_values: dict[pl.DataType, any] = None, return_null_columns: bool = False, log: bool = False):
        """
        Initializes the NullHandler component.
        
        Parameters:
        - fill_values (dict[pl.DataType, any]): A dictionary where keys are Polars data types
          (e.g., pl.Int64, pl.Float64, pl.Utf8), and values are the default values to replace nulls.
        - return_null_columns (bool): If True, returns the names of columns containing null values.
        """
        super().__init__(log)
        self.fill_values = fill_values or {}
        self.return_null_columns = return_null_columns

    def use(self, data: DF) -> DF:
        """
        Checks for null values in the specified columns and optionally replaces them.
        Always returns a Polars DataFrame (DF).

        Parameters:
        - data (DF): The Polars DataFrame to process.

        Returns:
        - DF: The modified DataFrame with nulls replaced if return_null_columns is False;
            otherwise, a DataFrame containing column names with null values.
        """
        # Get columns with null values
        null_columns = [col for col in data.columns if data.select(pl.col(col).is_null().any()).to_numpy()[0][0]]
        
        if self.return_null_columns:
            # Return a DataFrame with the column names that have null values
            return pl.DataFrame({"null_columns": null_columns})
        
        # Replace null values based on the specified fill_values
        transformations = [
            pl.col(column).fill_null(self.fill_values.get(data.schema[column], None)).alias(column)
            for column in data.columns
            if column in null_columns and data.schema[column] in self.fill_values
        ]
        
        # Apply transformations to the DataFrame
        return data.with_columns(transformations) if transformations else data
    

class HandleIndividualNullColumns(FlowComponent):
    """
    A FlowComponent for filling null values in specific columns based on user-defined rules.
    """
    def __init__(self, column_specific_fill: dict[frozenset, any], log: bool = False):
        """
        Initializes the NullFiller with specific column fill values.

        Parameters:
        - column_specific_fill (dict[frozenset, any]): A dictionary where each key is a frozenset
          of column names, and the value is the fill value for those specific columns.
        """
        super().__init__(log)
        if not column_specific_fill:
            raise ValueError("column_specific_fill cannot be empty.")
        self.column_specific_fill = column_specific_fill

    def use(self, dataframe: DF) -> DF:
        """
        Fills null values in the specified columns based on column-specific rules.

        Parameters:
        - dataframe (DF): The Polars DataFrame to process.

        Returns:
        - DF: The modified DataFrame with nulls filled in specified columns.
        """
        transformations = []

        # Apply column-specific fills
        for columns, fill_value in self.column_specific_fill.items():
            for column in columns:
                if column in dataframe.columns:
                    transformations.append(
                        pl.col(column).fill_null(fill_value).alias(column)
                    )

        # Apply transformations to the DataFrame
        dataframe = dataframe.with_columns(transformations) if transformations else dataframe

        return dataframe