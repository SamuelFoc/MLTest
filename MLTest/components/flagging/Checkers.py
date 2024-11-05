from MLTest.interfaces.Components import FlowComponent
from MLTest.interfaces.Typing import DF
import polars as pl


class CheckDataTypes(FlowComponent):
    def __init__(self, expected_types: dict = None, raise_on_mismatch: bool = False):
        """
        Initializes the CheckDataTypes component.

        Parameters:
        - expected_types (dict): A dictionary mapping column names to expected Polars data types (e.g., pl.Int64, pl.Float64).
        - raise_on_mismatch (bool): If True, raises an error when a data type mismatch is detected.
        """
        self.expected_types = expected_types or {}
        self.raise_on_mismatch = raise_on_mismatch

    def use(self, dataframe: DF) -> DF:
        """
        Checks the data types of columns in the dataframe against expected types.

        Parameters:
        - dataframe (DF): The Polars DataFrame to check.

        Returns:
        - DF: The original DataFrame if no mismatches are found or raise_on_mismatch is False.
        
        Raises:
        - TypeError: If a mismatch is found and raise_on_mismatch is True.
        """
        mismatches = []

        for column, expected_type in self.expected_types.items():
            if column in dataframe.columns:
                actual_type = dataframe.schema[column]
                if actual_type != expected_type:
                    mismatches.append((column, actual_type, expected_type))

        # Handling mismatches based on configuration
        if mismatches:
            for column, actual, expected in mismatches:
                print(f"Data type mismatch in column '{column}': expected {expected}, got {actual}")
            
            if self.raise_on_mismatch:
                raise TypeError("Data type mismatches found. See printed output for details.")

        return dataframe