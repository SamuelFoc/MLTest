from MLTest.interfaces.Components import FlowComponent
from MLTest.interfaces.Typing import DF
import polars as pl
import re

class RegexReplace(FlowComponent):
    def __init__(self, columns: list[str], pattern: str, replace: str, is_regex: bool = True):
        """
        Initializes the RegexReplace component.
        
        Parameters:
        - columns (list[str]): List of column names to apply the replacement on.
        - pattern (str): The pattern to match. Can be a string or regex.
        - replace (str): The replacement string.
        - is_regex (bool): Whether the pattern is a regex pattern (True) or a literal string (False).
        """
        self.columns = columns
        self.pattern = pattern
        self.replace = replace
        self.is_regex = is_regex

    def use(self, dataframe: DF) -> DF:
        """
        Applies a regex or string replacement on specified columns.
        
        Parameters:
        - dataframe (DF): The Polars DataFrame to process.
        
        Returns:
        - DF: The modified DataFrame with replacements applied.
        """
        # Determine replacement method based on is_regex flag
        transformations = []
        for column in self.columns:
            if self.is_regex:
                transformations.append(
                    pl.col(column)
                    .str.replace_all(self.pattern, self.replace)
                    .alias(column)
                )
            else:
                # If not regex, use literal string replacement
                transformations.append(
                    pl.col(column)
                    .str.replace(self.pattern, self.replace, literal=True)
                    .alias(column)
                )

        # Apply transformations to the DataFrame
        dataframe = dataframe.with_columns(transformations)
        
        return dataframe