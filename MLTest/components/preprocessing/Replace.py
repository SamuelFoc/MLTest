from MLTest.interfaces.Components import FlowComponent
from MLTest.interfaces.Typing import DF
import polars as pl


class ReplaceStringPattern(FlowComponent):
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

    def use(self, data: DF) -> DF:
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
        data = data.with_columns(transformations)
        
        return data
    

class BinaryReplace(FlowComponent):
    """
    A FlowComponent for replacing binary categorical column values based on predefined mappings.
    """
    def __init__(self, replacement: dict[str, dict[any, any]]):
        """
        Initializes the BinaryReplace component with the specified replacement rules.

        Parameters:
        - replacement (dict[str, dict[any, any]]): A dictionary where keys are column names,
          and values are dictionaries mapping current values to replacement values.
        Example:
            {
                "LikeCinema": {"Yes": 1, "No": 0},
                "LikesTea": {"Y": "Yes", "N": "No"}
            }
        """
        if not replacement:
            raise ValueError("Replacement rules cannot be empty.")
        self.replacement = replacement

    def use(self, dataframe: DF) -> DF:
        """
        Replaces values in specified binary categorical columns based on the replacement rules.

        Parameters:
        - dataframe (DF): The Polars DataFrame to process.

        Returns:
        - DF: The modified DataFrame with replaced values.
        """
        transformations = []

        # Apply replacement rules
        for column, mapping in self.replacement.items():
            if column in dataframe.columns:
                # Create a transformation using `when-then-otherwise` for replacements
                transformation = pl.col(column)
                for old_value, new_value in mapping.items():
                    transformation = transformation.when(pl.lit(old_value)).then(new_value)
                transformation = transformation.otherwise(pl.col(column)).alias(column)
                transformations.append(transformation)

        # Apply transformations to the DataFrame
        dataframe = dataframe.with_columns(transformations) if transformations else dataframe

        return dataframe
