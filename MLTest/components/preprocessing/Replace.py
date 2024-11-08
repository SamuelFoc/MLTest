from MLTest.interfaces.Components import FlowComponent
from MLTest.interfaces.Typing import DF
import polars as pl


class ReplaceStringPattern(FlowComponent):
    def __init__(self, columns: list[str], pattern: str, replace: str, is_regex: bool = True, log: bool = False):
        """
        Initializes the RegexReplace component.
        
        Parameters:
        - columns (list[str]): List of column names to apply the replacement on.
        - pattern (str): The pattern to match. Can be a string or regex.
        - replace (str): The replacement string.
        - is_regex (bool): Whether the pattern is a regex pattern (True) or a literal string (False).
        """
        super().__init__(log)
        self.columns = columns
        self.pattern = pattern
        self.replace = replace
        self.is_regex = is_regex

    def use(self, data: DF) -> DF:
        """
        Applies a regex or string replacement on specified columns.
        
        Parameters:
        - data (DF): The Polars DataFrame to process.
        
        Returns:
        - DF: The modified DataFrame with replacements applied.
        """
        self.log(f"Starting replacement in columns {self.columns} using pattern '{self.pattern}' "
                 f"with replacement '{self.replace}' (is_regex={self.is_regex}).", level="INFO")

        transformations = []
        for column in self.columns:
            if self.is_regex:
                self.log(f"Applying regex replacement in column '{column}'.", level="INFO")
                transformations.append(
                    pl.col(column)
                    .str.replace_all(self.pattern, self.replace)
                    .alias(column)
                )
            else:
                self.log(f"Applying literal string replacement in column '{column}'.", level="INFO")
                transformations.append(
                    pl.col(column)
                    .str.replace(self.pattern, self.replace, literal=True)
                    .alias(column)
                )

        try:
            data = data.with_columns(transformations)
            self.log("Replacement operation completed successfully.", level="INFO")
        except Exception as e:
            self.log(f"Failed to apply replacements: {e}", level="ERROR")
            raise

        return data
    

class BinaryReplace(FlowComponent):
    """
    A FlowComponent for replacing binary categorical column values based on predefined mappings.
    """
    def __init__(self, replacement: dict[str, dict[any, any]], log: bool = False):
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
        super().__init__(log)
        if not replacement:
            raise ValueError("Replacement rules cannot be empty.")
        self.replacement = replacement

    def use(self, data: DF) -> DF:
        """
        Replaces values in specified binary categorical columns based on the replacement rules.

        Parameters:
        - dataframe (DF): The Polars DataFrame to process.

        Returns:
        - DF: The modified DataFrame with replaced values.
        """
        self.log("Starting binary replacement operation.", level="INFO")
        transformations = []

        for column, mapping in self.replacement.items():
            if column in data.columns:
                self.log(f"Applying replacements in column '{column}' with mapping: {mapping}.", level="INFO")
                try:
                    # Create a transformation using `when-then-otherwise` for replacements
                    transformation = pl.col(column)
                    for old_value, new_value in mapping.items():
                        transformation = transformation.when(pl.lit(old_value)).then(new_value)
                    transformation = transformation.otherwise(pl.col(column)).alias(column)
                    transformations.append(transformation)
                except Exception as e:
                    self.log(f"Failed to apply replacements in column '{column}': {e}", level="ERROR")
                    raise
            else:
                self.log(f"Column '{column}' not found in DataFrame. Skipping replacement for this column.", level="WARNING")

        if transformations:
            try:
                data = data.with_columns(transformations)
                self.log("Binary replacement operation completed successfully.", level="INFO")
            except Exception as e:
                self.log(f"Failed to apply transformations: {e}", level="ERROR")
                raise
        else:
            self.log("No valid transformations were applied. Returning original DataFrame.", level="WARNING")

        return data
