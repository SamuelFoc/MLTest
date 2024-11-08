from MLTest.interfaces.Components import AggregatorComponent
from MLTest.interfaces.Typing import DF
from typing import List
import polars as pl


class MergeStorage(AggregatorComponent):
    """
    A component that accepts an array of dataframes and merges them into a single dataframe.
    """
    def __init__(self, how: str = "concat", on = None, log:bool = False):
        """
        Initializes the MergeStorage component.

        Parameters:
        - how (str): The merge method, either "concat" for vertical concatenation or
                     "join-inner", "join-outer", "join-left", or "join-right" for joins.
        - on (Optional[str]): The column name to join on (required if `how` is one of the join methods).
        """
        super().__init__(log)
        # Validate the `how` parameter
        if how == "concat":
            self.how = "concat"
            self.on = None  # `on` is not needed for concatenation
        elif how.startswith("join-"):
            join_type = how.split("-")[1]
            if join_type not in ["inner", "outer", "left", "right"]:
                raise ValueError(f"Unsupported join type '{join_type}'. Supported types: inner, outer, left, right.")
            if on is None:
                raise ValueError("A column name must be specified in `on` for joining.")
            self.how = join_type
            self.on = on
        else:
            raise ValueError(f"Unsupported merge method '{how}'. Use 'concat' or 'join-[type]' where [type] is inner, outer, left, or right.")

    def use(self, data: List[DF]) -> DF:
        """
        Merges the provided dataframes based on the specified method during initialization.

        Parameters:
        - data (List[pl.DataFrame]): The list of dataframes to merge.

        Returns:
        - pl.DataFrame: The merged dataframe.
        """
        self.log(f"Starting merge operation with method '{self.how}' and key '{self.on}' (if applicable).", level="INFO")
        
        # Ensure that all elements in `data` are of type `pl.DataFrame`
        if not all(isinstance(df, DF) for df in data):
            error_message = "All items in `data` must be Polars DataFrame instances."
            self.log(error_message, level="ERROR")
            raise TypeError(error_message)

        try:
            if self.how == "concat":
                # Concatenate dataframes vertically (stack them)
                self.log("Performing vertical concatenation of dataframes.", level="INFO")
                result = pl.concat(data)
                self.log("Concatenation completed successfully.", level="INFO")
            else:
                # Perform the join on the specified key
                self.log(f"Performing join operation with key '{self.on}' using '{self.how}' method.", level="INFO")
                result = data[0]
                for i, df in enumerate(data[1:], start=1):
                    self.log(f"Joining DataFrame {i} on column '{self.on}' using '{self.how}' method.", level="INFO")
                    result = result.join(df, on=self.on, how=self.how)
                self.log("Join operation completed successfully.", level="INFO")
            return result
        except Exception as e:
            self.log(f"Merge operation failed: {e}", level="ERROR")
            raise