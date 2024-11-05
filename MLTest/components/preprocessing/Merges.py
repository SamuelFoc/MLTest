from MLTest.interfaces.Components import FlowComponent
from MLTest.interfaces.Typing import DF, LIST
from typing import List
import polars as pl


class MergeStorage(FlowComponent):
    """
    A component that accepts an array of dataframes and merges them into a single dataframe.
    """
    def __init__(self, how: str = "concat", on = None):
        """
        Initializes the MergeStorage component.

        Parameters:
        - how (str): The merge method, either "concat" for vertical concatenation or
                     "join-inner", "join-outer", "join-left", or "join-right" for joins.
        - on (Optional[str]): The column name to join on (required if `how` is one of the join methods).
        """
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

    def use(self, dataframes: List[DF]) -> DF:
        """
        Merges the provided dataframes based on the specified method during initialization.

        Parameters:
        - dataframes (List[pl.DataFrame]): The list of dataframes to merge.

        Returns:
        - pl.DataFrame: The merged dataframe.
        """
        # Ensure that all elements in `dataframes` are of type `pl.DataFrame`
        if not all(isinstance(df, DF) for df in dataframes):
            raise TypeError("All items in `dataframes` must be Polars DataFrame instances.")

        if self.how == "concat":
            # Concatenate dataframes vertically (stack them)
            return pl.concat(dataframes)
        else:
            # Perform the join on the specified key
            result = dataframes[0]
            for df in dataframes[1:]:
                result = result.join(df, on=self.on, how=self.how)
            return result