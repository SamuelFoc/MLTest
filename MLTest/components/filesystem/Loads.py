from MLTest.interfaces.Components import DataImportComponent
from MLTest.interfaces.Typing import DF
import polars as pl


class ReadData(DataImportComponent):
    def __init__(self, src: str):
        self.src = src

    def use(self) -> DF:
        """
        Reads data from a file and returns a Polars DataFrame.

        Parameters:
        - src (str): The path to the data file.
        - file_type (str): The type of the file ('csv', 'parquet', 'json', or 'auto' to infer). Default is 'auto'.

        Returns:
        - pl.DataFrame: The loaded data as a Polars DataFrame.
        """
        # If file_type is not specified, try to infer from the file extension
        file_type = self.src.split('.')[-1].lower()

        # Read data based on the file type
        if file_type == 'csv':
            return pl.read_csv(self.src)
        elif file_type == 'parquet':
            return pl.read_parquet(self.src)
        elif file_type == 'json':
            return pl.read_json(self.src)
        else:
            raise ValueError(f"Unsupported file type '{file_type}'. Supported types: csv, parquet, json.")
        