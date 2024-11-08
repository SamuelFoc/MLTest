from MLTest.interfaces.Components import ImportComponent
from MLTest.interfaces.Typing import DF
import polars as pl


class LoadData(ImportComponent):
    """
    Component for loading data from a specified file path into a Polars DataFrame.
    Supports CSV, Parquet (pq), and JSON file formats.
    """
    def use(self) -> DF:
        """
        Reads data from the specified file and returns it as a Polars DataFrame.

        Returns:
            DF: The loaded data as a Polars DataFrame.

        Raises:
            ValueError: If the file type is unsupported.
        """
        self.log(message="Loading data..")
        # Infer the file type from the file extension
        file_type = self.src.split('.')[-1].lower()

        # Load data based on file type
        if file_type == 'csv':
            return pl.read_csv(self.src)
        elif file_type == 'pq':
            return pl.read_parquet(self.src)
        elif file_type == 'json':
            return pl.read_json(self.src)
        else:
            raise ValueError(f"Unsupported file type '{file_type}'. Supported types: csv, pq, json.")
