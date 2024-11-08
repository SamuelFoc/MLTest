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
        self.log(f"Starting to load data from {self.src}.", level="INFO")

        # Infer the file type from the file extension
        file_type = self.src.split('.')[-1].lower()
        self.log(f"Inferred file type: {file_type}.", level="INFO")

        # Load data based on file type
        try:
            if file_type == 'csv':
                self.log("Loading data as CSV.", level="INFO")
                data = pl.read_csv(self.src)
            elif file_type == 'pq':
                self.log("Loading data as Parquet.", level="INFO")
                data = pl.read_parquet(self.src)
            elif file_type == 'json':
                self.log("Loading data as JSON.", level="INFO")
                data = pl.read_json(self.src)
            else:
                raise ValueError(f"Unsupported file type '{file_type}'. Supported types: csv, pq, json.")
            
            self.log(f"Successfully loaded data from {self.src}.", level="INFO")
            return data
        except Exception as e:
            self.log(f"Failed to load data from {self.src}: {e}", level="ERROR")
            raise
