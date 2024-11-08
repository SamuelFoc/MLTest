from MLTest.interfaces.Components import ExportComponent, MultiExportComponent
from MLTest.interfaces.Typing import DF
from typing import List


class ExportData(ExportComponent):
    """
    Component for exporting a single DataFrame to a specified file path.
    Supports CSV, Parquet (pq), and JSON formats.
    """
    def __init__(self, save_to: str):
        """
        Initialize the ExportData component with the destination file path.

        Args:
            save_to (str): The path where the data will be saved.
        """
        self.save_to = save_to

    def use(self, data: DF) -> None:
        """
        Exports the provided DataFrame to the specified path. The format is 
        inferred from the file extension in `save_to`.

        Args:
            data (DF): The DataFrame to be exported.

        Raises:
            ValueError: If the file format is unsupported.
        """
        # Infer the file format from the save path
        file_type = self.save_to.split('.')[-1].lower()
        self.log(f"Starting export of DataFrame to {self.save_to} (inferred format: {file_type}).", level="INFO")

        # Export based on the inferred file format
        try:
            if file_type == 'csv':
                data.write_csv(self.save_to)
                self.log(f"Exported DataFrame to {self.save_to} as CSV.", level="INFO")
            elif file_type == 'pq':
                data.write_parquet(self.save_to)
                self.log(f"Exported DataFrame to {self.save_to} as Parquet.", level="INFO")
            elif file_type == 'json':
                data.write_json(self.save_to)
                self.log(f"Exported DataFrame to {self.save_to} as JSON.", level="INFO")
            else:
                raise ValueError(f"Unsupported file format '{file_type}'. Supported formats: csv, pq, json.")
        except Exception as e:
            self.log(f"Failed to export DataFrame to {self.save_to}: {e}", level="ERROR")
            raise


class ExportMany(MultiExportComponent):
    """
    Component for exporting multiple DataFrames to specified file paths.
    Each file path in `save_paths` should correspond to a DataFrame in `data`.
    Supports CSV, Parquet (pq), and JSON formats.
    """
    def __init__(self, save_to: List[str]):
        """
        Initialize the ExportMultipleDataFrames component with a list of file paths.

        Args:
            save_paths (List[str]): List of paths where each DataFrame will be saved.
        """
        self.save_to = save_to

    def use(self, data: List[DF]) -> None:
        """
        Exports each DataFrame in `data` to the corresponding path in `save_paths`. 
        The format for each export is inferred from the respective file extension.

        Args:
            data (List[DF]): List of DataFrames to be exported.

        Raises:
            ValueError: If the number of DataFrames does not match the number of save paths.
        """
        if len(data) != len(self.save_to):
            error_message = "Number of DataFrames does not match the number of save paths."
            self.log(error_message, level="ERROR")
            raise ValueError(error_message)

        for df, path in zip(data, self.save_to):
            file_type = path.split('.')[-1].lower()
            self.log(f"Starting export of DataFrame to {path} (inferred format: {file_type}).", level="INFO")

            # Export based on the inferred file format
            try:
                if file_type == 'csv':
                    df.write_csv(path)
                    self.log(f"Exported DataFrame to {path} as CSV.", level="INFO")
                elif file_type == 'pq':
                    df.write_parquet(path)
                    self.log(f"Exported DataFrame to {path} as Parquet.", level="INFO")
                elif file_type == 'json':
                    df.write_json(path)
                    self.log(f"Exported DataFrame to {path} as JSON.", level="INFO")
                else:
                    raise ValueError(f"Unsupported file format '{file_type}' for path '{path}'. Supported formats: csv, pq, json.")
            except Exception as e:
                self.log(f"Failed to export DataFrame to {path}: {e}", level="ERROR")
                raise