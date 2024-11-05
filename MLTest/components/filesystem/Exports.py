from MLTest.interfaces.Components import DataExportComponent
from MLTest.interfaces.Typing import DF


class ExportData(DataExportComponent):
    def __init__(self, save_to: str):
        """
        Initializes the DataExportComponent with a destination path.

        Parameters:
        - save_to (str): The path where the data will be saved.
        """
        self.save_to = save_to

    def use(self, data: DF) -> None:
        """
        Exports the provided dataframe to the specified location in the format 
        inferred from the file extension.

        Parameters:
        - data (DF): The dataframe to be exported.
        """
        # Infer the file format from the save path
        file_type = self.save_to.split('.')[-1].lower()

        # Export based on the inferred file format
        if file_type == 'csv':
            data.write_csv(self.save_to)
        elif file_type == 'pq':
            data.write_parquet(self.save_to)
        elif file_type == 'json':
            data.write_json(self.save_to)
        else:
            raise ValueError(f"Unsupported file format '{file_type}'. Supported formats: csv, parquet, json.")
