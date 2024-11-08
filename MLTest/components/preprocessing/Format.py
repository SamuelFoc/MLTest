from MLTest.interfaces.Components import FlowComponent
from MLTest.interfaces.Typing import DF
import polars as pl


class FormatDate(FlowComponent):
    def __init__(self, columns: list[str], format: str, strict: bool = False, log: bool = False):
        """
        Initializes the DateParsingComponent.
        
        Parameters:
        - columns (list[str]): List of column names to parse as dates.
        - format (str): The date format to use for parsing (e.g., "%m/%Y").
        - strict (bool): Whether to enforce strict date parsing. Defaults to False.
        """
        super().__init__(log)
        self.columns = columns
        self.format = format
        self.strict = strict

    def use(self, data: DF) -> DF:
        """
        Parses specified columns as dates according to the given format.
        
        Parameters:
        - data (DF): The Polars DataFrame to process.
        
        Returns:
        - DF: The modified DataFrame with parsed date columns.
        """
        self.log(f"Starting date parsing for columns: {self.columns} with format: '{self.format}' and strict mode: {self.strict}.", level="INFO")
        
        # Apply date parsing to each specified column
        for column in self.columns:
            self.log(f"Parsing column '{column}' as a date.", level="INFO")
            try:
                data = data.with_columns(
                    pl.col(column)
                    .str.strptime(pl.Date, format=self.format, strict=self.strict)
                    .alias(column)
                )
                self.log(f"Successfully parsed column '{column}'.", level="INFO")
            except Exception as e:
                self.log(f"Failed to parse column '{column}' as a date: {e}", level="ERROR")
                raise

        self.log("Date parsing completed.", level="INFO")
        return data
    

class GenerateTimeStamp(FlowComponent):
    def __init__(self, format: str, year_col: str = "Year", month_col: str = "Month", day_col: str = "Day", time_col: str = "Time", log: bool = False):
        """
        Initializes the GenerateTimeStamp component.

        Parameters:
        - format (str): The datetime format to parse the timestamp.
        - year_col (str): Name of the column containing the year.
        - month_col (str): Name of the column containing the month (optional).
        - day_col (str): Name of the column containing the day (optional).
        - time_col (str): Name of the column containing the time (optional).
        """
        super().__init__(log)
        self.year_col = year_col
        self.month_col = month_col
        self.day_col = day_col
        self.time_col = time_col
        self.format = format

    def use(self, data: DF) -> DF:
        """
        Validates and generates a timestamp column using the provided year, month, day, and time columns.

        Parameters:
        - data (DF): The Polars DataFrame to process.

        Returns:
        - DF: The modified DataFrame with a new 'Datetime' column.
        """
        self.log(f"Starting timestamp generation with format: '{self.format}'.", level="INFO")
        
        # Mapping of format specifiers to required columns
        format_to_column = {
            "%Y": self.year_col,
            "%m": self.month_col,
            "%d": self.day_col,
            "%H": self.time_col,
            "%M": self.time_col,
            "%S": self.time_col
        }

        # Extract format specifiers from the format string
        used_specifiers = [specifier for specifier in format_to_column if specifier in self.format]
        self.log(f"Using format specifiers: {used_specifiers}.", level="INFO")

        # Check if all required columns for the format are present
        missing_columns = [
            format_to_column[specifier] for specifier in used_specifiers if format_to_column[specifier] not in data.columns
        ]

        if missing_columns:
            error_message = f"Format '{self.format}' requires the following missing columns: {', '.join(missing_columns)}"
            self.log(error_message, level="ERROR")
            raise ValueError(error_message)

        self.log("All required columns for the format are present.", level="INFO")

        # Prepare components for creating the datetime string
        components = []
        for specifier in used_specifiers:
            column = format_to_column[specifier]
            if column == self.time_col:
                self.log(f"Processing time column '{column}'.", level="INFO")
                components.append(pl.col(column).cast(pl.Utf8))  # Handle time parts
            else:
                self.log(f"Processing column '{column}' with zero-padding.", level="INFO")
                components.append(pl.col(column).cast(pl.Utf8).str.zfill(2))  # Handle year, month, day

        # Concatenate columns to create a datetime string
        self.log("Concatenating columns to form a datetime string.", level="INFO")
        data = data.with_columns([
            pl.concat_str(components, separator="-").alias("Datetime_str")
        ])

        # Parse the concatenated datetime string into a proper Datetime column
        self.log("Parsing the concatenated datetime string into a Datetime column.", level="INFO")
        try:
            data = data.with_columns([
                pl.col("Datetime_str").str.strptime(pl.Datetime, format=self.format).alias("Datetime")
            ])
            self.log("Successfully generated the 'Datetime' column.", level="INFO")
        except Exception as e:
            self.log(f"Failed to parse datetime string: {e}", level="ERROR")
            raise

        # Drop the intermediate datetime string column
        data = data.drop("Datetime_str")
        self.log("Dropped the intermediate 'Datetime_str' column.", level="INFO")

        return data
