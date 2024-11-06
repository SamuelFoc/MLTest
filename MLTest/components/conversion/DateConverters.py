from MLTest.interfaces.Components import FlowComponent
from MLTest.interfaces.Typing import DF
import polars as pl


class DateTimeCreation(FlowComponent):
    def __init__(self, format: str, year_col: str = "Year", month_col: str = "Month", day_col: str = "Day", time_col: str = "Time"):
        """
        Initializes the DateTimeCreationComponent.
        
        Parameters:
        - year_col (str): Name of the column containing the year.
        - month_col (str): Name of the column containing the month.
        - day_col (str): Name of the column containing the day.
        - time_col (str): Name of the column containing the time.
        """
        self.year_col = year_col
        self.month_col = month_col
        self.day_col = day_col
        self.time_col = time_col
        self.format = format

    def use(self, dataframe: DF) -> DF:
        """
        Converts separate year, month, day, and time columns into a single datetime column.
        
        Parameters:
        - dataframe (DF): The Polars DataFrame to process.
        
        Returns:
        - DF: The modified DataFrame with a new 'Datetime' column.
        """
        # Ensure numeric columns are cast to integers, and time column is cast to string
        dataframe = dataframe.with_columns([
            pl.col(self.year_col).cast(pl.Int32).alias(self.year_col),
            pl.col(self.month_col).cast(pl.Int32).alias(self.month_col),
            pl.col(self.day_col).cast(pl.Int32).alias(self.day_col),
            pl.col(self.time_col).cast(pl.Utf8).alias(self.time_col),
        ])

        # Concatenate columns to create a datetime string
        dataframe = dataframe.with_columns([
            pl.concat_str(
                [
                    pl.col(self.year_col).cast(pl.Utf8),
                    pl.col(self.month_col).cast(pl.Utf8).str.zfill(2),
                    pl.col(self.day_col).cast(pl.Utf8).str.zfill(2),
                    pl.col(self.time_col)
                ],
                separator="-"
            ).alias("Datetime_str")
        ])

        # Parse the concatenated datetime string into a proper Datetime column
        dataframe = dataframe.with_columns([
            pl.col("Datetime_str").str.strptime(pl.Datetime, format=self.format).alias("Datetime")
        ])

        # Drop unnecessary columns
        dataframe = dataframe.drop([self.time_col, "Datetime_str"])
        
        return dataframe


from MLTest.interfaces.Components import FlowComponent
from MLTest.interfaces.Typing import DF
import polars as pl


class DateParsing(FlowComponent):
    def __init__(self, columns: list[str], date_format: str, strict: bool = False):
        """
        Initializes the DateParsingComponent.
        
        Parameters:
        - columns (list[str]): List of column names to parse as dates.
        - date_format (str): The date format to use for parsing (e.g., "%m/%Y").
        - strict (bool): Whether to enforce strict date parsing. Defaults to False.
        """
        self.columns = columns
        self.date_format = date_format
        self.strict = strict

    def use(self, dataframe: DF) -> DF:
        """
        Parses specified columns as dates according to the given format.
        
        Parameters:
        - dataframe (DF): The Polars DataFrame to process.
        
        Returns:
        - DF: The modified DataFrame with parsed date columns.
        """
        # Apply date parsing to each specified column
        for column in self.columns:
            dataframe = dataframe.with_columns(
                pl.col(column)
                .str.strptime(pl.Date, format=self.date_format, strict=self.strict)
                .alias(column)
            )
        
        return dataframe

