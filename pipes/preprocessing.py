# INTERFACES
from MLTest.core.Pipelines import ExportPipe, FlowThroughPipe, LoadingPipe
from typing import List, Any
import polars as pl

# COMPONENTS
from MLTest.components.storage.Input import StoreInputs
from MLTest.components.filesystem.Export import ExportData
from MLTest.components.filesystem.Input import LoadData
from MLTest.components.preprocessing.Format import GenerateTimeStamp, SplitTimeColumn, FormatDate
from MLTest.components.preprocessing.Regulation import MergeStorage
from MLTest.components.preprocessing.Replace import ReplaceStringPattern
from MLTest.components.preprocessing.Types import CastTypes, HandleNullValues


def _MergeData(inputs: List[str], merge_type: str, pk: str, log: bool = False):
    """
    Creates a pipeline to merge data from multiple inputs.

    Parameters:
    - inputs: List of input file paths.
    - merge_type: Merge type (e.g., join-inner, join-outer).
    - pk: Primary key to merge on.
    - log: Enable logging (default: False).

    Returns:
    - LoadingPipe: Configured pipeline instance.
    """
    input_loaders = [LoadData(input, log=log) for input in inputs]

    pipe = LoadingPipe([
        StoreInputs(input_loaders, log=log),
        MergeStorage(how=merge_type, on=pk, log=log),
    ])

    return pipe


def _HandleDateColumns_(cols: List[str], date_format: str = "%m/%Y", tms_format: str = "%Y-%m-%d-%H-%M", log: bool = False):
    """
    Creates a pipeline to handle date columns by formatting and splitting.

    Parameters:
    - cols: List of columns to process.
    - date_format: Format for parsing date columns.
    - tms_format: Format for generating timestamps.
    - log: Enable logging (default: False).

    Returns:
    - FlowThroughPipe: Configured pipeline instance.
    """
    pipe = FlowThroughPipe([
        FormatDate(columns=cols, format=date_format, log=log),
        SplitTimeColumn(time_col="Time", time_format="%H:%M", log=log),
        GenerateTimeStamp(format=tms_format, log=log),
    ])

    return pipe


def _ReplaceStrInColumns_(cols: List[str], pattern: str, replace: str, log: bool = False):
    """
    Creates a pipeline to replace string patterns in columns.

    Parameters:
    - cols: List of columns to process.
    - pattern: Pattern to replace.
    - replace: Replacement string.
    - log: Enable logging (default: False).

    Returns:
    - FlowThroughPipe: Configured pipeline instance.
    """
    pipe = FlowThroughPipe([
        ReplaceStringPattern(columns=cols, pattern=pattern, replace=replace, is_regex=False, log=log),
    ])

    return pipe


def CastFillAndExport_(cols_and_types: dict[str, pl.DataType], fill_by: dict[pl.DataType, Any], export_to: str, log: bool = False):
    """
    Creates a pipeline to cast column types, handle nulls, and export data.

    Parameters:
    - cols_and_types: Dictionary mapping column names to data types.
    - fill_by: Dictionary mapping data types to fill values for null handling.
    - export_to: Path to export the processed data.
    - log: Enable logging (default: False).

    Returns:
    - ExportPipe: Configured pipeline instance.
    """
    pipe = ExportPipe([
        CastTypes(columns_and_types=cols_and_types, log=log),
        HandleNullValues(fill_values=fill_by, log=log),
        ExportData(export_to=export_to, log=log),
    ])

    return pipe
