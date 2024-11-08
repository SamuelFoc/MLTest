from MLTest.core.Pipes import FlowPipe
from MLTest.components.conversion.DateConverters import DateTimeCreation, DateParsing
from MLTest.components.conversion.RegexReplace import RegexReplace
from MLTest.components.conversion.TypeCasting import TypeCasting
from MLTest.components.conversion.NullFiller import NullFiller
from MLTest.components.filesystem.Export import ExportData
import polars as pl


columns_and_types = {
    # Float64 Columns
    "Amount": pl.Float64,
    "Credit Limit": pl.Float64,
    "Yearly Income - Person": pl.Float64,
    "Total Debt": pl.Float64,
    "Per Capita Income - Zipcode": pl.Float64,
    "Latitude": pl.Float64,
    "Longitude": pl.Float64,

    # Int64 Columns
    "FICO Score": pl.Int64,
    "Num Credit Cards": pl.Int64,

    # Utf8 (String) Columns
    "Merchant Name": pl.Utf8,
    "Card Brand": pl.Utf8,
    "Card Type": pl.Utf8,
    "Merchant City": pl.Utf8,
    "Merchant State": pl.Utf8,
    "Zip": pl.Utf8,
    "Use Chip": pl.Utf8
}

default_fill = {
    pl.Utf8: "Unknown",
    pl.Int64: 0,
    pl.Float64: 0.0
}

def check_nulls(cols):
    return len(cols) > 0

PIPELINE = FlowPipe([
    DateTimeCreation(format="%Y-%m-%d-%H:%M"),
    DateParsing(columns=["Expires", "Acct Open Date"], date_format="%m/%Y"),
    RegexReplace(columns=[
    "Amount", "Credit Limit", "Yearly Income - Person", 
    "Total Debt", "Per Capita Income - Zipcode"
    ], pattern="$", replace="", is_regex=False),
    TypeCasting(columns_and_types),
    NullFiller(default_fill),
    ExportData(save_to="./exports/clean_data.pq")
]) 
