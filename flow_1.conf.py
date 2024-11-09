import polars as pl


seq_1_args = [
    {
        "inputs": ["./data/Users.pq", "./data/Cards.pq", "./data/transactions.pq"],
        "merge_type": "join-inner",
        "pk": "User"
    },
    {
        "cols": ["Expires", "Acct Open Date"]
    },
    {
        "cols": [
        "Amount", "Credit Limit", "Yearly Income - Person", 
        "Total Debt", "Per Capita Income - Zipcode"
        ],
        "pattern": "$",
        "replace": ""
    },
    {
        "cols_and_types": {
            "Amount": pl.Float64,
            "Credit Limit": pl.Float64,
            "Yearly Income - Person": pl.Float64,
            "Total Debt": pl.Float64,
            "Per Capita Income - Zipcode": pl.Float64,
            "Latitude": pl.Float64,
            "Longitude": pl.Float64,
            "FICO Score": pl.Int64,
            "Num Credit Cards": pl.Int64,
            "Merchant Name": pl.Utf8,
            "Card Brand": pl.Utf8,
            "Card Type": pl.Utf8,
            "Merchant City": pl.Utf8,
            "Merchant State": pl.Utf8,
            "Zip": pl.Utf8,
            "Use Chip": pl.Utf8,
        },
        "fill_by": {
            pl.Utf8: "Unknown",
            pl.Int64: 0,
            pl.Float64: 0.0,
        },
        "export_to": "./exports/preprocessed.pq"
    }
]