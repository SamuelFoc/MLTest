# from tests.test_1 import test_1

# test_1()

from MLTest.components.validation.Flag import ValidateOrFlag
import polars as pl

# Define expected types for validation
expected_types = {
    "column1": pl.Int64,
    "column2": pl.Int64,
    "column3": pl.Utf8,
}

# Define a lambda function for the validation condition
condition = lambda df: any(
    df.schema[col] != expected_type
    for col, expected_type in expected_types.items()
    if col in df.columns
)

validator = ValidateOrFlag(
    condition=condition,
    message="Data type mismatches detected!",
    raise_exception=True,
    log=True
)

# Example DataFrame
data = pl.DataFrame({
    "column1": [1, 2, 3],
    "column2": [1.0, 2.2, 3.3],
    "column3": ["a", "b", "c"],  # All types match
    # "column4" is not in `expected_types`, so it won't be checked
})

# Run the validation
try:
    validator.use(data)
    print("All data types are valid!")
except ValueError as e:
    print(e) 