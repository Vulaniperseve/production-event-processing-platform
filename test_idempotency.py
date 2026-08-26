import json

from src.validators.validator import DataValidator
from src.transformers.transformer import DataTransformer
from src.loaders.postgres_loader import PostgresLoader


# Find one of the existing raw API JSON files
RAW_FILE = "data/raw/AAPL_20260826_031059.json"


with open(RAW_FILE, "r") as file:
    data = json.load(file)


print("\n--- VALIDATING ---")
DataValidator.validate_structure(data)


print("\n--- TRANSFORMING ---")
df = DataTransformer.transform(data)

print(f"Records transformed: {len(df)}")


print("\n--- FIRST LOAD ---")
inserted, skipped = PostgresLoader.load(df)

print(f"Inserted: {inserted}")
print(f"Skipped: {skipped}")


print("\n--- SECOND LOAD ---")
inserted, skipped = PostgresLoader.load(df)

print(f"Inserted: {inserted}")
print(f"Skipped: {skipped}")