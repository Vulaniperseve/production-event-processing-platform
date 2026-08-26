import json

from src.validators.validator import DataValidator


RAW_FILE = "data/raw/AAPL_20260826_030153.json"


# Load existing raw data
with open(RAW_FILE, "r") as file:
    data = json.load(file)


print("\n--- STRUCTURE VALIDATION ---")

DataValidator.validate_structure(data)


print("\n--- RECORD VALIDATION ---")

valid_records, invalid_records = (
    DataValidator.validate_records(data)
)


print(f"Valid records: {len(valid_records)}")
print(f"Invalid records: {len(invalid_records)}")


if invalid_records:

    print("\n--- INVALID RECORDS ---")

    for item in invalid_records:
        print(item)