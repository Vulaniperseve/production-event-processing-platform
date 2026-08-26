from src.validators.validator import DataValidator


test_data = {
    "meta": {
        "symbol": "AAPL"
    },
    "status": "ok",
    "values": [
        {
            "datetime": "2026-08-26",
            "close": "230.50"
        },
        {
            "datetime": None,
            "close": "231.20"
        },
        {
            "datetime": "2026-08-26",
            "close": "ERROR"
        },
        {
            "close": "232.10"
        }
    ]
}


print("\n--- STRUCTURE VALIDATION ---")

DataValidator.validate_structure(test_data)


print("\n--- RECORD VALIDATION ---")

valid_records, invalid_records = (
    DataValidator.validate_records(test_data)
)


print(f"Valid records: {len(valid_records)}")
print(f"Invalid records: {len(invalid_records)}")


print("\n--- INVALID RECORDS ---")

for item in invalid_records:
    print(item)