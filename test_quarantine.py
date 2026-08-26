from src.validators.quarantine import QuarantineHandler


invalid_records = [
    {
        "record": {
            "datetime": None,
            "close": "231.20"
        },
        "reason": "Missing fields: ['datetime']"
    },
    {
        "record": {
            "datetime": "2026-08-26",
            "close": "ERROR"
        },
        "reason": "Invalid close price"
    }
]


print("\n--- QUARANTINE TEST ---")

filepath = QuarantineHandler.save(invalid_records)

print(f"Quarantine file created: {filepath}")