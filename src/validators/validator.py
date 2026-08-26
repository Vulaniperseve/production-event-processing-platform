import pandas as pd

from src.logger import logger


class DataValidator:
    """
    Validates API responses before transformation.
    """

    REQUIRED_KEYS = [
        "meta",
        "values",
        "status"
    ]

    @staticmethod
    def validate_structure(data):

        # 1. Check that data was received
        if data is None:
            raise ValueError("No data received from API.")

        # 2. Check required top-level fields
        for key in DataValidator.REQUIRED_KEYS:

            if key not in data:
                raise ValueError(
                    f"Missing required key: {key}"
                )

        # 3. Check API status
        if data["status"] != "ok":
            raise ValueError(
                "API did not return status='ok'."
            )

        # 4. Check that values exist
        if not data["values"]:
            raise ValueError(
                "API returned an empty values list."
            )

        logger.info(
            "API response structure is valid."
        )

        return True

    @staticmethod
    def validate_records(data):

        values = data["values"]

        required_fields = [
            "datetime",
            "close"
        ]

        valid_records = []
        invalid_records = []

        for record in values:

            # Check required fields
            missing_fields = [
                field
                for field in required_fields
                if field not in record
                or record[field] in (None, "")
            ]

            if missing_fields:

                invalid_records.append({
                    "record": record,
                    "reason": (
                        f"Missing fields: {missing_fields}"
                    )
                })

                continue

            # Validate datetime
            try:

                pd.to_datetime(
                    record["datetime"]
                )

            except Exception:

                invalid_records.append({
                    "record": record,
                    "reason": "Invalid datetime"
                })

                continue

            # Validate numeric close price
            try:

                float(record["close"])

            except (TypeError, ValueError):

                invalid_records.append({
                    "record": record,
                    "reason": "Invalid close price"
                })

                continue

            valid_records.append(record)

        logger.info(
            f"Record validation complete. "
            f"Valid: {len(valid_records)}, "
            f"Invalid: {len(invalid_records)}"
        )

        return valid_records, invalid_records