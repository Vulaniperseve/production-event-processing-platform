import math
import re
from datetime import datetime

import pandas as pd

from src.logger import logger

# pandas timestamps only cover 1677-09-21 to 2262-04-11.
_PANDAS_MIN_YEAR, _PANDAS_MAX_YEAR = 1678, 2261
# Fast path only for calendar dates (not ISO week/ordinal forms like 2026-W47).
_ISO_DATE_PREFIX = re.compile(r"\d{4}-\d{2}-\d{2}")


def is_valid_datetime(value):
    """
    True if pandas can parse `value` as a timestamp.

    Fast path: Twelve Data sends ISO dates ("2026-11-20" or
    "2026-11-20 15:30:00"), which datetime.fromisoformat parses in about a
    microsecond. Anything else (other formats, years pandas can't represent)
    falls back to pd.to_datetime, so the accepted set is unchanged. The
    fallback alone cost ~1 ms per record and dominated the pipeline.
    """
    if isinstance(value, str) and _ISO_DATE_PREFIX.match(value):
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            pass
        else:
            if _PANDAS_MIN_YEAR <= parsed.year <= _PANDAS_MAX_YEAR:
                return True
    try:
        pd.to_datetime(value)
    except Exception:
        return False
    return True


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
            if not is_valid_datetime(record["datetime"]):

                invalid_records.append({
                    "record": record,
                    "reason": "Invalid datetime"
                })

                continue

            # Validate numeric close price
            try:

                close = float(record["close"])

            except (TypeError, ValueError):

                invalid_records.append({
                    "record": record,
                    "reason": "Invalid close price"
                })

                continue

            # A traded price must be a positive, finite number
            # (rejects 0, negatives, "nan" and "inf", which float() accepts)
            if not math.isfinite(close) or close <= 0:

                invalid_records.append({
                    "record": record,
                    "reason": "Non-positive or non-finite close price"
                })

                continue

            valid_records.append(record)

        logger.info(
            f"Record validation complete. "
            f"Valid: {len(valid_records)}, "
            f"Invalid: {len(invalid_records)}"
        )

        return valid_records, invalid_records
