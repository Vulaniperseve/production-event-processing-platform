import hashlib
from pathlib import Path

import pandas as pd

from src.logger import logger


class DataTransformer:

    @staticmethod
    def transform(data):

        logger.info("Starting data transformation.")

        values = data["values"]

        df = pd.DataFrame(values)

        df["entity_id"] = data["meta"]["symbol"]

        df["source_system"] = "TwelveData"

        df["event_type"] = "MARKET_PRICE"

        df.rename(
            columns={
                "datetime": "event_time",
                "close": "value"
            },
            inplace=True
        )

        # Create deterministic event IDs
        df["event_id"] = df.apply(
            lambda row: hashlib.sha256(
                f"{row['entity_id']}|"
                f"{row['event_time']}|"
                f"{row['event_type']}".encode()
            ).hexdigest()[:36],
            axis=1
        )

        columns = [
            "event_id",
            "entity_id",
            "event_time",
            "event_type",
            "value",
            "source_system"
        ]

        df = df[columns]

        Path("data/processed").mkdir(
            parents=True,
            exist_ok=True
        )

        df.to_csv(
            "data/processed/events.csv",
            index=False
        )

        logger.info(
            f"Data transformation successful. "
            f"Records transformed: {len(df)}"
        )

        return df