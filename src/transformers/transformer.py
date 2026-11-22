import hashlib

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

        # Deterministic event IDs: sha256("SYMBOL|datetime|TYPE")[:36].
        # A list comprehension over the columns; DataFrame.apply(axis=1) builds
        # a Series per row and was ~20x slower.
        df["event_id"] = [
            hashlib.sha256(f"{entity}|{event_time}|{event_type}".encode()).hexdigest()[:36]
            for entity, event_time, event_type in zip(
                df["entity_id"], df["event_time"], df["event_type"], strict=True
            )
        ]

        columns = [
            "event_id",
            "entity_id",
            "event_time",
            "event_type",
            "value",
            "source_system"
        ]

        df = df[columns]

        logger.info(
            f"Data transformation successful. "
            f"Records transformed: {len(df)}"
        )

        return df
