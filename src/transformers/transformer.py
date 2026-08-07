import pandas as pd
import uuid
from pathlib import Path


class DataTransformer:

    @staticmethod
    def transform(data):

        values = data["values"]

        df = pd.DataFrame(values)

        df["event_id"] = [
            str(uuid.uuid4()) for _ in range(len(df))
        ]

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

        print("✓ Data transformed successfully.")

        return df