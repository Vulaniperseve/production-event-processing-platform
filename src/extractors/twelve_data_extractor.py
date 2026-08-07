import requests
import json
from datetime import datetime
from pathlib import Path

from config.settings import API_KEY, BASE_URL
from src.extractors.base_extractor import BaseExtractor


class TwelveDataExtractor(BaseExtractor):

    def __init__(self):
        self.endpoint = f"{BASE_URL}/time_series"

    def extract(self, symbol="AAPL", interval="1day", outputsize=10):

        params = {
            "symbol": symbol,
            "interval": interval,
            "outputsize": outputsize,
            "apikey": API_KEY
        }

        try:
            response = requests.get(self.endpoint, params=params, timeout=10)
            response.raise_for_status()

            data = response.json()

            # Create raw data folder if it doesn't exist
            Path("data/raw").mkdir(parents=True, exist_ok=True)

            # Save raw JSON
            filename = datetime.now().strftime(
                f"data/raw/{symbol}_%Y%m%d_%H%M%S.json"
            )

            with open(filename, "w") as file:
                json.dump(data, file, indent=4)

            print("Data successfully extracted.")
            print(f"Saved to: {filename}")

            return data

        except requests.exceptions.RequestException as e:
            print("Extraction failed.")
            print(e)

            return None