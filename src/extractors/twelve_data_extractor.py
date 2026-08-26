import requests
import json
import time
from datetime import datetime
from pathlib import Path

from config.settings import API_KEY, BASE_URL
from src.extractors.base_extractor import BaseExtractor
from src.logger import logger


class TwelveDataExtractor(BaseExtractor):

    def __init__(self):
        self.endpoint = f"{BASE_URL}/time_series"

    def extract(self, symbol="AAPL", interval="1day", outputsize=10):

        logger.info(
            f"Starting extraction: symbol={symbol}, "
            f"interval={interval}, outputsize={outputsize}"
        )

        params = {
            "symbol": symbol,
            "interval": interval,
            "outputsize": outputsize,
            "apikey": API_KEY
        }

        max_attempts = 3

        for attempt in range(1, max_attempts + 1):

            try:

                logger.info(
                    f"API request attempt {attempt}/{max_attempts}"
                )

                response = requests.get(
                    self.endpoint,
                    params=params,
                    timeout=10
                )

                response.raise_for_status()

                data = response.json()

                logger.info("API request successful.")

                break

            except requests.exceptions.RequestException as e:

                logger.warning(
                    f"API request attempt {attempt} failed: {e}"
                )

                if attempt == max_attempts:

                    logger.error(
                        "Maximum API retry attempts reached."
                    )

                    raise

                wait_time = 2 ** attempt

                logger.info(
                    f"Retrying API request in {wait_time} seconds."
                )

                time.sleep(wait_time)

        # Create raw data folder if it doesn't exist
        Path("data/raw").mkdir(
            parents=True,
            exist_ok=True
        )

        # Save raw JSON
        filename = datetime.now().strftime(
            f"data/raw/{symbol}_%Y%m%d_%H%M%S.json"
        )

        with open(filename, "w") as file:
            json.dump(data, file, indent=4)

        logger.info("Data successfully extracted.")
        logger.info(f"Saved to: {filename}")

        return data