import json
from datetime import datetime
from pathlib import Path

from src.logger import logger


class QuarantineHandler:

    @staticmethod
    def save(invalid_records):

        if not invalid_records:
            logger.info("No invalid records to quarantine.")
            return None

        quarantine_dir = Path("data/quarantine")

        quarantine_dir.mkdir(
            parents=True,
            exist_ok=True
        )

        filename = datetime.now().strftime(
            "invalid_records_%Y%m%d_%H%M%S.json"
        )

        filepath = quarantine_dir / filename

        with open(filepath, "w") as file:

            json.dump(
                invalid_records,
                file,
                indent=4,
                default=str
            )

        logger.warning(
            f"Quarantined {len(invalid_records)} invalid records."
        )

        logger.warning(
            f"Quarantine file saved to: {filepath}"
        )

        return filepath