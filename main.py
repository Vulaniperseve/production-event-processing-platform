from src.extractors.twelve_data_extractor import TwelveDataExtractor

from src.validators.validator import DataValidator
from src.validators.quarantine import QuarantineHandler

from src.transformers.transformer import DataTransformer

from src.loaders.postgres_loader import PostgresLoader


def main():

    # --------------------------------------------------
    # 1. EXTRACT
    # --------------------------------------------------

    extractor = TwelveDataExtractor()

    data = extractor.extract()


    # --------------------------------------------------
    # 2. STRUCTURE VALIDATION
    # --------------------------------------------------

    DataValidator.validate_structure(data)


    # --------------------------------------------------
    # 3. RECORD-LEVEL DATA QUALITY VALIDATION
    # --------------------------------------------------

    valid_records, invalid_records = (
        DataValidator.validate_records(data)
    )


    # --------------------------------------------------
    # 4. QUARANTINE INVALID RECORDS
    # --------------------------------------------------

    QuarantineHandler.save(invalid_records)


    # --------------------------------------------------
    # 5. STOP IF NO VALID DATA
    # --------------------------------------------------

    if not valid_records:

        print("No valid records available for processing.")

        return


    # --------------------------------------------------
    # 6. TRANSFORM VALID DATA
    # --------------------------------------------------

    validated_data = {
        "meta": data["meta"],
        "status": data["status"],
        "values": valid_records
    }

    df = DataTransformer.transform(validated_data)


    # --------------------------------------------------
    # 7. LOAD INTO POSTGRESQL
    # --------------------------------------------------

    PostgresLoader.load(df)


    # --------------------------------------------------
    # 8. DISPLAY SAMPLE
    # --------------------------------------------------

    print("\nPipeline completed successfully.")

    print("\nSample processed records:")

    print(df.head())


if __name__ == "__main__":
    main()