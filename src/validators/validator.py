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

        if data is None:
            raise ValueError("No data received from API.")

        for key in DataValidator.REQUIRED_KEYS:
            if key not in data:
                raise ValueError(f"Missing required key: {key}")

        if data["status"] != "ok":
            raise ValueError("API did not return status='ok'.")

        print("✓ API response structure is valid.")

        return True