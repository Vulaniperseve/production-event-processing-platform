from src.extractors.twelve_data_extractor import TwelveDataExtractor
from src.validators.validator import DataValidator
from src.transformers.transformer import DataTransformer
from src.loaders.postgres_loader import PostgresLoader

extractor = TwelveDataExtractor()

data = extractor.extract()

DataValidator.validate_structure(data)

df = DataTransformer.transform(data)

PostgresLoader.load(df)

print(df.head())