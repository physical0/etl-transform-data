import pandas as pd
from app.etl.schemas.crypto_schema import validate_and_cast
from app.etl.core.logging_config import logger

class Transformer:
    def transform(self, data):
        try:
            df = pd.DataFrame(data)
            df = validate_and_cast(df)
            logger.info(f"Transformed data shape: {df.shape}")
            return df
        except Exception as e:
            logger.error(f"Failed to transform data: {e}")
            raise

transformer = Transformer()
