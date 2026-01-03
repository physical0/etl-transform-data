import pandas as pd
from etl.schemas.crypto_schema import validate_and_cast
from etl.core.logging_config import logger
from etl.models.crypto_model import CryptoPrice
from sqlalchemy import inspect
from typing import List

class Transformer:
    def __init__(self):
        self.model_columns: List[str] = []

    def source_to_model(self, source: str):
        print("source_to_model function test validation.")
        if source == "coingecko":
            return CryptoPrice
        else:
            raise ValueError(f"Unsupported source: {source}")
    
    def transform(self, source, data):
        try:
            df = pd.DataFrame(data)
            
            df["loaded_at"] = pd.Timestamp.now()

            self.model_columns = [c.name for c in inspect(self.source_to_model(source)).columns]
            
            available_columns = [col for col in self.model_columns if col in df.columns]
            df = df[available_columns]
            
            logger.info(f"Filtered to {len(available_columns)} model columns: {available_columns}")
            
            df = validate_and_cast(df)
            logger.info(f"Transformed data shape: {df.shape}")
            return df
        except Exception as e:
            logger.error(f"Failed to transform data: {e}")
            raise

transformer = Transformer()
