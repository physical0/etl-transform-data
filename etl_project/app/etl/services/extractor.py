import requests
from app.etl.core.logging_config import logger

class Extractor:
    def __init__(self):
        self.base_url = "https://api.coingecko.com/api/v3"

    def get_crypto_data(self):
        url = f"{self.base_url}/coins/markets"
        params = {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": 100,
            "page": 1,
            "sparkline": False
        }
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            logger.info(f"Extracted {len(data)} records from CoinGecko")
            return data
        except Exception as e:
            logger.error(f"Failed to extract data: {e}")
            raise

extractor = Extractor()
