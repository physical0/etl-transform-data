import os
import sys

app_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, app_dir)

from typing import Any, Dict, List
from etl.services.api_extractor import APIExtractor
from etl.core.logging_config import logger

class CoinGeckoExtractor(APIExtractor):
    """
    Extractor for CoinGecko cryptocurrency API.
    Fetches market data for top cryptocurrencies.
    """
    
    def __init__(self):
        super().__init__(base_url="https://api.coingecko.com/api/v3")
    
    def extract(self) -> List[Dict[str, Any]]:
        """
        Extract cryptocurrency market data from CoinGecko API.
        
        Returns:
            List[Dict[str, Any]]: List of cryptocurrency records
        """
        params = {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": 100,
            "page": 1,
            "sparkline": False
        }
        
        try:
            data = self.get("/coins/markets", params=params)
            logger.info(f"Extracted {len(data)} records from CoinGecko")
            return data
        except Exception as e:
            logger.error(f"Failed to extract data from CoinGecko: {e}")
            raise


# class testing
if __name__ == "__main__":
    coingecko_extractor = CoinGeckoExtractor()
    data = coingecko_extractor.extract()
    print(data)
