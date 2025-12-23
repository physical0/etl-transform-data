import requests
from abc import abstractmethod
from typing import Any, Dict, List, Optional
from etl.services.base_extractor import BaseExtractor
from etl.core.logging_config import logger

class APIExtractor(BaseExtractor):
    """
    Abstract base class for API-based extractors.
    Provides common functionality for making HTTP requests with retries and error handling.
    """
    
    def __init__(self, base_url: str, headers: Optional[Dict[str, str]] = None):
        """
        Initialize the API extractor.
        
        Args:
            base_url: Base URL for the API
            headers: Optional HTTP headers to include in requests
        """
        self.base_url = base_url
        self.headers = headers or {}
        self.session = requests.Session()
        self.session.headers.update(self.headers)
    
    @abstractmethod
    def extract(self) -> List[Dict[str, Any]]:
        """
        Extract data from the API.
        Must be implemented by subclasses.
        """
        pass
    
    def get(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Make a GET request to the API.
        
        Args:
            endpoint: API endpoint (will be appended to base_url)
            params: Optional query parameters
            
        Returns:
            Dict[str, Any]: JSON response from the API
        """
        url = f"{self.base_url}{endpoint}"
        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed for {url}: {e}")
            raise
    
    def post(self, endpoint: str, data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Make a POST request to the API.
        
        Args:
            endpoint: API endpoint (will be appended to base_url)
            data: Optional request body data
            
        Returns:
            Dict[str, Any]: JSON response from the API
        """
        url = f"{self.base_url}{endpoint}"
        try:
            response = self.session.post(url, json=data)
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed for {url}: {e}")
            raise
