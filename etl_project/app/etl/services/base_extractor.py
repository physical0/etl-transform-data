from abc import ABC, abstractmethod
from typing import Any, Dict, List

class BaseExtractor(ABC):
    """
    Abstract base class for all data extractors.
    Defines the common interface that all extractors must implement.
    """
    
    @abstractmethod
    def extract(self) -> List[Dict[str, Any]]:
        """
        Extract data from the source.
        
        Returns:
            List[Dict[str, Any]]: List of records as dictionaries
        """
        pass
    
    def validate(self, data: List[Dict[str, Any]]) -> bool:
        """
        Optional validation hook for extracted data.
        Override this method to add custom validation logic.
        
        Args:
            data: The extracted data to validate
            
        Returns:
            bool: True if data is valid, False otherwise
        """
        return True
    
    def transform_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Optional hook to transform individual records during extraction.
        Override this method to add custom transformation logic.
        
        Args:
            record: Single record to transform
            
        Returns:
            Dict[str, Any]: Transformed record
        """
        return record
