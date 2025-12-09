import json
import pandas as pd
from typing import Any, Dict, List
from app.etl.services.base_extractor import BaseExtractor
from app.etl.core.s3_client import s3_client
from app.etl.core.logging_config import logger

class S3Extractor(BaseExtractor):
    """
    Extractor for S3/MinIO storage.
    Downloads and parses files from S3-compatible storage.
    """
    
    def __init__(self, file_key: str):
        """
        Initialize the S3 extractor.
        
        Args:
            file_key: The S3 object key (file path in bucket) to extract
        """
        self.file_key = file_key
    
    def extract(self) -> List[Dict[str, Any]]:
        """
        Extract data from S3/MinIO file.
        Supports JSON and CSV formats.
        
        Returns:
            List[Dict[str, Any]]: List of records from the file
        """
        try:
            # Get file content from S3/MinIO
            content = s3_client.get_file_content(self.file_key)
            
            # Determine file type and parse accordingly
            if self.file_key.endswith('.json'):
                data = self._parse_json(content)
            elif self.file_key.endswith('.csv'):
                data = self._parse_csv(content)
            else:
                raise ValueError(f"Unsupported file format: {self.file_key}. Supported formats: .json, .csv")
            
            logger.info(f"Extracted {len(data)} records from S3: {self.file_key}")
            return data
        
        except Exception as e:
            logger.error(f"Failed to extract data from S3: {e}")
            raise
    
    def _parse_json(self, content: bytes) -> List[Dict[str, Any]]:
        """Parse JSON content"""
        data = json.loads(content.decode('utf-8'))
        
        # Handle both single object and array of objects
        if isinstance(data, dict):
            return [data]
        elif isinstance(data, list):
            return data
        else:
            raise ValueError("JSON must be an object or array of objects")
    
    def _parse_csv(self, content: bytes) -> List[Dict[str, Any]]:
        """Parse CSV content"""
        import io
        df = pd.read_csv(io.BytesIO(content))
        return df.to_dict(orient='records')

# Factory function to create S3 extractor
def create_s3_extractor(file_key: str) -> S3Extractor:
    """
    Create an S3 extractor for the specified file.
    
    Args:
        file_key: The S3 object key to extract
        
    Returns:
        S3Extractor: Configured S3 extractor instance
    """
    return S3Extractor(file_key)
