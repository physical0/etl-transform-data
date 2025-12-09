from typing import Optional
from app.etl.services.coingecko_extractor import CoinGeckoExtractor
from app.etl.services.s3_extractor import S3Extractor
from app.etl.services.base_extractor import BaseExtractor
from app.etl.services.transformer import transformer
from app.etl.services.loader import loader
from app.etl.utils.timing import time_execution
from app.etl.core.logging_config import logger

def get_extractor(source: str, **kwargs) -> BaseExtractor:
    """
    Factory function to get the appropriate extractor based on source.
    
    Args:
        source: Data source type ('coingecko', 's3', etc.)
        **kwargs: Additional arguments for the extractor
        
    Returns:
        BaseExtractor: Configured extractor instance
        
    Raises:
        ValueError: If source is not supported
    """
    extractors = {
        'coingecko': lambda: CoinGeckoExtractor(),
        's3': lambda: S3Extractor(kwargs.get('file_key', 'crypto_data.json')),
    }
    
    if source not in extractors:
        raise ValueError(f"Unsupported source: {source}. Available sources: {list(extractors.keys())}")
    
    return extractors[source]()

@time_execution
def run_etl_job(source: str = 'coingecko', **kwargs):
    """
    Run the ETL job with the specified data source.
    
    Args:
        source: Data source to extract from ('coingecko', 's3')
        **kwargs: Additional arguments passed to the extractor
        
    Examples:
        # Extract from CoinGecko API
        run_etl_job(source='coingecko')
        
        # Extract from S3/MinIO
        run_etl_job(source='s3', file_key='crypto_data.json')
    """
    logger.info(f"Starting ETL job with source: {source}")
    try:
        # Get the appropriate extractor
        extractor = get_extractor(source, **kwargs)
        
        # Extract data
        data = extractor.extract()
        
        # Transform data
        df = transformer.transform(data)
        
        # Load data
        loader.load(df)
        
        logger.info(f"ETL job completed successfully (source: {source})")
    except Exception as e:
        logger.error(f"ETL job failed: {e}")
        raise

if __name__ == "__main__":
    import sys
    
    # Parse command line arguments
    source = sys.argv[1] if len(sys.argv) > 1 else 'coingecko'
    file_key = sys.argv[2] if len(sys.argv) > 2 else 'crypto_data.json'
    
    # Run ETL job
    if source == 's3':
        run_etl_job(source=source, file_key=file_key)
    else:
        run_etl_job(source=source)
