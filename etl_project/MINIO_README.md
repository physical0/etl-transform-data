# MinIO Integration - Quick Start Guide

## What Was Added

### 1. **Extensible Extractor Architecture**
- `BaseExtractor` - Abstract base for all extractors
- `APIExtractor` - Generic API base class
- `CoinGeckoExtractor` - CoinGecko API implementation
- `S3Extractor` - MinIO/S3 file downloads (JSON/CSV)

### 2. **Configuration**
- MinIO settings in `config.py` with smart properties
- Toggle between AWS S3 and MinIO via `USE_MINIO` flag
- Default credentials: `minioadmin` / `minioadmin`

### 3. **S3 Client**
- Renamed `aws.py` → `s3_client.py`
- Works with both AWS S3 and MinIO
- Methods: `upload_file()`, `download_file()`, `list_files()`, `get_file_content()`

### 4. **ETL Pipeline**
- Factory pattern for extractor selection
- Run with: `python etl_job.py <source> [file_key]`
- Sources: `coingecko`, `s3`

### 5. **Docker Setup**
- MinIO service added to `docker-compose.yml`
- API: `http://localhost:9000`
- Console: `http://localhost:9001`

## Quick Start

### 1. Start MinIO
```bash
docker-compose up -d minio
```

### 2. Set Up Bucket & Upload Sample Data
```bash
python app/etl/setup_minio.py
```

### 3. Run ETL from Different Sources

**From CoinGecko API:**
```bash
python app/etl/pipeline/etl_job.py coingecko
```

**From MinIO:**
```bash
python app/etl/pipeline/etl_job.py s3 crypto_data.json
```

## MinIO Console

Access the web UI at: **http://localhost:9001**
- Username: `minioadmin`
- Password: `minioadmin`

Upload files, create buckets, and manage your S3-compatible storage!

## Adding New API Sources

To add a new API (e.g., CoinMarketCap):

1. Create `coinmarketcap_extractor.py`:
```python
from app.etl.services.api_extractor import APIExtractor

class CoinMarketCapExtractor(APIExtractor):
    def __init__(self):
        super().__init__(base_url="https://api.coinmarketcap.com/v1")
    
    def extract(self):
        return self.get("/cryptocurrency/listings/latest")
```

2. Register in `etl_job.py`:
```python
extractors = {
    'coingecko': lambda: CoinGeckoExtractor(),
    'coinmarketcap': lambda: CoinMarketCapExtractor(),  # Add this
    's3': lambda: S3Extractor(kwargs.get('file_key')),
}
```

3. Run: `python etl_job.py coinmarketcap`

That's it! 🚀
