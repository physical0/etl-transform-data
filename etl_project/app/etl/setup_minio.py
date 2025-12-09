"""
Helper script to set up MinIO bucket and upload sample data for testing.
Run this after starting MinIO with docker-compose.
"""
import boto3
import json
from app.etl.core.config import settings

def setup_minio():
    """Create bucket and upload sample crypto data to MinIO"""
    
    # Create S3 client for MinIO
    s3 = boto3.client(
        's3',
        endpoint_url=settings.S3_ENDPOINT,
        aws_access_key_id=settings.S3_ACCESS_KEY,
        aws_secret_access_key=settings.S3_SECRET_KEY
    )
    
    bucket_name = settings.BUCKET_NAME
    
    # Create bucket if it doesn't exist
    try:
        s3.head_bucket(Bucket=bucket_name)
        print(f"✓ Bucket '{bucket_name}' already exists")
    except:
        s3.create_bucket(Bucket=bucket_name)
        print(f"✓ Created bucket '{bucket_name}'")
    
    # Sample crypto data (matching CoinGecko API format)
    sample_data = [
        {
            "id": "bitcoin",
            "symbol": "btc",
            "name": "Bitcoin",
            "current_price": 43250.50,
            "market_cap": 845000000000,
            "total_volume": 25000000000,
            "last_updated": "2024-01-15T12:00:00.000Z"
        },
        {
            "id": "ethereum",
            "symbol": "eth",
            "name": "Ethereum",
            "current_price": 2280.75,
            "market_cap": 274000000000,
            "total_volume": 12000000000,
            "last_updated": "2024-01-15T12:00:00.000Z"
        },
        {
            "id": "cardano",
            "symbol": "ada",
            "name": "Cardano",
            "current_price": 0.52,
            "market_cap": 18000000000,
            "total_volume": 450000000,
            "last_updated": "2024-01-15T12:00:00.000Z"
        }
    ]
    
    # Upload sample JSON file
    json_data = json.dumps(sample_data, indent=2)
    s3.put_object(
        Bucket=bucket_name,
        Key='crypto_data.json',
        Body=json_data.encode('utf-8'),
        ContentType='application/json'
    )
    print(f"✓ Uploaded sample data to '{bucket_name}/crypto_data.json'")
    
    # List files in bucket
    response = s3.list_objects_v2(Bucket=bucket_name)
    if 'Contents' in response:
        print(f"\n✓ Files in bucket '{bucket_name}':")
        for obj in response['Contents']:
            print(f"  - {obj['Key']} ({obj['Size']} bytes)")
    
    print("\n✅ MinIO setup complete!")
    print(f"\n📊 MinIO Console: http://localhost:9001")
    print(f"   Username: minioadmin")
    print(f"   Password: minioadmin")

if __name__ == "__main__":
    setup_minio()
