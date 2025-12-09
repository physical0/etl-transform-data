import boto3
import json
import os
from app.etl.core.config import settings
from app.etl.core.logging_config import logger

class S3Client:
    """S3-compatible client that works with both AWS S3 and MinIO"""
    
    def __init__(self):
        # Configure boto3 client for either AWS S3 or MinIO
        client_config = {
            "aws_access_key_id": settings.S3_ACCESS_KEY,
            "aws_secret_access_key": settings.S3_SECRET_KEY,
        }
        
        # Add endpoint_url for MinIO
        if settings.S3_ENDPOINT:
            client_config["endpoint_url"] = settings.S3_ENDPOINT
        
        # Add region for AWS
        if not settings.USE_MINIO:
            client_config["region_name"] = settings.AWS_REGION
        
        self.s3 = boto3.client("s3", **client_config)
        self.bucket_name = settings.BUCKET_NAME
        
        # Initialize Secrets Manager only for AWS (not needed for MinIO)
        if not settings.USE_MINIO and settings.AWS_ACCESS_KEY_ID:
            self.secrets = boto3.client(
                "secretsmanager",
                aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
                aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
                region_name=settings.AWS_REGION
            )
        else:
            self.secrets = None

    def upload_file(self, file_path, object_name=None):
        """Upload a file to S3/MinIO bucket"""
        if object_name is None:
            object_name = os.path.basename(file_path)
        try:
            self.s3.upload_file(file_path, self.bucket_name, object_name)
            logger.info(f"Uploaded {file_path} to {self.bucket_name}/{object_name}")
        except Exception as e:
            logger.error(f"Failed to upload file: {e}")
            raise
    
    def download_file(self, object_name, file_path=None):
        """Download a file from S3/MinIO bucket"""
        if file_path is None:
            file_path = object_name
        try:
            self.s3.download_file(self.bucket_name, object_name, file_path)
            logger.info(f"Downloaded {self.bucket_name}/{object_name} to {file_path}")
            return file_path
        except Exception as e:
            logger.error(f"Failed to download file: {e}")
            raise
    
    def list_files(self, prefix=""):
        """List files in S3/MinIO bucket with optional prefix filter"""
        try:
            response = self.s3.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix)
            if 'Contents' not in response:
                logger.info(f"No files found in {self.bucket_name} with prefix '{prefix}'")
                return []
            
            files = [obj['Key'] for obj in response['Contents']]
            logger.info(f"Found {len(files)} files in {self.bucket_name}")
            return files
        except Exception as e:
            logger.error(f"Failed to list files: {e}")
            raise
    
    def get_file_content(self, object_name):
        """Get file content directly from S3/MinIO without downloading to disk"""
        try:
            response = self.s3.get_object(Bucket=self.bucket_name, Key=object_name)
            content = response['Body'].read()
            logger.info(f"Retrieved content from {self.bucket_name}/{object_name}")
            return content
        except Exception as e:
            logger.error(f"Failed to get file content: {e}")
            raise

    def get_secret(self, secret_name):
        """Get secret from AWS Secrets Manager (AWS only)"""
        if self.secrets is None:
            raise NotImplementedError("Secrets Manager is only available for AWS S3, not MinIO")
        
        try:
            response = self.secrets.get_secret_value(SecretId=secret_name)
            return json.loads(response['SecretString'])
        except Exception as e:
            logger.error(f"Failed to get secret {secret_name}: {e}")
            raise

s3_client = S3Client()
