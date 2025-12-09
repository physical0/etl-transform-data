import boto3
import json
from app.etl.core.config import settings
from app.etl.core.logging_config import logger

class AWSClient:
    def __init__(self):
        self.s3 = boto3.client(
            "s3",
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
            region_name=settings.AWS_REGION
        )
        self.secrets = boto3.client(
            "secretsmanager",
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
            region_name=settings.AWS_REGION
        )

    def upload_file(self, file_path, object_name=None):
        if object_name is None:
            object_name = file_path
        try:
            self.s3.upload_file(file_path, settings.S3_BUCKET_NAME, object_name)
            logger.info(f"Uploaded {file_path} to {settings.S3_BUCKET_NAME}/{object_name}")
        except Exception as e:
            logger.error(f"Failed to upload file: {e}")
            raise

    def get_secret(self, secret_name):
        try:
            response = self.secrets.get_secret_value(SecretId=secret_name)
            return json.loads(response['SecretString'])
        except Exception as e:
            logger.error(f"Failed to get secret {secret_name}: {e}")
            raise

aws_client = AWSClient()
