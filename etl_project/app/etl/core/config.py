from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # Database Configuration
    DB_USER: str
    DB_PASSWORD: str
    DB_HOST: str
    DB_PORT: str
    DB_NAME: str
    
    # AWS S3 Configuration (for production)
    AWS_ACCESS_KEY_ID: str = ""
    AWS_SECRET_ACCESS_KEY: str = ""
    AWS_REGION: str = "us-east-1"
    S3_BUCKET_NAME: str = "my-crypto-bucket"
    
    # MinIO Configuration (for local development)
    USE_MINIO: bool = True
    MINIO_ENDPOINT: str = "localhost:9000"
    MINIO_ACCESS_KEY: str = "minioadmin"
    MINIO_SECRET_KEY: str = "minioadmin"
    MINIO_BUCKET_NAME: str = "crypto-data"
    MINIO_SECURE: bool = False  # Use HTTP for local development

    @property
    def DATABASE_URL(self):
        return f"postgresql://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
    
    @property
    def S3_ENDPOINT(self):
        """Returns the appropriate S3 endpoint based on USE_MINIO setting"""
        if self.USE_MINIO:
            return f"http://{self.MINIO_ENDPOINT}" if not self.MINIO_SECURE else f"https://{self.MINIO_ENDPOINT}"
        return None  # AWS S3 uses default endpoint
    
    @property
    def S3_ACCESS_KEY(self):
        """Returns the appropriate access key based on USE_MINIO setting"""
        return self.MINIO_ACCESS_KEY if self.USE_MINIO else self.AWS_ACCESS_KEY_ID
    
    @property
    def S3_SECRET_KEY(self):
        """Returns the appropriate secret key based on USE_MINIO setting"""
        return self.MINIO_SECRET_KEY if self.USE_MINIO else self.AWS_SECRET_ACCESS_KEY
    
    @property
    def BUCKET_NAME(self):
        """Returns the appropriate bucket name based on USE_MINIO setting"""
        return self.MINIO_BUCKET_NAME if self.USE_MINIO else self.S3_BUCKET_NAME

    class Config:
        env_file = ".env"
        env_file_encoding = 'utf-8'

settings = Settings()
