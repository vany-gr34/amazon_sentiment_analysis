import os


class Settings:
    # MLflow
    MLFLOW_TRACKING_URI: str = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
    MLFLOW_S3_ENDPOINT_URL: str = os.getenv("MLFLOW_S3_ENDPOINT_URL", "http://minio:9000")

    # MinIO credentials are used by MLflow/boto3 internally
    AWS_ACCESS_KEY_ID: str = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
    AWS_SECRET_ACCESS_KEY: str = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin123")

    # Registered model
    MODEL_NAME: str = os.getenv("MODEL_NAME", "sentiment-model")
    MODEL_STAGE: str = os.getenv("MODEL_STAGE", "Staging")

    # MongoDB, optional for logging predictions later
    MONGO_URI: str = os.getenv("MONGO_URI", "mongodb://mongo:27017/mlplatform")

    # API
    API_NAME: str = "Amazon Sentiment ML Service"
    API_VERSION: str = "1.0.0"


settings = Settings()