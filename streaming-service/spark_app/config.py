import os


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "raw-events")

ML_SERVICE_URL = os.getenv("ML_SERVICE_URL", "http://ml-service:8000/predict-batch")
API_TIMEOUT_SECONDS = int(os.getenv("API_TIMEOUT_SECONDS", "30"))
API_BATCH_SIZE = int(os.getenv("API_BATCH_SIZE", "32"))

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017/mlplatform")
MONGO_DB = os.getenv("MONGO_DB", "mlplatform")
MONGO_PREDICTIONS_COLLECTION = os.getenv(
    "MONGO_PREDICTIONS_COLLECTION",
    "scored_reviews",
)
MONGO_FAILED_COLLECTION = os.getenv(
    "MONGO_FAILED_COLLECTION",
    "failed_records",
)

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "postgres")
POSTGRES_USER = os.getenv("POSTGRES_USER", "mluser")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "mlpassword123")

CHECKPOINT_LOCATION = os.getenv(
    "CHECKPOINT_LOCATION",
    "/tmp/spark-checkpoints/reviews-stream",
)