import os


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "raw-events")

CSV_PATH = os.getenv("CSV_PATH", "training-pipeline/data/Reviews.csv")
PRODUCER_RATE_SECONDS = float(os.getenv("PRODUCER_RATE_SECONDS", "1.0"))
MAX_ROWS = int(os.getenv("MAX_ROWS", "1000"))