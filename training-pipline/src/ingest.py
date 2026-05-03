"""
training-pipeline/src/ingestion.py

Airflow task: ingest_data
Loads Amazon Food Reviews from  or CSV (dev/test).
Returns a raw DataFrame saved to /tmp as parquet for downstream tasks.
"""

import logging
import os

import pandas as pd
from pymongo import MongoClient

logger = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────
REQUIRED_COLUMNS = ["Id", "ProductId", "UserId", "Score", "Summary", "Text",
                    "HelpfulnessNumerator", "HelpfulnessDenominator", "Time"]

RAW_OUTPUT_PATH = "/tmp/raw_reviews.parquet"


# ── Main task function ─────────────────────────────────────────────────────────
def ingest_data(
    source: str = "csv",                          # "csv" | "mongodb"
    data_path: str = "Reviews.csv",               # used when source="csv"
    mongo_uri: str = None,                        # used when source="mongodb"
    mongo_db: str = "mlplatform",
    mongo_collection: str = "processed_events",
    sample_size: int = 5000,                      # None = full dataset
    output_path: str = RAW_OUTPUT_PATH,
) -> str:
    """
    Ingest raw Amazon Food Reviews and persist as parquet.

    Supports two sources:
      - "csv"     : local CSV file (dev / notebook parity)
      - "mongodb" : production source — reads from MongoDB collection

    Args:
        source:           "csv" or "mongodb"
        data_path:        path to Reviews.csv (csv mode only)
        mongo_uri:        MongoDB connection string (mongodb mode only)
                          defaults to env var MONGO_URI
        mongo_db:         database name
        mongo_collection: collection name
        sample_size:      cap rows (None = all). Mirrors original notebook
                          cell 60: data = data.head(5000)
        output_path:      where to write the parquet file

    Returns:
        output_path  (passed via Airflow XCom to next task)
    """
    df = _load_from_source(source, data_path, mongo_uri, mongo_db, mongo_collection)

    df = _validate(df)

    if sample_size:
        df = df.head(sample_size)
        logger.info(f"Sampled to {len(df):,} rows")

    df.to_parquet(output_path, index=False)
    logger.info(f"Raw data saved → {output_path}  ({len(df):,} rows, {df.shape[1]} cols)")

    return output_path


# ── Helpers ───────────────────────────────────────────────────────────────────
def _load_from_source(source, data_path, mongo_uri, mongo_db, mongo_collection) -> pd.DataFrame:
    if source == "csv":
        logger.info(f"Loading from CSV: {data_path}")
        df = pd.read_csv(data_path)
        logger.info(f"Loaded {len(df):,} rows from CSV")
        return df

    if source == "mongodb":
        uri = mongo_uri or os.environ.get("MONGO_URI", "mongodb://mongo:27017/mlplatform")
        logger.info(f"Loading from MongoDB: {uri} / {mongo_db}.{mongo_collection}")
        client = MongoClient(uri)
        records = list(client[mongo_db][mongo_collection].find({}, {"_id": 0}))
        client.close()
        df = pd.DataFrame(records)
        logger.info(f"Loaded {len(df):,} documents from MongoDB")
        return df

    raise ValueError(f"Unknown source '{source}'. Use 'csv' or 'mongodb'.")


def _validate(df: pd.DataFrame) -> pd.DataFrame:
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    before = len(df)
    df = df.dropna(subset=["Text", "Score"])
    dropped = before - len(df)
    if dropped:
        logger.warning(f"Dropped {dropped} rows with null Text or Score")

    df["Score"] = df["Score"].astype(int)
    return df