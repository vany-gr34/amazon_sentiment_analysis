"""
training-pipeline/src/ingest.py
Airflow task: ingest_task

Loads Reviews.csv, validates schema, saves raw parquet.
No model-specific logic here — pure data loading.
"""

import logging
import os

import pandas as pd

from config import CFG

logger = logging.getLogger(__name__)

REQUIRED_COLUMNS = ["Id", "ProductId", "UserId", "Score", "Summary", "Text",
                    "HelpfulnessNumerator", "HelpfulnessDenominator", "Time"]
RAW_OUTPUT_PATH  = "/tmp/raw_reviews.parquet"


def ingest_data(output_path: str = RAW_OUTPUT_PATH) -> str:
    """
    Load Reviews.csv → validate → persist as parquet.

    Config keys used:
      data.path        — path to Reviews.csv
      data.sample_size — optional row cap (mirrors notebook head(5000))

    Returns:
        output_path (passed to next task via Airflow XCom)
    """
    data_path   = CFG["data"]["path"]
    sample_size = CFG["data"]["sample_size"]

    logger.info(f"[INGEST] START — loading from: {data_path}")

    if not os.path.exists(data_path):
        raise FileNotFoundError(f"Data file not found: {data_path}")

    df = pd.read_csv(data_path)
    logger.info(f"[INGEST] Loaded {len(df):,} rows, {df.shape[1]} columns")

    df = _validate(df)

    if sample_size:
        df = df.head(sample_size)
        logger.info(f"[INGEST] Sampled to {len(df):,} rows")

    df.to_parquet(output_path, index=False)
    logger.info(f"[INGEST] END — saved → {output_path}  ({len(df):,} rows)")
    return output_path


def _validate(df: pd.DataFrame) -> pd.DataFrame:
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"[INGEST] Missing columns: {missing}")

    before = len(df)
    df = df.dropna(subset=["Text", "Score"])
    if len(df) < before:
        logger.warning(f"[INGEST] Dropped {before - len(df)} rows with null Text/Score")

    df["Score"] = df["Score"].astype(int)
    logger.info(f"[INGEST] Validation passed — {len(df):,} rows remain")
    return df