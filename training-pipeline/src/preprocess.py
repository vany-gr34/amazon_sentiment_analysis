"""
training-pipeline/src/preprocess.py
Airflow task: preprocess_task

Applies the assignment 3-class label mapping.
No vectorization — VADER and RoBERTa both operate on raw text.
"""

import logging

import pandas as pd

from config import CFG

logger = logging.getLogger(__name__)

PREPROCESSED_OUTPUT_PATH = "/tmp/preprocessed_reviews.parquet"


def preprocess_data(
    input_path: str  = "/tmp/raw_reviews.parquet",
    output_path: str = PREPROCESSED_OUTPUT_PATH,
) -> str:
    """
    Apply assignment label mapping and light text cleaning.

    Steps:
      1. Map Score → True_label  (assignment 3-class target)
           score < 3  → Negative
           score == 3 → Neutral
           score > 3  → Positive
      2. Clean text (strip whitespace, handle nulls)

    NOTE: No TF-IDF or tokenization here.
    VADER and RoBERTa both need raw natural language text.

    Returns:
        output_path
    """
    logger.info("[PREPROCESS] START")
    df = pd.read_parquet(input_path)
    logger.info(f"[PREPROCESS] Loaded {len(df):,} rows")

    df = _map_labels(df)
    df = _clean_text(df)

    logger.info(
        f"[PREPROCESS] Label distribution:\n"
        f"{df['True_label'].value_counts().to_string()}"
    )

    df.to_parquet(output_path, index=False)
    logger.info(f"[PREPROCESS] END — saved → {output_path}")
    return output_path


def map_true_score(score: int) -> str:
    """
    Assignment target mapping.
      score < 3  → Negative
      score == 3 → Neutral
      score > 3  → Positive
    """
    if score < 3:
        return "Negative"
    elif score == 3:
        return "Neutral"
    else:
        return "Positive"


def _map_labels(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["True_label"] = df[CFG["labels"]["score_column"]].apply(map_true_score)
    return df


def _clean_text(df: pd.DataFrame) -> pd.DataFrame:
    """Minimal cleaning — preserve natural language for VADER and RoBERTa."""
    df = df.copy()
    df["Text"]    = df["Text"].fillna("").str.strip()
    df["Summary"] = df["Summary"].fillna("").str.strip()
    return df