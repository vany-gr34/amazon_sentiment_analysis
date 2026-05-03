"""
training-pipeline/src/preprocessing.py

Airflow task: preprocess_data
Applies the assignment label mapping and basic text cleaning.
Reads raw parquet → writes preprocessed parquet.
"""

import logging

import pandas as pd

logger = logging.getLogger(__name__)

PREPROCESSED_OUTPUT_PATH = "/tmp/preprocessed_reviews.parquet"


# ── Main task function ─────────────────────────────────────────────────────────
def preprocess_data(
    input_path: str = "/tmp/raw_reviews.parquet",
    output_path: str = PREPROCESSED_OUTPUT_PATH,
) -> str:
    """
    Clean the raw data and apply the 3-class assignment label mapping.

    Steps (mirrors original notebook cells 86–87):
      1. Map Score → True_label  (assignment target)
           score < 3  → Negative
           score == 3 → Neutral
           score > 3  → Positive
      2. Clean text (strip whitespace, handle nulls)
      3. Add derived features used in EDA (text length, word count)

    Args:
        input_path:  parquet from ingest_data()
        output_path: where to write the cleaned parquet

    Returns:
        output_path
    """
    df = pd.read_parquet(input_path)
    logger.info(f"Loaded {len(df):,} rows for preprocessing")

    df = _map_labels(df)
    df = _clean_text(df)
    df = _add_derived_features(df)

    df.to_parquet(output_path, index=False)
    logger.info(f"Preprocessed data saved → {output_path}")
    logger.info(f"Label distribution:\n{df['True_label'].value_counts().to_string()}")

    return output_path


# ── Label engineering ─────────────────────────────────────────────────────────
def map_true_score(score: int) -> str:
    """
    Assignment target mapping. Original notebook cell 86.

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
    df["True_label"] = df["Score"].apply(map_true_score)
    return df


# ── Text cleaning ─────────────────────────────────────────────────────────────
def _clean_text(df: pd.DataFrame) -> pd.DataFrame:
    """
    Minimal cleaning — keep original text intact for VADER and RoBERTa,
    both of which work on raw natural language (no stemming/stopword removal).
    """
    df = df.copy()
    df["Text"]    = df["Text"].fillna("").str.strip()
    df["Summary"] = df["Summary"].fillna("").str.strip()
    return df


# ── Derived features (used by EDA and correlation analysis) ───────────────────
def _add_derived_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add text length and word count columns.
    Used in EDA (original notebook cells 17 & 20).
    """
    df = df.copy()
    df["text_len"]   = df["Text"].str.len()
    df["word_count"] = df["Text"].apply(lambda x: len(str(x).split()))
    df["helpfulness_ratio"] = (
        df["HelpfulnessNumerator"]
        / df["HelpfulnessDenominator"].replace(0, 1)
    )
    return df