"""
training-pipeline/src/training.py

Airflow tasks: run_vader  |  run_roberta
Runs VADER and RoBERTa inference on the preprocessed reviews.
Logs params and artifacts to MLflow. Writes scored parquet for evaluation.

Note: "training" here means running inference with two sentiment models
(one lexicon-based, one pretrained transformer) — not fitting from scratch.
"""

import logging
import os

import mlflow
import numpy as np
import pandas as pd
import torch
from nltk.sentiment import SentimentIntensityAnalyzer
from scipy.special import softmax
from tqdm.auto import tqdm
from transformers import AutoModelForSequenceClassification, AutoTokenizer

logger = logging.getLogger(__name__)

# ── Output paths ──────────────────────────────────────────────────────────────
VADER_OUTPUT_PATH   = "/tmp/vader_scores.parquet"
ROBERTA_OUTPUT_PATH = "/tmp/roberta_scores.parquet"
SCORED_OUTPUT_PATH  = "/tmp/scored_reviews.parquet"

# ── RoBERTa config (mirrors original notebook cell 53) ───────────────────────
ROBERTA_MODEL_NAME = os.environ.get(
    "ROBERTA_MODEL", "cardiffnlp/twitter-roberta-base-sentiment"
)
ROBERTA_MAX_LEN = 512  # hard tokenizer limit — truncate silently

# ── VADER thresholds (standard practice, original notebook cell 91) ──────────
VADER_POS_THRESHOLD =  0.05
VADER_NEG_THRESHOLD = -0.05

# ── RoBERTa label mapping (original notebook cell 95) ────────────────────────
ROBERTA_COL_TO_LABEL = {
    "roberta_neg": "Negative",
    "roberta_neu": "Neutral",
    "roberta_pos": "Positive",
}


# ══════════════════════════════════════════════════════════════════════════════
# TASK 1 — VADER
# ══════════════════════════════════════════════════════════════════════════════
def run_vader(
    input_path: str = "/tmp/preprocessed_reviews.parquet",
    output_path: str = VADER_OUTPUT_PATH,
    mlflow_run_id: str = None,
) -> str:
    """
    Score every review with VADER SentimentIntensityAnalyzer.

    Adapted from original notebook cells 37–38 and 91–92.
    Adds columns: vader_neg, vader_neu, vader_pos, vader_compound,
                  Vader_Prediction (Negative / Neutral / Positive)

    Args:
        input_path:    preprocessed parquet from preprocess_data()
        output_path:   where to write VADER-scored parquet
        mlflow_run_id: active MLflow run id (optional, for artifact logging)

    Returns:
        output_path
    """
    df = pd.read_parquet(input_path)
    logger.info(f"Running VADER on {len(df):,} reviews...")

    sia = SentimentIntensityAnalyzer()

    # ── Score each row (original notebook cell 37) ────────────────────────────
    scores = df["Text"].apply(lambda text: sia.polarity_scores(str(text)))
    scores_df = pd.DataFrame(list(scores), index=df.index)
    scores_df = scores_df.rename(columns={
        "neg": "vader_neg",
        "neu": "vader_neu",
        "pos": "vader_pos",
        "compound": "vader_compound",
    })

    # ── Predict label (original notebook cell 91) ────────────────────────────
    scores_df["Vader_Prediction"] = scores_df["vader_compound"].apply(_map_vader_label)

    df = pd.concat([df, scores_df], axis=1)

    _log_prediction_dist("VADER", df["Vader_Prediction"])

    # ── Log to MLflow ─────────────────────────────────────────────────────────
    if mlflow_run_id:
        with mlflow.start_run(run_id=mlflow_run_id, nested=True):
            mlflow.log_param("vader_pos_threshold", VADER_POS_THRESHOLD)
            mlflow.log_param("vader_neg_threshold", VADER_NEG_THRESHOLD)

    df.to_parquet(output_path, index=False)
    logger.info(f"VADER scores saved → {output_path}")
    return output_path


def _map_vader_label(compound: float) -> str:
    """Original notebook cell 91 — standard VADER compound thresholds."""
    if compound >= VADER_POS_THRESHOLD:
        return "Positive"
    elif compound <= VADER_NEG_THRESHOLD:
        return "Negative"
    else:
        return "Neutral"


# ══════════════════════════════════════════════════════════════════════════════
# TASK 2 — RoBERTa
# ══════════════════════════════════════════════════════════════════════════════
def run_roberta(
    input_path: str = "/tmp/preprocessed_reviews.parquet",
    output_path: str = ROBERTA_OUTPUT_PATH,
    model_name: str = ROBERTA_MODEL_NAME,
    mlflow_run_id: str = None,
) -> str:
    """
    Score every review with cardiffnlp/twitter-roberta-base-sentiment.

    Adapted from original notebook cells 52–53, 59–62, 94–96.
    Adds columns: roberta_neg, roberta_neu, roberta_pos,
                  Roberta_Prediction (Negative / Neutral / Positive)

    Args:
        input_path:    preprocessed parquet from preprocess_data()
        output_path:   where to write RoBERTa-scored parquet
        model_name:    HuggingFace model ID
        mlflow_run_id: active MLflow run id (optional)

    Returns:
        output_path
    """
    df = pd.read_parquet(input_path)
    logger.info(f"Loading RoBERTa model: {model_name}")

    tokenizer = AutoTokenizer.from_pretrained(model_name)
    model     = AutoModelForSequenceClassification.from_pretrained(model_name)
    model.eval()

    logger.info(f"Running RoBERTa on {len(df):,} reviews...")

    results = []
    for text in tqdm(df["Text"], total=len(df), desc="RoBERTa"):
        results.append(_score_single(str(text), tokenizer, model))

    scores_df = pd.DataFrame(results, index=df.index)

    # ── Argmax → label (original notebook cell 95) ───────────────────────────
    scores_df["Roberta_Prediction"] = (
        scores_df[["roberta_neg", "roberta_neu", "roberta_pos"]]
        .idxmax(axis=1)
        .map(ROBERTA_COL_TO_LABEL)
    )

    df = pd.concat([df, scores_df], axis=1)

    _log_prediction_dist("RoBERTa", df["Roberta_Prediction"])

    # ── Log to MLflow ─────────────────────────────────────────────────────────
    if mlflow_run_id:
        with mlflow.start_run(run_id=mlflow_run_id, nested=True):
            mlflow.log_param("roberta_model",   model_name)
            mlflow.log_param("roberta_max_len", ROBERTA_MAX_LEN)

    df.to_parquet(output_path, index=False)
    logger.info(f"RoBERTa scores saved → {output_path}")
    return output_path


def _score_single(text: str, tokenizer, model) -> dict:
    """
    Score one text. Truncates at 512 tokens to avoid RuntimeError
    on long reviews (original notebook had bare except RuntimeError).
    """
    try:
        encoded = tokenizer(
            text,
            return_tensors="pt",
            truncation=True,
            max_length=ROBERTA_MAX_LEN,
        )
        with torch.no_grad():
            output = model(**encoded)
        probs = softmax(output.logits[0].numpy())
        return {
            "roberta_neg": float(probs[0]),
            "roberta_neu": float(probs[1]),
            "roberta_pos": float(probs[2]),
        }
    except Exception as e:
        logger.warning(f"Failed to score text (len={len(text)}): {e}")
        return {"roberta_neg": None, "roberta_neu": None, "roberta_pos": None}


# ══════════════════════════════════════════════════════════════════════════════
# TASK 3 — MERGE (joins VADER + RoBERTa output into one DataFrame)
# ══════════════════════════════════════════════════════════════════════════════
def merge_scores(
    vader_path: str   = VADER_OUTPUT_PATH,
    roberta_path: str = ROBERTA_OUTPUT_PATH,
    output_path: str  = SCORED_OUTPUT_PATH,
) -> str:
    """
    Join VADER and RoBERTa scored DataFrames on common columns.
    Produces the final scored_reviews.parquet used by evaluation.

    Args:
        vader_path:   output of run_vader()
        roberta_path: output of run_roberta()
        output_path:  merged output path

    Returns:
        output_path
    """
    vader_df   = pd.read_parquet(vader_path)
    roberta_df = pd.read_parquet(roberta_path)

    # Both start from the same preprocessed base — only add RoBERTa columns
    roberta_cols = ["roberta_neg", "roberta_neu", "roberta_pos", "Roberta_Prediction"]
    df = vader_df.copy()
    df[roberta_cols] = roberta_df[roberta_cols].values

    df.to_parquet(output_path, index=False)
    logger.info(f"Merged scores saved → {output_path}  ({len(df):,} rows)")
    return output_path


# ── Shared helper ─────────────────────────────────────────────────────────────
def _log_prediction_dist(model_name: str, predictions: pd.Series) -> None:
    dist = predictions.value_counts()
    logger.info(f"{model_name} prediction distribution:\n{dist.to_string()}")