"""
training-pipeline/src/train.py
Airflow task: train_task

Runs VADER and RoBERTa inference on preprocessed reviews.
Opens the MLflow run. Saves scored parquet for evaluate.py.

"Training" here = running inference with two sentiment models:
  - VADER: lexicon-based, no fitting required
  - RoBERTa: pretrained transformer, zero-shot inference
"""

import logging
import os

import mlflow
import nltk
import pandas as pd
import torch
from nltk.sentiment import SentimentIntensityAnalyzer
from scipy.special import softmax
from tqdm.auto import tqdm
from transformers import AutoModelForSequenceClassification, AutoTokenizer

from config import CFG

logger = logging.getLogger(__name__)

nltk.download("vader_lexicon", quiet=True)

# ── Output paths ──────────────────────────────────────────────────────────────
SCORED_OUTPUT_PATH = "/tmp/scored_reviews.parquet"
RUN_ID_PATH        = "/tmp/mlflow_run_id.txt"

# ── RoBERTa label mapping (notebook cell 95) ──────────────────────────────────
_ROBERTA_COL_TO_LABEL = {
    "roberta_neg": "Negative",
    "roberta_neu": "Neutral",
    "roberta_pos": "Positive",
}


def train_model(
    input_path:  str = "/tmp/preprocessed_reviews.parquet",
    output_path: str = SCORED_OUTPUT_PATH,
) -> dict:
    """
    Run VADER + RoBERTa on all reviews. Opens MLflow run.

    Config keys used:
      vader.pos_threshold / neg_threshold
      roberta.model / max_len
      mlflow.tracking_uri / experiment / run_name

    Returns:
        dict with scored_path and mlflow_run_id
    """
    logger.info("[TRAIN] START")
    df = pd.read_parquet(input_path)
    logger.info(f"[TRAIN] Loaded {len(df):,} reviews")

    # ── Configure MLflow ──────────────────────────────────────────────────────
    tracking_uri = os.environ.get(
        "MLFLOW_TRACKING_URI", CFG["mlflow"]["tracking_uri"]
    )
    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment(CFG["mlflow"]["experiment"])

    with mlflow.start_run(run_name=CFG["mlflow"]["run_name"]) as run:
        run_id = run.info.run_id
        logger.info(f"[TRAIN] MLflow run started — run_id: {run_id}")

        # Log params
        mlflow.log_param("sample_size",         len(df))
        mlflow.log_param("vader_pos_threshold",  CFG["vader"]["pos_threshold"])
        mlflow.log_param("vader_neg_threshold",  CFG["vader"]["neg_threshold"])
        mlflow.log_param("roberta_model",        CFG["roberta"]["model"])
        mlflow.log_param("roberta_max_len",      CFG["roberta"]["max_len"])
        mlflow.log_param("label_mapping",        "score<3=Neg, score=3=Neu, score>3=Pos")

        # ── Step 1: VADER ─────────────────────────────────────────────────────
        df = _run_vader(df)

        # ── Step 2: RoBERTa ───────────────────────────────────────────────────
        df = _run_roberta(df)

    # Persist run_id for evaluate + register tasks
    with open(RUN_ID_PATH, "w") as f:
        f.write(run_id)

    df.to_parquet(output_path, index=False)
    logger.info(f"[TRAIN] END — scored data saved → {output_path}")

    return {"scored_path": output_path, "mlflow_run_id": run_id}


# ── VADER ─────────────────────────────────────────────────────────────────────
def _run_vader(df: pd.DataFrame) -> pd.DataFrame:
    """
    Score each review with VADER SentimentIntensityAnalyzer.
    Adds: vader_neg, vader_neu, vader_pos, vader_compound, Vader_Prediction.
    Mirrors notebook cells 37–38 and 91–92.
    """
    pos_threshold = CFG["vader"]["pos_threshold"]
    neg_threshold = CFG["vader"]["neg_threshold"]
    sia = SentimentIntensityAnalyzer()

    logger.info("[TRAIN] Running VADER...")

    scores = df["Text"].apply(lambda text: sia.polarity_scores(str(text)))
    scores_df = pd.DataFrame(list(scores), index=df.index).rename(columns={
        "neg": "vader_neg",
        "neu": "vader_neu",
        "pos": "vader_pos",
        "compound": "vader_compound",
    })

    def _map_compound(compound: float) -> str:
        if compound >= pos_threshold:
            return "Positive"
        elif compound <= neg_threshold:
            return "Negative"
        else:
            return "Neutral"

    scores_df["Vader_Prediction"] = scores_df["vader_compound"].apply(_map_compound)
    df = pd.concat([df, scores_df], axis=1)

    logger.info(
        f"[TRAIN] VADER done:\n"
        f"{df['Vader_Prediction'].value_counts().to_string()}"
    )
    return df


# ── RoBERTa ───────────────────────────────────────────────────────────────────
def _run_roberta(df: pd.DataFrame) -> pd.DataFrame:
    """
    Score each review with cardiffnlp/twitter-roberta-base-sentiment.
    Adds: roberta_neg, roberta_neu, roberta_pos, Roberta_Prediction.
    Mirrors notebook cells 52–53, 59–62, 94–96.
    """
    model_name = CFG["roberta"]["model"]
    max_len    = CFG["roberta"]["max_len"]

    logger.info(f"[TRAIN] Loading RoBERTa: {model_name}")
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    model     = AutoModelForSequenceClassification.from_pretrained(model_name)
    model.eval()

    logger.info(f"[TRAIN] Running RoBERTa on {len(df):,} reviews...")

    results = []
    for text in tqdm(df["Text"], total=len(df), desc="RoBERTa"):
        results.append(_score_single(str(text), tokenizer, model, max_len))

    scores_df = pd.DataFrame(results, index=df.index)

    # argmax → label (notebook cell 95)
    scores_df["Roberta_Prediction"] = (
        scores_df[["roberta_neg", "roberta_neu", "roberta_pos"]]
        .idxmax(axis=1)
        .map(_ROBERTA_COL_TO_LABEL)
    )

    df = pd.concat([df, scores_df], axis=1)

    logger.info(
        f"[TRAIN] RoBERTa done:\n"
        f"{df['Roberta_Prediction'].value_counts().to_string()}"
    )
    return df


def _score_single(text: str, tokenizer, model, max_len: int) -> dict:
    """Score one text. Truncates at max_len to avoid RuntimeError on long reviews."""
    try:
        encoded = tokenizer(
            text,
            return_tensors="pt",
            truncation=True,
            max_length=max_len,
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
        logger.warning(f"[TRAIN] Failed to score text: {e}")
        return {"roberta_neg": None, "roberta_neu": None, "roberta_pos": None}