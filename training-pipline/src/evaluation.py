"""
training-pipeline/src/evaluation.py

Airflow task: evaluate_models
Computes classification metrics for VADER and RoBERTa,
logs everything to MLflow, saves JSON reports.
"""

import json
import logging
import os

import mlflow
import pandas as pd
from sklearn.metrics import classification_report

logger = logging.getLogger(__name__)

CLASSES = ["Negative", "Neutral", "Positive"]

REPORT_VADER_PATH   = "/tmp/report_vader.json"
REPORT_ROBERTA_PATH = "/tmp/report_roberta.json"


def evaluate_models(
    input_path: str          = "/tmp/scored_reviews.parquet",
    mlflow_tracking_uri: str = None,
    experiment_name: str     = "sentiment-analysis",
) -> dict:
    """
    Compute classification metrics for VADER and RoBERTa,
    log to MLflow, and save JSON reports.

    Metrics logged per model:
      - accuracy
      - weighted F1
      - per-class precision, recall, F1, support
        (Negative / Neutral / Positive)

    Args:
        input_path:          merged scored parquet from merge_scores()
        mlflow_tracking_uri: e.g. "http://mlflow:5000"
        experiment_name:     MLflow experiment name

    Returns:
        {"vader": report_dict, "roberta": report_dict}
    """
    df = pd.read_parquet(input_path)
    logger.info(f"Evaluating on {len(df):,} rows")

    reports = _compute_reports(df)
    _save_reports(reports)
    _log_to_mlflow(df, reports, mlflow_tracking_uri, experiment_name)

    return reports


# ── Metrics ───────────────────────────────────────────────────────────────────
def _compute_reports(df: pd.DataFrame) -> dict:
    report_vader = classification_report(
        df["True_label"], df["Vader_Prediction"],
        labels=CLASSES, output_dict=True, zero_division=0,
    )
    report_roberta = classification_report(
        df["True_label"], df["Roberta_Prediction"],
        labels=CLASSES, output_dict=True, zero_division=0,
    )

    logger.info("\n--- VADER ---\n" +
                classification_report(df["True_label"], df["Vader_Prediction"],
                                      labels=CLASSES, zero_division=0))
    logger.info("\n--- RoBERTa ---\n" +
                classification_report(df["True_label"], df["Roberta_Prediction"],
                                      labels=CLASSES, zero_division=0))

    return {"vader": report_vader, "roberta": report_roberta}


# ── Persist ───────────────────────────────────────────────────────────────────
def _save_reports(reports: dict) -> None:
    with open(REPORT_VADER_PATH, "w") as f:
        json.dump(reports["vader"], f, indent=2)
    with open(REPORT_ROBERTA_PATH, "w") as f:
        json.dump(reports["roberta"], f, indent=2)
    logger.info(f"Reports saved -> {REPORT_VADER_PATH}, {REPORT_ROBERTA_PATH}")


# ── MLflow ────────────────────────────────────────────────────────────────────
def _log_to_mlflow(
    df: pd.DataFrame,
    reports: dict,
    tracking_uri: str,
    experiment_name: str,
) -> None:
    uri = tracking_uri or os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000")
    mlflow.set_tracking_uri(uri)
    mlflow.set_experiment(experiment_name)

    with mlflow.start_run(run_name="sentiment-evaluation"):

        # params
        mlflow.log_param("sample_size",   len(df))
        mlflow.log_param("roberta_model", os.environ.get(
            "ROBERTA_MODEL", "cardiffnlp/twitter-roberta-base-sentiment"))
        mlflow.log_param("label_mapping", "score<3=Neg, score=3=Neu, score>3=Pos")

        # per-class metrics — both models
        for model_key in ("vader", "roberta"):
            report = reports[model_key]

            mlflow.log_metric(f"{model_key}_accuracy",           round(report["accuracy"], 4))
            mlflow.log_metric(f"{model_key}_weighted_f1",        round(report["weighted avg"]["f1-score"],  4))
            mlflow.log_metric(f"{model_key}_weighted_precision",  round(report["weighted avg"]["precision"], 4))
            mlflow.log_metric(f"{model_key}_weighted_recall",     round(report["weighted avg"]["recall"],    4))

            for cls in CLASSES:
                key = cls.lower()
                mlflow.log_metric(f"{model_key}_f1_{key}",        round(report[cls]["f1-score"],  4))
                mlflow.log_metric(f"{model_key}_precision_{key}",  round(report[cls]["precision"], 4))
                mlflow.log_metric(f"{model_key}_recall_{key}",     round(report[cls]["recall"],    4))
                mlflow.log_metric(f"{model_key}_support_{key}",    int(report[cls]["support"]))

        # reports as artifacts
        mlflow.log_artifact(REPORT_VADER_PATH,   artifact_path="reports")
        mlflow.log_artifact(REPORT_ROBERTA_PATH, artifact_path="reports")

        logger.info(
            f"MLflow run complete — "
            f"RoBERTa accuracy={reports['roberta']['accuracy']:.4f} | "
            f"weighted_f1={reports['roberta']['weighted avg']['f1-score']:.4f}"
        )