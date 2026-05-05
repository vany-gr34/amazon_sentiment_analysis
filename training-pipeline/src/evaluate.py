"""
training-pipeline/src/evaluate.py
Airflow task: evaluate_task

Computes classification metrics for VADER and RoBERTa.
Logs metrics and artifacts to the MLflow run opened by train.py.
"""

import json
import logging
import os

import mlflow
import pandas as pd
from sklearn.metrics import classification_report

from config import CFG

logger = logging.getLogger(__name__)

CLASSES          = ["Negative", "Neutral", "Positive"]
METRICS_PATH     = "/tmp/metrics.json"
PARAMS_PATH      = "/tmp/params.json"
RESULTS_CSV_PATH = "/tmp/scored_reviews.csv"
RUN_ID_PATH      = "/tmp/mlflow_run_id.txt"
ACCURACY_PATH    = "/tmp/accuracy.txt"          # read by register.py


def evaluate_model(
    input_path: str = "/tmp/scored_reviews.parquet",
) -> dict:
    """
    Evaluate VADER and RoBERTa against True_label.

    Metrics logged per model:
      accuracy, weighted precision/recall/f1,
      per-class precision/recall/f1/support (Neg/Neu/Pos)

    Artifacts logged to MLflow → pushed to MinIO:
      metrics.json, params.json, dataset.parquet, scored_reviews.csv

    Returns:
        {"vader": report_dict, "roberta": report_dict}
    """
    logger.info("[EVALUATE] START")
    df = pd.read_parquet(input_path)
    logger.info(f"[EVALUATE] Evaluating on {len(df):,} rows")

    reports = _compute_reports(df)
    _log_to_console(reports)
    _save_locally(df, reports)
    _log_to_mlflow(reports, input_path)

    logger.info("[EVALUATE] END")
    return reports


# ── Metrics ───────────────────────────────────────────────────────────────────
def _compute_reports(df: pd.DataFrame) -> dict:
    return {
        "vader": classification_report(
            df["True_label"], df["Vader_Prediction"],
            labels=CLASSES, output_dict=True, zero_division=0,
        ),
        "roberta": classification_report(
            df["True_label"], df["Roberta_Prediction"],
            labels=CLASSES, output_dict=True, zero_division=0,
        ),
    }


def _log_to_console(reports: dict) -> None:
    for model_key, pred_col in [("vader", "Vader_Prediction"),
                                 ("roberta", "Roberta_Prediction")]:
        r = reports[model_key]
        logger.info(
            f"\n[EVALUATE] {model_key.upper()} — "
            f"accuracy={r['accuracy']:.4f} | "
            f"weighted_f1={r['weighted avg']['f1-score']:.4f}\n" +
            f"  Negative  F1={r['Negative']['f1-score']:.3f}  "
            f"Neutral F1={r['Neutral']['f1-score']:.3f}  "
            f"Positive F1={r['Positive']['f1-score']:.3f}"
        )


# ── Persist locally ───────────────────────────────────────────────────────────
def _save_locally(df: pd.DataFrame, reports: dict) -> None:
    flat_metrics = {}
    for model_key, report in reports.items():
        flat_metrics[f"{model_key}_accuracy"] = round(report["accuracy"], 4)
        flat_metrics[f"{model_key}_weighted_f1"] = round(
            report["weighted avg"]["f1-score"], 4)
        for cls in CLASSES:
            key = cls.lower()
            flat_metrics[f"{model_key}_f1_{key}"]        = round(report[cls]["f1-score"],  4)
            flat_metrics[f"{model_key}_precision_{key}"]  = round(report[cls]["precision"], 4)
            flat_metrics[f"{model_key}_recall_{key}"]     = round(report[cls]["recall"],    4)
            flat_metrics[f"{model_key}_support_{key}"]    = int(report[cls]["support"])

    params = {
        "vader_pos_threshold": CFG["vader"]["pos_threshold"],
        "vader_neg_threshold": CFG["vader"]["neg_threshold"],
        "roberta_model":       CFG["roberta"]["model"],
        "roberta_max_len":     CFG["roberta"]["max_len"],
        "sample_size":         CFG["data"]["sample_size"],
        "label_mapping":       "score<3=Neg, score=3=Neu, score>3=Pos",
    }

    with open(METRICS_PATH, "w") as f:
        json.dump(flat_metrics, f, indent=2)
    with open(PARAMS_PATH, "w") as f:
        json.dump(params, f, indent=2)

    # Persist best accuracy (RoBERTa) for register.py
    with open(ACCURACY_PATH, "w") as f:
        f.write(str(round(reports["roberta"]["accuracy"], 4)))

    df.to_csv(RESULTS_CSV_PATH, index=False)
    logger.info(f"[EVALUATE] Artifacts saved locally")


# ── MLflow ────────────────────────────────────────────────────────────────────
def _log_to_mlflow(reports: dict, dataset_path: str) -> None:
    tracking_uri = os.environ.get(
        "MLFLOW_TRACKING_URI", CFG["mlflow"]["tracking_uri"]
    )
    mlflow.set_tracking_uri(tracking_uri)

    run_id = _read_run_id()

    with mlflow.start_run(run_id=run_id):

        # Metrics — both models
        for model_key, report in reports.items():
            mlflow.log_metric(f"{model_key}_accuracy",
                              round(report["accuracy"], 4))
            mlflow.log_metric(f"{model_key}_weighted_f1",
                              round(report["weighted avg"]["f1-score"], 4))
            mlflow.log_metric(f"{model_key}_weighted_precision",
                              round(report["weighted avg"]["precision"], 4))
            mlflow.log_metric(f"{model_key}_weighted_recall",
                              round(report["weighted avg"]["recall"], 4))

            for cls in CLASSES:
                key = cls.lower()
                mlflow.log_metric(f"{model_key}_f1_{key}",
                                  round(report[cls]["f1-score"],  4))
                mlflow.log_metric(f"{model_key}_precision_{key}",
                                  round(report[cls]["precision"], 4))
                mlflow.log_metric(f"{model_key}_recall_{key}",
                                  round(report[cls]["recall"],    4))
                mlflow.log_metric(f"{model_key}_support_{key}",
                                  int(report[cls]["support"]))

        # Artifacts → MinIO
        mlflow.log_artifact(METRICS_PATH,     artifact_path="reports")
        mlflow.log_artifact(PARAMS_PATH,      artifact_path="reports")
        mlflow.log_artifact(RESULTS_CSV_PATH, artifact_path="data")
        mlflow.log_artifact(dataset_path,     artifact_path="data")

    logger.info("[EVALUATE] Metrics and artifacts logged to MLflow")


def _read_run_id() -> str:
    with open(RUN_ID_PATH) as f:
        return f.read().strip()