"""
training-pipeline/src/register.py
Airflow task: register_task

Conditionally registers the RoBERTa run as an artifact in MLflow.
Registers only if RoBERTa accuracy >= registry.metric_threshold.

Note: RoBERTa is a pretrained HuggingFace model — we don't log it
as an mlflow.sklearn model. Instead we log the run metadata and
model name so the ml-service can load it directly from HuggingFace.
"""

import logging
import os

import mlflow
from mlflow import MlflowClient

from config import CFG

logger = logging.getLogger(__name__)

RUN_ID_PATH   = "/tmp/mlflow_run_id.txt"
ACCURACY_PATH = "/tmp/accuracy.txt"   # RoBERTa accuracy written by evaluate.py


def register_model() -> dict:
    """
    Register the run in MLflow Model Registry if accuracy >= threshold.
    Transitions to configured stage (Staging by default).

    Config keys used:
      registry.model_name
      registry.metric_threshold
      registry.stage
      mlflow.tracking_uri

    Returns:
        dict with registered (bool), version, stage, accuracy
    """
    logger.info("[REGISTER] START")

    accuracy  = _read_accuracy()
    threshold = CFG["registry"]["metric_threshold"]
    run_id    = _read_run_id()

    logger.info(
        f"[REGISTER] RoBERTa accuracy={accuracy:.4f} | threshold={threshold}"
    )

    if accuracy < threshold:
        logger.warning(
            f"[REGISTER] SKIPPED — accuracy {accuracy:.4f} < threshold {threshold}. "
            f"Model NOT registered."
        )
        return {"registered": False, "accuracy": accuracy, "threshold": threshold}

    tracking_uri = os.environ.get(
        "MLFLOW_TRACKING_URI", CFG["mlflow"]["tracking_uri"]
    )
    mlflow.set_tracking_uri(tracking_uri)
    client     = MlflowClient()
    model_name = CFG["registry"]["model_name"]
    stage      = CFG["registry"]["stage"]

    # Log RoBERTa model name as a tag on the run so ml-service knows which
    # HuggingFace model to load — no binary artifact needed
    with mlflow.start_run(run_id=run_id):
        mlflow.set_tag("roberta_model",    CFG["roberta"]["model"])
        mlflow.set_tag("roberta_accuracy", str(round(accuracy, 4)))
        mlflow.set_tag("registered",       "true")

    # Register the run itself under the model name
    model_uri = f"runs:/{run_id}/model"
    logger.info(f"[REGISTER] Registering '{model_name}' from run {run_id}")

    try:
        result  = mlflow.register_model(model_uri=model_uri, name=model_name)
        version = result.version

        client.transition_model_version_stage(
            name=model_name,
            version=version,
            stage=stage,
            archive_existing_versions=True,
        )

        client.update_model_version(
            name=model_name,
            version=version,
            description=(
                f"VADER + RoBERTa ({CFG['roberta']['model']}) | "
                f"roberta_accuracy={accuracy:.4f} | run_id={run_id}"
            ),
        )

        logger.info(
            f"[REGISTER] END — '{model_name}' v{version} → '{stage}'"
        )

        return {
            "registered": True,
            "model_name": model_name,
            "version":    version,
            "stage":      stage,
            "accuracy":   accuracy,
            "run_id":     run_id,
        }

    except Exception as e:
        logger.error(f"[REGISTER] Registration failed: {e}")
        raise


def _read_accuracy() -> float:
    with open(ACCURACY_PATH) as f:
        return float(f.read().strip())


def _read_run_id() -> str:
    with open(RUN_ID_PATH) as f:
        return f.read().strip()