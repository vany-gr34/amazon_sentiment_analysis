"""
training-pipeline/src/register.py
Airflow task: register_task

Conditionally logs and registers the RoBERTa + VADER model in MLflow.

Registers only if RoBERTa accuracy >= registry.metric_threshold.

Important:
- This version DOES log a real MLflow pyfunc model.
- The model artifact is stored in MinIO through MLflow.
- The registered model can be loaded later by ml-service using:

    mlflow.pyfunc.load_model("models:/sentiment-roberta/Staging")
"""

import logging
import os

import mlflow
import mlflow.pyfunc
from mlflow import MlflowClient

from config import CFG
from roberta_pyfunc import VaderRobertaModel


logger = logging.getLogger(__name__)

RUN_ID_PATH = "/tmp/mlflow_run_id.txt"
ACCURACY_PATH = "/tmp/accuracy.txt"


def register_model() -> dict:
    """
    Register the run in MLflow Model Registry if accuracy >= threshold.

    Steps:
      1. Read RoBERTa accuracy from /tmp/accuracy.txt
      2. Read MLflow run_id from /tmp/mlflow_run_id.txt
      3. If accuracy is below threshold, skip registration
      4. Log a real MLflow pyfunc model under artifact_path="model"
      5. Register runs:/<run_id>/model in MLflow Model Registry
      6. Transition the registered model to configured stage

    Returns:
        dict with registered status, model version, stage, accuracy, run_id
    """

    logger.info("[REGISTER] START")

    accuracy = _read_accuracy()
    threshold = CFG["registry"]["metric_threshold"]
    run_id = _read_run_id()

    logger.info(
        f"[REGISTER] RoBERTa accuracy={accuracy:.4f} | threshold={threshold}"
    )

    if accuracy < threshold:
        logger.warning(
            f"[REGISTER] SKIPPED — accuracy {accuracy:.4f} < threshold {threshold}. "
            "Model NOT registered."
        )

        return {
            "registered": False,
            "accuracy": accuracy,
            "threshold": threshold,
            "run_id": run_id,
        }

    tracking_uri = os.environ.get(
        "MLFLOW_TRACKING_URI",
        CFG["mlflow"]["tracking_uri"],
    )

    mlflow.set_tracking_uri(tracking_uri)

    client = MlflowClient()

    model_name = CFG["registry"]["model_name"]
    stage = CFG["registry"]["stage"]
    roberta_model_name = CFG["roberta"]["model"]

    logger.info(f"[REGISTER] Tracking URI: {tracking_uri}")
    logger.info(f"[REGISTER] Model name: {model_name}")
    logger.info(f"[REGISTER] Stage: {stage}")
    logger.info(f"[REGISTER] RoBERTa model: {roberta_model_name}")

    try:
        with mlflow.start_run(run_id=run_id):
            mlflow.set_tag("roberta_model", roberta_model_name)
            mlflow.set_tag("roberta_accuracy", str(round(accuracy, 4)))
            mlflow.set_tag("registered", "true")
            mlflow.set_tag("model_type", "vader_roberta_pyfunc")

            mlflow.log_param("roberta_model", roberta_model_name)
            mlflow.log_metric("roberta_accuracy", accuracy)

            logger.info("[REGISTER] Logging MLflow pyfunc model to artifact_path='model'")

            mlflow.pyfunc.log_model(
                artifact_path="model",
                python_model=VaderRobertaModel(),
                model_config={
                    "roberta_model": roberta_model_name,
                },
                pip_requirements=[
                    "mlflow==2.13.0",
                    "pandas==2.2.2",
                    "numpy==1.26.4",
                    "scipy==1.13.1",
                    "nltk==3.8.1",
                    "torch==2.3.0",
                    "transformers==4.41.2",
                ],
            )

        model_uri = f"runs:/{run_id}/model"

        logger.info(f"[REGISTER] Registering '{model_name}' from URI: {model_uri}")

        result = mlflow.register_model(
            model_uri=model_uri,
            name=model_name,
        )

        version = result.version

        logger.info(
            f"[REGISTER] Registered model '{model_name}' as version {version}"
        )

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
                f"VADER + RoBERTa ({roberta_model_name}) | "
                f"roberta_accuracy={accuracy:.4f} | run_id={run_id}"
            ),
        )

        logger.info(
            f"[REGISTER] END — '{model_name}' v{version} → '{stage}'"
        )

        return {
            "registered": True,
            "model_name": model_name,
            "version": version,
            "stage": stage,
            "accuracy": accuracy,
            "run_id": run_id,
            "model_uri": model_uri,
        }

    except Exception as exc:
        logger.error(f"[REGISTER] Registration failed: {exc}")
        raise


def _read_accuracy() -> float:
    if not os.path.exists(ACCURACY_PATH):
        raise FileNotFoundError(
            f"Accuracy file not found: {ACCURACY_PATH}. "
            "Make sure evaluate.py writes /tmp/accuracy.txt before register.py runs."
        )

    with open(ACCURACY_PATH, "r", encoding="utf-8") as file:
        return float(file.read().strip())


def _read_run_id() -> str:
    if not os.path.exists(RUN_ID_PATH):
        raise FileNotFoundError(
            f"Run ID file not found: {RUN_ID_PATH}. "
            "Make sure train/evaluate writes /tmp/mlflow_run_id.txt before register.py runs."
        )

    with open(RUN_ID_PATH, "r", encoding="utf-8") as file:
        return file.read().strip()