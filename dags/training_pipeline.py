"""
dags/training_dag.py

Sentiment Analysis Training Pipeline — Airflow DAG

Task graph:
  ingest → preprocess → [vader ∥ roberta] → merge → evaluate

VADER and RoBERTa run in parallel (fan-out after preprocess).
Merge joins both outputs before evaluation.
"""

import logging
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# ── Make src/ importable from the DAG file ────────────────────────────────────
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "training-pipeline", "src"))

from ingestion    import ingest_data
from preprocessing import preprocess_data
from training     import run_vader, run_roberta, merge_scores
from evaluation   import evaluate_models

logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════════════════
# CONFIG — change these without touching task logic
# ══════════════════════════════════════════════════════════════════════════════
CONFIG = {
    # ── Data source ───────────────────────────────────────────────────────────
    # "mongodb" in production, "csv" for local dev
    "source":          os.environ.get("DATA_SOURCE",  "mongodb"),
    "data_path":       os.environ.get("DATA_PATH",    "Reviews.csv"),
    "mongo_uri":       os.environ.get("MONGO_URI",    "mongodb://mongo:27017/mlplatform"),
    "mongo_db":        os.environ.get("MONGO_DB",     "mlplatform"),
    "mongo_collection": os.environ.get("MONGO_COLLECTION", "processed_events"),
    "sample_size":     int(os.environ.get("SAMPLE_SIZE", 5000)),

    # ── Model ─────────────────────────────────────────────────────────────────
    "roberta_model": os.environ.get(
        "ROBERTA_MODEL", "cardiffnlp/twitter-roberta-base-sentiment"
    ),

    # ── MLflow ────────────────────────────────────────────────────────────────
    "mlflow_tracking_uri": os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000"),
    "experiment_name":     os.environ.get("MLFLOW_EXPERIMENT",   "sentiment-analysis"),
}

# ══════════════════════════════════════════════════════════════════════════════
# DAG
# ══════════════════════════════════════════════════════════════════════════════
default_args = {
    "owner":            "ml-team",
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="sentiment_training_pipeline",
    default_args=default_args,
    description="VADER + RoBERTa sentiment scoring on Amazon food reviews",
    schedule="@weekly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["nlp", "sentiment", "roberta", "vader"],
) as dag:

    # ── Task 1: Ingest ────────────────────────────────────────────────────────
    t_ingest = PythonOperator(
        task_id="ingest_data",
        python_callable=ingest_data,
        op_kwargs={
            "source":           CONFIG["source"],
            "data_path":        CONFIG["data_path"],
            "mongo_uri":        CONFIG["mongo_uri"],
            "mongo_db":         CONFIG["mongo_db"],
            "mongo_collection": CONFIG["mongo_collection"],
            "sample_size":      CONFIG["sample_size"],
            "output_path":      "/tmp/raw_reviews.parquet",
        },
    )

    # ── Task 2: Preprocess ────────────────────────────────────────────────────
    t_preprocess = PythonOperator(
        task_id="preprocess_data",
        python_callable=preprocess_data,
        op_kwargs={
            "input_path":  "/tmp/raw_reviews.parquet",
            "output_path": "/tmp/preprocessed_reviews.parquet",
        },
    )

    # ── Task 3a: VADER (parallel) ─────────────────────────────────────────────
    t_vader = PythonOperator(
        task_id="run_vader",
        python_callable=run_vader,
        op_kwargs={
            "input_path":  "/tmp/preprocessed_reviews.parquet",
            "output_path": "/tmp/vader_scores.parquet",
        },
    )

    # ── Task 3b: RoBERTa (parallel) ───────────────────────────────────────────
    t_roberta = PythonOperator(
        task_id="run_roberta",
        python_callable=run_roberta,
        op_kwargs={
            "input_path":  "/tmp/preprocessed_reviews.parquet",
            "output_path": "/tmp/roberta_scores.parquet",
            "model_name":  CONFIG["roberta_model"],
        },
        # RoBERTa is slow — give it extra time before Airflow kills the task
        execution_timeout=timedelta(hours=2),
    )

    # ── Task 4: Merge ─────────────────────────────────────────────────────────
    t_merge = PythonOperator(
        task_id="merge_scores",
        python_callable=merge_scores,
        op_kwargs={
            "vader_path":   "/tmp/vader_scores.parquet",
            "roberta_path": "/tmp/roberta_scores.parquet",
            "output_path":  "/tmp/scored_reviews.parquet",
        },
    )

    # ── Task 5: Evaluate + MLflow ─────────────────────────────────────────────
    t_evaluate = PythonOperator(
        task_id="evaluate_models",
        python_callable=evaluate_models,
        op_kwargs={
            "input_path":          "/tmp/scored_reviews.parquet",
            "mlflow_tracking_uri": CONFIG["mlflow_tracking_uri"],
            "experiment_name":     CONFIG["experiment_name"],
        },
    )

    # ── DAG graph ─────────────────────────────────────────────────────────────
    #
    #   ingest → preprocess → vader   ──┐
    #                        → roberta ──┤ → merge → evaluate
    #
    t_ingest >> t_preprocess >> [t_vader, t_roberta] >> t_merge >> t_evaluate