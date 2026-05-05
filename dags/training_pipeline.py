"""
training-pipeline/dags/training_pipeline.py
Airflow DAG: training_pipeline

Tasks: ingest → preprocess → train → evaluate → register

train_task runs both VADER and RoBERTa inference.
Zero ML logic here — all logic lives in src/.
"""

import logging
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
sys.path.insert(0, '/opt/airflow/src')
from ingest    import ingest_data
from preprocess import preprocess_data
from train      import train_model
from evaluate   import evaluate_model
from register   import register_model

logger = logging.getLogger(__name__)

default_args = {
    "owner":            "ml-team",
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id       = "training_pipeline",
    default_args = default_args,
    description  = "VADER + RoBERTa sentiment analysis on Amazon food reviews",
    schedule     = "@weekly",
    start_date   = datetime(2024, 1, 1),
    catchup      = False,
    tags         = ["nlp", "sentiment", "vader", "roberta"],
) as dag:

    ingest_task = PythonOperator(
        task_id         = "ingest_task",
        python_callable = ingest_data,
    )

    preprocess_task = PythonOperator(
        task_id         = "preprocess_task",
        python_callable = preprocess_data,
    )

    # Runs VADER (fast) then RoBERTa (slow) sequentially
    train_task = PythonOperator(
        task_id           = "train_task",
        python_callable   = train_model,
        execution_timeout = timedelta(hours=3),  # RoBERTa on large dataset
    )

    evaluate_task = PythonOperator(
        task_id         = "evaluate_task",
        python_callable = evaluate_model,
    )

    register_task = PythonOperator(
        task_id         = "register_task",
        python_callable = register_model,
    )

    # ingest → preprocess → train → evaluate → register
    ingest_task >> preprocess_task >> train_task >> evaluate_task >> register_task