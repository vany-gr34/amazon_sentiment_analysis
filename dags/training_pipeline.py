"""
dags/training_pipeline.py
Airflow DAG — orchestrates the full model training pipeline:
  1. Extract data from MongoDB
  2. Train model with MLflow tracking
  3. Register best model in MLflow Model Registry
  4. Optionally deploy to ml-service
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# ── DAG defaults ─────────────────────────────────────────────
default_args = {
    "owner": "ml-team",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="model_training_pipeline",
    default_args=default_args,
    description="Train and register ML model",
    schedule="@daily",           # or use a cron expression
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ml", "training"],
) as dag:

    def extract_training_data(**context):
        """Pull data from MongoDB and save as CSV for training."""
        import os
        from pymongo import MongoClient
        import pandas as pd

        mongo = MongoClient(os.environ["MONGO_URI"])
        db = mongo.get_default_database()
        records = list(db.processed_events.find({}, {"_id": 0}))
        df = pd.DataFrame(records)
        df.to_csv("/tmp/training_data.csv", index=False)
        print(f"Extracted {len(df)} records for training")
        mongo.close()

    def train_model(**context):
        """Train model and log everything to MLflow."""
        import os
        import mlflow
        import mlflow.sklearn
        import pandas as pd
        from sklearn.ensemble import RandomForestClassifier
        from sklearn.model_selection import train_test_split
        from sklearn.metrics import accuracy_score

        mlflow.set_tracking_uri(os.environ["MLFLOW_TRACKING_URI"])
        mlflow.set_experiment("my-experiment")

        df = pd.read_csv("/tmp/training_data.csv")

        # Adjust columns to your actual schema
        X = df.drop("label", axis=1)
        y = df["label"]
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)

        with mlflow.start_run() as run:
            model = RandomForestClassifier(n_estimators=100)
            model.fit(X_train, y_train)
            acc = accuracy_score(y_test, model.predict(X_test))

            mlflow.log_param("n_estimators", 100)
            mlflow.log_metric("accuracy", acc)

            # Log model — artifacts stored in MinIO via S3 protocol
            mlflow.sklearn.log_model(
                model,
                artifact_path="model",
                registered_model_name=os.environ.get("MODEL_NAME", "my-model"),
            )

            # Push run_id to XCom for downstream tasks
            context["ti"].xcom_push(key="run_id", value=run.info.run_id)
            print(f"Model logged. Run ID: {run.info.run_id}, Accuracy: {acc:.4f}")

    def promote_model(**context):
        """Transition the latest model version to Production."""
        import os
        import mlflow
        from mlflow import MlflowClient

        mlflow.set_tracking_uri(os.environ["MLFLOW_TRACKING_URI"])
        client = MlflowClient()
        model_name = os.environ.get("MODEL_NAME", "my-model")

        # Get the latest version
        versions = client.get_latest_versions(model_name, stages=["None"])
        if versions:
            client.transition_model_version_stage(
                name=model_name,
                version=versions[0].version,
                stage="Production",
                archive_existing_versions=True,
            )
            print(f"Model v{versions[0].version} promoted to Production")

    # ── Task definitions ──────────────────────────────────────
    t_extract = PythonOperator(
        task_id="extract_training_data",
        python_callable=extract_training_data,
    )

    t_train = PythonOperator(
        task_id="train_model",
        python_callable=train_model,
    )

    t_promote = PythonOperator(
        task_id="promote_model",
        python_callable=promote_model,
    )

    # ── Optional: trigger ml-service reload ──────────────────
    t_reload = BashOperator(
        task_id="reload_ml_service",
        # ml-service reloads its model on /reload endpoint
        bash_command="curl -X POST http://ml-service:8000/reload || true",
    )

    # ── Task order ────────────────────────────────────────────
    t_extract >> t_train >> t_promote >> t_reload