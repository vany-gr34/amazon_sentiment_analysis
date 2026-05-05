# Project Structure

This repository implements a real-time NLP streaming pipeline with a separate ML training and serving stack.

## Root files

- `docker-compose.yml` - Docker Compose definition for the full platform, including Airflow, Postgres, MLflow, MinIO, Kafka, Spark, MongoDB, and services.
- `README.md` - High-level technical design and architecture documentation.
- `ENVIRONMENT_SETUP.md` - Environment setup instructions and dependency information.
- `LICENSE` - Project license.

## Top-level directories

### `dags/`
Contains Airflow DAG definitions.

- `training_pipeline.py` - Airflow DAG that orchestrates the ML training workflow: ingest → preprocess → train → evaluate → register.

### `training-pipeline/`
Holds the batch training pipeline code and configuration.

- `Dockerfile` - Container build definition for the training pipeline service.
- `requirements.txt` - Python dependencies required for the training pipeline, including MLflow, transformers, and scikit-learn.
- `configs/` - YAML configuration for the training pipeline.
  - `training.yaml` - Main configuration file loaded by the training code.
- `data/` - Data assets used by the training pipeline.
  - `Reviews.csv` - Amazon reviews dataset.
- `src/` - Training pipeline source code.
  - `config.py` - Loads YAML config and exposes `CFG`.
  - `ingest.py` - Data ingestion logic, likely reading and saving raw input.
  - `preprocess.py` - Data preprocessing and feature engineering.
  - `pipeline.py` - Orchestrates the training pipeline locally.
  - `train.py` - Model training logic with MLflow tracking.
  - `evaluate.py` - Evaluation and metrics logging.
  - `register.py` - Model registration logic for MLflow.

### `ml-service/`
Contains the model serving application.

- `api.py` - FastAPI app for inference and health checks.
- `Dockerfile` - Build the ML service container.
- `requirements.txt` - Python dependencies for the inference service.

### `streaming-service/`
Contains the real-time Kafka consumer or streaming application.

- `consumer.py` - Kafka consumer logic for real-time data processing.
- `Dockerfile` - Build instructions for the streaming service container.
- `requirements.txt` - Python dependencies for the streaming component.

### `spark-jobs/`
Contains Spark job scripts.

- `kafka_to_mango.py` - Spark job to move data from Kafka to MongoDB or another downstream store.

### `grafana/`
Contains Grafana provisioning configuration.

- `provisioning/` - Dashboard and datasource provisioning files.

### `init-db/`
Contains database initialization SQL scripts.

- `01-init.sql` - Creates initial PostgreSQL databases used by Airflow and MLflow.

### `plugins/`
Airflow plugin directory for custom operators or hooks, if any.

## Runtime details

The system is built as a multi-container platform:

- Airflow orchestrates the batch training DAG and requires access to `dags/`, `training-pipeline/src/`, and `training-pipeline/configs/`.
- MLflow is used for experiment tracking and model registry.
- MinIO provides an S3-compatible artifact store.
- Kafka and Spark support the real-time streaming ingestion pipeline.
- FastAPI exposes the model for inference.

## How the pieces fit together

1. `docker-compose.yml` brings up the platform and binds each service together.
2. The `dags/` folder contains workflows that execute Python code from the training pipeline source.
3. The `training-pipeline/` directory contains the actual ML logic, config, and dependencies.
4. `ml-service/` exposes inference results after models are tracked and registered with MLflow.
5. `streaming-service/` and `spark-jobs/` handle the streaming ingestion and processing side.
