# Project Progress Summary

## What we have built so far

This repository is implementing a production-style real-time sentiment analysis MLOps platform with the following major components:

- Apache Kafka for streaming ingestion
- Spark Structured Streaming for real-time processing
- FastAPI for model serving
- MongoDB for operational prediction storage
- PostgreSQL for relational metadata and Airflow persistence
- MLflow for experiment tracking and model registry
- MinIO for S3-compatible artifact storage
- Airflow for workflow orchestration
- Grafana for monitoring dashboards
- Metabase for offline analytics and BI
- Docker Compose for multi-service orchestration

## Completed platform layers

### 1. Streaming Layer

- `docker-compose.yml` includes Kafka, Spark master/worker, MongoDB, and the streaming service
- `streaming-service/consumer.py` contains the Kafka/Spark consumer logic
- `spark-jobs/kafka_to_mango.py` contains Spark job logic for moving data from Kafka into downstream storage
- The streaming service is configured to call the FastAPI inference endpoint in batch mode (`/predict-batch`)
- MongoDB stores real-time enriched prediction events

### 2. Training Layer

- `training-pipeline/` contains the batch training pipeline and MLflow integration
- `training-pipeline/src/ingest.py` handles dataset ingest
- `training-pipeline/src/preprocess.py` implements data cleaning and label mapping
- `training-pipeline/src/train.py` fits the model and logs experiments to MLflow
- `training-pipeline/src/evaluate.py` evaluates the model and records metrics
- `training-pipeline/src/register.py` registers models with MLflow model registry
- `training-pipeline/src/pipeline.py` orchestrates the full training workflow
- `dags/training_pipeline.py` defines the Airflow DAG for the training workflow

### 3. Serving & Monitoring Layer

- `ml-service/` is the FastAPI inference service
- The service is configured to load models from MLflow and access MongoDB
- Health checks are defined for container readiness
- `grafana/provisioning/` contains Grafana configuration for dashboards
- `docker-compose.yml` includes Grafana and Metabase services

## Data flow summary

1. Raw review events are produced to Kafka by the streaming pipeline
2. Spark Structured Streaming consumes Kafka events and applies lightweight preprocessing
3. Spark batches records and calls the FastAPI model service for inference
4. Predictions are enriched and written to MongoDB
5. PostgreSQL stores metadata and supports Airflow/BI workflows
6. MLflow tracks experiments and stores artifacts in MinIO
7. Grafana and Metabase provide observability and analytics

## Key files and directories

- `docker-compose.yml` — orchestrates the full environment
- `dags/training_pipeline.py` — Airflow DAG for training pipeline
- `training-pipeline/` — batch training and MLflow logic
- `ml-service/` — model serving API
- `streaming-service/` — real-time streaming ingestion logic
- `spark-jobs/` — Spark jobs for Kafka processing
- `grafana/` — monitoring dashboard provisioning
- `init-db/01-init.sql` — PostgreSQL initialization scripts

## Current status

- Core components are defined in Docker Compose
- Training pipeline code exists and is wired to MLflow/MinIO/Postgres
- Streaming architecture is wired to Kafka, Spark, and the FastAPI model service
- Serving and monitoring components are present in the repo

## Next focus areas

- Validate end-to-end streaming flow from Kafka through Spark to MongoDB
- Confirm the FastAPI `/predict-batch` inference contract
- Add explicit dataset producer logic if not yet implemented
- Complete Grafana dashboards and Metabase reporting
- Add documentation for running and verifying the full platform
