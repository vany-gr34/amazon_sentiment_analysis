# ML Platform Environment Setup

This document explains how the repository is structured, how the Docker Compose environment works, and how to start the project so a teammate can reproduce your setup.

## 1. What this project contains

This repository is a modular ML platform built as a Docker Compose solution. It has three main layers:

- **Streaming ingestion**: Kafka, a Kafka consumer service, Spark streaming, and MongoDB.
- **Training pipeline**: PostgreSQL, MinIO, MLflow, and Airflow.
- **Model serving**: FastAPI model API plus dashboards in Metabase and Grafana.

## 2. Directory structure

```
amazon_sentiment_analysis/
├── docker-compose.yml
├── .env
├── init-db/
│   └── 01-init.sql
├── dags/
│   └── training_pipeline.py
├── spark-jobs/
│   └── kafka_to_mongo.py
├── ml-service/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── api.py
├── streaming-service/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── consumer.py
├── grafana/
│   └── provisioning/
│       └── datasources/
│           └── datasources.yaml
├── logs/airflow/
├── plugins/
└── training-pipline/
    └── Dockerfile
```

### Notes

- `.env` contains secrets and service configuration. Never commit it to Git.
- `docker-compose.yml` defines the network, volumes, and all containers.
- `dags/training_pipeline.py` defines the Airflow DAG `model_training_pipeline`.
- `streaming-service/consumer.py` reads from Kafka and writes processed events to MongoDB.
- The training pipeline container is built from `./training-pipline`.

## 3. Core architecture and data flow

```
[External Data] → Kafka (kafka:29092)
      │
      ├─► streaming-service → MongoDB (mongo:27017)
      │
      └─► spark-master → Spark worker → MongoDB

[Airflow scheduler]
      │
      └─► MongoDB → Training DAG → MLflow
                    │
                    ├─► MLflow metadata → PostgreSQL
                    └─► artifacts → MinIO (S3)

[Model serving]
      ml-service → MLflow → MinIO
                └─► logs predictions → MongoDB

[Dashboards]
      Metabase → PostgreSQL
      Grafana  → PostgreSQL
```

## 4. Service relationships in docker-compose

### Internal hostnames

Inside Docker, services communicate using Docker Compose service names:

- `kafka` for Kafka
- `mongo` for MongoDB
- `postgres` for PostgreSQL
- `minio` for MinIO
- `mlflow` for MLflow
- `airflow-webserver`, `airflow-scheduler`, `airflow-init` for Airflow
- `ml-service` for the FastAPI model server

### Important connection rules

- Do not use `localhost` inside container code.
- Use the hostnames defined in `docker-compose.yml`.
- Example: `mongodb://mongo:27017/mlplatform` and `http://mlflow:5000`.

## 5. Environment variables

The project uses `.env` for secrets and connection strings.

### Main variables

- `KAFKA_BOOTSTRAP_SERVERS=kafka:29092`
- `KAFKA_TOPIC=raw-events`
- `MONGO_DB=mlplatform`
- `MONGO_URI=mongodb://mongo:27017/mlplatform`
- `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`
- `AIRFLOW_DB=airflow`
- `MLFLOW_DB=mlflow`
- `METABASE_DB=metabase`
- `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`
- `MLFLOW_S3_ENDPOINT_URL=http://minio:9000`
- `MLFLOW_TRACKING_URI=http://mlflow:5000`
- `MODEL_NAME`, `MODEL_STAGE`
- `AIRFLOW_FERNET_KEY`, `AIRFLOW_SECRET_KEY`
- `AIRFLOW_ADMIN_USER`, `AIRFLOW_ADMIN_PASSWORD`
- `GRAFANA_ADMIN_USER`, `GRAFANA_ADMIN_PASSWORD`

### Generate a Fernet key for Airflow

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Paste the output into `AIRFLOW_FERNET_KEY`.

## 6. Startup steps

The platform is easiest to start in phases. Verify each phase before adding the next.

### Phase 0 — Prepare your `.env`

1. Copy `.env` from a template or create it from scratch.
2. Set secrets and credentials.
3. Confirm the values match the ones expected by `docker-compose.yml`.

> If you do not have a `.env.example`, create one from this repo’s `.env` and keep it out of source control.

### Phase 1 — Start the model API and MongoDB

```bash
docker compose up -d mongo ml-service
```

Verify:

```bash
curl http://localhost:8000/health
```

Expected: the service should respond and the API start successfully. At this point, the model may not be loaded because `SKIP_MODEL_LOADING=true` is set in the compose file.

### Phase 2 — Add Kafka and the streaming consumer

```bash
docker compose up -d zookeeper kafka streaming-service
```

Test Kafka production:

```bash
docker exec -it kafka kafka-console-producer \
  --bootstrap-server kafka:29092 \
  --topic raw-events
```

Send a JSON event, then confirm it reached MongoDB:

```bash
docker exec -it mongo mongosh mlplatform --eval "db.processed_events.findOne()"
```

### Phase 3 — Add Spark

```bash
docker compose up -d spark-master spark-worker
```

Verify Spark UI:

- http://localhost:8080

Optional: submit the Spark job manually from the Spark master container.

### Phase 4 — Start the training pipeline

```bash
docker compose up -d postgres minio minio-init mlflow
```

Wait until PostgreSQL, MinIO, and MLflow are healthy.

```bash
docker compose up -d airflow-init
```

When `airflow-init` finishes successfully:

```bash
docker compose up -d airflow-webserver airflow-scheduler
```

Open Airflow UI:

- http://localhost:8081
- login: `admin` / `admin123`

Trigger the DAG `model_training_pipeline` from the UI or with:

```bash
docker exec -it airflow-webserver airflow dags trigger model_training_pipeline
```

After training, confirm MLflow contains a run and the model artifacts exist.

### Phase 5 — Start dashboards

```bash
docker compose up -d metabase grafana
```

Open dashboards:

- Metabase: http://localhost:3000
- Grafana: http://localhost:3001

## 7. Service URLs

| Service | URL | Notes |
|---|---|---|
| ml-service API | http://localhost:8000 | FastAPI model server |
| Spark UI | http://localhost:8080 | Spark master UI |
| Airflow | http://localhost:8081 | Airflow webserver |
| MLflow | http://localhost:5000 | Model tracking UI |
| MinIO Console | http://localhost:9001 | S3 artifact browser |
| Metabase | http://localhost:3000 | Dashboard UI |
| Grafana | http://localhost:3001 | Dashboard UI |

## 8. Common troubleshooting

### 1. Containers cannot connect to each other

Inside Docker, use service names, not `localhost`.

Bad:

```python
MongoClient("mongodb://localhost:27017")
```

Good:

```python
MongoClient("mongodb://mongo:27017/mlplatform")
```

### 2. MLflow cannot write artifacts to MinIO

Checklist:

- `MLFLOW_S3_ENDPOINT_URL=http://minio:9000`
- `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` match MinIO credentials
- MinIO is up before MLflow starts
- Bucket `mlflow-artifacts` exists (created by `minio-init`)

### 3. Kafka consumer receives no messages

Checklist:

- Producer uses `kafka:29092` inside containers
- Topic matches `KAFKA_TOPIC`
- `auto.offset.reset=earliest` is set for the consumer

### 4. Airflow errors on startup

If Airflow fails immediately, verify PostgreSQL is healthy and the `AIRFLOW_FERNET_KEY` is valid.

### 5. Data disappears after restart

Do not use `docker compose down -v` unless you want to delete persistent volumes. The project defines named volumes for MongoDB, PostgreSQL, MinIO, Grafana, and Metabase.

## 9. Reproduce this setup for a friend

To replicate your environment exactly:

1. Clone the repository.
2. Create a `.env` file with matching credentials.
3. Run `docker compose up -d` in phases.
4. Confirm each service with the URLs above.
5. Trigger the Airflow DAG once MongoDB has event data.

## 10. Helpful commands

```bash
# Show container status
docker compose ps

# Tail logs for a service
docker compose logs -f <service>

# Rebuild a service after code changes
docker compose up -d --build ml-service streaming-service mlflow
```

---

This file is intended to be a hands-on setup guide so a teammate can start the same Docker Compose ML platform and understand how the components work together.