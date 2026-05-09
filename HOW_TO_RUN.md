# How to Run the Project

This repository is a Docker Compose-based real-time sentiment analysis platform. The instructions below cover environment setup, startup order, and service verification.

## 1. Prepare the environment

1. Create a `.env` file in the repository root.
2. Add all required secrets and configuration values.
3. Make sure the values match those used in `docker-compose.yml`.

### Required environment variables

At minimum, set:

- `POSTGRES_USER`
- `POSTGRES_PASSWORD`
- `POSTGRES_DB`
- `AIRFLOW_DB`
- `MLFLOW_DB`
- `METABASE_DB`
- `MINIO_ROOT_USER`
- `MINIO_ROOT_PASSWORD`
- `MLFLOW_S3_ENDPOINT_URL=http://minio:9000`
- `MLFLOW_TRACKING_URI=http://mlflow:5000`
- `AIRFLOW_FERNET_KEY`
- `AIRFLOW_SECRET_KEY`
- `AIRFLOW_ADMIN_USER`
- `AIRFLOW_ADMIN_PASSWORD`
- `GRAFANA_ADMIN_USER`
- `GRAFANA_ADMIN_PASSWORD`
- `MODEL_NAME`
- `MODEL_STAGE`
- `MONGO_DB`

### Generate an Airflow Fernet key

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Copy the output into `AIRFLOW_FERNET_KEY`.

## 2. Start the platform

Use Docker Compose from the repository root.

```bash
docker compose up -d
```

This will start all services defined in `docker-compose.yml`.

## 3. Verify core services

Check the following endpoints in your browser or with `curl`:

- FastAPI model service: `http://localhost:8000/health`
- Spark UI: `http://localhost:8080`
- Airflow webserver: `http://localhost:8081`
- MLflow UI: `http://localhost:5000`
- MinIO console: `http://localhost:9001`
- Grafana: `http://localhost:3001`
- Metabase: `http://localhost:3000`

Also verify database connectivity:

- MongoDB: `mongodb://localhost:27017`
- PostgreSQL: `postgresql://localhost:5432`

## 4. Validate the pipeline

### FastAPI health check

```bash
curl http://localhost:8000/health
```

### Trigger a training DAG in Airflow

Open Airflow at `http://localhost:8081` and trigger the DAG from the UI.

Alternatively, use the Airflow container:

```bash
docker exec -it airflow-webserver airflow dags trigger model_training_pipeline
```

### Check MLflow

Open `http://localhost:5000` and confirm experiment runs and model artifacts are available.

### Confirm MinIO buckets

Open `http://localhost:9001` and verify that the `mlflow-artifacts` and `models` buckets exist.

## 5. Bring up the streaming layer

The Kafka and streaming services are defined in `docker-compose.yml`.

If you want to start them separately:

```bash
docker compose up -d zookeeper kafka streaming-service spark-master spark-worker
```

### Produce a test event

You can use the Kafka console producer from the Kafka container:

```bash
docker exec -it kafka kafka-console-producer --bootstrap-server kafka:29092 --topic raw-events
```

Send a sample JSON message and then confirm the event is processed.

### Check MongoDB for predictions

```bash
docker exec -it mongo mongosh --eval "db.getMongo().getDB('$(grep -oP '(?<=MONGO_DB=).*' .env)').processed_events.findOne()"
```

## 6. Start dashboards

```bash
docker compose up -d metabase grafana
```

### Metabase Setup (Analytics Dashboard)

**IMPORTANT**: Metabase requires web-based initial setup before connecting to databases.

1. **Start Metabase**
   ```bash
   docker compose up -d metabase
   ```

2. **Complete Web Setup First**
   - Go to `http://localhost:3000`
   - Complete the "Welcome to Metabase" setup wizard
   - Create an admin account
   - Skip adding a database for now

3. **Connect to MongoDB**
   ```bash
   ./metabase/setup_mongodb.sh
   ```
   Or follow the manual steps in `metabase/MANUAL_SETUP.md`

4. **Alternative: Manual Setup**
   - Admin → Databases → Add database
   - Select "MongoDB" as database type
   - Configure:
     - **Name**: MongoDB - Sentiment Analysis
     - **Host**: mongo
     - **Port**: 27017
     - **Database name**: mlplatform
     - **SSL**: Disabled

Open the dashboards:

- Grafana: `http://localhost:3001` (Real-time metrics)
- Metabase: `http://localhost:3000` (Analytics & historical data)

### Create Metabase Dashboards

Once Metabase is connected to MongoDB, you can create comprehensive analytics dashboards:

1. **Explore Your Data**
   - Go to `http://localhost:3000`
   - Click "Browse data" → "MongoDB - Sentiment Analysis"
   - Explore the collections: `scored_reviews`, `processed_events`, `failed_records`

2. **Create Your First Dashboard**
   - Follow the detailed guide in `metabase/DASHBOARD_GUIDE.md`
   - Or import the sample dashboard from `metabase/sample-dashboard.json`
   - Or open your own lightweight UI from `frontend/index.html`
     - `frontend/realtime.html` — realtime model and streaming view
     - `frontend/analytics.html` — MongoDB analytics view

3. **Key Metrics to Track**
   - Sentiment distribution (positive/negative/neutral)
   - Model confidence scores
   - API response times
   - Processing rates over time
   - Error rates and failed records

## 7. Stop the platform

```bash
docker compose down
```

## Notes

- The project uses Docker Compose service names internally, so containerized components should not use `localhost` to reach each other.
- If you make changes to `.env`, restart the affected containers.
- Use the logs to debug startup problems:

```bash
docker compose logs -f
```
