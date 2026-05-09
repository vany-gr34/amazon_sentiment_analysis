import logging
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pymongo import MongoClient

from app.config import settings
from app.model import model_manager
from app.predictor import predict_batch as run_batch_prediction
from app.predictor import predict_one
from app.schemas import (
    BatchRequest,
    BatchResponse,
    HealthResponse,
    PredictRequest,
    PredictResponse,
    ReloadResponse,
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


def check_ready() -> None:
    if not model_manager.model_loaded:
        raise HTTPException(
            status_code=503,
            detail=(
                "Model is not loaded. Train and register the model in MLflow first. "
                f"Expected model URI: {model_manager.model_uri}"
            ),
        )


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting ml-service...")
    model_manager.load()
    yield
    logger.info("Stopping ml-service...")


app = FastAPI(
    title=settings.API_NAME,
    version=settings.API_VERSION,
    description="Inference API used by Spark streaming jobs.",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

mongo_client = MongoClient(settings.MONGO_URI)
mongo_db = mongo_client.get_default_database()


def serialize_doc(doc: dict) -> dict:
    return {
        key: (str(value) if key == "_id" else value)
        for key, value in doc.items()
        if key != "_id" or True
    }


@app.get("/")
def root():
    return {
        "service": "ml-service",
        "description": "Amazon sentiment analysis inference API",
        "docs": "/docs",
        "health": "/health",
        "predict": "/predict",
        "predict_batch": "/predict-batch",
        "analytics_summary": "/api/ui/summary",
        "sentiment_distribution": "/api/ui/sentiment-distribution",
        "processed_rate": "/api/ui/processed-rate",
        "latest_predictions": "/api/ui/latest-predictions",
    }


@app.get("/api/ui/summary")
def analytics_summary():
    scored = mongo_db.scored_reviews
    failed = mongo_db.failed_records

    total_reviews = scored.count_documents({})
    failed_reviews = failed.count_documents({})

    metrics = list(
        scored.aggregate(
            [
                {
                    "$match": {
                        "confidence": {"$ne": None},
                    }
                },
                {
                    "$group": {
                        "_id": None,
                        "avg_confidence": {"$avg": "$confidence"},
                        "avg_latency": {"$avg": "$api_latency_ms"},
                    }
                },
            ]
        )
    )

    summary = metrics[0] if metrics else {}
    return {
        "total_reviews": total_reviews,
        "failed_reviews": failed_reviews,
        "avg_confidence": round(summary.get("avg_confidence", 0.0), 3),
        "avg_latency_ms": round(summary.get("avg_latency", 0.0), 2),
    }


@app.get("/api/ui/sentiment-distribution")
def sentiment_distribution():
    scored = mongo_db.scored_reviews
    pipeline = [
        {"$group": {"_id": "$prediction", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
    ]
    buckets = list(scored.aggregate(pipeline))
    return [{"prediction": item["_id"], "count": item["count"]} for item in buckets]


@app.get("/api/ui/processed-rate")
def processed_rate(hours: int = 24):
    scored = mongo_db.scored_reviews
    pipeline = [
        {
            "$addFields": {
                "processed_ts": {
                    "$convert": {
                        "input": "$processed_at",
                        "to": "date",
                        "onError": None,
                        "onNull": None,
                    }
                }
            }
        },
        {
            "$match": {
                "processed_ts": {"$ne": None},
            }
        },
        {
            "$project": {
                "bucket": {
                    "$dateToString": {
                        "format": "%Y-%m-%d %H:00",
                        "date": "$processed_ts",
                    }
                }
            }
        },
        {
            "$group": {
                "_id": "$bucket",
                "count": {"$sum": 1},
            }
        },
        {"$sort": {"_id": 1}},
    ]
    series = list(scored.aggregate(pipeline))
    return [{"time": item["_id"], "count": item["count"]} for item in series]


@app.get("/api/ui/latest-predictions")
def latest_predictions(limit: int = 10):
    scored = mongo_db.scored_reviews
    docs = scored.find({}, {"text": 1, "prediction": 1, "confidence": 1, "score": 1, "processed_at": 1}).sort("processed_at", -1).limit(limit)
    results = []
    for doc in docs:
        results.append(
            {
                "text": doc.get("text", ""),
                "prediction": doc.get("prediction", "unknown"),
                "confidence": doc.get("confidence"),
                "score": doc.get("score"),
                "processed_at": doc.get("processed_at"),
            }
        )
    return {"predictions": results}


@app.get("/health", response_model=HealthResponse)
def health():
    return HealthResponse(
        status="ok",
        model_loaded=model_manager.model_loaded,
        vectorizer_loaded=True,
        mlflow_tracking_uri=settings.MLFLOW_TRACKING_URI,
        mlflow_s3_endpoint_url=settings.MLFLOW_S3_ENDPOINT_URL,
        model_name=settings.MODEL_NAME,
        model_stage=settings.MODEL_STAGE,
        model_uri=model_manager.model_uri,
    )


@app.post("/predict", response_model=PredictResponse)
def predict(request: PredictRequest):
    check_ready()

    try:
        result = predict_one(request.text)

        return PredictResponse(
            text=result["text"],
            cleaned_text=result["cleaned_text"],
            prediction=result["prediction"],
            class_id=result["class_id"],
            confidence=result["confidence"],
            model_name=settings.MODEL_NAME,
            model_stage=settings.MODEL_STAGE,
        )

    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))

    except Exception as exc:
        logger.error(f"Prediction failed: {exc}")
        raise HTTPException(status_code=500, detail=f"Prediction failed: {str(exc)}")


@app.post("/predict-batch", response_model=BatchResponse)
def predict_batch(request: BatchRequest):
    check_ready()

    if not request.items:
        raise HTTPException(status_code=422, detail="items list must not be empty")

    logger.info(f"Received batch with {len(request.items)} items")

    return run_batch_prediction(request.items)


@app.post("/reload-model", response_model=ReloadResponse)
def reload_model():
    try:
        model_manager.load()

        success = model_manager.model_loaded

        return ReloadResponse(
            success=success,
            model_loaded=model_manager.model_loaded,
            vectorizer_loaded=True,
            message=(
                "Model reload completed."
                if success
                else "Reload completed but model is missing."
            ),
        )

    except Exception as exc:
        logger.error(f"Reload failed: {exc}")

        return ReloadResponse(
            success=False,
            model_loaded=model_manager.model_loaded,
            vectorizer_loaded=True,
            message=f"Reload failed: {str(exc)}",
        )