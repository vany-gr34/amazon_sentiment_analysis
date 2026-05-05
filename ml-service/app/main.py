import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException

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


@app.get("/")
def root():
    return {
        "service": "ml-service",
        "description": "Amazon sentiment analysis inference API",
        "docs": "/docs",
        "health": "/health",
        "predict": "/predict",
        "predict_batch": "/predict-batch",
    }


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