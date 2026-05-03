"""
ml-service/main.py
FastAPI inference service. Loads model from MLflow/MinIO on startup.
"""

import os
import logging
from contextlib import asynccontextmanager

import mlflow
import mlflow.pyfunc
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from pymongo import MongoClient
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── Config from environment ───────────────────────────────────
MLFLOW_TRACKING_URI = os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000")
MODEL_NAME          = os.environ.get("MODEL_NAME", "my-model")
MODEL_STAGE         = os.environ.get("MODEL_STAGE", "Production")
MONGO_URI           = os.environ.get("MONGO_URI", "mongodb://mongo:27017/")
SKIP_MODEL_LOADING  = os.environ.get("SKIP_MODEL_LOADING", "false").lower() == "true"

# Global model handle (loaded once at startup)
model = None
mongo_client = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Load model and connect to MongoDB on startup."""
    global model, mongo_client

    # Phase 1: Skip model loading if requested
    if SKIP_MODEL_LOADING:
        logger.info("SKIP_MODEL_LOADING=true - running in minimal mode (no MLflow/MongoDB)")
        yield
        return

    # Phase 4+: Full mode with MLflow and MongoDB
    try:
        # Configure MLflow (points to our MLflow server inside Docker)
        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

        # Load model from MLflow Model Registry
        model_uri = f"models:/{MODEL_NAME}/{MODEL_STAGE}"
        logger.info(f"Loading model from {model_uri}")
        model = mlflow.pyfunc.load_model(model_uri)
        logger.info("Model loaded successfully")

        # Connect to MongoDB (for logging predictions)
        mongo_client = MongoClient(MONGO_URI)
        logger.info("MongoDB connection established")

    except Exception as e:
        logger.error(f"Failed to initialize MLflow/MongoDB: {e}")
        # Don't crash the app - just log the error
        # Model will be None, predict will return 503

    yield  # app runs here

    # Cleanup
    if mongo_client:
        mongo_client.close()


app = FastAPI(title="ML Service", lifespan=lifespan)


# ── Request / Response schemas ────────────────────────────────
class PredictRequest(BaseModel):
    features: dict  # e.g. {"age": 30, "income": 50000}

class PredictResponse(BaseModel):
    prediction: float | int | list
    model_name: str
    model_stage: str


# ── Endpoints ─────────────────────────────────────────────────
@app.get("/health")
def health():
    return {
        "status": "ok", 
        "model_loaded": model is not None,
        "mode": "minimal" if SKIP_MODEL_LOADING else "full"
    }


@app.post("/predict", response_model=PredictResponse)
def predict(request: PredictRequest):
    if model is None:
        raise HTTPException(
            status_code=503, 
            detail="Model not loaded yet. If in Phase 1, this is expected. Use SKIP_MODEL_LOADING=false for full mode."
        )

    try:
        # Convert dict to single-row DataFrame (MLflow pyfunc standard)
        df = pd.DataFrame([request.features])
        prediction = model.predict(df)

        # Log prediction to MongoDB for monitoring / dashboards
        if mongo_client:
            db = mongo_client.get_default_database()
            db.predictions.insert_one({
                "features": request.features,
                "prediction": prediction.tolist() if hasattr(prediction, "tolist") else prediction,
                "model_name": MODEL_NAME,
                "model_stage": MODEL_STAGE,
            })

        return PredictResponse(
            prediction=prediction.tolist()[0] if hasattr(prediction, "tolist") else prediction,
            model_name=MODEL_NAME,
            model_stage=MODEL_STAGE,
        )
    except Exception as e:
        logger.error(f"Prediction error: {e}")
        raise HTTPException(status_code=500, detail=str(e))