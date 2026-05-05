import logging

import mlflow
import mlflow.pyfunc

from app.config import settings


logger = logging.getLogger(__name__)


class ModelManager:
    def __init__(self):
        self.model = None
        self.model_loaded = False

        self.model_name = settings.MODEL_NAME
        self.model_stage = settings.MODEL_STAGE
        self.model_uri = f"models:/{self.model_name}/{self.model_stage}"

        mlflow.set_tracking_uri(settings.MLFLOW_TRACKING_URI)

    def load(self) -> None:
        """
        Load the MLflow pyfunc model from the Model Registry.
        This model already contains the RoBERTa + VADER inference logic.
        No vectorizer is needed.
        """
        logger.info(f"Loading pyfunc model from MLflow URI: {self.model_uri}")

        self.model = None
        self.model_loaded = False

        try:
            self.model = mlflow.pyfunc.load_model(self.model_uri)
            self.model_loaded = True
            logger.info("Pyfunc model loaded successfully.")
        except Exception as exc:
            logger.error(f"Could not load pyfunc model: {exc}")
            self.model = None
            self.model_loaded = False


model_manager = ModelManager()