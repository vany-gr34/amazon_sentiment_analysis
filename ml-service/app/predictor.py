import logging
from typing import List

import pandas as pd

from app.model import model_manager
from app.schemas import BatchItem, BatchPrediction, BatchResponse


logger = logging.getLogger(__name__)


def predict_one(text: str) -> dict:
    """
    Send raw text to the MLflow pyfunc model.
    The pyfunc model handles cleaning, RoBERTa, and VADER internally.
    """
    input_df = pd.DataFrame({"text": [text]})
    output_df = model_manager.model.predict(input_df)

    row = output_df.iloc[0].to_dict()

    return {
        "text": row.get("text", text),
        "cleaned_text": row.get("cleaned_text", ""),
        "prediction": row.get("prediction", "unknown"),
        "class_id": int(row.get("class_id", -1)),
        "confidence": float(row.get("confidence", 0.0)),
    }


def predict_batch(items: List[BatchItem]) -> BatchResponse:
    """
    Batch endpoint used by Spark.
    Sends many texts to the MLflow pyfunc model at once.
    """
    input_df = pd.DataFrame(
        {
            "text": [item.text for item in items]
        }
    )

    try:
        output_df = model_manager.model.predict(input_df)

        predictions = []

        for item, (_, row) in zip(items, output_df.iterrows()):
            row_dict = row.to_dict()

            predictions.append(
                BatchPrediction(
                    id=item.id,
                    text=row_dict.get("text", item.text),
                    cleaned_text=row_dict.get("cleaned_text", ""),
                    prediction=row_dict.get("prediction", "unknown"),
                    class_id=int(row_dict.get("class_id", -1)),
                    confidence=float(row_dict.get("confidence", 0.0)),
                    error=None,
                )
            )

        return BatchResponse(
            predictions=predictions,
            total=len(predictions),
            failed=0,
        )

    except Exception as exc:
        logger.error(f"Batch prediction failed: {exc}")

        predictions = [
            BatchPrediction(
                id=item.id,
                text=item.text,
                cleaned_text=None,
                prediction="unknown",
                class_id=-1,
                confidence=0.0,
                error=str(exc),
            )
            for item in items
        ]

        return BatchResponse(
            predictions=predictions,
            total=len(predictions),
            failed=len(predictions),
        )