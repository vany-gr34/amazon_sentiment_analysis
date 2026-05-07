import time
from typing import Dict, List

import requests

from spark_app.config import (
    API_BATCH_SIZE,
    API_TIMEOUT_SECONDS,
    ML_SERVICE_URL,
)


def call_predict_batch(items: List[Dict], retries: int = 3) -> List[Dict]:
    payload = {"items": items}
    last_error = None

    for attempt in range(1, retries + 1):
        try:
            start = time.time()

            response = requests.post(
                ML_SERVICE_URL,
                json=payload,
                timeout=API_TIMEOUT_SECONDS,
            )

            latency_ms = round((time.time() - start) * 1000, 2)

            response.raise_for_status()
            data = response.json()

            predictions = data.get("predictions", [])

            for prediction in predictions:
                prediction["api_latency_ms"] = latency_ms

            return predictions

        except Exception as exc:
            last_error = str(exc)
            time.sleep(1.5 * attempt)

    failed = []

    for item in items:
        failed.append(
            {
                "id": item.get("id"),
                "text": item.get("text"),
                "cleaned_text": None,
                "prediction": "unknown",
                "class_id": -1,
                "confidence": 0.0,
                "error": last_error,
                "api_latency_ms": None,
            }
        )

    return failed


def predict_partition(rows):
    """
    Executed per Spark partition.

    It avoids row-by-row API calls by buffering rows and sending
    mini-batches to /predict-batch.
    """

    buffer = []

    for row in rows:
        row_dict = row.asDict()

        buffer.append(row_dict)

        if len(buffer) >= API_BATCH_SIZE:
            yield from enrich_batch(buffer)
            buffer = []

    if buffer:
        yield from enrich_batch(buffer)


def enrich_batch(records: List[Dict]):
    api_items = [
        {
            "id": record["review_id"],
            "text": record["text"],
        }
        for record in records
    ]

    predictions = call_predict_batch(api_items)

    prediction_by_id = {
        prediction.get("id"): prediction
        for prediction in predictions
    }

    for record in records:
        prediction = prediction_by_id.get(record["review_id"], {})

        yield {
            "review_id": record.get("review_id"),
            "product_id": record.get("product_id"),
            "user_id": record.get("user_id"),
            "profile_name": record.get("profile_name"),
            "helpfulness_numerator": record.get("helpfulness_numerator"),
            "helpfulness_denominator": record.get("helpfulness_denominator"),
            "score": record.get("score"),
            "review_time": record.get("review_time"),
            "summary": record.get("summary"),
            "text": record.get("text"),
            "cleaned_text": prediction.get("cleaned_text"),
            "prediction": prediction.get("prediction", "unknown"),
            "class_id": int(prediction.get("class_id", -1)),
            "confidence": float(prediction.get("confidence", 0.0)),
            "error": prediction.get("error"),
            "api_latency_ms": prediction.get("api_latency_ms"),
            "processed_at": record.get("processed_at"),
            "batch_id": str(record.get("batch_id")),
        }