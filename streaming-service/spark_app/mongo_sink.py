from typing import Iterable

from pymongo import MongoClient, UpdateOne

from spark_app.config import (
    MONGO_FAILED_COLLECTION,
    MONGO_PREDICTIONS_COLLECTION,
    MONGO_DB,
    MONGO_URI,
)


def write_predictions_partition(rows: Iterable):
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]

    predictions_collection = db[MONGO_PREDICTIONS_COLLECTION]
    failed_collection = db[MONGO_FAILED_COLLECTION]

    prediction_ops = []
    failed_ops = []

    for row in rows:
        doc = row.asDict()

        key = {
            "review_id": doc.get("review_id"),
            "batch_id": doc.get("batch_id"),
        }

        if doc.get("error"):
            failed_ops.append(
                UpdateOne(
                    key,
                    {"$set": doc},
                    upsert=True,
                )
            )
        else:
            prediction_ops.append(
                UpdateOne(
                    key,
                    {"$set": doc},
                    upsert=True,
                )
            )

    if prediction_ops:
        predictions_collection.bulk_write(prediction_ops, ordered=False)

    if failed_ops:
        failed_collection.bulk_write(failed_ops, ordered=False)

    client.close()