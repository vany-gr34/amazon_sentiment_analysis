from datetime import datetime

import psycopg2

from spark_app.config import (
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USER,
)


def get_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


def insert_metric_rows(rows):
    if not rows:
        return

    connection = get_connection()
    cursor = connection.cursor()

    query = """
        INSERT INTO streaming_metrics
            (batch_id, metric_time, metric_name, metric_value, sentiment)
        VALUES
            (%s, %s, %s, %s, %s)
    """

    cursor.executemany(query, rows)

    connection.commit()
    cursor.close()
    connection.close()


def write_batch_metrics(prediction_df, batch_id: int):
    """
    Collect only aggregated metrics, not full raw data.
    """

    metric_time = datetime.utcnow()
    rows = []

    total = prediction_df.count()

    rows.append(
        (
            batch_id,
            metric_time,
            "reviews_per_batch",
            float(total),
            None,
        )
    )

    failed = prediction_df.filter(prediction_df.error.isNotNull()).count()

    rows.append(
        (
            batch_id,
            metric_time,
            "failed_inference_count",
            float(failed),
            None,
        )
    )

    if total > 0:
        avg_confidence = prediction_df.selectExpr(
            "avg(confidence) as avg_confidence"
        ).first()["avg_confidence"]

        rows.append(
            (
                batch_id,
                metric_time,
                "average_confidence",
                float(avg_confidence or 0.0),
                None,
            )
        )

    sentiment_rows = (
        prediction_df.groupBy("prediction")
        .count()
        .collect()
    )

    for row in sentiment_rows:
        rows.append(
            (
                batch_id,
                metric_time,
                "sentiment_count",
                float(row["count"]),
                row["prediction"],
            )
        )

    latency_value = prediction_df.selectExpr(
        "avg(api_latency_ms) as avg_api_latency_ms"
    ).first()["avg_api_latency_ms"]

    rows.append(
        (
            batch_id,
            metric_time,
            "avg_api_latency_ms",
            float(latency_value or 0.0),
            None,
        )
    )

    insert_metric_rows(rows)