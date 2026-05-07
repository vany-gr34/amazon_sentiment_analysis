from typing import Iterable

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


def write_realtime_predictions_partition(rows: Iterable):
    """
    Writes streamed prediction rows to PostgreSQL for Grafana.

    MongoDB keeps the full historical/offline store.
    PostgreSQL keeps recent real-time rows for Grafana dashboards.
    """

    connection = get_connection()
    cursor = connection.cursor()

    query = """
        INSERT INTO realtime_predictions (
            batch_id,
            review_id,
            product_id,
            user_id,
            score,
            true_label,
            text,
            cleaned_text,
            prediction,
            class_id,
            confidence,
            api_latency_ms,
            processed_at
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
    """

    values = []

    for row in rows:
        doc = row.asDict()

        values.append(
            (
                int(doc.get("batch_id")) if doc.get("batch_id") else None,
                doc.get("review_id"),
                doc.get("product_id"),
                doc.get("user_id"),
                doc.get("score"),
                doc.get("true_label"),
                doc.get("text"),
                doc.get("cleaned_text"),
                doc.get("prediction"),
                doc.get("class_id"),
                doc.get("confidence"),
                doc.get("api_latency_ms"),
                doc.get("processed_at"),
            )
        )

    if values:
        cursor.executemany(query, values)
        connection.commit()

    cursor.close()
    connection.close()