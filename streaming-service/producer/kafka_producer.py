import csv
import json
import logging
import time
from datetime import datetime, timezone

from confluent_kafka import Producer

from producer.config import (
    CSV_PATH,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    MAX_ROWS,
    PRODUCER_RATE_SECONDS,
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


def delivery_report(error, message):
    if error is not None:
        logger.error(f"Delivery failed: {error}")
    else:
        logger.info(
            f"Delivered id={message.key().decode('utf-8')} "
            f"to topic={message.topic()} partition={message.partition()}"
        )


def safe_int(value, default=None):
    try:
        if value is None or value == "":
            return default
        return int(value)
    except Exception:
        return default


def build_event(row: dict, index: int) -> dict:
    """
    Kaggle Amazon Fine Food Reviews columns:
    Id, ProductId, UserId, ProfileName, HelpfulnessNumerator,
    HelpfulnessDenominator, Score, Time, Summary, Text
    """

    review_id = str(row.get("Id") or index)

    return {
        "review_id": review_id,
        "product_id": row.get("ProductId"),
        "user_id": row.get("UserId"),
        "profile_name": row.get("ProfileName"),
        "helpfulness_numerator": safe_int(row.get("HelpfulnessNumerator"), 0),
        "helpfulness_denominator": safe_int(row.get("HelpfulnessDenominator"), 0),
        "score": safe_int(row.get("Score")),
        "review_time": safe_int(row.get("Time")),
        "summary": (row.get("Summary") or "").strip(),
        "text": (row.get("Text") or "").strip(),
        "event_time": datetime.now(timezone.utc).isoformat(),
    }


def main():
    producer = Producer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "client.id": "amazon-reviews-csv-producer",
            "acks": "all",
            "retries": 5,
        }
    )

    logger.info(f"Starting CSV producer")
    logger.info(f"CSV path: {CSV_PATH}")
    logger.info(f"Kafka topic: {KAFKA_TOPIC}")
    logger.info(f"Max rows: {MAX_ROWS}")
    logger.info(f"Rate: 1 message every {PRODUCER_RATE_SECONDS}s")

    sent = 0

    with open(CSV_PATH, "r", encoding="utf-8", errors="ignore", newline="") as file:
        reader = csv.DictReader(file)

        for index, row in enumerate(reader):
            if sent >= MAX_ROWS:
                break

            event = build_event(row, index)

            if not event["text"]:
                continue

            producer.produce(
                topic=KAFKA_TOPIC,
                key=event["review_id"],
                value=json.dumps(event, ensure_ascii=False),
                callback=delivery_report,
            )

            producer.poll(0)
            sent += 1

            logger.info(
                f"Sent review_id={event['review_id']} "
                f"score={event['score']} "
                f"text_len={len(event['text'])}"
            )

            time.sleep(PRODUCER_RATE_SECONDS)

    producer.flush()
    logger.info(f"Producer finished. Sent {sent} events.")


if __name__ == "__main__":
    main()