import logging

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json, lit
from pyspark.sql.types import StringType

from spark_app.postgres_prediction_sink import write_realtime_predictions_partition
from spark_app.api_client import predict_partition
from spark_app.config import (
    CHECKPOINT_LOCATION,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
)
from spark_app.mongo_sink import write_predictions_partition
from spark_app.postgres_metrics import write_batch_metrics
from spark_app.preprocessing import preprocess_reviews
from spark_app.schemas import RAW_REVIEW_SCHEMA, PREDICTION_SCHEMA


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


def create_spark_session():
    return (
        SparkSession.builder
        .appName("AmazonFineFoodReviewsSentimentStreaming")
        .getOrCreate()
    )


def process_batch(batch_df, batch_id: int):
    if batch_df.rdd.isEmpty():
        logger.info(f"Batch {batch_id} is empty.")
        return

    logger.info(f"Processing batch_id={batch_id}")

    spark = batch_df.sparkSession

    preprocessed_df = (
        preprocess_reviews(batch_df)
        .withColumn("processed_at", current_timestamp().cast(StringType()))
        .withColumn("batch_id", lit(str(batch_id)))
        .select(
            "review_id",
            "product_id",
            "user_id",
            "profile_name",
            "helpfulness_numerator",
            "helpfulness_denominator",
            "score",
            "review_time",
            "summary",
            "text",
            "processed_at",
            "batch_id",
        )
    )

    prediction_rdd = preprocessed_df.rdd.mapPartitions(predict_partition)

    prediction_df = spark.createDataFrame(
        prediction_rdd,
        schema=PREDICTION_SCHEMA,
    )

    if prediction_df.rdd.isEmpty():
        logger.warning(f"No predictions generated for batch_id={batch_id}")
        return

    prediction_df.persist(StorageLevel.MEMORY_AND_DISK)

    try:
        # 1. Offline/history store
        prediction_df.foreachPartition(write_predictions_partition)

        # 2. Real-time Grafana row-level store
        prediction_df.foreachPartition(write_realtime_predictions_partition)

        # 3. Real-time Grafana aggregated metrics
        write_batch_metrics(prediction_df, batch_id)

        logger.info(f"Completed batch_id={batch_id}")
    finally:
        prediction_df.unpersist()


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    raw_kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed_df = (
        raw_kafka_df
        .selectExpr("CAST(value AS STRING) as json_value")
        .select(from_json(col("json_value"), RAW_REVIEW_SCHEMA).alias("data"))
        .select("data.*")
        .filter(col("review_id").isNotNull())
        .filter(col("text").isNotNull())
    )

    query = (
        parsed_df.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .start()
    )

    logger.info("Spark Structured Streaming query started.")
    query.awaitTermination()


if __name__ == "__main__":
    main()