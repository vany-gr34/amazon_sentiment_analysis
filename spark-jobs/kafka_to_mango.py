"""
spark-jobs/kafka_to_mongo.py
PySpark Structured Streaming job.
Reads from Kafka → processes → writes to MongoDB.

Submit with:
  spark-submit \
    --master spark://spark-master:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\
               org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 \
    /opt/spark-jobs/kafka_to_mongo.py
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

KAFKA_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC   = os.environ.get("KAFKA_TOPIC", "raw-events")
MONGO_URI     = os.environ.get("MONGO_URI", "mongodb://mongo:27017/mlplatform")

# Define the schema of your Kafka messages (adjust to your data)
EVENT_SCHEMA = StructType([
    StructField("user_id",  StringType(), True),
    StructField("feature1", DoubleType(), True),
    StructField("feature2", DoubleType(), True),
    StructField("label",    DoubleType(), True),
])

spark = (
    SparkSession.builder
    .appName("KafkaToMongo")
    .config("spark.mongodb.write.connection.uri", MONGO_URI)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ── Read stream from Kafka ────────────────────────────────────
raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

# ── Parse JSON payload ────────────────────────────────────────
events = (
    raw_stream
    .selectExpr("CAST(value AS STRING) as json_str")
    .select(from_json(col("json_str"), EVENT_SCHEMA).alias("data"))
    .select("data.*")
    .withColumn("processed_at", current_timestamp())
)

# ── Write to MongoDB ──────────────────────────────────────────
query = (
    events.writeStream
    .format("mongodb")
    .option("checkpointLocation", "/tmp/spark-checkpoint")
    .option("spark.mongodb.connection.uri", MONGO_URI)
    .option("spark.mongodb.database", "mlplatform")
    .option("spark.mongodb.collection", "processed_events")
    .outputMode("append")
    .start()
)

query.awaitTermination()