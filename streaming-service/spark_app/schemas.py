from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)


RAW_REVIEW_SCHEMA = StructType(
    [
        StructField("review_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("profile_name", StringType(), True),
        StructField("helpfulness_numerator", IntegerType(), True),
        StructField("helpfulness_denominator", IntegerType(), True),
        StructField("score", IntegerType(), True),
        StructField("review_time", IntegerType(), True),
        StructField("summary", StringType(), True),
        StructField("text", StringType(), True),
        StructField("event_time", StringType(), True),
    ]
)


PREDICTION_SCHEMA = StructType(
    [
        StructField("review_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("profile_name", StringType(), True),
        StructField("helpfulness_numerator", IntegerType(), True),
        StructField("helpfulness_denominator", IntegerType(), True),
        StructField("score", IntegerType(), True),
        StructField("true_label", StringType(), True),
        StructField("review_time", IntegerType(), True),
        StructField("summary", StringType(), True),
        StructField("text", StringType(), True),
        StructField("cleaned_text", StringType(), True),
        StructField("prediction", StringType(), True),
        StructField("class_id", IntegerType(), True),
        StructField("confidence", DoubleType(), True),
        StructField("error", StringType(), True),
        StructField("api_latency_ms", DoubleType(), True),
        StructField("processed_at", StringType(), True),
        StructField("batch_id", StringType(), True),
    ]
)