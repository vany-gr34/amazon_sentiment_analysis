"""
streaming-service/spark_app/preprocessing.py

Streaming preprocessing aligned with training-pipeline/src/preprocess.py.

Training preprocessing:
- Score < 3  -> Negative
- Score == 3 -> Neutral
- Score > 3  -> Positive
- Text/Summary: fill nulls and strip whitespace
- No vectorization
- No tokenization
- No heavy text cleaning

Reason:
VADER and RoBERTa operate on raw natural language text.
"""

from pyspark.sql.functions import (
    col,
    lit,
    regexp_replace,
    trim,
    when,
)


def map_true_label(score_col="score"):
    """
    Assignment target mapping:
      score < 3  -> Negative
      score == 3 -> Neutral
      score > 3  -> Positive
    """
    return (
        when(col(score_col) < 3, lit("Negative"))
        .when(col(score_col) == 3, lit("Neutral"))
        .when(col(score_col) > 3, lit("Positive"))
        .otherwise(lit(None))
    )


def preprocess_reviews(df):
    """
    Apply the same preprocessing philosophy used in training.

    Steps:
      1. Remove rows with null text
      2. Trim text and summary
      3. Normalize repeated spaces
      4. Map score to True_label if score is available

    Important:
      Do not tokenize, vectorize, stem, remove stopwords, or heavily clean text.
    """

    processed_df = (
        df.filter(col("text").isNotNull())
        .withColumn("text", trim(col("text")))
        .withColumn("summary", trim(col("summary")))
        .withColumn("text", regexp_replace(col("text"), r"\s+", " "))
        .withColumn("summary", regexp_replace(col("summary"), r"\s+", " "))
        .filter(col("text") != "")
    )

    if "score" in processed_df.columns:
        processed_df = processed_df.withColumn(
            "true_label",
            map_true_label("score"),
        )

    return processed_df