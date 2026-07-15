"""
Batch processing microservice (Phase 2 core implementation).

Reads raw accident partitions from the MinIO raw zone, cleans and
engineers features, computes the aggregations the severity-prediction
model will consume, and writes the result to the PostgreSQL feature
store.

In production this job is triggered quarterly by Airflow (see
../airflow_dag_example.py). For this Development phase, it is run
directly via `docker-compose up spark-job` to demonstrate the
transformation logic end-to-end against whatever has been ingested
so far.
"""

import os
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
RAW_BUCKET = os.environ.get("RAW_BUCKET", "accidents-raw")

POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "accidents")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "accidents_app")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "changeme")

JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:5432/{POSTGRES_DB}"


def build_spark():
    return (
        SparkSession.builder.appName("us-accidents-batch-processing")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def clean_and_engineer(df):
    # --- Cleaning ---
    df = df.dropna(subset=["Severity", "Start_Time", "State"])
    df = df.filter((F.col("Severity") >= 1) & (F.col("Severity") <= 4))

    # --- Feature engineering ---
    df = df.withColumn("hour_of_day", F.hour("Start_Time"))
    df = df.withColumn("day_of_week", F.date_format("Start_Time", "E"))
    df = df.withColumn(
        "is_weekend",
        F.when(F.col("day_of_week").isin("Sat", "Sun"), True).otherwise(False),
    )
    df = df.withColumn(
        "time_bucket",
        F.when((F.col("hour_of_day") >= 6) & (F.col("hour_of_day") < 12), "morning")
        .when((F.col("hour_of_day") >= 12) & (F.col("hour_of_day") < 18), "afternoon")
        .when((F.col("hour_of_day") >= 18) & (F.col("hour_of_day") < 24), "evening")
        .otherwise("night"),
    )
    return df


def aggregate(df):
    run_ts = datetime.now(timezone.utc).isoformat()

    # Accident counts and average severity per state / road feature / time bucket
    agg = (
        df.groupBy("State", "time_bucket", "is_weekend")
        .agg(
            F.count("*").alias("accident_count"),
            F.avg("Severity").alias("avg_severity"),
            F.avg(F.col("Junction").cast("int")).alias("junction_rate"),
            F.avg(F.col("Crossing").cast("int")).alias("crossing_rate"),
            F.avg(F.col("Traffic_Signal").cast("int")).alias("traffic_signal_rate"),
        )
        .withColumn("run_timestamp", F.lit(run_ts))
    )
    return agg


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw_path = f"s3a://{RAW_BUCKET}/"
    print(f"[spark-job] Reading raw partitions from {raw_path}")
    df = spark.read.parquet(raw_path)
    print(f"[spark-job] Loaded {df.count()} raw rows")

    df = clean_and_engineer(df)
    features = aggregate(df)

    print(f"[spark-job] Writing {features.count()} aggregated feature rows to PostgreSQL")
    (
        features.write.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "aggregated_features")
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )
    print("[spark-job] Done.")

    spark.stop()


if __name__ == "__main__":
    main()
