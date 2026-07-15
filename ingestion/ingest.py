"""
Ingestion microservice (Phase 2 core implementation).

Reads the US Accidents CSV in chunks, partitions records by year/month
derived from the `Start_Time` timestamp, and writes each partition as
Parquet into the MinIO raw zone (accidents-raw bucket).

In production this container is invoked monthly by an Airflow DAG
(see ../airflow_dag_example.py for the intended scheduling — full
Airflow orchestration is planned for the Finalization phase). For this
Development phase, it runs once, end-to-end, against whatever data is
mounted at SOURCE_CSV_PATH, to demonstrate the ingestion logic works.
"""

import io
import os
import sys
import boto3
import pandas as pd

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
RAW_BUCKET = os.environ.get("RAW_BUCKET", "accidents-raw")
SOURCE_CSV_PATH = os.environ.get("SOURCE_CSV_PATH", "/data/us_accidents.csv")
CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", "200000"))

TIMESTAMP_COL = "Start_Time"


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )


def write_partition(s3, df: pd.DataFrame, year: int, month: int, chunk_id: int):
    """Write one (year, month, chunk) group as a Parquet object in the raw zone."""
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)
    key = f"year={year:04d}/month={month:02d}/part-{chunk_id:05d}.parquet"
    s3.upload_fileobj(buffer, RAW_BUCKET, key)
    return key


def main():
    if not os.path.exists(SOURCE_CSV_PATH):
        print(
            f"[ingestion] ERROR: expected source CSV at {SOURCE_CSV_PATH} but it was not found.\n"
            f"[ingestion] Download the 'US Accidents (2016-2023)' dataset from Kaggle and place it "
            f"at ./data/us_accidents.csv before running docker-compose up.",
            file=sys.stderr,
        )
        sys.exit(1)

    s3 = get_s3_client()

    total_rows = 0
    written_partitions = 0
    chunk_id = 0

    print(f"[ingestion] Reading {SOURCE_CSV_PATH} in chunks of {CHUNK_SIZE} rows...")

    for chunk in pd.read_csv(
        SOURCE_CSV_PATH,
        chunksize=CHUNK_SIZE,
        parse_dates=[TIMESTAMP_COL],
        low_memory=False,
    ):
        chunk = chunk.dropna(subset=[TIMESTAMP_COL])
        chunk["_year"] = chunk[TIMESTAMP_COL].dt.year
        chunk["_month"] = chunk[TIMESTAMP_COL].dt.month

        for (year, month), group in chunk.groupby(["_year", "_month"]):
            group = group.drop(columns=["_year", "_month"])
            key = write_partition(s3, group, int(year), int(month), chunk_id)
            written_partitions += 1
            total_rows += len(group)
            print(f"[ingestion] wrote {len(group):>7} rows -> s3://{RAW_BUCKET}/{key}")

        chunk_id += 1

    print(f"[ingestion] Done. {total_rows} rows written across {written_partitions} partition files.")


if __name__ == "__main__":
    main()
