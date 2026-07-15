<<<<<<< HEAD
# US Accidents Batch Pipeline — Development Phase (Phase 2)

Implementation of the batch-processing architecture designed in the
Conception Phase, for the accident-severity-prediction backend.

## Status: what's implemented in this phase

This phase focuses on proving the **core data flow** works end-to-end,
per the architecture approved in Phase 1:

| Component | Status |
|---|---|
| Raw storage (MinIO, S3-compatible) | ✅ implemented, containerized |
| Feature store (PostgreSQL) | ✅ implemented, schema + least-privilege read-only role |
| Ingestion service | ✅ implemented — reads CSV, partitions by year/month, writes Parquet to MinIO |
| Batch processing (Spark) | ✅ implemented — cleans, engineers features, aggregates, writes to Postgres |
| Airflow orchestration | 🔜 designed (see `airflow_dag_example.py`), not yet wired into docker-compose |
| FastAPI delivery layer | 🔜 planned for Finalization phase |
| Production secrets management, TLS, API gateway | 🔜 planned for Finalization phase |

The ingestion and Spark services currently run as one-off `docker-compose`
jobs rather than on an Airflow schedule. The scheduling logic itself was
already designed in Phase 1 (monthly ingestion, quarterly processing) and
is captured in `airflow_dag_example.py`, ready to be wired into a running
Airflow service once the core transformation logic is finalized.

## How to run

1. Download the "US Accidents (2016-2023)" dataset from Kaggle and place
   the CSV at `data/us_accidents.csv`.
2. Copy `.env.example` to `.env` (defaults are fine for local development).
3. Build and start the storage layer:
   ```
   docker-compose up -d minio minio-init postgres
   ```
4. Run ingestion (writes raw partitions to MinIO):
   ```
   docker-compose up --build ingestion
   ```
5. Run the batch processing job (reads raw zone, writes aggregated
   features to Postgres):
   ```
   docker-compose up --build spark-job
   ```
6. Verify the result:
   ```
   docker exec -it accidents-postgres psql -U accidents_app -d accidents \
     -c "SELECT * FROM aggregated_features LIMIT 10;"
   ```

## Risks and open problems identified during implementation

- **Dataset size vs. local resources**: processing the full ~7.7M-row
  dataset through a single-node local Spark container is slow. The
  ingestion script chunks reads (`CHUNK_SIZE` env var) to keep memory
  bounded; for the Finalization phase, testing against a representative
  subset first, then the full dataset, is the planned approach.
- **S3A/Hadoop connector version drift**: the `hadoop-aws` and
  `aws-java-sdk-bundle` jar versions must match the Spark image's bundled
  Hadoop version exactly, or S3A reads fail silently with classpath
  errors. Versions are pinned in `spark/Dockerfile` for reproducibility.
- **Idempotency of re-runs**: re-running ingestion against the same month
  currently creates additional partition files rather than replacing them.
  Acceptable for this phase; a dedupe/overwrite strategy (e.g. partition
  overwrite mode) is a candidate improvement for Finalization.
- **Airflow not yet containerized**: orchestration is currently manual
  (`docker-compose up <service>`); the design is documented but not yet
  automated end-to-end. This is the main piece of work carried into the
  Finalization phase, alongside the FastAPI delivery layer.

## Repository layout

```
docker-compose.yml       # core services: MinIO, Postgres, ingestion, Spark job
ingestion/                # ingestion microservice (Python)
spark/                     # batch processing microservice (PySpark)
init-db/                   # PostgreSQL schema, applied on first startup
airflow_dag_example.py     # intended orchestration design (not yet wired in)
.env.example                # local environment defaults
data/                        # place us_accidents.csv here (git-ignored)
```
=======
# US-Accidents-2016-to-2023
>>>>>>> ca18fcb88dda6da02912f357acee5412457b3ea8
