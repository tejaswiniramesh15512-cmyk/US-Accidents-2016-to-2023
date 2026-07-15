"""
Intended Airflow orchestration for the batch pipeline.

This DAG defines the scheduling designed in the Conception phase:
monthly ingestion, quarterly batch processing. It is included here to
document the orchestration design and is ready to drop into an Airflow
DAGs folder — wiring up a running Airflow webserver/scheduler service
in docker-compose.yml is planned for the Finalization phase, once the
core ingestion and processing logic (already implemented and tested
here) is stable.

For this Development phase, the two tasks below are run directly via
`docker-compose up ingestion` and `docker-compose up spark-job`.
"""

from datetime import datetime

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

default_args = {
    "owner": "accidents-pipeline",
    "retries": 3,
}

with DAG(
    dag_id="us_accidents_batch_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,  # two schedules below are handled by separate DAGs in practice
    catchup=False,
    tags=["batch", "accidents"],
) as dag:

    ingest_monthly = DockerOperator(
        task_id="ingest_monthly_partition",
        image="phase2-ingestion:latest",
        docker_url="unix://var/run/docker.sock",
        network_mode="accidents-net",
        # Airflow schedule for this task alone: "0 0 1 * *" (monthly)
    )

    process_quarterly = DockerOperator(
        task_id="run_quarterly_batch_job",
        image="phase2-spark-job:latest",
        docker_url="unix://var/run/docker.sock",
        network_mode="accidents-net",
        # Airflow schedule for this task alone: "0 0 1 1,4,7,10 *" (quarterly)
    )

    ingest_monthly >> process_quarterly
