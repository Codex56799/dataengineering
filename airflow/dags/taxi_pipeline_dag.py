from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="taxi_spark_pipeline",
    description="Taxi pipeline: local -> landing -> prepared using Spark + MinIO",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,         # Run manually
    catchup=False,
    tags=["taxi", "spark", "minio"],
):

    ingest_to_landing = BashOperator(
        task_id="ingest_to_landing",
        bash_command=(
            "docker exec de_spark "
            "spark-submit --master local[*] "
            "/opt/de_project/spark_jobs/ingest_landing.py"
        ),
    )

    transform_to_prepared = BashOperator(
        task_id="transform_to_prepared",
        bash_command=(
            "docker exec de_spark "
            "spark-submit --master local[*] "
            "/opt/de_project/spark_jobs/transform_prepared.py"
        ),
    )

    ingest_to_landing >> transform_to_prepared
