# Taxi Trips Data Engineering Pipeline

A fully containerized data engineering project that ingests, processes,
and prepares NYC Yellow Taxi trip data using Apache Spark, Apache
Airflow, MinIO, DuckDB, and JupyterLab.

## Project Overview

This project simulates a modern analytics stack with clear data layers:

-   Raw data: NYC Yellow Taxi trip data (January 2024, Parquet)
-   Ingestion & transformation: Apache Spark
-   Orchestration: Apache Airflow
-   Storage: MinIO (S3-compatible) for Landing and Prepared zones
-   Warehouse: DuckDB as an embedded analytical database
-   Exploration: JupyterLab for notebooks and EDA

## Architecture

Local Parquet ↓ Spark Ingest Job ↓ MinIO Landing Zone (S3) ↓ Spark
Transform Job ↓ MinIO Prepared Zone (S3) ↓ DuckDB Warehouse ↓ JupyterLab
Exploration

## Data Pipeline

### Data Source

Download the January 2024 dataset:

mkdir -p data curl -L
"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
-o data/yellow_tripdata_2024-01.parquet

### Ingestion (Landing Zone)

Spark job: spark_jobs/ingest_landing.py

### Transformation (Prepared Zone)

Spark job: spark_jobs/transform_prepared.py

## Analytics (DuckDB + JupyterLab)

Warehouse file: warehouse/taxi.duckdb\
Notebook: notebooks/01_taxi_trips_exploration.ipynb

## Airflow Orchestration

Airflow DAG: airflow/dags/taxi_pipeline_dag.py

## Local Environment

Services: - Airflow UI: http://localhost:8080 - MinIO:
http://localhost:9001 - JupyterLab: http://localhost:8888

docker compose build\
docker compose up -d

## Running the Pipeline

1.  Open Airflow
2.  Trigger taxi_spark_pipeline
3.  Explore in JupyterLab

## Future Enhancements

-   Data quality checks
-   Incremental ingestion
-   dbt
-   Monitoring

## About

Portfolio project demonstrating end-to-end data engineering workflows.
