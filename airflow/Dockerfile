# airflow/Dockerfile

FROM apache/airflow:2.9.0-python3.11

# Install docker CLI so Airflow can run `docker exec`
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
        docker.io \
    && rm -rf /var/lib/apt/lists/*
USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

WORKDIR /opt/airflow
