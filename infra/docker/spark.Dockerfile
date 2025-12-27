# infra/docker/spark.Dockerfile

FROM python:3.11-slim

ARG SPARK_VERSION=3.5.1
ARG HADOOP_VERSION=3

# Install Java & curl
RUN apt-get update && apt-get install -y --no-install-recommends \
        default-jdk \
        curl \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME for Spark
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Set up Spark
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"
ENV PYSPARK_PYTHON=python3

RUN curl -fsSL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    | tar -xz -C /opt \
 && mv "/opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}" "${SPARK_HOME}"

RUN curl -fsSL "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar" \
      -o "${SPARK_HOME}/jars/hadoop-aws-3.3.4.jar" \
 && curl -fsSL "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar" \
      -o "${SPARK_HOME}/jars/aws-java-sdk-bundle-1.12.262.jar"

# Install pyspark (matching Spark)
RUN pip install --no-cache-dir "pyspark==${SPARK_VERSION}"

WORKDIR /opt/de_project
