import os
from pyspark.sql import SparkSession, functions as F, types as T

BUCKET = os.environ["S3_BUCKET_NAME"]
ENDPOINT = os.environ["S3_ENDPOINT_URL"]

LANDING_PATH = f"s3a://{BUCKET}/landing/taxi/year=2024/month=01/"
PREPARED_PATH = f"s3a://{BUCKET}/prepared/taxi/year=2024/month=01/"

def get_spark():
    return (
        SparkSession.builder
        .appName("TaxiTransformToPrepared")
        .config("spark.hadoop.fs.s3a.endpoint", ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def transform(df):
    """
    Basic cleaning + enrichment for analytics and warehouse loading.
    Adjust this later as you refine the portfolio story.
    """

    # Filter out obviously bad rows
    df = df.where(
        (F.col("tpep_pickup_datetime").isNotNull()) &
        (F.col("tpep_dropoff_datetime").isNotNull()) &
        (F.col("trip_distance") >= 0) &
        (F.col("total_amount") >= 0)
    )

    # Derive date & time features
    df = df.withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
    df = df.withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
    df = df.withColumn("dropoff_date", F.to_date("tpep_dropoff_datetime"))

    # Trip duration in minutes
    df = df.withColumn(
        "trip_duration_min",
        (
            F.unix_timestamp("tpep_dropoff_datetime") -
            F.unix_timestamp("tpep_pickup_datetime")
        ) / 60.0
    )

    # Filter out weird durations
    df = df.where((F.col("trip_duration_min") > 0))

    # Average speed
    df = df.withColumn(
        "avg_mph",
        F.when(F.col("trip_duration_min") > 0,
               F.col("trip_distance") / (F.col("trip_duration_min") / 60.0)
        ).otherwise(None)
    )

    # Cast few columns to more appropriate types
    df = df.withColumn("passenger_count", F.col("passenger_count").cast(T.IntegerType()))
    df = df.withColumn("payment_type", F.col("payment_type").cast(T.IntegerType()))
    df = df.withColumn("RatecodeID", F.col("RatecodeID").cast(T.IntegerType()))

    return df


def main():
    spark = get_spark()

    print(f"Reading landing data from: {LANDING_PATH}")
    df = spark.read.parquet(LANDING_PATH)
    print("Landing schema:")
    df.printSchema()
    print(f"Landing row count: {df.count()}")

    df_prep = transform(df)

    print("Prepared schema:")
    df_prep.printSchema()
    print(f"Prepared row count: {df_prep.count()}")

    print(f"Writing prepared data to: {PREPARED_PATH}")
    (
        df_prep
        .repartition("pickup_date")  # simple partitioning by day
        .write
        .mode("overwrite")
        .partitionBy("pickup_date")
        .parquet(PREPARED_PATH)
    )

    print("Done.")
    spark.stop()


if __name__ == "__main__":
    main()
