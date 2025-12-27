import os
from pyspark.sql import SparkSession

BUCKET = os.environ["S3_BUCKET_NAME"]        
ENDPOINT = os.environ["S3_ENDPOINT_URL"]    

LOCAL_FILE = "/opt/de_project/data/yellow_tripdata_2024-01.parquet"
LANDING_PATH = f"s3a://{BUCKET}/landing/taxi/year=2024/month=01/"

def get_spark():
    return (
        SparkSession.builder
        .appName("TaxiIngestToLanding")
        .config("spark.hadoop.fs.s3a.endpoint", ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )

def main():
    spark = get_spark()

    print(f"Reading local file: {LOCAL_FILE}")
    df = spark.read.parquet(LOCAL_FILE)

    print("Schema:")
    df.printSchema()
    print(f"Row count: {df.count()}")

    print(f"Writing to MinIO path: {LANDING_PATH}")
    (
        df.write
        .mode("overwrite")
        .parquet(LANDING_PATH)
    )

    print("Done.")
    spark.stop()

if __name__ == "__main__":
    main()
