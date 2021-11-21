import sys
import uuid
import random

from pyspark.sql import SparkSession, Row
from pyspark import SparkConf


def main():
    conf = SparkConf()
    conf.set("spark.yarn.maxAppAttempts", "1")
    conf.set("spark.hadoop.fs.s3a.endpoint", "192.168.2.39:9000")
    conf.set("spark.hadoop.fs.s3a.access.key", "kraikrai")
    conf.set("spark.hadoop.fs.s3a.secret.key", "kraikrai")
    conf.set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    # "Enable S3 path style access ie disabling the default virtual hosting behaviour.
    # Useful for S3A-compliant storage providers as it removes the need to set up DNS for virtual hosting."
    conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
    # Set Committer config compliant with MinIO
    conf.set("spark.hadoop.fs.s3a.committer.name", "directory")
    conf.set("spark.hadoop.fs.s3a.committer.staging.conflict-mode", "replace")
    conf.set("spark.hadoop.fs.s3a.committer.staging.tmp.path", "/tmp/staging")

    spark = SparkSession.builder.appName("Dockerized Minio Spark Test").config(conf=conf).getOrCreate()

    data = [
        {"first_col": "row 1", "id": str(uuid.uuid4()), "color": "blue", "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 2", "id": str(uuid.uuid4()), "color": "green", "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 3", "id": str(uuid.uuid4()), "color": "pink", "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 4", "id": str(uuid.uuid4()), "color": "yellow", "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 5", "id": str(uuid.uuid4()), "color": "red", "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 6", "id": str(uuid.uuid4()), "color": "orange", "numeric_val": random.randint(-100, 100)},
        {"first_col": "row 7", "id": str(uuid.uuid4()), "color": "magenta", "numeric_val": random.randint(-100, 100)},
    ]

    df = spark.createDataFrame([Row(**data_row) for data_row in data])
    df.write.csv(f"s3a://data/output/")

    spark.stop()


if __name__ == "__main__":
    main()

