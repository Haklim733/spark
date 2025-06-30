import os
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType,
)
from pyspark.sql import Row
import time
from datetime import datetime
import random
from datetime import datetime

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "admin")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "password")

if "spark" in locals():
    locals["spark"].stop()


def get_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder.appName("CreateIcebergTable")
        .config("spark.sql.streaming.schemaInference", "true")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.defaultCatalog", "iceberg")
        .config("spark.sql.catalog.iceberg.type", "rest")
        .config("spark.sql.catalog.iceberg.uri", "http://spark-rest:8181")
        .config("spark.sql.catalog.iceberg.s3.endpoint", "http://minio:9000")
        .config("spark.sql.catalog.iceberg.warehouse", "s3://data/wh")
        .config("spark.sql.catalog.iceberg.s3.access-key", AWS_ACCESS_KEY_ID)
        .config("spark.sql.catalog.iceberg.s3.secret-key", AWS_SECRET_ACCESS_KEY)
        .config("spark.sql.catalog.iceberg.s3.region", "us-east-1")
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.region", "us-east-1")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .master("spark://spark-master:7077")
        .getOrCreate()
    )

    # #parallelization
    # .config("spark.eventLog.dir", "file:/opt/bitnami/spark/logs/spark-events")
    # .config("spark.history.fs.logDirectory", "file:/opt/bitnami/spark/logs/spark-events")
    # .config("spark.executor.logs.rolling.strategy", "time")
    # .config("spark.executor.logs.rolling.time.interval", "daily")
    # .config("spark.executor.logs.rolling.maxRetainedFiles", "7")
    spark.sparkContext.setLogLevel("INFO")
    return spark


def main():
    # Create test data
    spark = get_spark_session()
    db_name = "test"
    table_name = "tps"
    table_identifier = f"{db_name}.{table_name}"
    spark.sql("SHOW NAMESPACES;").show()
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
    spark.sql("SHOW DATABASES;").show()
    spark.sql(f"DROP TABLE IF EXISTS {table_identifier}")

    # schema
    schema = StructType(
        [
            StructField("id", StringType(), False),
            StructField("value", StringType(), False),
        ]
    )

    # Create an empty dataframe with the schema to initialize the table
    empty_df = spark.createDataFrame([], schema)
    print("Creating Iceberg table...")
    empty_df.writeTo(f"iceberg.{db_name}.{table_name}").using("iceberg").tableProperty(
        "write.merge.isolation-level", "snapshot"
    ).tableProperty("write.format.default", "avro").create()
    # Append operation
    num_records = 1000000
    df = spark.range(0, num_records).selectExpr(
        "id", "concat('test-', cast(id as string)) as value"
    )
    start_time = time.time()
    df.writeTo(f"iceberg.{db_name}.{table_name}").using("iceberg").tableProperty(
        "write.merge.isolation-level", "snapshot"
    ).append()

    duration = time.time() - start_time
    throughput = num_records / duration
    print(f"Wrote {df.count()} records in {duration:.2f} seconds")
    print(f"Throughput: {throughput:.2f} records/second")
    spark.sql(f"SELECT COUNT(*) FROM {table_identifier}").show(truncate=False)
    spark.sql(f"SELECT COUNT(*) FROM {table_identifier}.files;").show(truncate=False)


if __name__ == "__main__":
    main()
