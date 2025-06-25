import os
from pathlib import Path
from pyspark.sql import SparkSession
import subprocess

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "admin")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "password")

if "spark" in locals():
    locals["spark"].stop()


def get_spark_session() -> SparkSession:
    # Create a SparkSession
    spark = (
        SparkSession.builder.appName("CreateIcebergTable")
        .master("spark://spark-master:7077")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.sql.streaming.schemaInference", "true")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.defaultCatalog", "default")
        .config("spark.sql.catalog.default", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.default.type", "rest")
        .config("spark.sql.catalog.default.uri", "http://spark-rest:8181")
        .config(
            "spark.sql.catalog.default.io-impl", "org.apache.iceberg.aws.s3.S3FileIO"
        )
        .config("spark.sql.catalog.default.s3.endpoint", "http://minio:9000")
        .config("spark.sql.catalog.default.warehouse", "s3a://data/wh")
        .config("spark.sql.catalog.default.s3.access-key", AWS_ACCESS_KEY_ID)
        .config("spark.sql.catalog.default.s3.secret-key", AWS_SECRET_ACCESS_KEY)
        .config(
            "spark.sql.catalog.default.s3.path-style-access", "true"
        )  # Add this for catalog
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("INFO")
    return spark


def main():
    spark = get_spark_session()
    spark.sql("SHOW DATABASES;").show()
    db_name = "nyc"
    table_name = "taxis"
    spark.sql("USE nyc;")
    spark.sql("SHOW TABLES;").show()


if __name__ == "__main__":
    main()
