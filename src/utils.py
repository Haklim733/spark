#!/usr/bin/env python3
"""
Spark Connect utilities with warning suppression
"""

import warnings
from pyspark.sql import SparkSession
import os

# Suppress protobuf version warnings
warnings.filterwarnings("ignore", message=".*Protobuf gencode version.*")
# Suppress Spark Connect configuration warnings
warnings.filterwarnings("ignore", message=".*Failed to set spark.eventLog.dir.*")
warnings.filterwarnings(
    "ignore", message=".*Cannot modify the value of the Spark config.*"
)


def create_spark_connect_session(app_name="SparkConnectApp", **configs):
    """
    Create a Spark Connect session with warning suppression

    Args:
        app_name: Name of the Spark application
        **configs: Additional Spark configurations

    Returns:
        SparkSession: Configured Spark session
    """
    builder = SparkSession.builder.appName(app_name).remote("sc://localhost:15002")

    # Apply additional configurations
    for key, value in configs.items():
        builder = builder.config(key, value)

    return builder.getOrCreate()


def create_iceberg_spark_session(app_name="IcebergSparkConnect"):
    """
    Create a Spark Connect session with Iceberg and MinIO configuration

    Args:
        app_name: Name of the Spark application

    Returns:
        SparkSession: Configured Spark session with Iceberg support
    """
    # Environment variables for AWS/MinIO
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID", "admin")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "password")

    return create_spark_connect_session(
        app_name=app_name,
        # Iceberg configurations
        spark_sql_extensions="org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        spark_sql_catalog_iceberg="org.apache.iceberg.spark.SparkCatalog",
        spark_sql_defaultCatalog="iceberg",
        spark_sql_catalog_iceberg_type="rest",
        spark_sql_catalog_iceberg_uri="http://localhost:8181",
        spark_sql_catalog_iceberg_s3_endpoint="http://localhost:9000",
        spark_sql_catalog_iceberg_warehouse="s3://data/wh",
        spark_sql_catalog_iceberg_s3_access_key=aws_access_key,
        spark_sql_catalog_iceberg_s3_secret_key=aws_secret_key,
        spark_sql_catalog_iceberg_s3_region="us-east-1",
        # S3/MinIO configurations
        spark_hadoop_fs_s3_impl="org.apache.hadoop.fs.s3a.S3AFileSystem",
        spark_hadoop_fs_s3a_access_key=aws_access_key,
        spark_hadoop_fs_s3a_secret_key=aws_secret_key,
        spark_hadoop_fs_s3a_endpoint="http://localhost:9000",
        spark_hadoop_fs_s3a_region="us-east-1",
        spark_hadoop_fs_s3a_path_style_access="true",
        spark_hadoop_fs_s3a_connection_ssl_enabled="false",
        # Performance optimizations
        spark_sql_adaptive_enabled="true",
        spark_sql_adaptive_coalescePartitions_enabled="true",
        spark_sql_adaptive_skewJoin_enabled="true",
        spark_sql_adaptive_localShuffleReader_enabled="true",
        spark_sql_shuffle_partitions="200",
    )


def test_connection():
    """Test Spark Connect connection with warning suppression"""
    print("Testing Spark Connect connection (with warning suppression)...")

    try:
        spark = create_spark_connect_session("ConnectionTest")
        print(f"✅ Successfully connected to Spark cluster!")
        print(f"   Spark version: {spark.version}")

        # Test basic operation
        data = [("test", 1), ("data", 2)]
        df = spark.createDataFrame(data, ["word", "count"])
        df.show()

        spark.stop()
        print("✅ Connection test passed!")
        return True

    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False
