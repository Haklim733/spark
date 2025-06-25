#!/usr/bin/env python3
from enum import Enum
import os
from typing import Dict, Optional

from pyspark.sql import SparkSession


class SparkVersion(Enum):
    """Enum for Spark versions and connection types"""

    SPARK_CONNECT_4_0 = "spark_connect_4_0"  # Spark Connect (4.0+)
    SPARK_3_5 = "spark_3_5"  # Regular PySpark (3.5)
    SPARK_4_0 = "spark_4_0"  # Regular PySpark (4.0)


class IcebergConfig:
    """Configuration class for Iceberg settings"""

    def __init__(
        self,
        catalog_type: str = "rest",
        catalog_uri: str = "http://spark-rest:8181",
        warehouse: str = "s3://data/wh",
        s3_endpoint: str = "http://minio:9000",
        s3_region: str = "us-east-1",
        s3_access_key: Optional[str] = None,
        s3_secret_key: Optional[str] = None,
    ):
        self.catalog_type = catalog_type
        self.catalog_uri = catalog_uri
        self.warehouse = warehouse
        self.s3_endpoint = s3_endpoint
        self.s3_region = s3_region
        self.s3_access_key = s3_access_key or os.getenv("AWS_ACCESS_KEY_ID", "admin")
        self.s3_secret_key = s3_secret_key or os.getenv(
            "AWS_SECRET_ACCESS_KEY", "password"
        )

    def get_spark_configs(self) -> Dict[str, str]:
        """Get Spark configuration dictionary for Iceberg"""
        return {
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            "spark.sql.catalog.iceberg": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.defaultCatalog": "iceberg",
            "spark.sql.catalog.iceberg.type": self.catalog_type,
            "spark.sql.catalog.iceberg.uri": self.catalog_uri,
            "spark.sql.catalog.iceberg.s3.endpoint": self.s3_endpoint,
            "spark.sql.catalog.iceberg.warehouse": self.warehouse,
            "spark.sql.catalog.iceberg.s3.access-key": self.s3_access_key,
            "spark.sql.catalog.iceberg.s3.secret-key": self.s3_secret_key,
            "spark.sql.catalog.iceberg.s3.region": self.s3_region,
            "spark.hadoop.fs.s3.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.access.key": self.s3_access_key,
            "spark.hadoop.fs.s3a.secret.key": self.s3_secret_key,
            "spark.hadoop.fs.s3a.endpoint": self.s3_endpoint,
            "spark.hadoop.fs.s3a.region": self.s3_region,
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        }


def create_spark_session(
    spark_version: SparkVersion = SparkVersion.SPARK_3_5,
    app_name: str = "SparkApp",
    iceberg_config: Optional[IcebergConfig] = None,
    **additional_configs,
) -> SparkSession:
    """
    Create a Spark session based on version enum with optional Iceberg configuration

    Args:
        spark_version: SparkVersion enum specifying the type of session to create
        app_name: Name of the Spark application (used as prefix for event logs)
        iceberg_config: Optional IcebergConfig for Iceberg integration
        **additional_configs: Additional Spark configurations (overrides defaults)

    Returns:
        SparkSession: Configured Spark session
    """
    # Create event log directory if it doesn't exist
    log_dir = f"/opt/bitnami/spark/logs/{app_name}"
    os.makedirs(log_dir, exist_ok=True)

    # Default performance configurations
    default_performance_configs = {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.sql.adaptive.localShuffleReader.enabled": "true",
        "spark.sql.shuffle.partitions": "200",
        "spark.sql.adaptive.advisoryPartitionSizeInBytes": "128m",
        "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "256m",
        "spark.sql.adaptive.skewJoin.skewedPartitionFactor": "5",
        "spark.eventLog.enabled": "true",
        "spark.eventLog.dir": f"file:///opt/bitnami/spark/logs/{app_name}",
        "spark.sql.execution.arrow.pyspark.enabled": "true",
    }

    # Merge configurations: additional_configs override defaults
    merged_configs = {**default_performance_configs, **additional_configs}

    if spark_version == SparkVersion.SPARK_CONNECT_4_0:
        return _create_spark_connect_session(app_name, iceberg_config, **merged_configs)
    elif spark_version == SparkVersion.SPARK_3_5:
        return _create_pyspark_session(app_name, iceberg_config, **merged_configs)
    else:
        raise ValueError(f"Unsupported Spark version: {spark_version}")


def _create_spark_connect_session(
    app_name: str, iceberg_config: Optional[IcebergConfig] = None, **additional_configs
) -> SparkSession:
    """Create a Spark Connect session (4.0+)"""
    # Create event log directory if it doesn't exist
    log_dir = f"/opt/bitnami/spark/logs/{app_name}"
    os.makedirs(log_dir, exist_ok=True)

    builder = SparkSession.builder.appName(app_name).remote("sc://localhost:15002")

    # Apply Iceberg configurations if provided
    if iceberg_config:
        for key, value in iceberg_config.get_spark_configs().items():
            builder = builder.config(key, value)

    # Apply additional configurations
    for key, value in additional_configs.items():
        builder = builder.config(key, value)

    return builder.getOrCreate()


def _create_pyspark_session(
    app_name: str, iceberg_config: Optional[IcebergConfig] = None, **additional_configs
) -> SparkSession:
    """Create a regular PySpark session (3.5)"""
    # Create event log directory if it doesn't exist
    log_dir = f"/opt/bitnami/spark/logs/{app_name}"
    os.makedirs(log_dir, exist_ok=True)

    builder = SparkSession.builder.appName(app_name)

    # Apply Iceberg configurations if provided
    if iceberg_config:
        for key, value in iceberg_config.get_spark_configs().items():
            builder = builder.config(key, value)

    # Apply additional configurations
    for key, value in additional_configs.items():
        builder = builder.config(key, value)

    # Set master for regular PySpark
    builder = builder.master("spark://spark-master:7077")

    return builder.getOrCreate()


# def create_shuffling_spark_session(
#     app_name: str = "ShufflingIssuesDemo",
# ) -> SparkSession:
#     """
#     Create and configure Spark session with shuffling monitoring using regular PySpark

#     Args:
#         app_name: Name of the Spark application

#     Returns:
#         SparkSession: Configured Spark session for shuffling demonstrations
#     """
#     # Get AWS credentials from environment
#     aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID", "admin")
#     aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY", "password")

#     spark = (
#         SparkSession.builder.appName(app_name)
#         .config("spark.sql.streaming.schemaInference", "true")
#         .config(
#             "spark.sql.extensions",
#             "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
#         )
#         .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
#         .config("spark.sql.defaultCatalog", "iceberg")
#         .config("spark.sql.catalog.iceberg.type", "rest")
#         .config("spark.sql.catalog.iceberg.uri", "http://spark-rest:8181")
#         .config("spark.sql.catalog.iceberg.s3.endpoint", "http://minio:9000")
#         .config("spark.sql.catalog.iceberg.warehouse", "s3://data/wh")
#         .config("spark.sql.catalog.iceberg.s3.access-key", aws_access_key_id)
#         .config("spark.sql.catalog.iceberg.s3.secret-key", aws_secret_access_key)
#         .config("spark.sql.catalog.iceberg.s3.region", "us-east-1")
#         .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
#         .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id)
#         .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key)
#         .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
#         .config("spark.hadoop.fs.s3a.region", "us-east-1")
#         .config("spark.hadoop.fs.s3a.path.style.access", "true")
#         .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
#         # Shuffling-specific configurations
#         .config("spark.sql.adaptive.enabled", "true")
#         .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
#         .config("spark.sql.adaptive.skewJoin.enabled", "true")
#         .config("spark.sql.adaptive.localShuffleReader.enabled", "true")
#         .config("spark.sql.shuffle.partitions", "200")  # Default shuffle partitions
#         .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128m")
#         .config("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256m")
#         .config("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
#         # Monitoring configurations
#         .config("spark.eventLog.enabled", "true")
#         .config("spark.eventLog.dir", f"file:///opt/bitnami/spark/logs/{app_name}")
#         .config("spark.sql.execution.arrow.pyspark.enabled", "true")
#         .master(
#             "spark://spark-master:7077"
#         )  # Use Spark cluster instead of Spark Connect
#         .getOrCreate()
#     )

#     return spark
