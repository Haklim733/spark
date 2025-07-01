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


class S3FileSystemConfig:
    """Configuration class for S3 Filesystem settings"""

    def __init__(
        self,
        endpoint: str = "http://minio:9000",
        region: str = "us-east-1",
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        path_style_access: bool = True,
        ssl_enabled: bool = False,
    ):
        self.endpoint = endpoint
        self.region = region
        self.access_key = access_key or os.getenv("AWS_ACCESS_KEY_ID", "admin")
        self.secret_key = secret_key or os.getenv("AWS_SECRET_ACCESS_KEY", "password")
        self.path_style_access = path_style_access
        self.ssl_enabled = ssl_enabled

    def get_spark_configs(self) -> Dict[str, str]:
        """Get Spark configuration dictionary for S3 Filesystem"""
        return {
            "spark.hadoop.fs.s3.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.access.key": self.access_key,
            "spark.hadoop.fs.s3a.secret.key": self.secret_key,
            "spark.hadoop.fs.s3a.endpoint": self.endpoint,
            "spark.hadoop.fs.s3a.region": self.region,
            "spark.hadoop.fs.s3a.path.style.access": str(
                self.path_style_access
            ).lower(),
            "spark.hadoop.fs.s3a.connection.ssl.enabled": str(self.ssl_enabled).lower(),
            # Enable S3A metrics to suppress warning and collect useful metrics
            "spark.hadoop.fs.s3a.metrics.enabled": "true",
            "spark.hadoop.fs.s3a.metrics.reporting.interval": "60",
            # Additional S3A configurations for better compatibility
            "spark.hadoop.fs.s3a.connection.timeout": "60000",
            "spark.hadoop.fs.s3a.socket.timeout": "60000",
            "spark.hadoop.fs.s3a.max.connections": "100",
            "spark.hadoop.fs.s3a.threads.max": "20",
            "spark.hadoop.fs.s3a.threads.core": "10",
            "spark.hadoop.fs.s3a.buffer.dir": "/tmp",
            "spark.hadoop.fs.s3a.experimental.input.fadvise": "normal",
        }


class IcebergConfig:
    """Configuration class for Iceberg settings"""

    def __init__(
        self,
        s3_config: S3FileSystemConfig,
        catalog_type: str = "rest",
        catalog_uri: str = "http://iceberg-rest:8181",
        warehouse: str = "s3://data/wh",
    ):
        self.s3_config = s3_config
        self.catalog_type = catalog_type
        self.catalog_uri = catalog_uri
        self.warehouse = warehouse

    def get_spark_configs(self) -> Dict[str, str]:
        """Get Spark configuration dictionary for Iceberg"""
        configs = {
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            "spark.sql.defaultCatalog": "iceberg",
            "spark.sql.catalog.iceberg.warehouse": self.warehouse,
            "spark.sql.execution.metrics.enabled": "true",
            "spark.sql.execution.metrics.persist": "true",
        }

        if self.catalog_type == "hadoop":
            configs.update(
                {
                    "spark.sql.catalog.iceberg": "org.apache.iceberg.spark.SparkCatalog",
                    "spark.sql.catalog.iceberg.type": "hadoop",
                }
            )
        elif self.catalog_type == "jdbc":
            configs.update(
                {
                    "spark.sql.catalog.iceberg": "org.apache.iceberg.jdbc.JdbcCatalog",
                    "spark.sql.catalog.iceberg.catalog-impl": "org.apache.iceberg.jdbc.JdbcCatalog",
                    "spark.sql.catalog.iceberg.uri": self.catalog_uri,
                }
            )
        elif self.catalog_type == "rest":
            configs.update(
                {
                    "spark.sql.catalog.iceberg": "org.apache.iceberg.spark.SparkCatalog",
                    "spark.sql.catalog.iceberg.catalog-impl": "org.apache.iceberg.rest.RESTCatalog",
                    "spark.sql.catalog.iceberg.uri": self.catalog_uri,
                    "spark.sql.catalog.iceberg.s3.endpoint": self.s3_config.endpoint,
                    "spark.sql.catalog.iceberg.s3.access-key": self.s3_config.access_key,
                    "spark.sql.catalog.iceberg.s3.secret-key": self.s3_config.secret_key,
                    "spark.sql.catalog.iceberg.s3.region": self.s3_config.region,
                    "spark.sql.catalog.iceberg.s3.path-style-access": str(
                        self.s3_config.path_style_access
                    ).lower(),
                    "spark.sql.catalog.iceberg.s3.ssl-enabled": str(
                        self.s3_config.ssl_enabled
                    ).lower(),
                    "spark.sql.catalog.iceberg.s3.connection-timeout": "60000",
                    "spark.sql.catalog.iceberg.s3.socket-timeout": "60000",
                    "spark.sql.catalog.iceberg.s3.max-connections": "100",
                }
            )

        # S3 configuration for data storage
        configs.update(self.s3_config.get_spark_configs())

        return configs


class PerformanceConfig:
    """Configuration class for Spark performance settings"""

    def __init__(
        self,
        adaptive_query_execution: bool = True,
        shuffle_partitions: int = 200,
        max_partition_bytes: str = "128m",
        advisory_partition_size: str = "128m",
        skew_join_enabled: bool = True,
        skewed_partition_threshold: str = "256m",
        arrow_pyspark_enabled: bool = True,
        use_kryo_serializer: bool = False,
    ):
        self.adaptive_query_execution = adaptive_query_execution
        self.shuffle_partitions = shuffle_partitions
        self.max_partition_bytes = max_partition_bytes
        self.advisory_partition_size = advisory_partition_size
        self.skew_join_enabled = skew_join_enabled
        self.skewed_partition_threshold = skewed_partition_threshold
        self.arrow_pyspark_enabled = arrow_pyspark_enabled
        self.use_kryo_serializer = use_kryo_serializer

    def get_spark_configs(self) -> Dict[str, str]:
        """Get Spark configuration dictionary for performance settings"""
        configs = {
            "spark.sql.adaptive.enabled": str(self.adaptive_query_execution).lower(),
            "spark.sql.adaptive.coalescePartitions.enabled": str(
                self.adaptive_query_execution
            ).lower(),
            "spark.sql.adaptive.skewJoin.enabled": str(self.skew_join_enabled).lower(),
            "spark.sql.adaptive.localShuffleReader.enabled": str(
                self.adaptive_query_execution
            ).lower(),
            "spark.sql.shuffle.partitions": str(self.shuffle_partitions),
            "spark.sql.files.maxPartitionBytes": self.max_partition_bytes,
            "spark.sql.adaptive.advisoryPartitionSizeInBytes": self.advisory_partition_size,
            "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": self.skewed_partition_threshold,
            "spark.sql.execution.arrow.pyspark.enabled": str(
                self.arrow_pyspark_enabled
            ).lower(),
            "spark.sql.execution.arrow.pyspark.fallback.enabled": str(
                self.arrow_pyspark_enabled
            ).lower(),
        }
        if self.use_kryo_serializer:
            configs["spark.serializer"] = "org.apache.spark.serializer.KryoSerializer"

        return configs


def create_spark_session(
    spark_version: SparkVersion = SparkVersion.SPARK_3_5,
    app_name: str = "SparkApp",
    iceberg_config: Optional[IcebergConfig] = None,
    performance_config: Optional[PerformanceConfig] = None,
    **additional_configs,
) -> SparkSession:
    """
    Create a Spark session based on version enum with optional Iceberg configuration

    Args:
        spark_version: SparkVersion enum specifying the type of session to create
        app_name: Name of the Spark application (used as prefix for event logs)
        iceberg_config: Optional IcebergConfig for Iceberg integration
        performance_config: Optional PerformanceConfig for performance tuning
        **additional_configs: Additional Spark configurations (overrides defaults)

    Returns:
        SparkSession: Configured Spark session
    """
    # Use provided performance config or create a default one
    log_dir = f"/opt/bitnami/spark/logs/app/{app_name}"
    os.makedirs(log_dir, exist_ok=True)
    if not performance_config:
        performance_config = PerformanceConfig()

    if not iceberg_config:
        iceberg_config = IcebergConfig(s3_config=S3FileSystemConfig())

    # Merge configurations: additional_configs override performance configs
    merged_configs = {
        **performance_config.get_spark_configs(),
        **iceberg_config.get_spark_configs(),
        "spark.eventLog.enabled": "true",
        "spark.eventLog.dir": f"file:///opt/bitnami/spark/logs/app/{app_name}",
        **additional_configs,
    }

    if spark_version == SparkVersion.SPARK_CONNECT_4_0:
        return _create_spark_connect_session(
            app_name=app_name, spark_params=merged_configs
        )
    elif spark_version in [SparkVersion.SPARK_3_5, SparkVersion.SPARK_4_0]:
        return _create_pyspark_session(app_name=app_name, spark_params=merged_configs)
    else:
        raise ValueError(f"Unsupported Spark version: {spark_version}")


def _create_spark_connect_session(
    app_name: str, spark_params: Dict[str, str]
) -> SparkSession:
    """Create a Spark Connect session (4.0+)"""
    builder = SparkSession.builder.appName(app_name).remote("sc://localhost:15002")
    # Apply additional configurations
    for key, value in spark_params.items():
        builder = builder.config(key, value)

    return builder.getOrCreate()


def _get_iceberg_jars() -> str:
    """Get the Iceberg JARs for the classpath"""
    spark_home = "/opt/bitnami/spark"
    iceberg_jars = [
        f"{spark_home}/jars/iceberg-spark-runtime-3.5_2.12-1.9.1.jar",
        f"{spark_home}/jars/iceberg-aws-bundle-1.9.1.jar",
        f"{spark_home}/jars/spark-avro_2.12-3.5.6.jar",
    ]
    return ",".join([jar for jar in iceberg_jars if os.path.exists(jar)])


def _create_pyspark_session(
    app_name: str, spark_params: Dict[str, str]
) -> SparkSession:
    """Create a regular PySpark session (3.5)"""
    # Create event log directory in app folder

    builder = SparkSession.builder.appName(app_name)

    # Load spark-defaults.conf configurations
    spark_defaults_path = "/opt/bitnami/spark/conf/spark-defaults.conf"
    if os.path.exists(spark_defaults_path):
        with open(spark_defaults_path, "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and " " in line:
                    key, value = line.split(" ", 1)
                    # Only set if not already in spark_params (spark_params takes precedence)
                    if key not in spark_params:
                        builder = builder.config(key, value)

    # Add Iceberg JARs to classpath
    iceberg_jars = _get_iceberg_jars()
    if iceberg_jars:
        builder = builder.config("spark.jars", iceberg_jars)

    # Apply additional configurations (these override spark-defaults.conf)
    for key, value in spark_params.items():
        builder = builder.config(key, value)

    # Set master for regular PySpark
    builder = builder.master("spark://spark-master:7077")

    return builder.getOrCreate()
