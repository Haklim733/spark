#!/usr/bin/env python3
from enum import Enum
import os
import zipfile
import tempfile
from typing import Dict, Optional

from pyspark.sql import SparkSession


class SparkVersion(Enum):
    """Enum for Spark versions and connection types"""

    SPARK_CONNECT_3_5 = "spark_connect_3_5"  # Spark Connect (3.5)
    SPARK_CONNECT_4_0 = "spark_connect_4_0"  # Spark Connect (4.0+)
    SPARK_3_5 = "spark_3_5"  # Regular PySpark (3.5)
    SPARK_4_0 = "spark_4_0"  # Regular PySpark (4.0)


class S3FileSystemConfig:
    """Configuration class for S3 Filesystem settings (security/credential overrides only)"""

    def __init__(
        self,
        endpoint: str = None,
        region: str = None,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        path_style_access: bool = True,
        ssl_enabled: bool = False,
    ):
        # Security settings (per-app overrides)
        self.endpoint = endpoint or os.getenv("S3_ENDPOINT", "localhost:9000")
        self.region = region or os.getenv("AWS_REGION", "us-east-1")
        self.access_key = access_key or os.getenv("AWS_ACCESS_KEY_ID")
        self.secret_key = secret_key or os.getenv("AWS_SECRET_ACCESS_KEY")
        self.path_style_access = path_style_access
        self.ssl_enabled = ssl_enabled

    def get_spark_configs(self) -> Dict[str, str]:
        """Get Spark configuration dictionary for S3 Filesystem (security/credential overrides only)"""
        configs = {
            "spark.hadoop.fs.s3a.access.key": self.access_key,
            "spark.hadoop.fs.s3a.secret.key": self.secret_key,
            "spark.hadoop.fs.s3a.region": self.region,
            "spark.hadoop.fs.s3a.endpoint": f"http://{self.endpoint}",
            "spark.hadoop.fs.s3a.path.style.access": str(
                self.path_style_access
            ).lower(),
            "spark.hadoop.fs.s3a.connection.ssl.enabled": str(self.ssl_enabled).lower(),
            "spark.hadoop.fs.s3a.ssl.enabled": str(self.ssl_enabled).lower(),
        }
        return configs


class IcebergConfig:
    """Configuration class for Iceberg settings"""

    def __init__(
        self,
        s3_config: S3FileSystemConfig,
        catalog_uri: str = "http://iceberg-rest:8181",
        warehouse: str = "s3://iceberg/wh",
        catalog_type: str = "rest",
        catalog: str = "iceberg",
    ):
        self.s3_config = s3_config
        self.catalog_uri = catalog_uri
        self.warehouse = warehouse
        self.catalog_type = catalog_type
        self.catalog = catalog

    def get_spark_configs(self) -> Dict[str, str]:
        """Get Spark configuration dictionary for Iceberg (application-specific only)"""
        configs = {
            # Application-specific Iceberg settings (server-side configs are in spark-defaults.conf)
            # Only set credentials and endpoint - let server-side handle catalog type and other settings
            "spark.sql.catalog.iceberg.s3.endpoint": f"http://{self.s3_config.endpoint}",
            "spark.sql.catalog.iceberg.s3.access-key": self.s3_config.access_key
            or "admin",
            "spark.sql.catalog.iceberg.s3.secret-key": self.s3_config.secret_key
            or "password",
            "spark.sql.catalog.iceberg.s3.region": self.s3_config.region or "us-east-1",
            "spark.sql.defaultCatalog": self.catalog,
            "spark.sql.catalog.iceberg.type": self.catalog_type,
            "spark.sql.catalog.iceberg.uri": self.catalog_uri,
            "spark.sql.catalog.iceberg.warehouse": self.warehouse,
            "spark.sql.catalog.iceberg.s3.path-style-access": str(
                self.s3_config.path_style_access
            ).lower(),
            "spark.sql.catalog.iceberg.s3.ssl-enabled": str(
                self.s3_config.ssl_enabled
            ).lower(),
        }

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
    spark_version: SparkVersion = SparkVersion.SPARK_CONNECT_3_5,
    app_name: str = None,
    iceberg_config: Optional[IcebergConfig] = None,
    performance_config: Optional[PerformanceConfig] = None,
    s3_config: Optional[S3FileSystemConfig] = None,
    **additional_configs,
) -> SparkSession:
    """
    Create a Spark session based on version enum with optional Iceberg configuration

    Args:
        spark_version: SparkVersion enum specifying the type of session to create
        app_name: Name of the Spark application (used as prefix for event logs)
        iceberg_config: Optional IcebergConfig for Iceberg integration (defaults to Iceberg)
        performance_config: Optional PerformanceConfig for performance tuning
        s3_config: Optional S3FileSystemConfig for S3/MinIO access without Iceberg
        **additional_configs: Additional Spark configurations (overrides defaults)

    Returns:
        SparkSession: Configured Spark session
    """
    # Auto-detect app name from __file__ if not provided
    if app_name is None:
        import inspect

        try:
            # Get the calling frame
            frame = inspect.currentframe()
            while frame:
                frame = frame.f_back
                if frame and frame.f_globals.get("__file__"):
                    # Extract filename without extension
                    app_name = os.path.splitext(
                        os.path.basename(frame.f_globals["__file__"])
                    )[0]
                    break
        except Exception:
            app_name = "SparkApp"

    # Use provided performance config or create a default one
    if not performance_config:
        performance_config = PerformanceConfig()

    # If iceberg_config is not provided, use default IcebergConfig
    if iceberg_config is None:
        if s3_config is None:
            s3_config = S3FileSystemConfig()
        iceberg_config = IcebergConfig(s3_config)

    # Start with performance configs (application-specific)
    merged_configs = {
        **performance_config.get_spark_configs(),
        **additional_configs,
    }

    # Add S3A filesystem configs if s3_config is provided
    if s3_config:
        merged_configs.update(s3_config.get_spark_configs())

    # Add Iceberg configs (these will be merged with S3A configs)
    if iceberg_config:
        merged_configs.update(iceberg_config.get_spark_configs())

    # Add application-specific configurations (not in spark-defaults.conf)
    app_specific_configs = {
        # Application-specific settings that can be overridden per app
        # (server-side defaults are in spark-defaults.conf)
    }

    # Merge all configurations (additional_configs takes precedence)
    merged_configs = {**app_specific_configs, **merged_configs}

    # Set up event logging (only for regular Spark, not Spark Connect)
    if spark_version not in [
        SparkVersion.SPARK_CONNECT_4_0,
        SparkVersion.SPARK_CONNECT_3_5,
    ]:
        log_dir = f"/opt/bitnami/spark/logs/app/{app_name}"
        if os.path.exists("/opt/bitnami/spark") or os.getenv(
            "SPARK_HOME", ""
        ).startswith("/opt/bitnami"):
            # We're in Docker environment
            try:
                os.makedirs(log_dir, exist_ok=True)
                merged_configs.update(
                    {
                        "spark.eventLog.enabled": "true",
                        "spark.eventLog.dir": f"file:///opt/bitnami/spark/logs/app/{app_name}",
                    }
                )
            except PermissionError:
                print(
                    f"⚠️  Warning: Could not create log directory {log_dir}. Event logging disabled."
                )
        else:
            # We're running locally, use a local log directory
            local_log_dir = f"./spark-logs/app/{app_name}"
            try:
                os.makedirs(local_log_dir, exist_ok=True)
                merged_configs.update(
                    {
                        "spark.eventLog.enabled": "true",
                        "spark.eventLog.dir": f"file://{os.path.abspath(local_log_dir)}",
                    }
                )
            except Exception as e:
                print(f"⚠️  Warning: Could not set up event logging: {e}")
    else:
        # For Spark Connect, only create directory structure (no event logging config)
        log_dir = f"/opt/bitnami/spark/logs/app/{app_name}"
        if os.path.exists("/opt/bitnami/spark") or os.getenv(
            "SPARK_HOME", ""
        ).startswith("/opt/bitnami"):
            # We're in Docker environment
            try:
                os.makedirs(log_dir, exist_ok=True)
            except PermissionError:
                print(f"⚠️  Warning: Could not create log directory {log_dir}.")
        else:
            # We're running locally, use a local log directory
            local_log_dir = f"./spark-logs/app/{app_name}"
            try:
                os.makedirs(local_log_dir, exist_ok=True)
            except Exception as e:
                print(f"⚠️  Warning: Could not create log directory: {e}")

    if spark_version in [
        SparkVersion.SPARK_CONNECT_4_0,
        SparkVersion.SPARK_CONNECT_3_5,
    ]:
        # For Spark Connect, use the consolidated session function
        return create_spark_connect_session(
            app_name=app_name,
            iceberg_config=iceberg_config,
            s3_config=s3_config,
            **merged_configs,
        )
    elif spark_version in [SparkVersion.SPARK_3_5, SparkVersion.SPARK_4_0]:
        return _create_pyspark_session(app_name=app_name, spark_params=merged_configs)
    else:
        raise ValueError(f"Unsupported Spark version: {spark_version}")


def _create_src_archive() -> str:
    """Create a zip archive of the src directory for Spark Connect artifacts"""
    src_dir = "src"
    if not os.path.exists(src_dir):
        raise FileNotFoundError(f"src directory not found: {src_dir}")

    # Create temporary zip file
    temp_file = tempfile.NamedTemporaryFile(suffix=".zip", delete=False)
    temp_path = temp_file.name
    temp_file.close()

    with zipfile.ZipFile(temp_path, "w", zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(src_dir):
            # Skip cache and git directories
            dirs[:] = [d for d in dirs if not d.startswith("__") and d != ".git"]
            for file in files:
                if file.endswith((".py", ".json", ".yaml", ".yml")):
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, src_dir)
                    zipf.write(file_path, arcname)

    return temp_path


def create_spark_connect_session(
    app_name: str = None,
    iceberg_config: Optional[IcebergConfig] = None,
    s3_config: Optional[S3FileSystemConfig] = None,
    add_artifacts: bool = False,
    **additional_configs,
) -> SparkSession:
    """
    Create a Spark Connect session with optional Iceberg configuration

    Args:
        app_name: Name of the Spark application
        iceberg_config: Optional IcebergConfig for Iceberg integration
        s3_config: Optional S3FileSystemConfig for S3/MinIO access
        add_artifacts: Whether to add src directory as artifacts
        **additional_configs: Additional Spark configurations

    Returns:
        SparkSession: Configured Spark Connect session
    """
    # Auto-detect app name from __file__ if not provided
    if app_name is None:
        import inspect

        try:
            # Get the calling frame
            frame = inspect.currentframe()
            while frame:
                frame = frame.f_back
                if frame and frame.f_globals.get("__file__"):
                    # Extract filename without extension
                    app_name = os.path.splitext(
                        os.path.basename(frame.f_globals["__file__"])
                    )[0]
                    break
        except Exception:
            app_name = "SparkConnectApp"

    # Start with additional configs
    spark_params = {**additional_configs}

    # Add S3A filesystem configuration if provided
    if s3_config:
        spark_params.update(s3_config.get_spark_configs())

    # Add Iceberg configuration if provided
    if iceberg_config:
        spark_params.update(iceberg_config.get_spark_configs())

    # Add artifacts flag if requested
    if add_artifacts:
        spark_params["spark.connect.add.artifacts"] = "true"

    # Note: Spark Connect doesn't support event logging configuration
    # Event logs are handled by the Spark Connect server itself
    # We only create the directory structure for consistency
    log_dir = f"/opt/bitnami/spark/logs/app/{app_name}"
    if os.path.exists("/opt/bitnami/spark") or os.getenv("SPARK_HOME", "").startswith(
        "/opt/bitnami"
    ):
        # We're in Docker environment
        try:
            os.makedirs(log_dir, exist_ok=True)
        except PermissionError:
            print(f"⚠️  Warning: Could not create log directory {log_dir}.")
    else:
        # We're running locally, use a local log directory
        local_log_dir = f"./spark-logs/app/{app_name}"
        try:
            os.makedirs(local_log_dir, exist_ok=True)
        except Exception as e:
            print(f"⚠️  Warning: Could not create log directory: {e}")

    return _create_spark_connect_session(app_name, spark_params)


def _create_spark_connect_session(
    app_name: str,
    spark_params: Dict[str, str] = None,
    add_artifacts: bool = False,
) -> SparkSession:
    """
    Internal function to create a Spark Connect session (3.5+)

    Args:
        app_name: Name of the Spark application
        spark_params: Dictionary of Spark configuration parameters
        add_artifacts: Whether to add src directory as artifacts

    Returns:
        SparkSession: Configured Spark Connect session
    """
    if spark_params is None:
        spark_params = {}

    builder = SparkSession.builder.appName(app_name).remote("sc://localhost:15002")

    # Apply additional configurations
    for key, value in spark_params.items():
        builder = builder.config(key, value)

    spark = builder.getOrCreate()

    # Add src directory as artifact for Spark Connect (optional)
    # Check both the parameter and the config flag
    should_add_artifacts = (
        add_artifacts
        or spark_params.get("spark.connect.add.artifacts", "false").lower() == "true"
    )

    if should_add_artifacts:
        try:
            src_archive = _create_src_archive()
            spark.addArtifact(src_archive, pyfile=True)
            # Clean up the temporary file after adding to Spark
            os.unlink(src_archive)
        except Exception as e:
            print(f"Warning: Could not add src directory as artifact: {e}")

    return spark


def _get_iceberg_jars() -> str:
    """Get the Iceberg JARs for the classpath"""
    # Check if we're in Docker environment
    if os.path.exists("/opt/bitnami/spark"):
        spark_home = "/opt/bitnami/spark"
    else:
        # We're running locally, Iceberg JARs should be available via Spark Connect
        return ""

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

    # Load spark-defaults.conf configurations (only if in Docker environment)
    spark_defaults_path = "/opt/bitnami/spark/conf/spark-defaults.conf"
    if os.path.exists(spark_defaults_path):
        try:
            with open(spark_defaults_path, "r") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and " " in line:
                        key, value = line.split(" ", 1)
                        # Only set if not already in spark_params (spark_params takes precedence)
                        if key not in spark_params:
                            builder = builder.config(key, value)
        except Exception as e:
            print(f"⚠️  Warning: Could not read spark-defaults.conf: {e}")

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
