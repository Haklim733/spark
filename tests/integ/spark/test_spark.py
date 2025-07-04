#!/usr/bin/env python3
"""
Pytest tests for the enum-based Spark session creation system
"""

import pytest

from pyspark.sql import SparkSession

from src.utils import (
    create_spark_session,
    SparkVersion,
    IcebergConfig,
)


@pytest.fixture(scope="session")
def spark_session_pyspark():
    """Fixture for PySpark 3.5 session"""
    spark = create_spark_session(
        spark_version=SparkVersion.SPARK_3_5, app_name="TestPySpark"
    )
    yield spark
    spark.stop()


@pytest.fixture(scope="session")
def iceberg_config():
    """Fixture for Iceberg configuration"""
    from src.utils.session import S3FileSystemConfig

    return IcebergConfig(
        s3_config=S3FileSystemConfig(),
        catalog_uri="http://spark-rest:8181",
        warehouse="s3://data/wh",
    )


@pytest.mark.spark_integration
class TestBasicSessionCreation:
    """Test basic Spark session creation"""

    def test_pyspark_session_creation(self, spark_session_pyspark):
        """Test PySpark 3.5 session creation"""
        assert spark_session_pyspark is not None
        assert isinstance(spark_session_pyspark, SparkSession)
        assert "3.5" in spark_session_pyspark.version

        # Debug: Check if session is properly connected
        print(f"✅ PySpark session created: {spark_session_pyspark.version}")
        print(f"   Master: {spark_session_pyspark.conf.get('spark.master')}")
        print(f"   App ID: {spark_session_pyspark.sparkContext.applicationId}")
        print(f"   Spark Context: {spark_session_pyspark.sparkContext is not None}")

        # Verify the session is properly connected
        assert spark_session_pyspark.sparkContext is not None
        assert spark_session_pyspark.sparkContext.applicationId is not None

    def test_invalid_spark_version(self):
        """Test that invalid Spark version raises error"""
        with pytest.raises(ValueError, match="Unsupported Spark version"):
            create_spark_session(spark_version="INVALID_VERSION", app_name="Test")

    def test_pyspark_with_iceberg(self, iceberg_config):
        """Test PySpark session with Iceberg configuration"""
        spark = create_spark_session(
            spark_version=SparkVersion.SPARK_3_5,
            app_name="PySparkIcebergTest",
            iceberg_config=iceberg_config,
        )

        assert spark is not None
        assert isinstance(spark, SparkSession)

        # Check that Iceberg configurations are applied
        extensions = spark.conf.get("spark.sql.extensions", "")
        assert "IcebergSparkSessionExtensions" in extensions

        # Check other Iceberg configs
        assert (
            spark.conf.get("spark.sql.catalog.iceberg")
            == "org.apache.iceberg.spark.SparkCatalog"
        )
        assert spark.conf.get("spark.sql.defaultCatalog") == "iceberg"
        assert spark.conf.get("spark.sql.catalog.iceberg.type") == "rest"
        assert (
            spark.conf.get("spark.sql.catalog.iceberg.uri") == "http://spark-rest:8181"
        )

        spark.stop()

    def test_iceberg_convenience_function(self):
        """Test convenience function for Iceberg sessions"""
        spark = create_spark_session(
            spark_version=SparkVersion.SPARK_3_5, app_name="IcebergConvenienceTest"
        )

        assert spark is not None
        assert isinstance(spark, SparkSession)

        spark.stop()


@pytest.mark.spark_integration
class TestDataFrameOperations:
    """Test DataFrame operations with different session types"""

    def test_basic_dataframe_operations_pyspark(self, spark_session_pyspark):
        """Test basic DataFrame operations with PySpark"""
        # Ensure the session is properly connected
        assert spark_session_pyspark.sparkContext is not None
        assert spark_session_pyspark.sparkContext.applicationId is not None

        data = [("key1", 100), ("key2", 200), ("key3", 300)]
        df = spark_session_pyspark.createDataFrame(data, ["key", "value"])

        assert df.count() == 3
        assert len(df.columns) == 2
        assert "key" in df.columns
        assert "value" in df.columns

        # Test show operation
        result = df.collect()
        assert len(result) == 3
        assert result[0]["key"] == "key1"
        assert result[0]["value"] == 100


@pytest.mark.spark_integration
class TestConfigurationManagement:
    """Test configuration management and application"""

    def test_additional_configurations(self):
        """Test applying additional Spark configurations"""
        additional_configs = {
            "spark.sql.shuffle.partitions": "100",  # Override default 200
            "spark.executor.memory": "1g",
            "spark.driver.memory": "1g",
        }

        spark = create_spark_session(
            spark_version=SparkVersion.SPARK_3_5,
            app_name="AdditionalConfigsTest",
            **additional_configs,
        )

        # Verify configurations are applied
        assert spark.conf.get("spark.sql.shuffle.partitions") == "100"  # Overridden
        assert spark.conf.get("spark.executor.memory") == "1g"
        assert spark.conf.get("spark.driver.memory") == "1g"

        # Verify default performance configs are still applied
        assert spark.conf.get("spark.sql.adaptive.enabled") == "true"
        assert spark.conf.get("spark.sql.adaptive.coalescePartitions.enabled") == "true"
        assert spark.conf.get("spark.sql.adaptive.skewJoin.enabled") == "true"

        spark.stop()

    def test_config_override_behavior(self):
        """Test that additional_configs properly override default performance configs"""
        # Override a default performance config
        override_configs = {
            "spark.sql.adaptive.enabled": "false",  # Override default true
            "spark.sql.shuffle.partitions": "50",  # Override default 200
        }

        spark = create_spark_session(
            spark_version=SparkVersion.SPARK_3_5,
            app_name="OverrideTest",
            **override_configs,
        )

        # Verify overrides work
        assert spark.conf.get("spark.sql.adaptive.enabled") == "false"  # Overridden
        assert spark.conf.get("spark.sql.shuffle.partitions") == "50"  # Overridden

        # Verify other defaults are still applied
        assert spark.conf.get("spark.sql.adaptive.coalescePartitions.enabled") == "true"
        assert spark.conf.get("spark.sql.adaptive.skewJoin.enabled") == "true"
        assert spark.conf.get("spark.eventLog.enabled") == "true"

        spark.stop()

    def test_duplicate_key_merging_behavior(self):
        """Test the exact behavior when same keys exist in both default and additional configs"""
        # Test with same value
        same_value_configs = {
            "spark.sql.shuffle.partitions": "200",  # Same as default
        }

        spark1 = create_spark_session(
            spark_version=SparkVersion.SPARK_3_5,
            app_name="SameValueTest",
            **same_value_configs,
        )
        assert spark1.conf.get("spark.sql.shuffle.partitions") == "200"
        spark1.stop()

        # Test with different value
        different_value_configs = {
            "spark.sql.shuffle.partitions": "300",  # Different from default 200
        }

        spark2 = create_spark_session(
            spark_version=SparkVersion.SPARK_3_5,
            app_name="DifferentValueTest",
            **different_value_configs,
        )
        assert (
            spark2.conf.get("spark.sql.shuffle.partitions") == "300"
        )  # Additional wins
        spark2.stop()

        # Test with multiple overrides
        multiple_overrides = {
            "spark.sql.shuffle.partitions": "150",
            "spark.sql.adaptive.enabled": "false",
            "spark.sql.adaptive.advisoryPartitionSizeInBytes": "64m",
        }

        spark3 = create_spark_session(
            spark_version=SparkVersion.SPARK_3_5,
            app_name="MultipleOverridesTest",
            **multiple_overrides,
        )
        assert spark3.conf.get("spark.sql.shuffle.partitions") == "150"
        assert spark3.conf.get("spark.sql.adaptive.enabled") == "false"
        assert (
            spark3.conf.get("spark.sql.adaptive.advisoryPartitionSizeInBytes") == "64m"
        )
        # Verify non-overridden defaults are still there
        assert (
            spark3.conf.get("spark.sql.adaptive.coalescePartitions.enabled") == "true"
        )
        spark3.stop()

    def test_optimized_session_configurations(self, spark_session_pyspark):
        """Test that optimized session has expected configurations"""
        # Check for performance optimizations
        assert spark_session_pyspark.conf.get("spark.sql.adaptive.enabled") == "true"
        assert (
            spark_session_pyspark.conf.get(
                "spark.sql.adaptive.coalescePartitions.enabled"
            )
            == "true"
        )
        assert (
            spark_session_pyspark.conf.get("spark.sql.adaptive.skewJoin.enabled")
            == "true"
        )
        assert spark_session_pyspark.conf.get("spark.sql.shuffle.partitions") == "200"
        assert spark_session_pyspark.conf.get("spark.eventLog.enabled") == "true"


@pytest.mark.spark_integration
class TestErrorHandling:
    """Test error handling and edge cases"""

    def test_invalid_app_name(self):
        """Test session creation with invalid app name"""
        # Should not raise an error, just use the invalid name
        spark = create_spark_session(spark_version=SparkVersion.SPARK_3_5, app_name="")
        assert spark is not None
        spark.stop()

    def test_none_iceberg_config(self):
        """Test session creation with None Iceberg config"""
        spark = create_spark_session(
            spark_version=SparkVersion.SPARK_3_5,
            app_name="NoneIcebergTest",
            iceberg_config=None,
        )
        assert spark is not None
        spark.stop()


@pytest.mark.spark_integration
def test_session_creation_performance():
    """Test that session creation is reasonably fast"""
    import time

    start_time = time.time()
    spark = create_spark_session(
        spark_version=SparkVersion.SPARK_3_5, app_name="PerformanceTest"
    )
    creation_time = time.time() - start_time

    assert spark is not None
    assert creation_time < 30.0  # Should create session in under 30 seconds

    spark.stop()
