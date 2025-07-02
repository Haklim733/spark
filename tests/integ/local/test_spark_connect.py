#!/usr/bin/env python3
"""
Minimal Spark Connect client test
Tests basic connection and simple operations to avoid serialization issues
"""

import warnings
import pytest
from pyspark.sql import SparkSession
from pyspark.errors.exceptions.base import PySparkNotImplementedError


class TestSparkConnectBasic:
    """Test basic Spark Connect functionality"""

    @pytest.fixture(scope="class")
    def spark_connect_session(self):
        """Create Spark Connect session"""
        try:
            # Create Spark session with Spark Connect
            spark = (
                SparkSession.builder.appName("SparkConnectBasicTest")
                .remote("sc://localhost:15002")
                .getOrCreate()
            )
            yield spark
            spark.stop()
        except Exception as e:
            pytest.skip(f"Spark Connect not available: {e}")

    def test_connection(self, spark_connect_session):
        """Test basic connection to Spark Connect server"""
        print(f"✅ Connected to Spark Connect server!")
        print(f"   Spark version: {spark_connect_session.version}")
        print(f"   Master: {spark_connect_session.conf.get('spark.master', 'N/A')}")

    def test_simple_dataframe_creation(self, spark_connect_session):
        """Test simple DataFrame creation without complex operations"""
        # Create very simple test data
        data = [("test", 1)]
        df = spark_connect_session.createDataFrame(data, ["word", "count"])

        # Only test basic properties, avoid operations that might cause serialization issues
        assert len(df.columns) == 2
        assert "word" in df.columns
        assert "count" in df.columns

        print("✅ Simple DataFrame creation successful")

    def test_spark_connect_limitations(self, spark_connect_session):
        """Test Spark Connect limitations"""
        # Test that sparkContext is not available (Spark Connect limitation)
        with pytest.raises(PySparkNotImplementedError):
            # Spark Connect doesn't expose sparkContext
            spark_connect_session.sparkContext

        print("✅ Spark Connect limitations correctly enforced")

    def test_session_properties(self, spark_connect_session):
        """Test session properties and configuration"""
        # Test that we can access session properties
        version = spark_connect_session.version
        assert version is not None
        assert len(version) > 0

        # Test that we can access configuration
        master = spark_connect_session.conf.get("spark.master", "N/A")
        assert master is not None

        print("✅ Session properties accessible")
