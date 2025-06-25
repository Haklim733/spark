#!/usr/bin/env python3
"""
Simple test script to verify Spark Connect is working
1. cannot use sparkContext
"""

import warnings
import pytest
from pyspark.sql import SparkSession


@pytest.mark.skip(reason="Spark Connect (4.0) is not enabled in this environment.")
def test_spark_connect():
    """Test basic Spark Connect functionality"""
    print("Testing Spark Connect connection...")

    try:
        # Stop any existing local Spark session (as per official documentation)
        try:
            SparkSession.builder.master("local[*]").getOrCreate().stop()
            print("✅ Stopped existing local Spark session")
        except Exception as e:
            print(f"ℹ️  No existing local Spark session to stop: {e}")

        # Create Spark session with Spark Connect
        spark = (
            SparkSession.builder.appName("SparkConnectTest")
            .remote("sc://localhost:15002")
            .getOrCreate()
        )

        print(f"✅ Successfully connected to Spark cluster!")
        print(f"   Spark version: {spark.version}")

        # Test basic DataFrame operation
        data = [("test", 1), ("data", 2)]
        df = spark.createDataFrame(data, ["word", "count"])

        print("✅ Basic DataFrame operation successful!")
        print("Sample data:")
        df.show()

        # Test SQL
        df.createOrReplaceTempView("test_table")
        result = spark.sql("SELECT * FROM test_table WHERE count > 0")
        print("✅ SQL operation successful!")
        result.show()

        spark.stop()
        print("✅ All tests passed! Spark Connect is working correctly.")

    except Exception as e:
        print(f"❌ Error: {e}")
        print("\nTroubleshooting tips:")
        print("1. Make sure your Spark cluster is running: docker-compose up -d")
        print("2. Check that Spark Connect is enabled on port 15002")
        print("3. Verify the cluster is healthy: docker-compose ps")
        print("4. Check logs: docker-compose logs spark-master")


if __name__ == "__main__":
    test_spark_connect()
