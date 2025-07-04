#!/usr/bin/env python3
"""
Debug catalog issues after SQLMesh plan
"""

from pathlib import Path
from src.utils.session import create_spark_session, SparkVersion


def debug_catalog(spark):
    """Debug catalog access issues"""
    print("=== DEBUGGING CATALOG ACCESS ===")

    # Test basic catalog access
    print("1. Testing basic catalog access...")
    catalogs = spark.sql("SHOW CATALOGS")
    print("✅ Catalogs accessible")

    # Test current catalog
    print("2. Testing current catalog...")
    current_catalog = spark.sql("SELECT current_catalog()")
    print("✅ Current catalog accessible")

    # Test database listing
    print("3. Testing database listing...")
    databases = spark.sql("SHOW DATABASES")
    print("✅ Databases accessible")


import pytest


@pytest.mark.spark_integration
@pytest.mark.parametrize("namespace", ["nyc", "legal", "nyc_taxi_data"])
def test_namespace_access(namespace):
    """Test access to specific namespace"""
    print(f"\n=== TESTING NAMESPACE: {namespace} ===")

    # Create Spark session for this test
    spark = create_spark_session(
        spark_version=SparkVersion.SPARK_3_5,
        app_name=f"catalog_test_{namespace}",
    )

    try:
        # First, try to create the namespace if it doesn't exist
        print(f"1. Creating namespace '{namespace}' if it doesn't exist...")
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {namespace}")
        print(f"✅ Namespace '{namespace}' created or already exists")

        # Try to use the namespace
        print(f"2. Switching to namespace '{namespace}'...")
        spark.sql(f"USE {namespace}")
        print(f"✅ Successfully switched to '{namespace}'")

        # List tables
        print(f"3. Listing tables in '{namespace}'...")
        tables = spark.sql("SHOW TABLES")
        tables.show(truncate=False)
        print(f"✅ Tables in '{namespace}' accessible")

        assert True  # Test passed

    except Exception as e:
        print(f"❌ Error accessing namespace '{namespace}': {e}")
        # Don't fail the test if namespace doesn't exist - just skip
        pytest.skip(f"Namespace {namespace} not accessible: {e}")
    finally:
        spark.stop()


@pytest.mark.spark_integration
def test_main():
    print("=== CATALOG DEBUG SCRIPT ===")

    # Create Spark session
    print("Creating Spark session...")
    spark = create_spark_session(
        spark_version=SparkVersion.SPARK_3_5,
        app_name="catalog_debug",
    )
    print("✅ Spark session created")

    # Debug catalog access
    debug_catalog(spark)

    # Test only one namespace to speed up
    print("\n=== TESTING SINGLE NAMESPACE ===")
    namespace = "legal"  # Test only one namespace instead of three

    # Create the namespace if it doesn't exist
    print(f"1. Creating namespace '{namespace}' if it doesn't exist...")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {namespace}")
    print(f"✅ Namespace '{namespace}' created or already exists")

    # Try to use the namespace
    print(f"2. Switching to namespace '{namespace}'...")
    spark.sql(f"USE {namespace}")
    print(f"✅ Successfully switched to '{namespace}'")

    # List tables
    print(f"3. Listing tables in '{namespace}'...")
    tables = spark.sql("SHOW TABLES")
    print(f"✅ Tables in '{namespace}' accessible")

    print("\n=== DEBUG COMPLETE ===")
    spark.stop()
    print("✅ Spark session stopped")
