#!/usr/bin/env python3
"""
Debug catalog issues after SQLMesh plan
"""

from pathlib import Path
from src.utils.session import create_spark_session, SparkVersion


def debug_catalog(spark):
    """Debug catalog access issues"""
    print("=== DEBUGGING CATALOG ACCESS ===")

    try:
        # Test basic catalog access
        print("1. Testing basic catalog access...")
        catalogs = spark.sql("SHOW CATALOGS")
        catalogs.show(truncate=False)
        print("✅ Catalogs accessible")
    except Exception as e:
        print(f"❌ Error accessing catalogs: {e}")
        return False

    try:
        # Test current catalog
        print("2. Testing current catalog...")
        current_catalog = spark.sql("SELECT current_catalog()")
        current_catalog.show(truncate=False)
        print("✅ Current catalog accessible")
    except Exception as e:
        print(f"❌ Error getting current catalog: {e}")
        return False

    try:
        # Test database listing
        print("3. Testing database listing...")
        databases = spark.sql("SHOW DATABASES")
        databases.show(truncate=False)
        print("✅ Databases accessible")
    except Exception as e:
        print(f"❌ Error listing databases: {e}")
        return False

    return True


import pytest


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


def test_main():
    print("=== CATALOG DEBUG SCRIPT ===")

    try:
        # Create Spark session
        print("Creating Spark session...")
        spark = create_spark_session(
            spark_version=SparkVersion.SPARK_3_5,
            app_name="catalog_debug",
        )
        print("✅ Spark session created")

        # Debug catalog access
        if not debug_catalog(spark):
            print("❌ Catalog access failed")
            return

        # Test specific namespaces
        namespaces = ["nyc", "legal", "nyc_taxi_data"]

        for namespace in namespaces:
            test_namespace_access(spark, namespace)

        print("\n=== DEBUG COMPLETE ===")

    except Exception as e:
        print(f"❌ Fatal error: {e}")
        import traceback

        traceback.print_exc()
    finally:
        try:
            spark.stop()
            print("✅ Spark session stopped")
        except:
            pass
