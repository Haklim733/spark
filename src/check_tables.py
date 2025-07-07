#!/usr/bin/env python3
"""
Simplified Table Checker for Iceberg Tables
Checks table existence and provides basic metadata information
"""

import os
from pathlib import Path
from src.utils import (
    create_spark_session,
    SparkVersion,
    IcebergConfig,
    S3FileSystemConfig,
)

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "admin")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "password")


def check_namespace_tables(spark, namespace):
    """Check tables in a specific namespace"""
    print(f"\n=== TABLES IN {namespace.upper()} NAMESPACE ===")

    # Use the Iceberg catalog explicitly
    spark.sql(f"USE iceberg.{namespace};")
    tables = spark.sql("SHOW TABLES;")

    if tables.count() == 0:
        print(f"❌ No tables found in {namespace} namespace")
        return

    print(f"Found {tables.count()} table(s):")
    tables.show(truncate=False)

    # Check each table
    for row in tables.collect():
        table_name = row.tableName
        full_table_name = f"iceberg.{namespace}.{table_name}"

        # Get table description
        desc_result = spark.sql(f"DESCRIBE EXTENDED {full_table_name}")
        print(f"\n📋 Table: {table_name}")
        desc_result.show(truncate=False)

        count_result = spark.sql(f"SELECT COUNT(*) as count FROM {full_table_name}")
        count = count_result.collect()[0]["count"]
        print(f"📊 Row count: {count:,}")

        spark.sql(f"SELECT name, snapshot_id FROM {full_table_name}.refs").show()


def check_all_namespaces(spark):
    """Check all available namespaces"""
    print("=== AVAILABLE NAMESPACES ===")

    try:
        # Show all databases/namespaces
        databases = spark.sql("SHOW DATABASES;")
        databases.show(truncate=False)

        # Check each namespace
        for row in databases.collect():
            namespace = row.namespace
            if namespace not in [
                "information_schema",
                "system",
            ]:  # Skip system namespaces
                check_namespace_tables(spark, namespace)

    except Exception as e:
        print(f"❌ Error showing databases: {e}")


def main():
    """Main function to check tables"""

    # Create S3 configuration
    s3_config = S3FileSystemConfig(
        endpoint="minio:9000",
        region="us-east-1",
        access_key=AWS_ACCESS_KEY_ID,
        secret_key=AWS_SECRET_ACCESS_KEY,
    )

    # Create Iceberg configuration
    iceberg_config = IcebergConfig(s3_config)

    # Create Spark session
    spark = create_spark_session(
        spark_version=SparkVersion.SPARK_CONNECT_3_5,
        app_name="CheckTables",
        iceberg_config=iceberg_config,
        s3_config=s3_config,
    )

    try:
        print("🔍 Checking Iceberg tables...")
        check_all_namespaces(spark)

        print(f"\n{'='*50}")
        print("✅ Table check complete!")
        print(f"{'='*50}")

    except Exception as e:
        print(f"❌ Error during table check: {e}")
        import traceback

        traceback.print_exc()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
