import os
from pathlib import Path
from utils.session import (
    create_spark_session,
    SparkVersion,
    IcebergConfig,
    S3FileSystemConfig,
)

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "admin")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "password")


def check_namespace_tables(spark, namespace):
    """Check tables in a specific namespace"""
    print(f"\n=== SHOWING TABLES IN {namespace.upper()} DATABASE ===")
    try:
        spark.sql(f"USE {namespace};")
        tables = spark.sql("SHOW TABLES;")
        tables.show(truncate=False)

        print(f"\n=== TABLE DETAILS FOR {namespace.upper()} ===")
        # Get more detailed information about tables
        for row in tables.collect():
            table_name = row.tableName
            print(f"\n--- Table: {table_name} ---")
            try:
                desc_result = spark.sql(f"DESCRIBE {table_name}")
                desc_result.show(truncate=False)
            except Exception as e:
                print(f"Error describing table {table_name}: {e}")

    except Exception as e:
        print(f"❌ Error accessing namespace '{namespace}': {e}")
        print(f"Namespace '{namespace}' may not exist yet.")


def main():
    # Use Iceberg configuration to connect to the same catalog as create_tables.py
    s3_config = S3FileSystemConfig()
    iceberg_config = IcebergConfig(s3_config)

    spark = create_spark_session(
        spark_version=SparkVersion.SPARK_CONNECT_3_5,
        app_name=Path(__file__).stem,
        iceberg_config=iceberg_config,
    )
    try:
        print("=== SHOWING DATABASES ===")
        try:
            databases = spark.sql("SHOW DATABASES;")
            databases.show(truncate=False)
        except Exception as e:
            print(f"❌ Error showing databases: {e}")

        # Check tables in different namespaces
        namespaces = [
            "legal",
            "nyc_taxi",
            "admin",
        ]  # Check all namespaces used in this setup

        for namespace in namespaces:
            check_namespace_tables(spark, namespace)

        print(f"\n{'='*50}")
        print("Table check complete!")
        print(f"{'='*50}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
