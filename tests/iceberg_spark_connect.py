#!/usr/bin/env python3
"""
Create Iceberg Table using Spark Connect (Spark 4.0)
Adapted from create_table.py to work with Spark Connect
"""

import os
import warnings
from pyspark.sql import SparkSession

# Suppress warnings
warnings.filterwarnings("ignore", message=".*Protobuf gencode version.*")
warnings.filterwarnings("ignore", message=".*Failed to set spark.eventLog.dir.*")
warnings.filterwarnings(
    "ignore", message=".*Cannot modify the value of the Spark config.*"
)

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "admin")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "password")


def get_spark_connect_session() -> SparkSession:
    """
    Create Spark Connect session with Iceberg configuration for Spark 4.0
    """
    spark = (
        SparkSession.builder.appName("CreateIcebergTableSparkConnect")
        .config("spark.sql.streaming.schemaInference", "true")
        # Iceberg configurations
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.defaultCatalog", "iceberg")
        .config("spark.sql.catalog.iceberg.type", "rest")
        .config("spark.sql.catalog.iceberg.uri", "http://localhost:8181")
        .config("spark.sql.catalog.iceberg.s3.endpoint", "http://localhost:9000")
        .config("spark.sql.catalog.iceberg.warehouse", "s3://data/wh")
        .config("spark.sql.catalog.iceberg.s3.access-key", AWS_ACCESS_KEY_ID)
        .config("spark.sql.catalog.iceberg.s3.secret-key", AWS_SECRET_ACCESS_KEY)
        .config("spark.sql.catalog.iceberg.s3.region", "us-east-1")
        # S3/MinIO configurations
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
        .config("spark.hadoop.fs.s3a.region", "us-east-1")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        # Performance optimizations for Spark 4.0
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.adaptive.localShuffleReader.enabled", "true")
        .config("spark.sql.shuffle.partitions", "200")
        # Spark Connect endpoint
        .remote("sc://localhost:15002")
        .getOrCreate()
    )

    return spark


def test_iceberg_connection(spark: SparkSession):
    """Test basic Iceberg catalog connection"""
    print("Testing Iceberg catalog connection...")

    try:
        # Test namespace listing
        print("Available namespaces:")
        spark.sql("SHOW NAMESPACES").show()

        # Test database creation
        db_name = "nyc_test"
        print(f"Creating database: {db_name}")
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")

        # Show databases
        print("Available databases:")
        spark.sql("SHOW DATABASES").show()

        return True

    except Exception as e:
        print(f"❌ Iceberg connection test failed: {e}")
        return False


def create_iceberg_table(
    spark: SparkSession, db_name: str = "nyc", table_name: str = "taxis"
):
    """Create Iceberg table from Parquet data"""
    table_identifier = f"{db_name}.{table_name}"

    print(f"Creating Iceberg table: {table_identifier}")

    try:
        # Create database if not exists
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        print(f"✅ Database '{db_name}' created/verified")

        # Drop table if exists
        spark.sql(f"DROP TABLE IF EXISTS {table_identifier}")
        print(f"✅ Dropped existing table if any")

        # Test reading a sample Parquet file
        file_path = "s3://data/yellow_tripdata_2021-04.parquet"
        print(f"Reading Parquet file: {file_path}")

        # Read the Parquet file
        df = spark.read.parquet(file_path)
        print(f"✅ Read Parquet file with {df.count():,} records")

        # Show schema
        print("DataFrame schema:")
        df.printSchema()

        # Show sample data
        print("Sample data:")
        df.show(5)

        # Create Iceberg table
        print("Creating Iceberg table...")
        df.writeTo(f"iceberg.{table_identifier}").using("iceberg").tableProperty(
            "write.merge.isolation-level", "snapshot"
        ).create()

        print(f"✅ Successfully created Iceberg table: {table_identifier}")

        # Verify table creation
        table_count = spark.table(f"iceberg.{table_identifier}").count()
        print(f"✅ Table verification: {table_count:,} records in {table_identifier}")

        return True

    except Exception as e:
        print(f"❌ Error creating table: {e}")
        return False


def test_table_operations(
    spark: SparkSession, db_name: str = "nyc", table_name: str = "taxis"
):
    """Test various table operations"""
    table_identifier = f"{db_name}.{table_name}"

    print(f"\nTesting table operations on: {table_identifier}")

    try:
        # Read from table
        df = spark.table(f"iceberg.{table_identifier}")
        print(f"✅ Read table with {df.count():,} records")

        # Show table schema
        print("Table schema:")
        df.printSchema()

        # Basic query
        print("Sample query results:")
        df.select("vendor_id", "pickup_datetime", "total_amount").show(5)

        # Aggregation query
        print("Vendor statistics:")
        vendor_stats = df.groupBy("vendor_id").agg(
            spark.sql.functions.count("*").alias("trip_count"),
            spark.sql.functions.avg("total_amount").alias("avg_amount"),
        )
        vendor_stats.show()

        return True

    except Exception as e:
        print(f"❌ Error testing table operations: {e}")
        return False


def main():
    """Main function to test Iceberg table creation with Spark Connect"""
    print("Starting Iceberg Table Creation Test with Spark Connect...")

    try:
        # Create Spark Connect session
        spark = get_spark_connect_session()
        print(f"✅ Connected to Spark cluster (version: {spark.version})")

        # Test Iceberg connection
        if not test_iceberg_connection(spark):
            print("❌ Iceberg connection test failed. Exiting.")
            return

        # Create table
        if not create_iceberg_table(spark):
            print("❌ Table creation failed. Exiting.")
            return

        # Test table operations
        if not test_table_operations(spark):
            print("❌ Table operations test failed.")
            return

        print("\n✅ All tests completed successfully!")
        print("\nYou can now access the table using:")
        print("  - Spark SQL: SELECT * FROM iceberg.nyc.taxis")
        print("  - Iceberg REST API: http://localhost:8181")
        print("  - MinIO Console: http://localhost:9001")

        spark.stop()

    except Exception as e:
        print(f"❌ Error: {e}")
        print("\nTroubleshooting tips:")
        print("1. Make sure Spark cluster is running: docker-compose up -d")
        print("2. Check Spark Connect is enabled: docker-compose logs spark-master")
        print("3. Verify Iceberg REST server is running: docker-compose logs rest")
        print("4. Check MinIO is accessible: docker-compose logs minio")
        print("5. Ensure Parquet files exist in MinIO bucket")


if __name__ == "__main__":
    main()
