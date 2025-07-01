#!/usr/bin/env python3
"""
Load NYC taxi data into Iceberg table
"""

import argparse
from datetime import datetime
from pyspark.sql import SparkSession
from utils.session import SparkVersion, S3FileSystemConfig
from utils.logger import HybridLogger


def load_nyc_taxi_data(spark, source_path, table_name):
    """Load NYC taxi Parquet files into Iceberg table"""

    print(f"🔄 Loading NYC taxi data from {source_path} into {table_name}")

    # Read Parquet files
    df = spark.read.parquet(source_path)

    # Add load tracking columns
    df = df.withColumn(
        "load_timestamp", spark.sql("SELECT current_timestamp()").collect()[0][0]
    )
    df = df.withColumn(
        "load_batch_id", spark.sql("SELECT 'nyc_taxi_batch'").collect()[0][0]
    )

    # Write to Iceberg table
    df.writeTo(table_name).append()

    # Validate
    count = spark.sql(f"SELECT COUNT(*) as count FROM {table_name}").collect()[0][
        "count"
    ]
    print(f"✅ Loaded {count:,} records into {table_name}")

    return True


def main():
    parser = argparse.ArgumentParser(description="Load NYC taxi data into Iceberg")
    parser.add_argument(
        "--file-path", default="s3a://data/nyc/taxi/*", help="Path to Parquet files"
    )
    parser.add_argument(
        "--table-name", default="nyc_taxi_data.yellow_tripdata", help="Target table"
    )

    args = parser.parse_args()

    with HybridLogger(
        app_name="nyc_taxi_loader",
        spark_config={"spark_version": SparkVersion.SPARK_3_5},
        manage_spark=True,
    ) as logger:

        success = load_nyc_taxi_data(logger.spark, args.source_path, args.table_name)

        if success:
            print("✅ NYC taxi data loaded successfully")
            return 0
        else:
            print("❌ Failed to load NYC taxi data")
            return 1


if __name__ == "__main__":
    exit(main())
