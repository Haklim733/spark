#!/usr/bin/env python3
"""
Spark Shuffling Issues Demonstration
Shows common shuffling problems and how to identify them
"""

import os
import random
from typing import List, Dict, Tuple
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    udf,
    col,
    explode,
    array,
    lit,
    row_number,
    count,
    sum as spark_sum,
    avg,
    max as spark_max,
    min as spark_min,
)
from pyspark.sql.types import (
    StringType,
    ArrayType,
    StructType,
    StructField,
    IntegerType,
    LongType,
    DoubleType,
)
from pyspark.sql.window import Window
import time

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "admin")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "password")


def get_spark_session() -> SparkSession:
    """Create and configure Spark session with shuffling monitoring"""

    spark = (
        SparkSession.builder.appName("ShufflingIssuesDemo")
        .config("spark.sql.streaming.schemaInference", "true")
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
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
        .config("spark.hadoop.fs.s3a.region", "us-east-1")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # Shuffling-specific configurations
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.adaptive.localShuffleReader.enabled", "true")
        .config("spark.sql.shuffle.partitions", "200")  # Default shuffle partitions
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128m")
        .config("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256m")
        .config("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
        # Monitoring configurations
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", "/opt/bitnami/spark/logs/spark-events")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .master("spark://localhost:7077")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("INFO")
    return spark


def generate_skewed_data(
    spark: SparkSession, num_records: int = 1000000
) -> SparkSession.DataFrame:
    """
    Generate data with intentional skew to demonstrate shuffling issues

    Args:
        spark: SparkSession
        num_records: Number of records to generate

    Returns:
        DataFrame with skewed data
    """
    print(f"Generating {num_records:,} records with intentional skew...")

    # Create skewed data where some keys appear much more frequently
    skewed_data = []

    # Generate normal distribution of keys (1-1000)
    for i in range(num_records):
        if i < num_records * 0.8:  # 80% of data has normal distribution
            key = random.randint(1, 1000)
        elif i < num_records * 0.95:  # 15% of data has moderate skew
            key = random.choice([1001, 1002, 1003, 1004, 1005])
        else:  # 5% of data has extreme skew
            key = 9999  # Single key with many records

        value = random.randint(1, 1000)
        category = f"category_{key % 10}"
        timestamp = random.randint(1600000000, 1700000000)

        skewed_data.append((key, value, category, timestamp))

    # Create DataFrame
    schema = StructType(
        [
            StructField("key", IntegerType(), False),
            StructField("value", IntegerType(), False),
            StructField("category", StringType(), False),
            StructField("timestamp", LongType(), False),
        ]
    )

    df = spark.createDataFrame(skewed_data, schema)

    print(f"Generated DataFrame with {df.count():,} records")
    print("Data distribution preview:")
    df.groupBy("key").count().orderBy("count", ascending=False).show(10)

    return df


def demonstrate_shuffling_issues(spark: SparkSession, df: SparkSession.DataFrame):
    """
    Demonstrate various shuffling issues and their symptoms
    """
    print("\n" + "=" * 60)
    print("DEMONSTRATING SPARK SHUFFLING ISSUES")
    print("=" * 60)

    # Issue 1: Data Skew
    print("\n1. DATA SKEW ISSUE")
    print("-" * 30)
    demonstrate_data_skew(spark, df)

    # Issue 2: Large Shuffle Partitions
    print("\n2. LARGE SHUFFLE PARTITIONS ISSUE")
    print("-" * 30)
    demonstrate_large_shuffle_partitions(spark, df)

    # Issue 3: Small Shuffle Partitions
    print("\n3. SMALL SHUFFLE PARTITIONS ISSUE")
    print("-" * 30)
    demonstrate_small_shuffle_partitions(spark, df)

    # Issue 4: Cartesian Product
    print("\n4. CARTESIAN PRODUCT ISSUE")
    print("-" * 30)
    demonstrate_cartesian_product(spark, df)

    # Issue 5: Broadcast Join vs Shuffle Join
    print("\n5. BROADCAST JOIN vs SHUFFLE JOIN")
    print("-" * 30)
    demonstrate_join_strategies(spark, df)

    # Issue 6: Window Functions Shuffling
    print("\n6. WINDOW FUNCTIONS SHUFFLING")
    print("-" * 30)
    demonstrate_window_shuffling(spark, df)


def demonstrate_data_skew(spark: SparkSession, df: SparkSession.DataFrame):
    """
    Demonstrate data skew issue and its impact on shuffling
    """
    print("Performing groupBy operation on skewed data...")

    start_time = time.time()

    # This will cause severe data skew during shuffle
    result = df.groupBy("key").agg(
        spark_sum("value").alias("total_value"),
        count("*").alias("record_count"),
        avg("value").alias("avg_value"),
    )

    # Force execution
    result_count = result.count()
    end_time = time.time()

    print(f"GroupBy operation completed in {end_time - start_time:.2f} seconds")
    print(f"Result count: {result_count}")

    # Show the skewed distribution
    print("\nSkewed data distribution:")
    result.orderBy("record_count", ascending=False).show(10)

    # Analyze partition distribution
    print("\nPartition distribution analysis:")
    analyze_partition_distribution(result, "Data Skew GroupBy")


def demonstrate_large_shuffle_partitions(
    spark: SparkSession, df: SparkSession.DataFrame
):
    """
    Demonstrate issue with too many shuffle partitions
    """
    print("Setting high number of shuffle partitions...")

    # Set very high number of partitions
    spark.conf.set("spark.sql.shuffle.partitions", "1000")

    start_time = time.time()

    # Simple aggregation that will use many partitions
    result = df.groupBy("category").agg(
        spark_sum("value").alias("total_value"), count("*").alias("record_count")
    )

    result_count = result.count()
    end_time = time.time()

    print(
        f"High partition count operation completed in {end_time - start_time:.2f} seconds"
    )
    print(f"Result count: {result_count}")

    # Reset to reasonable number
    spark.conf.set("spark.sql.shuffle.partitions", "200")

    print("Reset shuffle partitions to 200")


def demonstrate_small_shuffle_partitions(
    spark: SparkSession, df: SparkSession.DataFrame
):
    """
    Demonstrate issue with too few shuffle partitions
    """
    print("Setting low number of shuffle partitions...")

    # Set very low number of partitions
    spark.conf.set("spark.sql.shuffle.partitions", "2")

    start_time = time.time()

    # Aggregation that will be bottlenecked by few partitions
    result = df.groupBy("category").agg(
        spark_sum("value").alias("total_value"), count("*").alias("record_count")
    )

    result_count = result.count()
    end_time = time.time()

    print(
        f"Low partition count operation completed in {end_time - start_time:.2f} seconds"
    )
    print(f"Result count: {result_count}")

    # Reset to reasonable number
    spark.conf.set("spark.sql.shuffle.partitions", "200")

    print("Reset shuffle partitions to 200")


def demonstrate_cartesian_product(spark: SparkSession, df: SparkSession.DataFrame):
    """
    Demonstrate cartesian product issue (very expensive shuffle)
    """
    print("Creating small DataFrame for cartesian product...")

    # Create small DataFrame
    small_data = [(1, "A"), (2, "B"), (3, "C")]
    small_df = spark.createDataFrame(small_data, ["id", "label"])

    print(f"Small DataFrame: {small_df.count()} records")
    print(f"Large DataFrame: {df.count():,} records")

    start_time = time.time()

    # This will create a cartesian product - very expensive!
    try:
        result = df.crossJoin(small_df)
        result_count = result.count()
        end_time = time.time()

        print(f"Cartesian product completed in {end_time - start_time:.2f} seconds")
        print(f"Result count: {result_count:,}")
        print("WARNING: This operation is very expensive and should be avoided!")

    except Exception as e:
        print(f"Cartesian product failed (expected): {str(e)}")
        print("This demonstrates why cartesian products should be avoided")


def demonstrate_join_strategies(spark: SparkSession, df: SparkSession.DataFrame):
    """
    Demonstrate broadcast join vs shuffle join
    """
    print("Creating small DataFrame for join comparison...")

    # Create small DataFrame for join
    small_data = [(1, "Category_A"), (2, "Category_B"), (3, "Category_C")]
    small_df = spark.createDataFrame(small_data, ["id", "category_name"])

    print(f"Small DataFrame: {small_df.count()} records")
    print(f"Large DataFrame: {df.count():,} records")

    # Shuffle Join (default)
    print("\nPerforming shuffle join...")
    start_time = time.time()

    shuffle_result = df.join(small_df, df.category == small_df.category_name, "inner")
    shuffle_count = shuffle_result.count()
    shuffle_time = time.time() - start_time

    print(f"Shuffle join completed in {shuffle_time:.2f} seconds")
    print(f"Result count: {shuffle_count:,}")

    # Broadcast Join (optimized)
    print("\nPerforming broadcast join...")
    start_time = time.time()

    broadcast_result = df.join(
        small_df.hint("broadcast"), df.category == small_df.category_name, "inner"
    )
    broadcast_count = broadcast_result.count()
    broadcast_time = time.time() - start_time

    print(f"Broadcast join completed in {broadcast_time:.2f} seconds")
    print(f"Result count: {broadcast_count:,}")

    # Performance comparison
    if broadcast_time > 0:
        speedup = shuffle_time / broadcast_time
        print(f"\nBroadcast join is {speedup:.1f}x faster than shuffle join")


def demonstrate_window_shuffling(spark: SparkSession, df: SparkSession.DataFrame):
    """
    Demonstrate window functions and their shuffling behavior
    """
    print("Performing window function operations...")

    # Window function that requires shuffling
    window_spec = Window.partitionBy("category").orderBy("timestamp")

    start_time = time.time()

    result = (
        df.withColumn("row_number", row_number().over(window_spec))
        .withColumn("running_total", spark_sum("value").over(window_spec))
        .withColumn(
            "category_rank",
            row_number().over(
                Window.partitionBy("category").orderBy(col("value").desc())
            ),
        )
    )

    result_count = result.count()
    end_time = time.time()

    print(f"Window function operation completed in {end_time - start_time:.2f} seconds")
    print(f"Result count: {result_count:,}")

    # Show sample results
    print("\nWindow function results sample:")
    result.select(
        "key", "category", "value", "row_number", "running_total", "category_rank"
    ).show(10)


def analyze_partition_distribution(df: SparkSession.DataFrame, operation_name: str):
    """
    Analyze partition distribution to identify skew
    """
    print(f"\nPartition distribution for: {operation_name}")

    # Get RDD to analyze partitions
    rdd = df.rdd

    # Count records per partition
    partition_counts = rdd.mapPartitionsWithIndex(
        lambda idx, iterator: [(idx, sum(1 for _ in iterator))]
    ).collect()

    # Sort by partition index
    partition_counts.sort(key=lambda x: x[0])

    # Calculate statistics
    counts = [count for _, count in partition_counts]
    total_partitions = len(partition_counts)
    total_records = sum(counts)
    avg_records = total_records / total_partitions if total_partitions > 0 else 0
    max_records = max(counts) if counts else 0
    min_records = min(counts) if counts else 0

    print(f"Total partitions: {total_partitions}")
    print(f"Total records: {total_records:,}")
    print(f"Average records per partition: {avg_records:.1f}")
    print(f"Max records in a partition: {max_records:,}")
    print(f"Min records in a partition: {min_records:,}")

    if avg_records > 0:
        skew_ratio = max_records / avg_records
        print(f"Skew ratio (max/avg): {skew_ratio:.2f}")

        if skew_ratio > 3:
            print("⚠️  HIGH SKEW DETECTED - This will cause shuffling issues!")
        elif skew_ratio > 2:
            print("⚠️  MODERATE SKEW DETECTED - Consider optimization")
        else:
            print("✅ Good partition distribution")

    # Show top 10 partitions by record count
    print("\nTop 10 partitions by record count:")
    sorted_partitions = sorted(partition_counts, key=lambda x: x[1], reverse=True)
    for i, (partition_id, count) in enumerate(sorted_partitions[:10]):
        print(f"  Partition {partition_id}: {count:,} records")


def demonstrate_shuffling_optimizations(
    spark: SparkSession, df: SparkSession.DataFrame
):
    """
    Demonstrate various shuffling optimizations
    """
    print("\n" + "=" * 60)
    print("SHUFFLING OPTIMIZATIONS")
    print("=" * 60)

    # Optimization 1: Repartitioning
    print("\n1. REPARTITIONING OPTIMIZATION")
    print("-" * 30)
    demonstrate_repartitioning(spark, df)

    # Optimization 2: Coalescing
    print("\n2. COALESCING OPTIMIZATION")
    print("-" * 30)
    demonstrate_coalescing(spark, df)

    # Optimization 3: Bucketing
    print("\n3. BUCKETING OPTIMIZATION")
    print("-" * 30)
    demonstrate_bucketing(spark, df)

    # Optimization 4: Adaptive Query Execution
    print("\n4. ADAPTIVE QUERY EXECUTION")
    print("-" * 30)
    demonstrate_adaptive_query_execution(spark, df)


def demonstrate_repartitioning(spark: SparkSession, df: SparkSession.DataFrame):
    """
    Demonstrate repartitioning to fix skew
    """
    print("Repartitioning data to reduce skew...")

    start_time = time.time()

    # Repartition by a different column to distribute load
    repartitioned_df = df.repartition(50, "category")

    # Perform the same groupBy operation
    result = repartitioned_df.groupBy("key").agg(
        spark_sum("value").alias("total_value"), count("*").alias("record_count")
    )

    result_count = result.count()
    end_time = time.time()

    print(f"Repartitioned operation completed in {end_time - start_time:.2f} seconds")
    print(f"Result count: {result_count}")

    # Analyze partition distribution
    analyze_partition_distribution(result, "Repartitioned GroupBy")


def demonstrate_coalescing(spark: SparkSession, df: SparkSession.DataFrame):
    """
    Demonstrate coalescing to reduce partition overhead
    """
    print("Coalescing partitions to reduce overhead...")

    # First, create many small partitions
    many_partitions_df = df.repartition(1000)
    print(
        f"Created DataFrame with {many_partitions_df.rdd.getNumPartitions()} partitions"
    )

    start_time = time.time()

    # Coalesce to reduce partition count
    coalesced_df = many_partitions_df.coalesce(50)

    # Perform operation
    result = coalesced_df.groupBy("category").agg(
        spark_sum("value").alias("total_value"), count("*").alias("record_count")
    )

    result_count = result.count()
    end_time = time.time()

    print(f"Coalesced operation completed in {end_time - start_time:.2f} seconds")
    print(f"Result count: {result_count}")
    print(f"Final partition count: {coalesced_df.rdd.getNumPartitions()}")


def demonstrate_bucketing(spark: SparkSession, df: SparkSession.DataFrame):
    """
    Demonstrate bucketing for join optimization
    """
    print("Creating bucketed tables for join optimization...")

    # Create bucketed table
    df.writeTo("iceberg.bucketed_data").using("iceberg").bucketBy(
        50, "category"
    ).sortBy("key").createOrReplace()

    # Read bucketed table
    bucketed_df = spark.table("iceberg.bucketed_data")

    print(f"Created bucketed table with {bucketed_df.rdd.getNumPartitions()} buckets")

    # Create small table for join
    small_data = [(1, "category_1"), (2, "category_2"), (3, "category_3")]
    small_df = spark.createDataFrame(small_data, ["id", "category"])

    # Perform join on bucketed table
    start_time = time.time()

    result = bucketed_df.join(
        small_df, bucketed_df.category == small_df.category, "inner"
    )
    result_count = result.count()
    end_time = time.time()

    print(f"Bucketed join completed in {end_time - start_time:.2f} seconds")
    print(f"Result count: {result_count:,}")


def demonstrate_adaptive_query_execution(
    spark: SparkSession, df: SparkSession.DataFrame
):
    """
    Demonstrate adaptive query execution features
    """
    print("Demonstrating adaptive query execution...")

    # Enable adaptive query execution
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

    start_time = time.time()

    # Complex query that benefits from adaptive execution
    result = (
        df.groupBy("category")
        .agg(
            spark_sum("value").alias("total_value"),
            count("*").alias("record_count"),
            avg("value").alias("avg_value"),
            spark_max("value").alias("max_value"),
            spark_min("value").alias("min_value"),
        )
        .filter(col("record_count") > 100)
    )

    result_count = result.count()
    end_time = time.time()

    print(f"Adaptive query execution completed in {end_time - start_time:.2f} seconds")
    print(f"Result count: {result_count}")

    # Show the results
    print("\nAdaptive query results:")
    result.show()


def monitor_shuffling_metrics(spark: SparkSession):
    """
    Monitor shuffling-related metrics
    """
    print("\n" + "=" * 60)
    print("SHUFFLING METRICS MONITORING")
    print("=" * 60)

    # Get Spark context
    sc = spark.sparkContext

    # Get application ID
    app_id = sc.applicationId
    print(f"Application ID: {app_id}")

    # Get Spark Web UI URL
    web_ui_url = f"http://localhost:8080"
    print(f"Spark Web UI: {web_ui_url}")

    # Get current configuration
    print("\nCurrent Spark Configuration:")
    print(
        f"spark.sql.shuffle.partitions: {spark.conf.get('spark.sql.shuffle.partitions')}"
    )
    print(f"spark.sql.adaptive.enabled: {spark.conf.get('spark.sql.adaptive.enabled')}")
    print(
        f"spark.sql.adaptive.coalescePartitions.enabled: {spark.conf.get('spark.sql.adaptive.coalescePartitions.enabled')}"
    )
    print(
        f"spark.sql.adaptive.skewJoin.enabled: {spark.conf.get('spark.sql.adaptive.skewJoin.enabled')}"
    )

    print("\nTo monitor shuffling issues:")
    print("1. Open Spark Web UI at http://localhost:8080")
    print("2. Go to 'Stages' tab to see shuffle read/write metrics")
    print("3. Look for stages with high shuffle read/write times")
    print("4. Check for skewed partition distributions")
    print("5. Monitor executor memory and disk usage")


def main():
    """Main function to demonstrate shuffling issues"""

    print("Starting Spark Shuffling Issues Demonstration...")

    try:
        # Create Spark session
        spark = get_spark_session()

        # Generate skewed data
        df = generate_skewed_data(spark, num_records=500000)  # Reduced for demo

        # Demonstrate shuffling issues
        demonstrate_shuffling_issues(spark, df)

        # Demonstrate optimizations
        demonstrate_shuffling_optimizations(spark, df)

        # Monitor metrics
        monitor_shuffling_metrics(spark)

        print("\n" + "=" * 60)
        print("SHUFFLING DEMONSTRATION COMPLETED")
        print("=" * 60)
        print("\nKey takeaways:")
        print("1. Data skew causes severe shuffling issues")
        print("2. Too many/few partitions can hurt performance")
        print("3. Cartesian products are extremely expensive")
        print("4. Broadcast joins are much faster than shuffle joins")
        print("5. Window functions can cause significant shuffling")
        print("6. Use adaptive query execution for automatic optimization")
        print("7. Monitor Spark Web UI for shuffling metrics")

    except Exception as e:
        print(f"Error in shuffling demonstration: {str(e)}")
        import traceback

        traceback.print_exc()
    finally:
        if "spark" in locals():
            print("\nStopping Spark session...")
            spark.stop()


if __name__ == "__main__":
    main()
