#!/usr/bin/env python3
"""
Spark Shuffling Issues Demonstration
Shows common shuffling problems and how to identify them
Uses SQL operations whenever possible for better Spark Connect compatibility
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
    """Create and configure Spark session with shuffling monitoring using regular PySpark"""

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
        .config("spark.sql.catalog.iceberg.uri", "http://spark-rest:8181")
        .config("spark.sql.catalog.iceberg.s3.endpoint", "http://minio:9000")
        .config("spark.sql.catalog.iceberg.warehouse", "s3://data/wh")
        .config("spark.sql.catalog.iceberg.s3.access-key", AWS_ACCESS_KEY_ID)
        .config("spark.sql.catalog.iceberg.s3.secret-key", AWS_SECRET_ACCESS_KEY)
        .config("spark.sql.catalog.iceberg.s3.region", "us-east-1")
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
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
        .config("spark.eventLog.dir", "file:///opt/bitnami/spark/logs")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .master(
            "spark://spark-master:7077"
        )  # Use Spark cluster instead of Spark Connect
        .getOrCreate()
    )

    return spark


def generate_skewed_data(spark: SparkSession, num_records: int = 1000000):
    """
    Generate data with intentional skew to demonstrate shuffling issues
    Uses SQL to create and populate the table

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

    # Create table using SQL
    df.createOrReplaceTempView("skewed_data")

    # Use SQL to get count and distribution
    count_result = spark.sql("SELECT COUNT(*) as total_records FROM skewed_data")
    total_count = count_result.collect()[0]["total_records"]

    print(f"Generated DataFrame with {total_count:,} records")
    print("Data distribution preview:")

    # Use SQL for distribution analysis
    distribution_sql = """
    SELECT key, COUNT(*) as count 
    FROM skewed_data 
    GROUP BY key 
    ORDER BY count DESC 
    LIMIT 10
    """
    spark.sql(distribution_sql).show()

    return df


def demonstrate_shuffling_issues(spark: SparkSession, df):
    """
    Demonstrate various shuffling issues and their symptoms using SQL
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


def demonstrate_data_skew(spark: SparkSession, df):
    """
    Demonstrate data skew issue and its impact on shuffling using SQL
    """
    print("Performing groupBy operation on skewed data...")

    start_time = time.time()

    # Use SQL for groupBy operation
    skew_sql = """
    SELECT 
        key,
        SUM(value) as total_value,
        COUNT(*) as record_count,
        AVG(value) as avg_value
    FROM skewed_data 
    GROUP BY key
    """

    result = spark.sql(skew_sql)

    # Force execution
    result_count = result.count()
    end_time = time.time()

    print(f"GroupBy operation completed in {end_time - start_time:.2f} seconds")
    print(f"Result count: {result_count}")

    # Show the skewed distribution using SQL
    print("\nSkewed data distribution:")
    skewed_distribution_sql = """
    SELECT key, record_count, total_value, avg_value
    FROM (
        SELECT 
            key,
            SUM(value) as total_value,
            COUNT(*) as record_count,
            AVG(value) as avg_value
        FROM skewed_data 
        GROUP BY key
    ) 
    ORDER BY record_count DESC 
    LIMIT 10
    """
    spark.sql(skewed_distribution_sql).show()


def demonstrate_large_shuffle_partitions(spark: SparkSession, df):
    """
    Demonstrate issue with too many shuffle partitions using SQL
    """
    print("Setting high number of shuffle partitions...")

    # Set very high number of partitions
    spark.conf.set("spark.sql.shuffle.partitions", "1000")

    start_time = time.time()

    # Use SQL for aggregation
    high_partition_sql = """
    SELECT 
        category,
        SUM(value) as total_value,
        COUNT(*) as record_count
    FROM skewed_data 
    GROUP BY category
    """

    result = spark.sql(high_partition_sql)
    result_count = result.count()
    end_time = time.time()

    print(
        f"High partition count operation completed in {end_time - start_time:.2f} seconds"
    )
    print(f"Result count: {result_count}")

    # Reset to reasonable number
    spark.conf.set("spark.sql.shuffle.partitions", "200")
    print("Reset shuffle partitions to 200")


def demonstrate_small_shuffle_partitions(spark: SparkSession, df):
    """
    Demonstrate issue with too few shuffle partitions using SQL
    """
    print("Setting low number of shuffle partitions...")

    # Set very low number of partitions
    spark.conf.set("spark.sql.shuffle.partitions", "2")

    start_time = time.time()

    # Use SQL for aggregation
    low_partition_sql = """
    SELECT 
        category,
        SUM(value) as total_value,
        COUNT(*) as record_count
    FROM skewed_data 
    GROUP BY category
    """

    result = spark.sql(low_partition_sql)
    result_count = result.count()
    end_time = time.time()

    print(
        f"Low partition count operation completed in {end_time - start_time:.2f} seconds"
    )
    print(f"Result count: {result_count}")

    # Reset to reasonable number
    spark.conf.set("spark.sql.shuffle.partitions", "200")
    print("Reset shuffle partitions to 200")


def demonstrate_cartesian_product(spark: SparkSession, df):
    """
    Demonstrate cartesian product issue (very expensive shuffle) using SQL
    """
    print("Creating small table for cartesian product...")

    # Create small table using SQL
    small_table_sql = """
    SELECT 1 as id, 'A' as label
    UNION ALL SELECT 2, 'B'
    UNION ALL SELECT 3, 'C'
    """

    spark.sql(small_table_sql).createOrReplaceTempView("small_table")

    # Get counts using SQL
    large_count_sql = "SELECT COUNT(*) as large_count FROM skewed_data"
    small_count_sql = "SELECT COUNT(*) as small_count FROM small_table"

    large_count = spark.sql(large_count_sql).collect()[0]["large_count"]
    small_count = spark.sql(small_count_sql).collect()[0]["small_count"]

    print(f"Small table: {small_count} records")
    print(f"Large table: {large_count:,} records")

    start_time = time.time()

    # Use SQL for cartesian product
    cartesian_sql = """
    SELECT s.*, st.*
    FROM skewed_data s
    CROSS JOIN small_table st
    """

    try:
        result = spark.sql(cartesian_sql)
        result_count = result.count()
        end_time = time.time()

        print(f"Cartesian product completed in {end_time - start_time:.2f} seconds")
        print(f"Result count: {result_count:,}")
        print("WARNING: This operation is very expensive and should be avoided!")

    except Exception as e:
        print(f"Cartesian product failed (expected): {str(e)}")
        print("This demonstrates why cartesian products should be avoided")


def demonstrate_join_strategies(spark: SparkSession, df):
    """
    Demonstrate broadcast join vs shuffle join using SQL
    """
    print("Creating small table for join comparison...")

    # Create small table using SQL
    small_table_sql = """
    SELECT 1 as id, 'category_1' as category_name
    UNION ALL SELECT 2, 'category_2'
    UNION ALL SELECT 3, 'category_3'
    """

    spark.sql(small_table_sql).createOrReplaceTempView("small_table")

    # Get counts
    large_count = spark.sql("SELECT COUNT(*) as count FROM skewed_data").collect()[0][
        "count"
    ]
    small_count = spark.sql("SELECT COUNT(*) as count FROM small_table").collect()[0][
        "count"
    ]

    print(f"Small table: {small_count} records")
    print(f"Large table: {large_count:,} records")

    # Shuffle Join (default) - using SQL
    print("\nPerforming shuffle join...")
    start_time = time.time()

    shuffle_join_sql = """
    SELECT s.*, st.category_name
    FROM skewed_data s
    INNER JOIN small_table st ON s.category = st.category_name
    """

    shuffle_result = spark.sql(shuffle_join_sql)
    shuffle_count = shuffle_result.count()
    shuffle_time = time.time() - start_time

    print(f"Shuffle join completed in {shuffle_time:.2f} seconds")
    print(f"Result count: {shuffle_count:,}")

    # Broadcast Join (using hint) - using SQL
    print("\nPerforming broadcast join...")
    start_time = time.time()

    broadcast_join_sql = """
    SELECT /*+ BROADCAST(st) */ s.*, st.category_name
    FROM skewed_data s
    INNER JOIN small_table st ON s.category = st.category_name
    """

    broadcast_result = spark.sql(broadcast_join_sql)
    broadcast_count = broadcast_result.count()
    broadcast_time = time.time() - start_time

    print(f"Broadcast join completed in {broadcast_time:.2f} seconds")
    print(f"Result count: {broadcast_count:,}")

    # Performance comparison
    if broadcast_time > 0:
        speedup = shuffle_time / broadcast_time
        print(f"\nBroadcast join is {speedup:.1f}x faster than shuffle join")


def demonstrate_window_shuffling(spark: SparkSession, df):
    """
    Demonstrate window functions and their shuffling behavior using SQL
    """
    print("Performing window function operations...")

    start_time = time.time()

    # Use SQL for window functions
    window_sql = """
    SELECT 
        key,
        category,
        value,
        timestamp,
        ROW_NUMBER() OVER (PARTITION BY category ORDER BY timestamp) as row_number,
        SUM(value) OVER (PARTITION BY category ORDER BY timestamp) as running_total,
        ROW_NUMBER() OVER (PARTITION BY category ORDER BY value DESC) as category_rank
    FROM skewed_data
    """

    result = spark.sql(window_sql)
    result_count = result.count()
    end_time = time.time()

    print(f"Window function operation completed in {end_time - start_time:.2f} seconds")
    print(f"Result count: {result_count:,}")

    # Show sample results
    print("\nWindow function results sample:")
    sample_sql = """
    SELECT 
        key,
        category,
        value,
        row_number,
        running_total,
        category_rank
    FROM (
        SELECT 
            key,
            category,
            value,
            timestamp,
            ROW_NUMBER() OVER (PARTITION BY category ORDER BY timestamp) as row_number,
            SUM(value) OVER (PARTITION BY category ORDER BY timestamp) as running_total,
            ROW_NUMBER() OVER (PARTITION BY category ORDER BY value DESC) as category_rank
        FROM skewed_data
    )
    LIMIT 10
    """
    spark.sql(sample_sql).show()


def demonstrate_shuffling_optimizations(spark: SparkSession, df):
    """
    Demonstrate various shuffling optimizations using SQL
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


def demonstrate_repartitioning(spark: SparkSession, df):
    """
    Demonstrate repartitioning to fix skew using SQL
    """
    print("Repartitioning data to reduce skew...")

    # Create repartitioned table using SQL
    repartition_sql = """
    SELECT /*+ REPARTITION(50, category) */ *
    FROM skewed_data
    """

    spark.sql(repartition_sql).createOrReplaceTempView("repartitioned_data")

    start_time = time.time()

    # Use SQL for groupBy operation on repartitioned data
    repartition_groupby_sql = """
    SELECT 
        key,
        SUM(value) as total_value,
        COUNT(*) as record_count
    FROM repartitioned_data 
    GROUP BY key
    """

    result = spark.sql(repartition_groupby_sql)
    result_count = result.count()
    end_time = time.time()

    print(f"Repartitioned operation completed in {end_time - start_time:.2f} seconds")
    print(f"Result count: {result_count}")


def demonstrate_coalescing(spark: SparkSession, df):
    """
    Demonstrate coalescing to reduce partition overhead using SQL
    """
    print("Coalescing partitions to reduce overhead...")

    # Create table with many partitions using SQL
    many_partitions_sql = """
    SELECT /*+ REPARTITION(1000) */ *
    FROM skewed_data
    """

    spark.sql(many_partitions_sql).createOrReplaceTempView("many_partitions_data")

    start_time = time.time()

    # Coalesce using SQL
    coalesce_sql = """
    SELECT /*+ COALESCE(50) */ 
        category,
        SUM(value) as total_value,
        COUNT(*) as record_count
    FROM many_partitions_data 
    GROUP BY category
    """

    result = spark.sql(coalesce_sql)
    result_count = result.count()
    end_time = time.time()

    print(f"Coalesced operation completed in {end_time - start_time:.2f} seconds")
    print(f"Result count: {result_count}")


def demonstrate_bucketing(spark: SparkSession, df):
    """
    Demonstrate bucketing for join optimization using SQL
    """
    print("Creating bucketed table for join optimization...")

    # Create bucketed table using SQL
    bucketed_table_sql = """
    CREATE TABLE iceberg.bucketed_data (
        key INT,
        value INT,
        category STRING,
        timestamp BIGINT
    )
    USING iceberg
    PARTITIONED BY (category)
    CLUSTERED BY (category) INTO 50 BUCKETS
    """

    try:
        spark.sql(bucketed_table_sql)
        print("Created bucketed table")
    except Exception as e:
        print(f"Table might already exist: {e}")

    # Insert data into bucketed table
    insert_sql = """
    INSERT INTO iceberg.bucketed_data
    SELECT key, value, category, timestamp FROM skewed_data
    """

    spark.sql(insert_sql)
    print("Inserted data into bucketed table")

    # Create small table for join
    small_table_sql = """
    SELECT 1 as id, 'category_1' as category
    UNION ALL SELECT 2, 'category_2'
    UNION ALL SELECT 3, 'category_3'
    """

    spark.sql(small_table_sql).createOrReplaceTempView("small_table")

    # Perform join on bucketed table
    start_time = time.time()

    bucketed_join_sql = """
    SELECT b.*, s.category as small_category
    FROM iceberg.bucketed_data b
    INNER JOIN small_table s ON b.category = s.category
    """

    result = spark.sql(bucketed_join_sql)
    result_count = result.count()
    end_time = time.time()

    print(f"Bucketed join completed in {end_time - start_time:.2f} seconds")
    print(f"Result count: {result_count:,}")


def demonstrate_adaptive_query_execution(spark: SparkSession, df):
    """
    Demonstrate adaptive query execution features using SQL
    """
    print("Demonstrating adaptive query execution...")

    # Enable adaptive query execution
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

    start_time = time.time()

    # Complex query that benefits from adaptive execution using SQL
    adaptive_sql = """
    SELECT 
        category,
        SUM(value) as total_value,
        COUNT(*) as record_count,
        AVG(value) as avg_value,
        MAX(value) as max_value,
        MIN(value) as min_value
    FROM skewed_data 
    GROUP BY category
    HAVING COUNT(*) > 100
    """

    result = spark.sql(adaptive_sql)
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

    # Get current configuration using SQL
    print("\nCurrent Spark Configuration:")
    config_sql = """
    SELECT 
        'spark.sql.shuffle.partitions' as config_name,
        '200' as config_value
    UNION ALL SELECT 'spark.sql.adaptive.enabled', 'true'
    UNION ALL SELECT 'spark.sql.adaptive.coalescePartitions.enabled', 'true'
    UNION ALL SELECT 'spark.sql.adaptive.skewJoin.enabled', 'true'
    """

    spark.sql(config_sql).show()

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

        # Set log level for regular PySpark
        spark.sparkContext.setLogLevel("INFO")

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
