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
from datetime import datetime
import json

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "admin")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "password")


# Global metrics tracking
class MetricsTracker:
    def __init__(self):
        self.metrics = {}
        self.current_job_group = None
        self.current_operation = None

    def start_operation(self, job_group: str, operation: str):
        """Start tracking metrics for a new operation"""
        self.current_job_group = job_group
        self.current_operation = operation
        if job_group not in self.metrics:
            self.metrics[job_group] = {}
        if operation not in self.metrics[job_group]:
            self.metrics[job_group][operation] = {
                "start_time": time.time(),
                "data_skew_ratio": 0,
                "partition_count": 0,
                "record_count": 0,
                "unique_keys": 0,
                "max_key_frequency": 0,
                "min_key_frequency": 0,
                "shuffle_partitions": 0,
                "memory_usage_mb": 0,
                "execution_plan_complexity": "low",
            }

    def record_data_metrics(self, spark: SparkSession, table_name: str = "skewed_data"):
        """Record data distribution metrics and output to Spark logs"""
        if not self.current_job_group or not self.current_operation:
            return

        try:
            # Get data distribution
            distribution_df = spark.sql(
                f"""
                SELECT key, COUNT(*) as frequency
                FROM {table_name}
                GROUP BY key
                ORDER BY frequency DESC
            """
            )

            distribution = distribution_df.collect()

            if distribution:
                frequencies = [row["frequency"] for row in distribution]
                max_freq = max(frequencies)
                min_freq = min(frequencies)
                total_records = sum(frequencies)
                unique_keys = len(frequencies)

                # Calculate skew ratio (max frequency / average frequency)
                avg_freq = total_records / unique_keys
                skew_ratio = max_freq / avg_freq if avg_freq > 0 else 0

                # Get partition count
                partition_count = spark.sql(
                    f"SELECT COUNT(DISTINCT key) as partitions FROM {table_name}"
                ).collect()[0]["partitions"]

                # Get shuffle partitions setting
                shuffle_partitions = spark.conf.get(
                    "spark.sql.shuffle.partitions", "200"
                )

                # Get memory configuration
                executor_memory = spark.conf.get("spark.executor.memory", "1g")
                driver_memory = spark.conf.get("spark.driver.memory", "1g")
                memory_fraction = spark.conf.get("spark.memory.fraction", "0.6")
                storage_fraction = spark.conf.get("spark.memory.storageFraction", "0.5")

                # Calculate memory usage (approximate)
                # This is a rough estimate based on data size and operations
                estimated_memory_mb = (total_records * 100) / (
                    1024 * 1024
                )  # Rough estimate: 100 bytes per record

                # Update metrics
                self.metrics[self.current_job_group][self.current_operation].update(
                    {
                        "data_skew_ratio": round(skew_ratio, 2),
                        "partition_count": partition_count,
                        "record_count": total_records,
                        "unique_keys": unique_keys,
                        "max_key_frequency": max_freq,
                        "min_key_frequency": min_freq,
                        "shuffle_partitions": int(shuffle_partitions),
                        "execution_plan_complexity": (
                            "high"
                            if unique_keys > 1000
                            else "medium" if unique_keys > 100 else "low"
                        ),
                        "executor_memory": executor_memory,
                        "driver_memory": driver_memory,
                        "memory_fraction": float(memory_fraction),
                        "storage_fraction": float(storage_fraction),
                        "estimated_memory_mb": round(estimated_memory_mb, 2),
                    }
                )

                # Output custom metrics as structured log entry
                custom_metrics_event = {
                    "Event": "CustomMetricsEvent",
                    "Job Group": self.current_job_group,
                    "Operation": self.current_operation,
                    "Timestamp": int(time.time() * 1000),
                    "Metrics": {
                        "data_skew_ratio": round(skew_ratio, 2),
                        "partition_count": partition_count,
                        "record_count": total_records,
                        "unique_keys": unique_keys,
                        "max_key_frequency": max_freq,
                        "min_key_frequency": min_freq,
                        "shuffle_partitions": int(shuffle_partitions),
                        "execution_plan_complexity": (
                            "high"
                            if unique_keys > 1000
                            else "medium" if unique_keys > 100 else "low"
                        ),
                        "executor_memory": executor_memory,
                        "driver_memory": driver_memory,
                        "memory_fraction": float(memory_fraction),
                        "storage_fraction": float(storage_fraction),
                        "estimated_memory_mb": round(estimated_memory_mb, 2),
                    },
                }

                # Log the custom metrics event
                print(f"CUSTOM_METRICS: {json.dumps(custom_metrics_event)}")

        except Exception as e:
            print(f"Error recording data metrics: {e}")

    def end_operation(self, execution_time: float, result_count: int = 0):
        """End tracking metrics for current operation and output to Spark logs"""
        if not self.current_job_group or not self.current_operation:
            return

        self.metrics[self.current_job_group][self.current_operation].update(
            {
                "execution_time": round(execution_time, 2),
                "result_count": result_count,
                "end_time": time.time(),
            }
        )

        # Output operation completion metrics as structured log entry
        completion_event = {
            "Event": "CustomOperationCompletion",
            "Job Group": self.current_job_group,
            "Operation": self.current_operation,
            "Timestamp": int(time.time() * 1000),
            "Execution Time": round(execution_time, 2),
            "Result Count": result_count,
        }

        # Log the completion event
        print(f"CUSTOM_COMPLETION: {json.dumps(completion_event)}")

    def get_metrics(self) -> Dict:
        """Get all recorded metrics"""
        return self.metrics

    def save_metrics(self, filename: str = "shuffling_metrics.json"):
        """Save metrics to JSON file (kept for backward compatibility)"""
        try:
            with open(filename, "w") as f:
                json.dump(self.metrics, f, indent=2)
            print(f"Metrics saved to {filename}")
        except Exception as e:
            print(f"Error saving metrics: {e}")


# Global metrics tracker instance
metrics_tracker = MetricsTracker()


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

    # Set job group for data generation
    spark.sparkContext.setJobGroup(
        "data_generation", "Create DataFrame and Count Records"
    )

    # Start metrics tracking
    metrics_tracker.start_operation(
        "data_generation", "Create DataFrame and Count Records"
    )

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

    # Record data metrics
    metrics_tracker.record_data_metrics(spark, "skewed_data")
    metrics_tracker.end_operation(0, total_count)  # Data generation time is negligible

    # Use SQL for distribution analysis - this creates a separate job
    spark.sparkContext.setJobGroup(
        "data_generation", "GROUP BY Analysis - Show Top 10 Keys by Frequency"
    )

    # Start metrics tracking for distribution preview
    metrics_tracker.start_operation(
        "data_generation", "GROUP BY Analysis - Show Top 10 Keys by Frequency"
    )
    start_time = time.time()

    distribution_sql = """
    SELECT key, COUNT(*) as count 
    FROM skewed_data 
    GROUP BY key 
    ORDER BY count DESC 
    LIMIT 10
    """
    result = spark.sql(distribution_sql)
    result.show()

    # End metrics tracking
    execution_time = time.time() - start_time
    metrics_tracker.end_operation(execution_time, result.count())

    # Clear job group
    spark.sparkContext.clearJobGroup()

    return df


def log_with_timestamp(message):
    """Log message with timestamp"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")


def demonstrate_shuffling_issues(spark: SparkSession, df):
    """
    Demonstrate various shuffling issues and their symptoms using SQL
    """
    log_with_timestamp("Starting Shuffling Issues Demonstration")
    spark.sparkContext.setJobGroup(
        "shuffling_issues_demo",
        "Shuffling Issues Demo",
    )

    # Issue 1: Data Skew
    demonstrate_data_skew(spark, df)

    # Issue 2: Large Shuffle Partitions
    demonstrate_large_shuffle_partitions(spark, df)

    # Issue 3: Small Shuffle Partitions
    demonstrate_small_shuffle_partitions(spark, df)

    # Issue 4: Cartesian Product
    demonstrate_cartesian_product(spark, df)

    # Issue 5: Broadcast Join vs Shuffle Join
    demonstrate_join_strategies(spark, df)

    # Issue 6: Window Functions Shuffling
    demonstrate_window_shuffling(spark, df)

    log_with_timestamp("Completed Shuffling Issues Demonstration")
    spark.sparkContext.clearJobGroup()


def demonstrate_data_skew(spark: SparkSession, df):
    """
    Demonstrate data skew issue and its impact on shuffling using SQL
    """
    log_with_timestamp("Starting Data Skew Demo...")

    # Set job name for this operation
    spark.sparkContext.setJobGroup(
        "data_skew_demo",
        "Data Skew GroupBy Operation - High Shuffle Expected",
    )

    # Start metrics tracking
    metrics_tracker.start_operation("data_skew_demo", "Data Skew GroupBy Operation")

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
    execution_time = end_time - start_time

    # Record metrics
    metrics_tracker.record_data_metrics(spark, "skewed_data")
    metrics_tracker.end_operation(execution_time, result_count)

    log_with_timestamp(f"Data Skew Demo completed in {execution_time:.2f} seconds")

    # Performance assessment
    if execution_time > 10:
        log_with_timestamp(
            f"⚠️  SLOW PERFORMANCE: This operation took {execution_time:.2f}s - likely due to data skew!"
        )
    elif execution_time > 5:
        log_with_timestamp(
            f"⚠️  MODERATE PERFORMANCE: This operation took {execution_time:.2f}s - some skew detected"
        )
    else:
        log_with_timestamp(
            f"✅ GOOD PERFORMANCE: This operation took {execution_time:.2f}s"
        )

    # Show the skewed distribution using SQL
    spark.sparkContext.setJobGroup(
        "data_skew_demo",
        "Show Skewed Distribution (ORDER BY DESC)",
    )

    # Start metrics tracking for distribution display
    metrics_tracker.start_operation("data_skew_demo", "Show Skewed Distribution")
    start_time = time.time()

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
    result = spark.sql(skewed_distribution_sql)
    result.show()

    # End metrics tracking
    execution_time = time.time() - start_time
    metrics_tracker.end_operation(execution_time, result.count())

    # Clear job group
    spark.sparkContext.clearJobGroup()


def demonstrate_large_shuffle_partitions(spark: SparkSession, df):
    """
    Demonstrate issue with too many shuffle partitions using SQL
    """
    log_with_timestamp("Starting Large Shuffle Partitions Demo...")

    # Set job name for this operation
    spark.sparkContext.setJobGroup(
        "large_partitions_demo",
        "Large Shuffle Partitions Demo - 1000 Partitions",
    )

    # Start metrics tracking
    metrics_tracker.start_operation(
        "large_partitions_demo", "Large Shuffle Partitions Demo"
    )

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
    execution_time = end_time - start_time

    # Record metrics
    metrics_tracker.record_data_metrics(spark, "skewed_data")
    metrics_tracker.end_operation(execution_time, result_count)

    log_with_timestamp(
        f"Large Partitions Demo completed in {execution_time:.2f} seconds"
    )

    # Performance assessment
    if execution_time > 8:
        log_with_timestamp(
            f"⚠️  SLOW PERFORMANCE: Too many partitions (1000) caused overhead!"
        )
    elif execution_time > 4:
        log_with_timestamp(
            f"⚠️  MODERATE PERFORMANCE: High partition count may be causing overhead"
        )
    else:
        log_with_timestamp(
            f"✅ ACCEPTABLE PERFORMANCE: Operation completed in reasonable time"
        )

    # Reset to reasonable number
    spark.conf.set("spark.sql.shuffle.partitions", "200")

    # Clear job group
    spark.sparkContext.clearJobGroup()


def demonstrate_small_shuffle_partitions(spark: SparkSession, df):
    """
    Demonstrate issue with too few shuffle partitions using SQL
    """
    log_with_timestamp("Starting Small Shuffle Partitions Demo...")

    # Set job name for this operation
    spark.sparkContext.setJobGroup(
        "small_partitions_demo",
        "Small Shuffle Partitions Demo - 2 Partitions",
    )

    # Start metrics tracking
    metrics_tracker.start_operation(
        "small_partitions_demo", "Small Shuffle Partitions Demo"
    )

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
    execution_time = end_time - start_time

    # Record metrics
    metrics_tracker.record_data_metrics(spark, "skewed_data")
    metrics_tracker.end_operation(execution_time, result_count)

    log_with_timestamp(
        f"Small Partitions Demo completed in {execution_time:.2f} seconds"
    )

    # Performance assessment
    if execution_time > 10:
        log_with_timestamp(
            f"⚠️  SLOW PERFORMANCE: Too few partitions (2) causing memory pressure!"
        )
    elif execution_time > 6:
        log_with_timestamp(
            f"⚠️  MODERATE PERFORMANCE: Low partition count may cause memory issues"
        )
    else:
        log_with_timestamp(
            f"✅ ACCEPTABLE PERFORMANCE: Operation completed despite low partitions"
        )

    # Reset to reasonable number
    spark.conf.set("spark.sql.shuffle.partitions", "200")

    # Clear job group
    spark.sparkContext.clearJobGroup()


def demonstrate_cartesian_product(spark: SparkSession, df):
    """
    Demonstrate cartesian product issue (very expensive shuffle) using SQL
    """
    log_with_timestamp("Starting Cartesian Product Demo...")

    # Set job name for this operation
    spark.sparkContext.setJobGroup(
        "cartesian_demo",
        "Cartesian Product Demo - Very Expensive Operation",
    )

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

    log_with_timestamp(
        f"Small table: {small_count} records, Large table: {large_count:,} records"
    )
    log_with_timestamp(
        f"Expected result size: {large_count * small_count:,} records (very large!)"
    )

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
        execution_time = end_time - start_time

        log_with_timestamp(
            f"Cartesian Product Demo completed in {execution_time:.2f} seconds"
        )

        # Performance assessment
        if execution_time > 15:
            log_with_timestamp(
                f"⚠️  VERY SLOW: Cartesian product took {execution_time:.2f}s - extremely expensive!"
            )
        elif execution_time > 8:
            log_with_timestamp(
                f"⚠️  SLOW: Cartesian product took {execution_time:.2f}s - expensive operation"
            )
        else:
            log_with_timestamp(
                f"⚠️  MODERATE: Cartesian product completed in {execution_time:.2f}s"
            )

        log_with_timestamp(
            "WARNING: This operation is very expensive and should be avoided!"
        )

    except Exception as e:
        log_with_timestamp(f"Cartesian Product Demo failed (expected): {str(e)}")
        log_with_timestamp("This demonstrates why cartesian products should be avoided")

    # Clear job group
    spark.sparkContext.clearJobGroup()


def demonstrate_join_strategies(spark: SparkSession, df):
    """
    Demonstrate broadcast join vs shuffle join using SQL
    """
    log_with_timestamp("Starting Join Strategies Demo...")

    # Set job name for this operation
    spark.sparkContext.setJobGroup(
        "join_strategies_demo",
        "Join Strategies Demo - Broadcast vs Shuffle",
    )

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

    log_with_timestamp(
        f"Small table: {small_count} records, Large table: {large_count:,} records"
    )

    # Shuffle Join (default) - using SQL
    spark.sparkContext.setJobGroup("shuffle_join_demo", "Shuffle Join Demo")
    start_time = time.time()

    shuffle_join_sql = """
    SELECT s.*, st.category_name
    FROM skewed_data s
    INNER JOIN small_table st ON s.category = st.category_name
    """

    shuffle_result = spark.sql(shuffle_join_sql)
    shuffle_count = shuffle_result.count()
    shuffle_time = time.time() - start_time

    log_with_timestamp(f"Shuffle Join completed in {shuffle_time:.2f} seconds")
    spark.sparkContext.clearJobGroup()

    # Broadcast Join (using hint) - using SQL
    spark.sparkContext.setJobGroup("broadcast_join_demo", "Broadcast Join Demo")
    start_time = time.time()

    broadcast_join_sql = """
    SELECT /*+ BROADCAST(st) */ s.*, st.category_name
    FROM skewed_data s
    INNER JOIN small_table st ON s.category = st.category_name
    """

    broadcast_result = spark.sql(broadcast_join_sql)
    broadcast_count = broadcast_result.count()
    broadcast_time = time.time() - start_time

    log_with_timestamp(f"Broadcast Join completed in {broadcast_time:.2f} seconds")
    spark.sparkContext.clearJobGroup()

    # Performance comparison
    if broadcast_time > 0:
        speedup = shuffle_time / broadcast_time
        log_with_timestamp(f"Broadcast join is {speedup:.1f}x faster than shuffle join")

        if speedup > 5:
            log_with_timestamp(
                f"✅ EXCELLENT: Broadcast join is {speedup:.1f}x faster - significant improvement!"
            )
        elif speedup > 2:
            log_with_timestamp(
                f"✅ GOOD: Broadcast join is {speedup:.1f}x faster - noticeable improvement"
            )
        else:
            log_with_timestamp(
                f"⚠️  MODERATE: Broadcast join is {speedup:.1f}x faster - minimal improvement"
            )

    # Clear main job group
    spark.sparkContext.clearJobGroup()


def demonstrate_window_shuffling(spark: SparkSession, df):
    """
    Demonstrate window functions and their shuffling behavior using SQL
    """
    log_with_timestamp("Starting Window Functions Demo...")

    # Set job name for this operation
    spark.sparkContext.setJobGroup(
        "window_functions_demo",
        "Window Functions Demo - Partitioned Operations",
    )

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
    execution_time = end_time - start_time

    log_with_timestamp(
        f"Window Functions Demo completed in {execution_time:.2f} seconds"
    )

    # Performance assessment
    if execution_time > 12:
        log_with_timestamp(
            f"⚠️  SLOW PERFORMANCE: Window functions took {execution_time:.2f}s - high shuffle overhead!"
        )
    elif execution_time > 6:
        log_with_timestamp(
            f"⚠️  MODERATE PERFORMANCE: Window functions took {execution_time:.2f}s - some shuffle overhead"
        )
    else:
        log_with_timestamp(
            f"✅ GOOD PERFORMANCE: Window functions completed in {execution_time:.2f}s"
        )

    # Show sample results
    spark.sparkContext.setJobGroup(
        "window_functions_demo",
        "Show Window Function Results Sample",
    )
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

    # Clear job group
    spark.sparkContext.clearJobGroup()


def demonstrate_shuffling_optimizations(spark: SparkSession, df):
    """
    Demonstrate various shuffling optimizations using SQL
    """
    log_with_timestamp("Starting Shuffling Optimizations Demo...")

    # Optimization 1: Repartitioning
    demonstrate_repartitioning(spark, df)

    # Optimization 2: Coalescing
    demonstrate_coalescing(spark, df)

    # Optimization 3: Bucketing
    demonstrate_bucketing(spark, df)

    # Optimization 4: Adaptive Query Execution
    demonstrate_adaptive_query_execution(spark, df)

    log_with_timestamp("Completed Shuffling Optimizations Demo...")


def demonstrate_repartitioning(spark: SparkSession, df):
    """
    Demonstrate repartitioning to fix skew using SQL
    """
    log_with_timestamp("Starting Repartitioning Demo...")

    # Set job group for this operation
    spark.sparkContext.setJobGroup(
        "repartitioning_demo",
        "Repartitioning to Fix Skew",
    )

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

    log_with_timestamp(
        f"Repartitioned operation completed in {end_time - start_time:.2f} seconds"
    )

    # Clear job group
    spark.sparkContext.clearJobGroup()


def demonstrate_coalescing(spark: SparkSession, df):
    """
    Demonstrate coalescing to reduce partition overhead using SQL
    """
    log_with_timestamp("Starting Coalescing Demo...")

    # Set job group for this operation
    spark.sparkContext.setJobGroup(
        "coalescing_demo",
        "Coalescing to Reduce Partition Overhead",
    )

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

    log_with_timestamp(
        f"Coalesced operation completed in {end_time - start_time:.2f} seconds"
    )

    # Clear job group
    spark.sparkContext.clearJobGroup()


def demonstrate_bucketing(spark: SparkSession, df):
    """
    Demonstrate bucketing for join optimization using SQL
    """
    log_with_timestamp("Starting Bucketing Demo...")

    # Set job group for this operation
    spark.sparkContext.setJobGroup("bucketing_demo", "Bucketing for Join Optimization")

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
        log_with_timestamp("Created bucketed table")
    except Exception as e:
        log_with_timestamp(f"Table might already exist: {e}")

    # Insert data into bucketed table
    insert_sql = """
    INSERT INTO iceberg.bucketed_data
    SELECT key, value, category, timestamp FROM skewed_data
    """

    spark.sql(insert_sql)
    log_with_timestamp("Inserted data into bucketed table")

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

    log_with_timestamp(
        f"Bucketed join completed in {end_time - start_time:.2f} seconds"
    )

    # Clear job group
    spark.sparkContext.clearJobGroup()


def demonstrate_adaptive_query_execution(spark: SparkSession, df):
    """
    Demonstrate adaptive query execution features using SQL
    """
    log_with_timestamp("Starting Adaptive Query Execution Demo...")

    # Set job group for this operation
    spark.sparkContext.setJobGroup(
        "adaptive_query_demo",
        "Adaptive Query Execution Demo",
    )

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

    log_with_timestamp(
        f"Adaptive query execution completed in {end_time - start_time:.2f} seconds"
    )

    # Show the results
    result.show()

    # Clear job group
    spark.sparkContext.clearJobGroup()


def monitor_memory_configuration(spark: SparkSession):
    """
    Monitor and display Spark memory configuration
    """
    log_with_timestamp("Starting Memory Configuration Analysis...")

    # Get memory configuration
    executor_memory = spark.conf.get("spark.executor.memory", "1g")
    driver_memory = spark.conf.get("spark.driver.memory", "1g")
    memory_fraction = spark.conf.get("spark.memory.fraction", "0.6")
    storage_fraction = spark.conf.get("spark.memory.storageFraction", "0.5")

    # Get cluster information
    try:
        # Get executor information
        executors = spark.sparkContext.statusTracker().getExecutorMetrics()
        num_executors = len(executors) if executors else 1

        log_with_timestamp(f"📊 Memory Configuration:")
        log_with_timestamp(f"  🖥️  Executor Memory: {executor_memory}")
        log_with_timestamp(f"  🖥️  Driver Memory: {driver_memory}")
        log_with_timestamp(
            f"  📊 Memory Fraction: {memory_fraction} (60% for execution + storage)"
        )
        log_with_timestamp(
            f"  💾 Storage Fraction: {storage_fraction} (50% of fraction for caching)"
        )
        log_with_timestamp(f"  🔢 Number of Executors: {num_executors}")

        # Calculate total cluster memory
        if executor_memory.endswith("g"):
            executor_mb = int(executor_memory[:-1]) * 1024
        elif executor_memory.endswith("m"):
            executor_mb = int(executor_memory[:-1])
        else:
            executor_mb = int(executor_memory) // (1024 * 1024)

        total_cluster_memory = executor_mb * num_executors
        log_with_timestamp(f"  🏗️  Total Cluster Memory: {total_cluster_memory}MB")

        # Memory recommendations
        log_with_timestamp(f"💡 Memory Recommendations:")
        log_with_timestamp(f"  • For large datasets: Increase executor.memory to 4g-8g")
        log_with_timestamp(f"  • For caching: Increase memory.fraction to 0.8")
        log_with_timestamp(f"  • For complex joins: Increase storage.fraction to 0.6")
        log_with_timestamp(
            f"  • Monitor spills: If you see disk spills, increase memory"
        )

    except Exception as e:
        log_with_timestamp(f"Could not get detailed cluster info: {e}")
        log_with_timestamp(
            f"Basic memory config: Executor={executor_memory}, Driver={driver_memory}"
        )


def monitor_shuffling_metrics(spark: SparkSession):
    """
    Monitor shuffling-related metrics
    """
    log_with_timestamp("Starting Shuffling Metrics Monitoring...")

    # Get current configuration using SQL
    config_sql = """
    SELECT 
        'spark.sql.shuffle.partitions' as config_name,
        '200' as config_value
    UNION ALL SELECT 'spark.sql.adaptive.enabled', 'true'
    UNION ALL SELECT 'spark.sql.adaptive.coalescePartitions.enabled', 'true'
    UNION ALL SELECT 'spark.sql.adaptive.skewJoin.enabled', 'true'
    """

    spark.sql(config_sql).show()

    log_with_timestamp("Shuffling metrics monitoring completed")
    log_with_timestamp("To monitor shuffling issues:")
    log_with_timestamp("1. Open Spark Web UI at http://localhost:8080")
    log_with_timestamp("2. Go to 'Stages' tab to see shuffle read/write metrics")
    log_with_timestamp("3. Look for stages with high shuffle read/write times")
    log_with_timestamp("4. Check for skewed partition distributions")
    log_with_timestamp("5. Monitor executor memory and disk usage")


def main():
    """Main function to demonstrate shuffling issues"""

    overall_start_time = time.time()
    log_with_timestamp("Starting Spark Shuffling Issues Demonstration...")

    try:
        # Create Spark session
        spark = get_spark_session()

        # Set log level for regular PySpark
        spark.sparkContext.setLogLevel("INFO")

        # Generate skewed data
        log_with_timestamp("Generating test data...")
        df = generate_skewed_data(spark, num_records=500000)  # Reduced for demo

        # Demonstrate shuffling issues
        demonstrate_shuffling_issues(spark, df)

        # Demonstrate optimizations
        demonstrate_shuffling_optimizations(spark, df)

        # Monitor metrics
        monitor_shuffling_metrics(spark)

        # Monitor memory configuration
        monitor_memory_configuration(spark)

        overall_end_time = time.time()
        total_execution_time = overall_end_time - overall_start_time

        log_with_timestamp(
            f"Shuffling demonstration completed in {total_execution_time:.2f} seconds"
        )

        # Save and display custom metrics
        metrics_tracker.save_metrics("shuffling_metrics.json")

        # Display metrics summary
        log_with_timestamp("Custom Metrics Summary:")
        metrics = metrics_tracker.get_metrics()
        for job_group, operations in metrics.items():
            log_with_timestamp(f"Job Group: {job_group}")
            for operation, data in operations.items():
                if "data_skew_ratio" in data and data["data_skew_ratio"] > 0:
                    log_with_timestamp(f"  {operation}:")
                    log_with_timestamp(
                        f"    - Data Skew Ratio: {data['data_skew_ratio']}"
                    )
                    log_with_timestamp(f"    - Record Count: {data['record_count']:,}")
                    log_with_timestamp(f"    - Unique Keys: {data['unique_keys']}")
                    log_with_timestamp(
                        f"    - Max Key Frequency: {data['max_key_frequency']}"
                    )
                    log_with_timestamp(
                        f"    - Shuffle Partitions: {data['shuffle_partitions']}"
                    )
                    if "execution_time" in data:
                        log_with_timestamp(
                            f"    - Execution Time: {data['execution_time']}s"
                        )

        log_with_timestamp("Key takeaways:")
        log_with_timestamp("1. Data skew causes severe shuffling issues")
        log_with_timestamp("2. Too many/few partitions can hurt performance")
        log_with_timestamp("3. Cartesian products are extremely expensive")
        log_with_timestamp("4. Broadcast joins are much faster than shuffle joins")
        log_with_timestamp("5. Window functions can cause significant shuffling")
        log_with_timestamp("6. Use adaptive query execution for automatic optimization")
        log_with_timestamp("7. Monitor Spark Web UI for shuffling metrics")
        log_with_timestamp("Next steps:")
        log_with_timestamp(
            "• Run 'python src/parse_logs.py --file-path spark-logs/app-*' to analyze performance"
        )
        log_with_timestamp(
            "• Check Spark Web UI at http://localhost:8080 for detailed metrics"
        )
        log_with_timestamp(
            "• Look for the job descriptions in the log analysis to identify bottlenecks"
        )
        log_with_timestamp(
            "• Review shuffling_metrics.json for custom performance metrics"
        )

    except Exception as e:
        log_with_timestamp(f"Error in shuffling demonstration: {str(e)}")
        import traceback

        traceback.print_exc()
    finally:
        if "spark" in locals():
            log_with_timestamp("Stopping Spark session...")
            spark.stop()


if __name__ == "__main__":
    main()
