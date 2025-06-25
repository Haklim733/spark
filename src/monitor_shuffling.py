#!/usr/bin/env python3
"""
Shuffling Issues Monitoring and Analysis Script
Helps understand what happened during shuffling operations
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    sum as spark_sum,
    avg,
    max as spark_max,
    min as spark_min,
)
import time

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "admin")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "password")


def get_spark_session() -> SparkSession:
    """Create Spark session for monitoring"""
    spark = (
        SparkSession.builder.appName("ShufflingMonitor")
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
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .master("spark://spark-master:7077")
        .getOrCreate()
    )
    return spark


def analyze_data_distribution(spark):
    """Analyze data distribution to identify skew"""
    print("=" * 60)
    print("DATA DISTRIBUTION ANALYSIS")
    print("=" * 60)

    # Check if skewed_data table exists
    try:
        # Analyze key distribution
        print("\n1. Key Distribution Analysis:")
        key_distribution = spark.sql(
            """
            SELECT 
                key,
                COUNT(*) as record_count,
                AVG(value) as avg_value,
                MIN(value) as min_value,
                MAX(value) as max_value
            FROM skewed_data 
            GROUP BY key 
            ORDER BY record_count DESC 
            LIMIT 20
        """
        )

        key_distribution.show()

        # Calculate skew metrics
        print("\n2. Skew Metrics:")
        skew_metrics = spark.sql(
            """
            WITH key_counts AS (
                SELECT key, COUNT(*) as count
                FROM skewed_data 
                GROUP BY key
            )
            SELECT 
                COUNT(*) as unique_keys,
                SUM(count) as total_records,
                AVG(count) as avg_records_per_key,
                MAX(count) as max_records_per_key,
                MIN(count) as min_records_per_key,
                MAX(count) / AVG(count) as skew_ratio
            FROM key_counts
        """
        )

        skew_metrics.show()

        # Identify most skewed keys
        print("\n3. Most Skewed Keys (Top 10):")
        most_skewed = spark.sql(
            """
            SELECT 
                key,
                COUNT(*) as record_count,
                ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM skewed_data), 2) as percentage
            FROM skewed_data 
            GROUP BY key 
            ORDER BY record_count DESC 
            LIMIT 10
        """
        )

        most_skewed.show()

    except Exception as e:
        print(f"Error analyzing data: {e}")
        print("Make sure to run shuffling.py first to create the skewed_data table")


def analyze_partition_distribution(spark):
    """Analyze partition distribution"""
    print("\n" + "=" * 60)
    print("PARTITION DISTRIBUTION ANALYSIS")
    print("=" * 60)

    try:
        # Get partition information
        print("\n1. Current Partition Configuration:")
        partitions_config = spark.sql(
            """
            SELECT 
                'spark.sql.shuffle.partitions' as config_name,
                '200' as current_value
            UNION ALL SELECT 'spark.sql.adaptive.enabled', 'true'
            UNION ALL SELECT 'spark.sql.adaptive.coalescePartitions.enabled', 'true'
        """
        )
        partitions_config.show()

        # Analyze category distribution (for partitioning)
        print("\n2. Category Distribution (for partitioning):")
        category_dist = spark.sql(
            """
            SELECT 
                category,
                COUNT(*) as record_count,
                ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM skewed_data), 2) as percentage
            FROM skewed_data 
            GROUP BY category 
            ORDER BY record_count DESC
        """
        )

        category_dist.show()

    except Exception as e:
        print(f"Error analyzing partitions: {e}")


def provide_monitoring_guidance():
    """Provide guidance on how to monitor shuffling issues"""
    print("\n" + "=" * 60)
    print("MONITORING GUIDANCE")
    print("=" * 60)

    print("\n1. SPARK WEB UI (http://localhost:8080):")
    print("   - Jobs Tab: Click on completed jobs to see stages")
    print("   - Stages Tab: Look for high Shuffle Read/Write values")
    print("   - Executors Tab: Check memory and disk usage")
    print("   - SQL Tab: View query plans and execution details")

    print("\n2. KEY METRICS TO WATCH:")
    print("   - Shuffle Read/Write: High values indicate expensive shuffling")
    print("   - Stage Duration: Long stages may indicate skew")
    print("   - Input/Output: Large data transfers between stages")
    print("   - Memory Spills: Check for disk spills in executor logs")

    print("\n3. COMMON SHUFFLING ISSUES:")
    print("   - Data Skew: Some partitions much larger than others")
    print("   - Too Many Partitions: Many small tasks")
    print("   - Too Few Partitions: Memory pressure and spills")
    print("   - Cartesian Products: Extremely expensive operations")

    print("\n4. OPTIMIZATION TECHNIQUES:")
    print("   - Repartitioning: Redistribute data more evenly")
    print("   - Broadcast Joins: For small tables")
    print("   - Bucketing: Pre-partition data for joins")
    print("   - Adaptive Query Execution: Let Spark optimize automatically")

    print("\n5. COMMAND LINE MONITORING:")
    print("   - Check executor logs: docker logs spark-spark-worker-1")
    print("   - Monitor resource usage: docker stats")
    print("   - Check temporary files: docker exec spark-master ls -la /tmp/")


def run_diagnostic_queries(spark):
    """Run diagnostic queries to identify issues"""
    print("\n" + "=" * 60)
    print("DIAGNOSTIC QUERIES")
    print("=" * 60)

    try:
        print("\n1. Testing GroupBy Performance:")
        start_time = time.time()

        groupby_result = spark.sql(
            """
            SELECT 
                category,
                COUNT(*) as record_count,
                SUM(value) as total_value,
                AVG(value) as avg_value
            FROM skewed_data 
            GROUP BY category
        """
        )

        result_count = groupby_result.count()
        end_time = time.time()

        print(f"   - GroupBy completed in {end_time - start_time:.2f} seconds")
        print(f"   - Result count: {result_count}")

        if end_time - start_time > 10:
            print(
                "   ⚠️  WARNING: GroupBy took longer than 10 seconds - possible skew issue"
            )

        print("\n2. Testing Join Performance:")
        start_time = time.time()

        # Create small table for join test
        spark.sql(
            """
            SELECT 1 as id, 'category_1' as category_name
            UNION ALL SELECT 2, 'category_2'
            UNION ALL SELECT 3, 'category_3'
        """
        ).createOrReplaceTempView("small_table")

        join_result = spark.sql(
            """
            SELECT s.*, st.category_name
            FROM skewed_data s
            INNER JOIN small_table st ON s.category = st.category_name
        """
        )

        join_count = join_result.count()
        join_time = time.time() - start_time

        print(f"   - Join completed in {join_time:.2f} seconds")
        print(f"   - Result count: {join_count}")

        if join_time > 5:
            print(
                "   ⚠️  WARNING: Join took longer than 5 seconds - consider broadcast join"
            )

    except Exception as e:
        print(f"Error running diagnostic queries: {e}")


def main():
    """Main monitoring function"""
    print("Starting Shuffling Issues Monitoring...")

    try:
        spark = get_spark_session()
        spark.sparkContext.setLogLevel("WARN")  # Reduce log noise

        # Run analyses
        analyze_data_distribution(spark)
        analyze_partition_distribution(spark)
        run_diagnostic_queries(spark)
        provide_monitoring_guidance()

        print("\n" + "=" * 60)
        print("MONITORING COMPLETE")
        print("=" * 60)
        print("\nNext steps:")
        print("1. Open Spark Web UI: http://localhost:8080")
        print("2. Look for the 'ShufflingIssuesDemo' application")
        print("3. Analyze the stages and their metrics")
        print("4. Check executor logs for memory spills")
        print("5. Use the guidance above to identify specific issues")

    except Exception as e:
        print(f"Error in monitoring: {str(e)}")
        import traceback

        traceback.print_exc()
    finally:
        if "spark" in locals():
            spark.stop()


if __name__ == "__main__":
    main()
