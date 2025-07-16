#!/usr/bin/env python3
"""
Log Inserter - Functions to insert HybridLogger data into dataops.job_logs table
"""

import json
import os
import glob
import re
from datetime import datetime
from typing import Dict, Any, List, Optional
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    lit,
    current_timestamp,
    regexp_extract,
    to_timestamp,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    TimestampType,
    MapType,
    ArrayType,
)

from src.utils.session import create_spark_session, SparkVersion
from src.utils.logger import HybridLogger


def parse_log_line(log_line: str) -> Optional[Dict[str, Any]]:
    """Parse a single log line from hybrid observability log"""
    try:
        # Skip empty lines and non-JSON lines
        log_line = log_line.strip()
        if not log_line or not log_line.startswith("{"):
            return None

        # Try to parse as JSON
        log_data = json.loads(log_line)

        # Only process structured log entries with specific prefixes
        valid_prefixes = [
            "PERFORMANCE_METRICS:",
            "BUSINESS_EVENT:",
            "CUSTOM_METRICS:",
            "CUSTOM_COMPLETION:",
            "FILE_SUCCESS:",
            "FILE_FAILURE:",
        ]

        # Check if this is a structured log entry
        raw_message = log_data
        if isinstance(log_data, str):
            # Sometimes the JSON is double-encoded
            try:
                log_data = json.loads(log_data)
            except:
                return None

        return log_data

    except (json.JSONDecodeError, TypeError) as e:
        return None


def extract_metrics_from_log_file(log_file_path: str) -> List[Dict[str, Any]]:
    """Extract structured metrics from a hybrid observability log file"""
    metrics = []

    try:
        with open(log_file_path, "r", encoding="utf-8") as file:
            for line_num, line in enumerate(file, 1):
                parsed_data = parse_log_line(line)
                if parsed_data:
                    # Add metadata about the log entry
                    parsed_data["_log_file_path"] = log_file_path
                    parsed_data["_log_line_number"] = line_num
                    parsed_data["_parsed_at"] = datetime.now().isoformat()
                    metrics.append(parsed_data)

    except Exception as e:
        print(f"❌ Error reading log file {log_file_path}: {e}")

    return metrics


def find_log_files(logs_directory: str, app_name: Optional[str] = None) -> List[str]:
    """Find hybrid observability log files in the logs directory"""
    log_files = []

    # Pattern for hybrid observability logs
    if app_name:
        pattern = os.path.join(
            logs_directory, "app", app_name, f"{app_name}-hybrid-observability.log*"
        )
    else:
        pattern = os.path.join(
            logs_directory, "app", "*", "*-hybrid-observability.log*"
        )

    log_files = glob.glob(pattern, recursive=True)

    print(f"📁 Found {len(log_files)} log files matching pattern: {pattern}")
    for log_file in log_files:
        print(f"   📄 {log_file}")

    return log_files


def transform_metrics_to_job_logs_schema(
    metrics: List[Dict[str, Any]], app_name: str
) -> List[Dict[str, Any]]:
    """Transform parsed metrics to match dataops.job_logs schema"""
    job_logs_records = []

    for metric in metrics:
        try:
            # Extract common fields
            job_id = metric.get("job_id", metric.get("Job Group", "unknown"))
            operation_name = metric.get("operation", metric.get("Operation", None))

            # Determine log level and type
            log_level = "operation"  # Default
            job_type = "unknown"
            operation_type = "unknown"

            # Parse different types of log entries
            if "Event" in metric:
                event_type = metric.get("Event", "")
                if event_type == "CustomOperationCompletion":
                    log_level = "operation"
                    job_type = "data_processing"
                    operation_type = "complete"
                elif event_type == "CustomMetricsEvent":
                    log_level = "operation"
                    job_type = "metrics"
                    operation_type = "log"
                elif "BusinessEvent" in event_type:
                    log_level = "operation"
                    job_type = "business"
                    operation_type = "event"

            # Extract timing information
            start_time = None
            end_time = None
            duration_ms = None

            if "timestamp" in metric:
                # Convert timestamp to datetime
                ts = metric["timestamp"]
                if isinstance(ts, (int, float)):
                    start_time = datetime.fromtimestamp(ts / 1000 if ts > 1e12 else ts)
                elif isinstance(ts, str):
                    try:
                        start_time = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                    except:
                        pass

            if "Execution Time" in metric:
                duration_ms = int(float(metric["Execution Time"]) * 1000)

            if "execution_time_ms" in metric:
                duration_ms = metric["execution_time_ms"]

            # Calculate end_time if we have start_time and duration
            if start_time and duration_ms:
                end_time = datetime.fromtimestamp(
                    start_time.timestamp() + duration_ms / 1000
                )

            # Extract record metrics
            records_processed = metric.get(
                "records_processed", metric.get("Result Count", 0)
            )
            records_inserted = metric.get(
                "successful_inserts", metric.get("records_inserted", 0)
            )
            records_failed = metric.get(
                "failed_inserts", metric.get("records_failed", 0)
            )

            # Build the job_logs record
            job_log_record = {
                "log_id": f"{job_id}_{operation_name}_{metric.get('_log_line_number', 0)}",
                "job_id": job_id,
                "batch_id": metric.get("batch_id", None),
                "operation_name": operation_name,
                "log_level": log_level,
                "source": "minio",  # Default source
                "target_table": metric.get("table_name", None),
                "app_name": app_name,
                "job_type": job_type,
                "operation_type": operation_type,
                "start_time": start_time,
                "end_time": end_time,
                "duration_ms": duration_ms,
                "records_listed": 0,
                "records_processed": int(records_processed) if records_processed else 0,
                "records_inserted": int(records_inserted) if records_inserted else 0,
                "records_failed": int(records_failed) if records_failed else 0,
                "records_missing": 0,
                "records_updated": 0,
                "records_deleted": 0,
                "total_file_size_bytes": metric.get("file_size_bytes", 0),
                "bytes_transferred": metric.get("bytes_transferred", 0),
                "error_count": 1 if records_failed and records_failed > 0 else 0,
                "error_codes": {},  # Will be populated as Map
                "error_summary": metric.get("error_message", ""),
                "error_types": [],  # Will be populated as Array
                "failed_files_by_partition": {},  # Complex nested structure
                "business_events_count": 1 if job_type == "business" else 0,
                "business_event_types": (
                    [metric.get("event_type", "")] if job_type == "business" else []
                ),
                "custom_metrics_count": 1 if job_type == "metrics" else 0,
                "custom_operation_names": [operation_name] if operation_name else [],
                "total_logs_generated": 1,
                "async_logging_enabled": True,
                "validation_status": metric.get("validation_status", "unknown"),
                "status": metric.get("status", "success"),
                "spark_version": None,
                "executor_count": None,
                "executor_memory_mb": None,
                "driver_memory_mb": None,
                "created_by": "hybrid_logger",
                "created_at": datetime.now(),
                "updated_at": datetime.now(),
            }

            job_logs_records.append(job_log_record)

        except Exception as e:
            print(f"❌ Error transforming metric: {e}")
            print(f"   Metric: {metric}")
            continue

    return job_logs_records


def create_job_logs_dataframe(
    spark: SparkSession, job_logs_records: List[Dict[str, Any]]
) -> DataFrame:
    """Create a Spark DataFrame from job logs records"""

    # Define the schema to match dataops.job_logs table
    schema = StructType(
        [
            StructField("log_id", StringType(), True),
            StructField("job_id", StringType(), True),
            StructField("batch_id", StringType(), True),
            StructField("operation_name", StringType(), True),
            StructField("log_level", StringType(), True),
            StructField("source", StringType(), True),
            StructField("target_table", StringType(), True),
            StructField("app_name", StringType(), True),
            StructField("job_type", StringType(), True),
            StructField("operation_type", StringType(), True),
            StructField("start_time", TimestampType(), True),
            StructField("end_time", TimestampType(), True),
            StructField("duration_ms", LongType(), True),
            StructField("records_listed", IntegerType(), True),
            StructField("records_processed", IntegerType(), True),
            StructField("records_inserted", IntegerType(), True),
            StructField("records_failed", IntegerType(), True),
            StructField("records_missing", IntegerType(), True),
            StructField("records_updated", IntegerType(), True),
            StructField("records_deleted", IntegerType(), True),
            StructField("total_file_size_bytes", LongType(), True),
            StructField("bytes_transferred", LongType(), True),
            StructField("error_count", IntegerType(), True),
            StructField("error_codes", MapType(StringType(), IntegerType()), True),
            StructField("error_summary", StringType(), True),
            StructField("error_types", ArrayType(StringType()), True),
            StructField("business_events_count", IntegerType(), True),
            StructField("business_event_types", ArrayType(StringType()), True),
            StructField("custom_metrics_count", IntegerType(), True),
            StructField("custom_operation_names", ArrayType(StringType()), True),
            StructField("total_logs_generated", IntegerType(), True),
            StructField(
                "async_logging_enabled", StringType(), True
            ),  # Boolean as String for compatibility
            StructField("validation_status", StringType(), True),
            StructField("status", StringType(), True),
            StructField("spark_version", StringType(), True),
            StructField("executor_count", IntegerType(), True),
            StructField("executor_memory_mb", IntegerType(), True),
            StructField("driver_memory_mb", IntegerType(), True),
            StructField("created_by", StringType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("updated_at", TimestampType(), True),
        ]
    )

    # Convert records to proper format for DataFrame creation
    formatted_records = []
    for record in job_logs_records:
        formatted_record = []
        for field in schema.fields:
            value = record.get(field.name, None)

            # Handle special type conversions
            if field.name == "async_logging_enabled" and isinstance(value, bool):
                value = str(value).lower()
            elif field.dataType == TimestampType() and value is None:
                value = None
            elif field.dataType == IntegerType() and value is None:
                value = 0
            elif field.dataType == LongType() and value is None:
                value = 0
            elif field.dataType == MapType(StringType(), IntegerType()) and not value:
                value = {}
            elif field.dataType == ArrayType(StringType()) and not value:
                value = []

            formatted_record.append(value)

        formatted_records.append(tuple(formatted_record))

    # Create DataFrame
    if formatted_records:
        df = spark.createDataFrame(formatted_records, schema)
    else:
        df = spark.createDataFrame([], schema)

    return df


def insert_logs_to_job_logs_table(
    spark: SparkSession,
    logs_directory: str,
    app_name: Optional[str] = None,
    target_table: str = "dataops.job_logs",
    observability_logger: Optional[HybridLogger] = None,
) -> Dict[str, Any]:
    """
    Main function to insert log files into dataops.job_logs table

    Args:
        spark: Spark session
        logs_directory: Directory containing log files
        app_name: Specific app name to process (None for all apps)
        target_table: Target Iceberg table name
        observability_logger: Logger for this operation

    Returns:
        Dictionary with operation results
    """

    if observability_logger:
        observability_logger.start_operation("log_insertion", "insert_logs_to_job_logs")

    start_time = datetime.now()

    try:
        # Step 1: Find log files
        log_files = find_log_files(logs_directory, app_name)

        if not log_files:
            result = {
                "success": False,
                "message": f"No log files found in {logs_directory}",
                "files_processed": 0,
                "records_inserted": 0,
            }

            if observability_logger:
                observability_logger.log_error(
                    Exception("No log files found"),
                    {"error_code": "E001", "error_type": "FILE_NOT_FOUND"},
                )

            return result

        # Step 2: Process each log file
        all_metrics = []
        files_processed = 0

        for log_file in log_files:
            print(f"📄 Processing log file: {log_file}")

            # Extract app name from file path if not provided
            if not app_name:
                file_app_name = Path(log_file).parent.name
            else:
                file_app_name = app_name

            # Extract metrics from log file
            metrics = extract_metrics_from_log_file(log_file)

            if metrics:
                # Transform to job_logs schema
                job_logs_records = transform_metrics_to_job_logs_schema(
                    metrics, file_app_name
                )
                all_metrics.extend(job_logs_records)
                files_processed += 1

                print(
                    f"   ✅ Extracted {len(metrics)} metrics, transformed to {len(job_logs_records)} job log records"
                )
            else:
                print(f"   ⚠️  No metrics found in {log_file}")

        # Step 3: Create DataFrame and insert
        if all_metrics:
            df = create_job_logs_dataframe(spark, all_metrics)

            print(f"📊 Created DataFrame with {df.count()} records")

            # Insert into table
            df.writeTo(target_table).append()

            records_inserted = len(all_metrics)

            print(
                f"✅ Successfully inserted {records_inserted} records into {target_table}"
            )

            if observability_logger:
                observability_logger.log_performance(
                    "log_insertion_complete",
                    {
                        "files_processed": files_processed,
                        "records_inserted": records_inserted,
                        "target_table": target_table,
                        "status": "success",
                    },
                )

            result = {
                "success": True,
                "message": f"Successfully processed {files_processed} files",
                "files_processed": files_processed,
                "records_inserted": records_inserted,
            }

        else:
            result = {
                "success": False,
                "message": "No valid metrics found in any log files",
                "files_processed": files_processed,
                "records_inserted": 0,
            }

            if observability_logger:
                observability_logger.log_error(
                    Exception("No valid metrics found"),
                    {"error_code": "E003", "error_type": "NO_VALID_DATA"},
                )

        return result

    except Exception as e:
        error_message = f"Error inserting logs to job_logs table: {e}"
        print(f"❌ {error_message}")

        if observability_logger:
            observability_logger.log_error(
                e,
                {
                    "error_code": "E999",
                    "error_type": "LOG_INSERTION_ERROR",
                    "target_table": target_table,
                },
            )

        return {
            "success": False,
            "message": error_message,
            "files_processed": 0,
            "records_inserted": 0,
        }

    finally:
        if observability_logger:
            execution_time = (datetime.now() - start_time).total_seconds()
            observability_logger.end_operation(
                execution_time=execution_time,
                result_count=result.get("records_inserted", 0),
            )


def main():
    """CLI interface for log insertion"""
    import argparse

    parser = argparse.ArgumentParser(
        description="Insert HybridLogger log files into dataops.job_logs table"
    )
    parser.add_argument(
        "--logs-dir",
        default="./spark-logs",
        help="Directory containing log files (default: ./spark-logs)",
    )
    parser.add_argument(
        "--app-name",
        help="Specific app name to process (processes all apps if not specified)",
    )
    parser.add_argument(
        "--table",
        default="dataops.job_logs",
        help="Target table name (default: dataops.job_logs)",
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Parse logs but don't insert into table"
    )

    args = parser.parse_args()

    # Create Spark session
    spark = create_spark_session(
        spark_version=SparkVersion.SPARK_CONNECT_3_5,
        app_name="log_inserter",
        catalog="iceberg",
    )

    try:
        if args.dry_run:
            print("🔍 DRY RUN MODE - No data will be inserted")

            # Just find and parse log files
            log_files = find_log_files(args.logs_dir, args.app_name)

            total_metrics = 0
            for log_file in log_files:
                metrics = extract_metrics_from_log_file(log_file)
                print(f"📄 {log_file}: {len(metrics)} metrics found")
                total_metrics += len(metrics)

            print(
                f"📊 Total: {total_metrics} metrics found across {len(log_files)} files"
            )
        else:
            # Actual insertion
            result = insert_logs_to_job_logs_table(
                spark=spark,
                logs_directory=args.logs_dir,
                app_name=args.app_name,
                target_table=args.table,
            )

            print(f"📋 Operation result: {result}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
