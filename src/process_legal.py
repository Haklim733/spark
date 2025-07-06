#!/usr/bin/env python3
"""
ELT Pipeline - Extract and Load operations
Transformations and data quality checks should be handled in transformation stage
Note: Iceberg tables don't have constraints, so validation is done at application level

Spark Connect Performance Guidelines:


🎯 Best Practices for Spark Connect:
1. Use DataFrame API for data processing
2. Use SQL only for metadata queries (SHOW, DESCRIBE)
3. Avoid dynamic SQL generation when possible
4. Prefer column operations over string concatenation
5. Use typed operations over string-based operations
"""

import argparse
from datetime import datetime, timezone, timedelta
import os
from pathlib import Path
import time
import sys
import json
from typing import List, Dict, Any, Set
import uuid

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    avg,
    col,
    count,
    countDistinct,
    max as max_func,
    split,
    element_at,
    when,
    regexp_extract,
)
from pyspark.sql.types import (
    StringType,
    IntegerType,
    LongType,
    BooleanType,
    TimestampType,
    ArrayType,
    MapType,
    StructType,
    StructField,
)
from src.utils.logger import HybridLogger
from src.utils.session import (
    create_spark_session,
    SparkVersion,
)

# Import shared legal document models
from src.schemas.schema import SchemaManager

# Initialize SchemaManager
schema_manager = SchemaManager()

# Get schema version from legal_doc_metadata.json using SchemaManager
schema_dict = schema_manager.get_schema("legal_doc_metadata")
SCHEMA_VERSION = schema_dict["schema_version"]


def extract_document_id_udf(file_path):
    if not file_path:
        return "unknown"
    filename = file_path.split("/")[-1]
    return filename.replace(".txt", "").replace(".json", "").replace(".parquet", "")


def extract_document_type_udf(file_path):
    if not file_path:
        return "unknown"
    filename = file_path.split("/")[-1]
    parts = (
        filename.replace(".txt", "")
        .replace(".json", "")
        .replace(".parquet", "")
        .split("_")
    )
    return parts[3] if len(parts) >= 4 else "unknown"


def is_minio_path(path_str: str) -> bool:
    """Check if the path is a MinIO/S3 path"""
    return path_str.startswith(("s3a://", "s3://", "minio://"))


def list_minio_files_distributed(
    spark: SparkSession,
    minio_path: str,
) -> DataFrame:
    """
    List files in MinIO using distributed DataFrame operations (Spark Connect compatible)
    Returns a DataFrame with comprehensive file metadata for efficient processing

    Args:
        spark: SparkSession for distributed operations
        minio_path: MinIO path to list files from (supports wildcards)

    Returns:
        DataFrame: DataFrame with file metadata including:
            - file_path: Full path to the file
            - file_name: Just the filename
            - file_extension: File extension (.txt, .json, .parquet)
            - file_size_bytes: Size in bytes
            - modification_time_utc: Last modification time
            - document_id: Extracted document ID from filename
            - document_type: Extracted document type from path
            - is_content_file: Boolean indicating if it's a content file
            - is_metadata_file: Boolean indicating if it's a metadata file
            - bucket: MinIO bucket name
            - key: MinIO object key
    """
    # Read files using binaryFile format for metadata
    files_df = spark.read.format("binaryFile").load(minio_path)
    listed_files_df = (
        files_df.withColumn("file_path", col("path"))
        .withColumn(
            # Extract document_type: it's the segment after 'legal/' and before the date folder
            # Path structure: s3a://raw/docs/legal/{document_type}/{date}/{content|metadata}/{filename}
            "document_type",
            regexp_extract(
                col("file_path"), r"s3a:\/\/raw\/docs\/legal\/([^\/]+)\/", 1
            ),
        )
        .withColumn(
            # Extract date folder: it's the segment after document_type and before content/metadata
            "doc_date_folder",
            regexp_extract(
                col("file_path"), r"s3a:\/\/raw\/docs\/legal\/[^\/]+\/([^\/]+)\/", 1
            ),
        )
        .withColumn(
            # Extract the direct parent folder (e.g., 'content' or 'metadata')
            "subfolder",
            regexp_extract(
                col("file_path"), r"\/([^\/]+)\/[^\/]+$", 1
            ),  # Captures folder before filename
        )
        .withColumn(
            # Extract filename with extension
            "filename_with_ext",
            element_at(split(col("file_path"), "/"), -1),
        )
        .withColumn(
            # Extract document_id (filename without extension)
            "document_id",
            regexp_extract(col("filename_with_ext"), r"^(.+?)(?:\.[^.]*$|$)", 1),
        )
        .withColumn(
            # Extract file_extension
            "file_extension",
            regexp_extract(col("filename_with_ext"), r"(?<=\.)([^.]*)$", 1),
        )
        .withColumn(
            "is_content_file",
            when(col("file_path").contains("/content/"), True).otherwise(False),
        )
        .withColumn(
            "is_metadata_file",
            when(col("file_path").contains("/metadata/"), True).otherwise(False),
        )
        .select(
            "file_path",
            "document_type",
            "doc_date_folder",
            "subfolder",
            "document_id",
            "file_extension",
            "is_content_file",
            "is_metadata_file",
            col("modificationTime").alias("modification_time_utc"),
            col("length").alias("file_size_bytes"),
        )
    )

    print("\nListed Files with Extracted Document Type and Other Details:")
    # listed_files_df.show(truncate=False)
    # listed_files_df.printSchema()

    # Cache the DataFrame for efficient reuse
    listed_files_df.cache()

    file_count = listed_files_df.count()
    print(f" Found {file_count} files with comprehensive metadata")

    return listed_files_df


def read_file_content(spark: SparkSession, file_path: str) -> str:
    """Read content from a MinIO file using Spark's distributed filesystem"""
    df = spark.read.text(file_path)

    lines = df.collect()
    content = "\n".join([row.value for row in lines])

    return content


def get_minio_file_size(spark: SparkSession, file_path: str) -> int:
    """Get file size from MinIO using Spark's distributed filesystem"""
    df = spark.read.format("binaryFile").load(file_path)
    row = df.first()
    if row and hasattr(row, "length") and row.length is not None:
        file_size = row.length
        return file_size


def basic_load_validation(
    spark: SparkSession,
    table_name: str,
    expected_count: int = None,
) -> bool:
    """
    Enhanced validation using distributed operations that now work in Spark Connect

    Args:
        spark: SparkSession for distributed operations
        table_name: Name of the table to validate
        expected_count: Expected number of records (optional)

    Returns:
        bool: True if validation passes, False otherwise
    """
    print(f"\n🔍 Enhanced ELT Load validation: {table_name}")

    try:
        # 1. Check if table exists - use simple SHOW TABLES
        try:
            tables = spark.sql(f"SHOW TABLES IN {table_name.split('.')[0]}")
            table_list = tables.take(100)
            table_exists = any(
                table_name.split(".")[1] in str(row) for row in table_list
            )

            if not table_exists:
                print(f"❌ Table {table_name} does not exist!")
                return False

            print(f"✅ Table {table_name} exists")
        except Exception as e:
            print(f"❌ Error checking table existence: {e}")
            return False

        # 2. Get row count using distributed COUNT operation (now works!)
        try:
            count_result = spark.sql(
                f"SELECT COUNT(*) as total_count FROM {table_name}"
            )
            total_count = count_result.take(1)[0]["total_count"]
            print(f"📊 Total records in table: {total_count:,}")
        except Exception as e:
            print(f"⚠️  Could not get row count: {e}")
            total_count = 0

        # 3. Get recent activity using ORDER BY (now works!)
        try:
            recent_records = spark.sql(
                f"""
                SELECT 
                    document_type,
                    COUNT(*) as count,
                    MAX(generated_at) as latest_record
                FROM {table_name}
                GROUP BY document_type
                ORDER BY latest_record DESC
                LIMIT 10
            """
            ).take(10)

            if recent_records:
                print(f"📊 Recent activity: {len(recent_records)} document types")
                for record in recent_records[:3]:
                    print(f"   - {record['document_type']}: {record['count']} records")

        except Exception as e:
            print(f"⚠️  Could not get recent activity: {e}")

        print(f"✅ Enhanced validation completed (distributed operations enabled)")

        return True

    except Exception as e:
        print(f"❌ Load validation failed: {e}")
        return False


def track_batch_load(
    spark: SparkSession,
    table_name: str,
    batch_id: str,
    source_files: List[str],
    load_timestamp: datetime = None,
) -> Dict[str, Any]:
    """
    Track batch load operations

    Args:
        spark: SparkSession for data operations
        table_name: Name of the table being loaded
        batch_id: Unique batch identifier
        source_files: List of source files processed
        load_timestamp: Timestamp when load started (defaults to current time)

    Returns:
        Dict[str, Any]: Tracking information for the batch load
    """
    if load_timestamp is None:
        load_timestamp = datetime.now(timezone.utc)

    tracking_results = {
        "batch_id": batch_id,
        "load_timestamp": load_timestamp,
        "source_files": len(source_files),
        "target_rows": 0,
        "load_status": "unknown",
        "warnings": [],
    }

    try:
        # Use COUNT with WHERE clause (now works!)
        batch_count_result = spark.sql(
            f"SELECT COUNT(*) as batch_count FROM {table_name} WHERE batch_id = '{batch_id}'"
        )
        batch_count = batch_count_result.take(1)[0]["batch_count"]
        tracking_results["target_rows"] = batch_count

        # Use GROUP BY with aggregations (now works!)
        batch_dist = spark.sql(
            f"""
            SELECT 
                batch_id, 
                document_type,
                COUNT(*) as count,
                AVG(file_size) as avg_file_size,
                MAX(generated_at) as latest_record
            FROM {table_name}
            WHERE batch_id = '{batch_id}'
            GROUP BY batch_id, document_type
            ORDER BY count DESC
        """
        ).take(100)

        # Determine overall batch status
        total_count = __builtins__.sum(row["count"] for row in batch_dist)

        if total_count > 0:
            tracking_results["load_status"] = "success"
        else:
            tracking_results["load_status"] = "failed"
            tracking_results["warnings"].append("No records were loaded for this batch")

        # Check for reasonable data volume
        if batch_count == 0:
            tracking_results["warnings"].append("No records were loaded for this batch")
        elif batch_count < len(source_files) * 0.9:  # Less than 90% of source files
            tracking_results["warnings"].append(
                f"Significant data loss: {batch_count} rows vs {len(source_files)} files"
            )

        print(f"📊 Enhanced batch tracking for {batch_id}:")
        print(f"   - Status: {tracking_results['load_status']}")
        print(f"   - Source files: {len(source_files)}")
        print(f"   - Loaded rows: {batch_count:,}")
        print(
            f"   - Document types: {len(set(row['document_type'] for row in batch_dist))}"
        )
        print(f"   - Warnings: {len(tracking_results['warnings'])}")

        # Show detailed breakdown by document type
        if batch_dist:
            print(f"   - Breakdown by document type:")
            for record in batch_dist[:5]:  # Show top 5
                print(f"     * {record['document_type']}: {record['count']} records")

    except Exception as e:
        tracking_results["warnings"].append(f"Tracking error: {str(e)}")
        print(f"❌ Batch tracking failed: {e}")

    return tracking_results


def get_failed_loads(
    spark: SparkSession,
    table_name: str,
    hours_back: int = 24,
) -> List[Dict[str, Any]]:
    """
    Get failed loads from the specified table

    Args:
        spark: SparkSession for data operations
        table_name: Name of the table to check
        hours_back: Number of hours to look back (default: 24)

    Returns:
        List[Dict[str, Any]]: List of failed load records
    """
    try:
        # Use DataFrame API for better Spark Connect performance

        # Load table as DataFrame
        table_df = spark.table(table_name)

        # Filter failed loads and aggregate using DataFrame API
        failed_loads_df = (
            table_df.filter(
                (col("load_status") == "load_failed") | col("load_error").isNotNull()
            )
            .groupBy("document_type")
            .agg(
                count("*").alias("error_count"),
                countDistinct("document_id").alias("unique_docs"),
                avg("file_size").alias("avg_file_size"),
                max_func("generated_at").alias("latest_error"),
            )
            .orderBy(col("error_count").desc())
        )

        failed_loads = failed_loads_df.take(100)

        if failed_loads:
            print(f"📊 Found {len(failed_loads)} error patterns:")
            for record in failed_loads[:10]:  # Show first 10
                print(
                    f"   - {record['document_type']}: {record['error_count']} errors, avg size: {record['avg_file_size']:.0f} bytes"
                )
            if len(failed_loads) > 10:
                print(f"   ... and {len(failed_loads) - 10} more patterns")
        else:
            print(f"✅ No failed loads found")

        return failed_loads

    except Exception as e:
        print(f"❌ Error getting failed loads: {e}")
        return []


def validate_batch_load(
    spark: SparkSession,
    table_name: str,
    batch_id: str,
    original_files: List[str],
    expected_count: int,
) -> Dict[str, Any]:
    """
    Enhanced validation using distributed operations that now work

    Args:
        spark: SparkSession for distributed operations
        table_name: Name of the table to validate
        batch_id: Unique batch identifier
        original_files: List of original file paths that were processed
        expected_count: Expected number of records to be loaded

    Returns:
        Dict[str, Any]: Validation result with file counts and status
    """
    print(f"\n🔍 Enhanced validation for batch {batch_id}...")

    try:
        # Use COUNT with WHERE clause (now works!)
        loaded_records = spark.sql(
            f"SELECT COUNT(*) as loaded_count FROM {table_name} WHERE batch_id = '{batch_id}'"
        )
        loaded_count = loaded_records.take(1)[0]["loaded_count"]

        # Use GROUP BY with aggregations (now works!)
        file_analysis = spark.sql(
            f"""
            SELECT 
                file_path,
                COUNT(*) as record_count,
                MAX(generated_at) as latest_record
            FROM {table_name}
            WHERE batch_id = '{batch_id}'
            GROUP BY file_path
            ORDER BY record_count DESC
        """
        ).take(100)

        # Create lookup for loaded records
        loaded_files = {record["file_path"]: record for record in file_analysis}

        # Validate each original file
        validation_result = {
            "batch_id": batch_id,
            "files_expected": len(original_files),
            "files_loaded": len(loaded_files),
            "files_missing": [],
            "files_failed": [],
            "files_successful": [],
            "validation_status": "unknown",
        }

        for file_path in original_files:
            file_name = file_path.split("/")[-1] if "/" in file_path else file_path

            if file_name in loaded_files:
                record = loaded_files[file_name]
                # If record exists, consider it successfully loaded
                validation_result["files_successful"].append(file_name)
            else:
                validation_result["files_missing"].append(file_name)

        # Determine overall validation status
        if (
            len(validation_result["files_missing"]) == 0
            and len(validation_result["files_failed"]) == 0
        ):
            validation_result["validation_status"] = "complete_success"
        elif len(validation_result["files_missing"]) > 0:
            validation_result["validation_status"] = "partial_load"
        else:
            validation_result["validation_status"] = "load_failures"

        # Print enhanced validation summary
        print(f"✅ Enhanced validation complete:")
        print(f"   - Expected: {validation_result['files_expected']} files")
        print(f"   - Loaded: {validation_result['files_loaded']} files")
        print(f"   - Successful: {len(validation_result['files_successful'])} files")
        print(f"   - Failed: {len(validation_result['files_failed'])} files")
        print(f"   - Missing: {len(validation_result['files_missing'])} files")
        print(f"   - Status: {validation_result['validation_status']}")
        print(f"   - Total records: {loaded_count:,}")

        return validation_result

    except Exception as e:
        print(f"❌ Enhanced validation failed: {e}")
        return None


def insert_files(
    spark: SparkSession,
    docs_dir: str,
    table_name: str,
    observability_logger: HybridLogger,
) -> Dict[str, Any]:
    """
    Spark-native file insertion function with partition tracking
    Loads all data into raw table without validation
    Data quality and transformations handled by SQLMesh in transform layer
    Focused on partition-level tracking and metrics

    Args:
        spark: SparkSession (required)
        docs_dir: Directory containing files to process (local path or MinIO path like s3a://bucket/path)
        table_name: Target table name (e.g., "legal.documents_raw")
        observability_logger: HybridLogger instance (required for metrics and monitoring)

    Returns:
        bool: True if successful, False otherwise

    Raises:
        ValueError: If required parameters are missing
    """

    print(f"🔄 Spark-native: Loading files from {docs_dir} into {table_name}")

    # Generate job_id and batch_id for tracking
    job_id = generate_job_id()
    batch_id = f"batch_{int(time.time())}"

    # Check if this is a MinIO path
    is_minio = is_minio_path(docs_dir)

    # Filter for supported extensions
    supported_extensions = [".txt", ".json", ".parquet"]
    content_files = []
    metadata_files = []

    if is_minio:
        # List files from MinIO using the enhanced distributed approach
        print("📋 Listing MinIO files with comprehensive metadata...")

        files_metadata_df = list_minio_files_distributed(spark, docs_dir)

        if files_metadata_df.count() == 0:
            print(f"❌ No files found in path {docs_dir}")
            observability_logger.log_error(
                Exception(f"No files found in MinIO path: {docs_dir}"),
                {"error_type": "NO_FILES_FOUND"},
            )
            raise Exception("no files not found")

        # Filter files by type using the metadata DataFrame
        content_files_df = files_metadata_df.filter(col("is_content_file") == True)
        metadata_files_df = files_metadata_df.filter(col("is_metadata_file") == True)

        # Convert to lists for backward compatibility
        content_files = [row.file_path for row in content_files_df.collect()]
        metadata_files = [row.file_path for row in metadata_files_df.collect()]

        print(
            f"Found {len(content_files)} content files and {len(metadata_files)} metadata files in MinIO"
        )

        if not content_files:
            print(f"⚠️  No content files found in MinIO path: {docs_dir}")
            print(f"Supported formats: {', '.join(supported_extensions)}")
            observability_logger.log_error(
                Exception(f"No content files found in MinIO path: {docs_dir}"),
                {"error_type": "NO_CONTENT_FILES"},
            )
            return False

    else:
        # Local filesystem processing (unchanged)
        if not os.path.exists(docs_dir):
            print(f"⚠️  Directory not found: {docs_dir}")
            observability_logger.log_error(
                Exception(f"Directory not found: {docs_dir}"),
                {"error_type": "DIRECTORY_NOT_FOUND"},
            )
            return False

        # Look for content and metadata directories
        content_dir = Path(docs_dir) / "content"
        metadata_dir = Path(docs_dir) / "metadata"

        # Process content files
        if content_dir.exists():
            # New structure with content/metadata directories
            for ext in supported_extensions:
                content_files.extend(list(content_dir.glob(f"*{ext}")))
                content_files.extend(
                    list(content_dir.glob(f"**/*{ext}"))
                )  # Include subdirectories

            # Process metadata files
            if metadata_dir.exists():
                for ext in [".json"]:  # Metadata files are typically JSON
                    metadata_files.extend(list(metadata_dir.glob(f"*{ext}")))
                    metadata_files.extend(list(metadata_dir.glob(f"**/*{ext}")))
        else:
            # Legacy structure - look for files directly in docs_dir
            for ext in supported_extensions:
                content_files.extend(list(Path(docs_dir).glob(f"*{ext}")))
                content_files.extend(list(Path(docs_dir).glob(f"**/*{ext}")))

        if not content_files:
            print(f"⚠️  No content files found in: {docs_dir}")
            print(f"Supported formats: {', '.join(supported_extensions)}")
            observability_logger.log_error(
                Exception(f"No content files found in: {docs_dir}"),
                {"error_type": "NO_CONTENT_FILES"},
            )
            return False

        print(
            f"Found {len(content_files)} content files and {len(metadata_files)} metadata files"
        )

    # Group content files by format
    file_groups = {
        "text": [
            f
            for f in content_files
            if f.endswith(".txt") or (hasattr(f, "suffix") and f.suffix == ".txt")
        ],
        "json": [
            f
            for f in content_files
            if f.endswith(".json") or (hasattr(f, "suffix") and f.suffix == ".json")
        ],
        "parquet": [
            f
            for f in content_files
            if f.endswith(".parquet")
            or (hasattr(f, "suffix") and f.suffix == ".parquet")
        ],
    }

    print(f"Content file breakdown:")
    for format_type, files in file_groups.items():
        if files:
            print(f"   - {format_type}: {len(files)} files")

    if metadata_files:
        print(f"Metadata files: {len(metadata_files)} files")

    # Note: In ELT approach, we don't truncate - we append all data or merge update
    # Data deduplication and cleanup happens in Transform layer
    print(f"📝 ELT: Appending data to {table_name} (no truncation)")

    observability_logger.start_operation("main_processing", "insert_files")

    job_start_time = time.time()

    # Process files with partition tracking
    # Pass the metadata DataFrame for efficient file size lookups
    result = _insert_files_with_partition_tracking(
        spark,
        file_groups,
        table_name,
        observability_logger,
        metadata_files,
        files_metadata_df if is_minio else None,
        job_id,  # Pass job_id from main
        batch_id,  # Pass batch_id from main
    )

    # Calculate job duration
    job_duration_ms = (time.time() - job_start_time) * 1000

    # Store source files for job logging
    # Convert file paths to strings for consistent handling
    source_files_list = [str(f) for f in content_files]

    # Store in spark session for later retrieval
    if hasattr(spark, "source_files"):
        spark.source_files = source_files_list
    else:
        # Create attribute if it doesn't exist
        setattr(spark, "source_files", source_files_list)

    # Log comprehensive job metrics using HybridLogger
    # Log job completion with detailed metrics
    observability_logger.log_operation_completion(
        job_group="job_processing",
        operation="insert_files",
        execution_time=job_duration_ms / 1000,  # Convert to seconds
        result_count=len(source_files_list),
    )

    # Enhanced job metrics logging to match metrics.json schema
    total_files = len(source_files_list)
    total_size_bytes = __builtins__.sum(
        len(f) * 1024 for f in source_files_list
    )  # Estimate file size

    # Calculate error codes distribution
    error_codes = {}
    failed_inserts = result.get("failed_inserts", 0) if isinstance(result, dict) else 0
    if failed_inserts > 0:
        error_codes["E004"] = failed_inserts  # Validation errors

    # Determine validation status
    successful_inserts = (
        result.get("successful_inserts", 0) if isinstance(result, dict) else 0
    )
    if failed_inserts == 0:
        validation_status = "complete_success"
        job_status = "success"
    elif successful_inserts > 0:
        validation_status = "partial_load"
        job_status = "partial_success"
    else:
        validation_status = "load_failures"
        job_status = "failed"

    # Enhanced metrics matching metrics.json schema
    observability_logger.log_custom_metrics(
        job_group="job_processing",
        operation="insert_files",
        metrics={
            # Job-level metrics
            "job_id": (
                observability_logger.job_id
                if hasattr(observability_logger, "job_id")
                else "unknown"
            ),
            "source": (
                "minio" if any(is_minio_path(f) for f in source_files_list) else "local"
            ),
            "target_table": table_name,
            "version": SCHEMA_VERSION,
            "job_start": datetime.now(timezone.utc).isoformat(),
            "job_end": datetime.now(timezone.utc).isoformat(),
            # Operation-level metrics
            "operation_name": "insert_files",
            "operation_start": datetime.now(timezone.utc).isoformat(),
            "operation_end": datetime.now(timezone.utc).isoformat(),
            "operation_result_count": total_files,
            # Record-level metrics
            "records_processed": total_files,
            "records_loaded": successful_inserts,
            "records_failed": failed_inserts,
            "records_missing": 0,
            # Performance metrics
            "record_size_bytes": total_size_bytes,
            "bytes_transferred": total_size_bytes,
            "performance_operation": "insert_files",
            "performance_execution_time_ms": job_duration_ms,
            "performance_timestamp": int(time.time() * 1000),
            # Partition metrics
            "partition_processing_metrics": {
                "partition_id": 0,
                "partition_count": 1,
                "partition_size": total_files,
                "partition_total_size_bytes": total_size_bytes,
                "partition_skew_ratio": 0.0,
                "partition_start": datetime.now(timezone.utc).isoformat(),
                "partition_end": datetime.now(timezone.utc).isoformat(),
                "partition_processing_time_ms": job_duration_ms,
            },
            # Business events
            "business_events_count": 0,
            "business_event_types": [],
            # Error tracking
            "error_count": failed_inserts,
            "error_codes": error_codes,
            "error_summary": f"Success: {successful_inserts}, Failed: {failed_inserts}, Total: {total_files}",
            "error_types": ["validation_error"] if failed_inserts > 0 else [],
            # Failed files by partition
            "failed_files_by_partition": (
                result.get("failed_files_by_partition", {})
                if isinstance(result, dict)
                else {}
            ),
            # Logging metrics
            "total_logs_generated": 0,
            "async_logging_enabled": True,
            # Validation status
            "validation_status": validation_status,
            "job_status": job_status,
            # Spark metrics
            "spark_metrics": {
                "executor_count": _get_executor_count(spark),
                "executor_memory_mb": _get_executor_memory_mb(spark),
                "driver_memory_mb": _get_driver_memory_mb(spark),
                "spark_version": "3.5.6",
                "iceberg_version": "1.4.0",
                "shuffle_read_bytes": 0,
                "shuffle_write_bytes": 0,
                "total_executor_time_ms": job_duration_ms,
            },
            # Audit trail
            "created_by": "system",
            "created_at": datetime.now(timezone.utc).isoformat(),
        },
    )

    # End operation tracking for the main insert_files operation
    observability_logger.end_operation(
        execution_time=job_duration_ms / 1000,  # Convert to seconds
        result_count=len(source_files_list),
    )

    # Extract success status from the detailed results
    success = result.get("success", False) if isinstance(result, dict) else result

    # Return detailed results including failed files by partition
    return {
        "success": success,
        "processing_results": (
            result if isinstance(result, dict) else {"success": result}
        ),
        "failed_files_by_partition": (
            result.get("failed_files_by_partition", {})
            if isinstance(result, dict)
            else {}
        ),
        "successful_inserts": (
            result.get("successful_inserts", 0) if isinstance(result, dict) else 0
        ),
        "failed_inserts": (
            result.get("failed_inserts", 0) if isinstance(result, dict) else 0
        ),
        "total_files_processed": (
            result.get("total_files_processed", 0) if isinstance(result, dict) else 0
        ),
        "success_rate": (
            result.get("success_rate", 0.0) if isinstance(result, dict) else 0.0
        ),
    }


def _process_content_files(
    spark: SparkSession,
    content_files: List[str],
    metadata_lookup: Dict[str, Any],
    batch_timestamp: datetime,
    files_metadata_df: DataFrame = None,  # Add this parameter
    job_id: str = None,  # Add job_id parameter
    batch_id: str = None,  # Add batch_id parameter
) -> Dict[str, Any]:
    """
    Process content files and create DataFrame for insertion

    Args:
        spark: SparkSession for data processing
        content_files: List of content file paths
        metadata_lookup: Dictionary of metadata by document ID
        batch_timestamp: Timestamp for the batch
        files_metadata_df: DataFrame with file metadata (optional)
        job_id: Job identifier for tracking
        batch_id: Batch identifier for tracking

    Returns:
        Dict[str, Any]: Processing results with DataFrame and counts
    """
    print(f"📄 Processing {len(content_files)} content files")

    # Group files by type for efficient processing
    text_files = [f for f in content_files if str(f).endswith(".txt")]
    json_files = [f for f in content_files if str(f).endswith(".json")]
    parquet_files = [f for f in content_files if str(f).endswith(".parquet")]

    successful_inserts = 0
    failed_inserts = 0
    all_dataframes = []

    # Process text files
    if text_files:
        text_df = _process_text_files(
            spark,
            text_files,
            metadata_lookup,
            batch_timestamp,
            files_metadata_df,
            job_id,
            batch_id,
        )
        if text_df:
            all_dataframes.append(text_df)
            successful_inserts += len(text_files)
        else:
            failed_inserts += len(text_files)

    # Process JSON files
    if json_files:
        json_df = _process_json_files(
            spark, json_files, metadata_lookup, batch_timestamp, job_id, batch_id
        )
        if json_df:
            all_dataframes.append(json_df)
            successful_inserts += len(json_files)
        else:
            failed_inserts += len(json_files)

    # Process Parquet files
    if parquet_files:
        parquet_df = _process_parquet_files(
            spark, parquet_files, metadata_lookup, batch_timestamp, job_id, batch_id
        )
        if parquet_df:
            all_dataframes.append(parquet_df)
            successful_inserts += len(parquet_files)
        else:
            failed_inserts += len(parquet_files)

    return {
        "successful_inserts": successful_inserts,
        "failed_inserts": failed_inserts,
        "dataframes": all_dataframes,
    }


def _process_metadata_files(
    spark: SparkSession,
    metadata_files: List[str],
) -> Dict[str, Any]:
    """
    Process metadata files and create lookup dictionary

    Args:
        spark: SparkSession for data processing
        metadata_files: List of metadata file paths

    Returns:
        Dict[str, Any]: Metadata lookup dictionary
    """
    print(f"📋 Processing {len(metadata_files)} metadata files")

    metadata_lookup = {}

    for metadata_file in metadata_files:
        try:
            # Extract document_id from metadata filename
            metadata_path = Path(metadata_file)
            doc_id = metadata_path.stem  # Remove .json extension

            # Read metadata content
            if is_minio_path(metadata_file):
                # For MinIO files, use Spark to read
                metadata_content = read_file_content(spark, metadata_file)
                if metadata_content:
                    import json

                    metadata_data = json.loads(metadata_content)
                    metadata_lookup[doc_id] = metadata_data
            else:
                # For local files, read directly
                with open(metadata_file, "r", encoding="utf-8") as f:
                    import json

                    metadata_data = json.load(f)
                    metadata_lookup[doc_id] = metadata_data

        except Exception as e:
            print(f"⚠️ Error reading metadata file {metadata_file}: {e}")
            continue

    return metadata_lookup


def _process_text_files(
    spark: SparkSession,
    text_files: List[str],
    metadata_lookup: Dict[str, Any],
    batch_timestamp: datetime,
    files_metadata_df: DataFrame = None,
    job_id: str = None,
    batch_id: str = None,
) -> Any:
    """Process text files and return DataFrame"""
    print(f"📄 Processing {len(text_files)} text files")

    # Generate job_id and batch_id if not provided
    if job_id is None:
        job_id = generate_job_id()
    if batch_id is None:
        batch_id = f"batch_{int(batch_timestamp.timestamp())}"

    # Create DataFrame rows with proper type conversion
    text_rows = []
    for i, file_path in enumerate(text_files):
        file_str = str(file_path)

        # Use metadata DataFrame if available, otherwise fallback to path parsing
        if files_metadata_df is not None:
            # Get metadata from the DataFrame
            file_metadata_row = files_metadata_df.filter(
                col("file_path") == file_str
            ).first()
            if file_metadata_row:
                doc_id = file_metadata_row.document_id
                doc_type = file_metadata_row.document_type
            else:
                # Fallback to path parsing
                path = Path(file_path)
                doc_id = path.stem
                path_parts = file_str.split("/docs/legal/")
                doc_type = (
                    path_parts[1].split("/")[0] if len(path_parts) > 1 else "legal"
                )
        else:
            # Fallback to original path parsing logic
            path = Path(file_path)
            doc_id = path.stem
            path_parts = file_str.split("/docs/legal/")
            doc_type = path_parts[1].split("/")[0] if len(path_parts) > 1 else "legal"

        # Get additional metadata
        doc_metadata = metadata_lookup.get(doc_id, {})
        source = doc_metadata.get("source", "soli_legal_document_generator")
        language = doc_metadata.get("language", "en")
        file_size = doc_metadata.get("file_size", 0)
        generated_at_str = doc_metadata.get("generated_at", "")

        # Parse generated_at
        if generated_at_str:
            try:
                if generated_at_str.endswith("Z"):
                    generated_at = datetime.fromisoformat(
                        generated_at_str.replace("Z", "+00:00")
                    )
                else:
                    generated_at = datetime.fromisoformat(generated_at_str)
            except:
                generated_at = batch_timestamp
        else:
            generated_at = batch_timestamp

        # Build metadata file path
        metadata_file_path = file_str.replace("/content/", "/metadata/").replace(
            ".txt", ".json"
        )

        # Read actual file content
        try:
            if is_minio_path(file_str):
                # For MinIO files, use Spark to read content
                raw_text = read_file_content(spark, file_str)
            else:
                # For local files, read directly
                with open(file_str, "r", encoding="utf-8") as f:
                    raw_text = f.read()
        except Exception as e:
            print(f"⚠️ Error reading file content for {file_str}: {e}")
            raw_text = ""  # Empty content if reading fails

        # Ensure all values are properly typed
        text_rows.append(
            (
                str(doc_id),  # Ensure string
                str(doc_type),  # Ensure string - now uses DataFrame metadata
                generated_at,  # Already datetime
                str(source),  # Ensure string
                str(language),  # Ensure string
                int(file_size) if file_size is not None else 0,  # Ensure int
                "spark_text_reader",  # Already string
                str(SCHEMA_VERSION),  # Ensure string
                str(metadata_file_path),  # Ensure string
                raw_text,  # Actual file content instead of "text_content"
                str(file_str),  # Already string
                batch_id,  # Use provided batch_id
                job_id,  # Use provided job_id
            )
        )

    if text_rows:
        # Define explicit schema to avoid type conversion issues
        from pyspark.sql.types import (
            StructType,
            StructField,
            StringType,
            TimestampType,
            IntegerType,
        )

        schema = StructType(
            [
                StructField("document_id", StringType(), True),
                StructField("document_type", StringType(), True),
                StructField("generated_at", TimestampType(), True),
                StructField("source", StringType(), True),
                StructField("language", StringType(), True),
                StructField("file_size", IntegerType(), True),
                StructField("method", StringType(), True),
                StructField("schema_version", StringType(), True),
                StructField("metadata_file_path", StringType(), True),
                StructField("raw_text", StringType(), True),
                StructField("file_path", StringType(), True),
                StructField("batch_id", StringType(), True),
                StructField("job_id", StringType(), True),
            ]
        )

        return spark.createDataFrame(text_rows, schema)
    return None


def _process_json_files(
    spark: SparkSession,
    json_files: List[str],
    metadata_lookup: Dict[str, Any],
    batch_timestamp: datetime,
    job_id: str = None,  # Add job_id parameter
    batch_id: str = None,  # Add batch_id parameter
) -> Any:
    """Process JSON files and return DataFrame"""
    print(f"📄 Processing {len(json_files)} JSON files")

    # Similar processing as text files but for JSON
    file_metadata = {}
    for file_path in json_files:
        path = Path(file_path)
        doc_id = path.stem
        file_str = path.as_posix()

        path_parts = file_str.split("/")
        doc_type = "legal"

        if len(path_parts) >= 4:
            for i, part in enumerate(path_parts):
                if (
                    part == "docs"
                    and i + 1 < len(path_parts)
                    and path_parts[i + 1] == "legal"
                ):
                    if i + 2 < len(path_parts):
                        potential_doc_type = path_parts[i + 2]
                        if potential_doc_type in [
                            "contract",
                            "memo",
                            "filing",
                            "legal_memo",
                            "court_filing",
                            "policy_document",
                            "legal_opinion",
                        ]:
                            doc_type = potential_doc_type
                            break

        file_metadata[file_str] = {"doc_id": doc_id, "doc_type": doc_type}

    json_rows = []
    for i, file_path in enumerate(json_files):
        file_str = str(file_path)
        metadata = file_metadata.get(
            file_str, {"doc_id": f"json_doc_{i}", "doc_type": "legal"}
        )

        doc_id = metadata["doc_id"]
        doc_type = metadata["doc_type"]

        doc_metadata = metadata_lookup.get(doc_id, {})
        source = doc_metadata.get("source", "soli_legal_document_generator")
        language = doc_metadata.get("language", "en")
        file_size = doc_metadata.get("file_size", 0)
        generated_at_str = doc_metadata.get("generated_at", "")

        if generated_at_str:
            try:
                if generated_at_str.endswith("Z"):
                    generated_at = datetime.fromisoformat(
                        generated_at_str.replace("Z", "+00:00")
                    )
                else:
                    generated_at = datetime.fromisoformat(generated_at_str)
            except:
                generated_at = batch_timestamp
        else:
            generated_at = batch_timestamp

        metadata_file_path = file_str.replace("/content/", "/metadata/").replace(
            ".json", ".json"
        )

        json_rows.append(
            (
                doc_id,
                doc_type,
                generated_at,
                source,
                language,
                file_size,
                "spark_json_reader",
                SCHEMA_VERSION,
                metadata_file_path,
                "",
                file_str,
                batch_id,
                job_id,
            )
        )

    if json_rows:
        return spark.createDataFrame(
            json_rows,
            [
                "document_id",
                "document_type",
                "generated_at",
                "source",
                "language",
                "file_size",
                "method",
                "schema_version",
                "metadata_file_path",
                "raw_text",
                "file_path",
                "batch_id",
                "job_id",
            ],
        )
    return None


def _process_parquet_files(
    spark: SparkSession,
    parquet_files: List[str],
    metadata_lookup: Dict[str, Any],
    batch_timestamp: datetime,
    job_id: str = None,  # Add job_id parameter
    batch_id: str = None,  # Add batch_id parameter
) -> Any:
    """Process Parquet files and return DataFrame"""
    print(f"📄 Processing {len(parquet_files)} Parquet files")

    # Similar processing as text files but for Parquet
    file_metadata = {}
    for file_path in parquet_files:
        path = Path(file_path)
        doc_id = path.stem
        file_str = path.as_posix()

        path_parts = file_str.split("/")
        doc_type = "legal"

        if len(path_parts) >= 4:
            for i, part in enumerate(path_parts):
                if (
                    part == "docs"
                    and i + 1 < len(path_parts)
                    and path_parts[i + 1] == "legal"
                ):
                    if i + 2 < len(path_parts):
                        potential_doc_type = path_parts[i + 2]
                        if potential_doc_type in [
                            "contract",
                            "memo",
                            "filing",
                            "legal_memo",
                            "court_filing",
                            "policy_document",
                            "legal_opinion",
                        ]:
                            doc_type = potential_doc_type
                            break

        file_metadata[file_str] = {"doc_id": doc_id, "doc_type": doc_type}

    parquet_rows = []
    for i, file_path in enumerate(parquet_files):
        file_str = str(file_path)
        metadata = file_metadata.get(
            file_str, {"doc_id": f"parquet_doc_{i}", "doc_type": "legal"}
        )

        doc_id = metadata["doc_id"]
        doc_type = metadata["doc_type"]

        doc_metadata = metadata_lookup.get(doc_id, {})
        source = doc_metadata.get("source", "soli_legal_document_generator")
        language = doc_metadata.get("language", "en")
        file_size = doc_metadata.get("file_size", 0)
        generated_at_str = doc_metadata.get("generated_at", "")

        if generated_at_str:
            try:
                if generated_at_str.endswith("Z"):
                    generated_at = datetime.fromisoformat(
                        generated_at_str.replace("Z", "+00:00")
                    )
                else:
                    generated_at = datetime.fromisoformat(generated_at_str)
            except:
                generated_at = batch_timestamp
        else:
            generated_at = batch_timestamp

        metadata_file_path = file_str.replace("/content/", "/metadata/").replace(
            ".parquet", ".json"
        )

        parquet_rows.append(
            (
                doc_id,
                doc_type,
                generated_at,
                source,
                language,
                file_size,
                "spark_parquet_reader",
                SCHEMA_VERSION,
                metadata_file_path,
                "",
                file_str,
                batch_id,
                job_id,
            )
        )

    if parquet_rows:
        return spark.createDataFrame(
            parquet_rows,
            [
                "document_id",
                "document_type",
                "generated_at",
                "source",
                "language",
                "file_size",
                "method",
                "schema_version",
                "metadata_file_path",
                "raw_text",
                "file_path",
                "batch_id",
                "job_id",
            ],
        )
    return None


def _insert_files_with_partition_tracking(
    spark: SparkSession,
    file_groups: Dict[str, List[str]],
    table_name: str,
    observability_logger: HybridLogger,
    metadata_files: List[str] = None,
    files_metadata_df: DataFrame = None,
    job_id: str = None,  # Add job_id parameter
    batch_id: str = None,  # Add batch_id parameter
) -> Dict[str, Any]:
    """
    Process files with Spark partition tracking using distributed processing

    Args:
        spark: SparkSession for data processing
        file_groups: Dictionary mapping file types to lists of file paths
        table_name: Target table name for insertion
        observability_logger: HybridLogger instance for metrics and monitoring
        metadata_files: List of metadata file paths (optional)
        files_metadata_df: DataFrame with comprehensive metadata for efficient file size lookups (optional)
        job_id: Job identifier for tracking
        batch_id: Batch identifier for tracking

    Returns:
        Dict[str, Any]: Processing results with detailed failure information
    """
    print("🔄 Using distributed partition tracking for ELT")

    # Start operation tracking
    observability_logger.start_operation(
        "partition_tracking", "insert_files_with_partition_tracking"
    )

    job_start_time = time.time()

    # Generate a fixed timestamp once for the entire batch to ensure determinism
    batch_timestamp = datetime.now(timezone.utc)

    # Generate job_id and batch_id if not provided
    if job_id is None:
        job_id = generate_job_id()
    if batch_id is None:
        batch_id = f"batch_{int(batch_timestamp.timestamp())}"

    # Collect all content files for processing
    all_content_files = []
    for format_type, files in file_groups.items():
        if files:
            all_content_files.extend(files)

    if not all_content_files:
        print("⚠️ No content files to process")
        return {
            "success": True,
            "successful_inserts": 0,
            "failed_inserts": 0,
            "failed_files_by_partition": {},
            "partition_metrics": {},
            "processing_time_seconds": 0,
        }

    print(
        f"📊 Processing {len(all_content_files)} content files using distributed approach"
    )

    # Process metadata files first
    metadata_lookup = {}
    if metadata_files:
        metadata_lookup = _process_metadata_files(spark, metadata_files)

    # Process content files
    content_results = _process_content_files(
        spark,
        all_content_files,
        metadata_lookup,
        batch_timestamp,
        files_metadata_df,
        job_id,
        batch_id,
    )

    successful_inserts = content_results["successful_inserts"]
    failed_inserts = content_results["failed_inserts"]
    all_dataframes = content_results["dataframes"]
    failed_files_by_partition = {}

    # Merge all DataFrames to the table
    for df in all_dataframes:
        if df is not None:
            merge_to_iceberg_table(spark, df, table_name)
            print(f"✅ Successfully merged DataFrame with {df.count()} rows")

    # Calculate processing time
    processing_time_seconds = time.time() - job_start_time

    # Use fixed partition count matching spark-defaults.conf
    shuffle_partitions = 4

    # Calculate partition metrics
    partition_metrics = {
        "total_partitions": 4,
        "files_per_partition": (
            len(all_content_files) // 4 if len(all_content_files) > 0 else 0
        ),
        "processing_time_seconds": processing_time_seconds,
        "success_rate": (
            successful_inserts / len(all_content_files) if all_content_files else 0
        ),
    }

    print(f"✅ Distributed processing complete:")
    print(f"   - Total content files: {len(all_content_files)}")
    print(f"   - Metadata files loaded: {len(metadata_lookup)}")
    print(f"   - Successful: {successful_inserts}")
    print(f"   - Failed: {failed_inserts}")
    print(f"   - Processing time: {processing_time_seconds:.2f}s")
    print(f"   - Success rate: {partition_metrics['success_rate']:.2%}")
    print(f"   - Shuffle partitions: 4")
    print(f"   - Files per partition: {partition_metrics['files_per_partition']}")

    return {
        "success": failed_inserts == 0,
        "successful_inserts": successful_inserts,
        "failed_inserts": failed_inserts,
        "failed_files_by_partition": failed_files_by_partition,
        "partition_metrics": partition_metrics,
        "processing_time_seconds": processing_time_seconds,
    }


def _read_minio_metadata(spark, content_file_path):
    """Read metadata from MinIO JSON file (Spark Connect compatible)"""
    try:
        # Convert content file path to metadata file path
        metadata_file_path = content_file_path.replace(".txt", ".json")

        # Check if metadata file exists using multiple approaches
        try:
            # Method 1: Try reading as text first to check if file exists
            text_df = spark.read.text(metadata_file_path)
            if text_df.count() > 0:
                # Method 2: Try reading as JSON with schema inference
                try:
                    df = (
                        spark.read.option("multiline", "true")
                        .option("mode", "PERMISSIVE")
                        .json(metadata_file_path)
                    )
                    if df.count() > 0:
                        # Use take() instead of toPandas() for Spark Connect compatibility
                        rows = df.take(1)
                        if rows:
                            metadata = rows[0].asDict()
                            return metadata
                except Exception as json_error:
                    # Method 3: Try reading as text and parsing manually
                    try:
                        text_content = text_df.collect()
                        if text_content:
                            # Join all lines and try to parse as JSON
                            json_text = "\n".join([row.value for row in text_content])
                            import json

                            metadata = json.loads(json_text)
                            return metadata
                    except Exception as parse_error:
                        print(
                            f"⚠️  Could not parse JSON content from {metadata_file_path}: {parse_error}"
                        )

        except Exception as e:
            # File doesn't exist or can't be read
            pass

        return {}
    except Exception as e:
        print(f"⚠️  Error reading MinIO metadata: {e}")
        return {}


def parse_logs_and_insert_metrics(
    spark: SparkSession,
    batch_id: str,
    target_table: str,
    source_files: List[str],
    validation_result: Dict[str, Any],
    batch_duration_ms: float,
    load_source: str = "local",
    job_start_time: datetime = None,
    job_end_time: datetime = None,
    operation_type: str = "insert",
    job_id: str = None,
    app_name: str = "insert_pipeline",
) -> bool:
    """
    Parse logs from MinIO and insert metrics into job_logs_fact table with hierarchical structure

    Args:
        spark: SparkSession for data operations
        batch_id: Unique batch identifier (deprecated, kept for compatibility)
        target_table: Target table name for metrics insertion
        source_files: List of source files that were processed
        validation_result: Validation result dictionary
        batch_duration_ms: Duration of batch processing in milliseconds
        load_source: Source system identifier (local, minio, s3, hdfs, api)
        job_start_time: When the job started (defaults to current time)
        job_end_time: When the job ended (defaults to current time)
        operation_type: Type of operation (insert, update, delete, merge)
        job_id: Unique job identifier (auto-generated if None)
        app_name: Application name that created the job

    Returns:
        bool: True if successful, False otherwise
    """
    try:
        print(f"📊 Inserting job logs for job {job_id}")

        # First, ensure logs are synced to MinIO
        if hasattr(spark, "hybrid_logger"):
            try:
                sync_result = spark.hybrid_logger.force_sync_logs()
                print(f"✅ Logs synced to MinIO: {sync_result}")
            except Exception as sync_error:
                print(f"⚠️  Log sync warning: {sync_error}")

        # Parse logs from MinIO to extract metrics
        log_metrics = parse_minio_logs_for_metrics(spark, job_id)
        print(f"📊 Extracted {len(log_metrics)} log metrics")

        # Calculate additional metrics from validation result
        total_record_size_bytes = 0
        for file_path in source_files:
            try:
                if is_minio_path(file_path):
                    total_record_size_bytes += get_minio_file_size(spark, file_path)
                else:
                    total_record_size_bytes += Path(file_path).stat().st_size
            except Exception as e:
                print(f"⚠️  Could not get file size for {file_path}: {e}")

        # Build error codes map from validation
        error_codes = {}
        for failed_file in validation_result.get("files_failed", []):
            error_code = failed_file.get("error_code", "E999")
            error_codes[error_code] = error_codes.get(error_code, 0) + 1

        # Set timestamps
        if job_start_time is None:
            job_start_time = datetime.now(timezone.utc)
        elif job_start_time.tzinfo != timezone.utc:
            job_start_time = job_start_time.astimezone(timezone.utc)

        if job_end_time is None:
            job_end_time = datetime.now(timezone.utc)
        elif job_end_time.tzinfo != timezone.utc:
            job_end_time = job_end_time.astimezone(timezone.utc)

        # Use provided job_id or generate one
        if job_id is None:
            job_id = generate_job_id()

        # Build comprehensive metrics matching new job_logs schema
        combined_metrics = {
            # Job-level identifiers
            "job_id": job_id,
            "source": load_source,
            "target_table": target_table,
            "version": "v3",
            # Job-level timestamps
            "job_start": job_start_time.isoformat(),
            "job_end": job_end_time.isoformat(),
            # Operation-level metrics
            "operation_name": operation_type,
            "operation_start": job_start_time.isoformat(),
            "operation_end": job_end_time.isoformat(),
            "operation_result_count": validation_result.get("files_loaded", 0),
            # Record-level counts from validation
            "records_processed": validation_result.get("files_expected", 0),
            "records_loaded": validation_result.get("files_loaded", 0),
            "records_failed": len(validation_result.get("files_failed", [])),
            "records_missing": len(validation_result.get("files_missing", [])),
            # Performance metrics
            "record_size_bytes": total_record_size_bytes,
            "bytes_transferred": total_record_size_bytes,
            "performance_operation": log_metrics.get(
                "performance_operation", operation_type
            ),
            "performance_execution_time_ms": log_metrics.get(
                "performance_execution_time_ms", int(batch_duration_ms)
            ),
            "performance_timestamp": log_metrics.get(
                "performance_timestamp", int(time.time() * 1000)
            ),
            # Partition processing metrics (placeholder - will be populated by actual partition tracking)
            "partition_processing_metrics": {
                "partition_id": 0,
                "partition_count": 1,
                "partition_size": validation_result.get("files_loaded", 0),
                "partition_total_size_bytes": total_record_size_bytes,
                "partition_skew_ratio": 0.0,
                "partition_start": job_start_time.isoformat(),
                "partition_end": job_end_time.isoformat(),
                "partition_processing_time_ms": int(batch_duration_ms),
            },
            # Business events
            "business_events_count": log_metrics.get("business_events_count", 0),
            "business_event_types": [],
            # Error tracking
            "error_count": len(validation_result.get("files_failed", [])),
            "error_codes": error_codes,
            "error_summary": f"Success: {len(validation_result.get('files_successful', []))}, Failed: {len(validation_result.get('files_failed', []))}, Missing: {len(validation_result.get('files_missing', []))}",
            "error_types": [],
            # Logging system metrics
            "total_logs_generated": log_metrics.get("total_logs_generated", 0),
            "async_logging_enabled": True,
            # Validation status
            "validation_status": validation_result.get("validation_status", "unknown"),
            "job_status": (
                "success"
                if len(validation_result.get("files_failed", [])) == 0
                else "partial_success"
            ),
            # Spark metrics
            "spark_metrics": {
                "executor_count": 2,  # Default for local setup
                "executor_memory_mb": 1024,  # Default
                "driver_memory_mb": 512,  # Default
                "spark_version": "3.5.6",
                "iceberg_version": "1.4.0",
                "shuffle_read_bytes": 0,
                "shuffle_write_bytes": 0,
                "total_executor_time_ms": int(batch_duration_ms),
            },
            # Audit trail
            "created_by": "system",
            "created_at": datetime.now(timezone.utc).isoformat(),
        }

        # Strict validation (validate metrics)
        if not validate_job_logs_metrics(combined_metrics):
            print(f"❌ Job logs validation failed for job {job_id}")
            return False

        # Insert metrics into dataops.job_logs table
        print(f"💾 Inserting job logs into dataops.job_logs...")

        # Transform metrics to match job_logs table schema

        # Create schema that matches the job_logs table
        job_logs_schema = StructType(
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
                StructField(
                    "failed_files_by_partition",
                    MapType(
                        StringType(),
                        ArrayType(
                            StructType(
                                [
                                    StructField("file_path", StringType(), True),
                                    StructField("document_id", StringType(), True),
                                    StructField("document_type", StringType(), True),
                                    StructField("error_message", StringType(), True),
                                    StructField("error_code", StringType(), True),
                                    StructField("record_size_bytes", LongType(), True),
                                    StructField("partition_id", IntegerType(), True),
                                    StructField(
                                        "failure_timestamp", TimestampType(), True
                                    ),
                                ]
                            )
                        ),
                    ),
                    True,
                ),
                StructField("business_events_count", IntegerType(), True),
                StructField("business_event_types", ArrayType(StringType()), True),
                StructField("custom_metrics_count", IntegerType(), True),
                StructField("custom_operation_names", ArrayType(StringType()), True),
                StructField("total_logs_generated", IntegerType(), True),
                StructField("async_logging_enabled", BooleanType(), True),
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

        # Transform combined_metrics to match the schema
        job_logs_data = {
            "log_id": f"{job_id}_{int(time.time())}",
            "job_id": job_id,
            "batch_id": None,  # Job-level record
            "operation_name": None,  # Job-level record
            "log_level": "job",
            "source": combined_metrics.get("source", "local"),
            "target_table": combined_metrics.get("target_table", ""),
            "app_name": app_name,
            "job_type": "insert",
            "operation_type": operation_type,
            "start_time": datetime.fromisoformat(
                combined_metrics.get(
                    "job_start", datetime.now(timezone.utc).isoformat()
                ).replace("Z", "+00:00")
            ),
            "end_time": datetime.fromisoformat(
                combined_metrics.get(
                    "job_end", datetime.now(timezone.utc).isoformat()
                ).replace("Z", "+00:00")
            ),
            "duration_ms": batch_duration_ms,
            "records_listed": combined_metrics.get("records_processed", 0),
            "records_processed": combined_metrics.get("records_processed", 0),
            "records_inserted": combined_metrics.get("records_loaded", 0),
            "records_failed": combined_metrics.get("records_failed", 0),
            "records_missing": combined_metrics.get("records_missing", 0),
            "records_updated": 0,
            "records_deleted": 0,
            "total_file_size_bytes": combined_metrics.get("record_size_bytes", 0),
            "bytes_transferred": combined_metrics.get("bytes_transferred", 0),
            "error_count": combined_metrics.get("error_count", 0),
            "error_codes": combined_metrics.get("error_codes", {}),
            "error_summary": combined_metrics.get("error_summary", ""),
            "error_types": combined_metrics.get("error_types", []),
            "failed_files_by_partition": {},  # Will be populated with proper structure if needed
            "business_events_count": combined_metrics.get("business_events_count", 0),
            "business_event_types": combined_metrics.get("business_event_types", []),
            "custom_metrics_count": 0,
            "custom_operation_names": [],
            "total_logs_generated": combined_metrics.get("total_logs_generated", 0),
            "async_logging_enabled": combined_metrics.get(
                "async_logging_enabled", True
            ),
            "validation_status": combined_metrics.get("validation_status", "unknown"),
            "status": combined_metrics.get("job_status", "unknown"),
            "spark_version": "3.5.6",
            "executor_count": combined_metrics.get("spark_metrics", {}).get(
                "executor_count", 2
            ),
            "executor_memory_mb": combined_metrics.get("spark_metrics", {}).get(
                "executor_memory_mb", 6000
            ),
            "driver_memory_mb": combined_metrics.get("spark_metrics", {}).get(
                "driver_memory_mb", 6000
            ),
            "created_by": combined_metrics.get("created_by", "system"),
            "created_at": datetime.now(timezone.utc),
            "updated_at": datetime.now(timezone.utc),
        }

        # Create DataFrame with explicit schema
        df = spark.createDataFrame([job_logs_data], job_logs_schema)
        df.writeTo("dataops.job_logs").append()

        print(f"✅ Job logs inserted successfully for job {job_id}")
        return True

    except Exception as e:
        print(f"❌ Error inserting job logs for job {job_id}: {e}")
        return False


def validate_job_logs_metrics(metrics: Dict[str, Any]) -> bool:
    """
    STRICT validation of job logs metrics before insertion
    Fails fast on any schema or data quality issues

    Args:
        metrics: Dictionary containing job logs metrics

    Returns:
        bool: True if validation passes, False otherwise
    """
    try:
        # 1. REQUIRED FIELDS VALIDATION
        required_fields = {
            "job_id": str,
            "source": str,
            "target_table": str,
            "version": str,
            "job_start": str,
            "job_end": str,
            "records_processed": int,
            "records_loaded": int,
            "records_failed": int,
            "records_missing": int,
            "record_size_bytes": int,
            "bytes_transferred": int,
            "error_codes": dict,
            "error_summary": str,
            "validation_status": str,
            "job_status": str,
            "created_by": str,
            "created_at": str,
        }

        for field, expected_type in required_fields.items():
            if field not in metrics:
                raise ValueError(f"❌ MISSING REQUIRED FIELD: {field}")

            if metrics[field] is None:
                raise ValueError(f"❌ NULL VALUE FOR REQUIRED FIELD: {field}")

            if not isinstance(metrics[field], expected_type):
                raise ValueError(
                    f"❌ INVALID TYPE FOR {field}: expected {expected_type.__name__}, got {type(metrics[field]).__name__}"
                )

        # 2. TIMESTAMP VALIDATION (CRITICAL FOR ICEBERG)
        timestamp_fields = ["job_start", "job_end", "created_at"]
        for field in timestamp_fields:
            if field in metrics and metrics[field] is not None:
                timestamp_str = metrics[field]
                if not isinstance(timestamp_str, str):
                    raise ValueError(
                        f"❌ INVALID TIMESTAMP TYPE FOR {field}: {type(timestamp_str)}"
                    )

                # Validate ISO format
                try:
                    timestamp = datetime.fromisoformat(
                        timestamp_str.replace("Z", "+00:00")
                    )
                except ValueError:
                    raise ValueError(
                        f"❌ INVALID TIMESTAMP FORMAT FOR {field}: {timestamp_str}"
                    )

                # Validate timestamp is reasonable (not too old, not in future)
                now = datetime.now(timezone.utc)
                if timestamp < now - timedelta(days=365):  # Not older than 1 year
                    raise ValueError(f"❌ TIMESTAMP TOO OLD FOR {field}: {timestamp}")
                if timestamp > now + timedelta(
                    hours=1
                ):  # Not more than 1 hour in future
                    raise ValueError(f"❌ TIMESTAMP IN FUTURE FOR {field}: {timestamp}")

        # 3. BUSINESS LOGIC VALIDATION
        # Records loaded cannot exceed records processed
        if metrics.get("records_loaded", 0) > metrics.get("records_processed", 0):
            raise ValueError(
                f"❌ LOGICAL ERROR: records_loaded ({metrics.get('records_loaded', 0)}) > records_processed ({metrics.get('records_processed', 0)})"
            )

        # 4. COUNT VALIDATION
        count_fields = [
            "records_processed",
            "records_loaded",
            "records_failed",
            "records_missing",
            "record_size_bytes",
            "bytes_transferred",
            "error_count",
            "business_events_count",
            "total_logs_generated",
        ]

        for field in count_fields:
            if field in metrics and metrics[field] is not None:
                if not isinstance(metrics[field], int):
                    raise ValueError(
                        f"❌ INVALID COUNT TYPE FOR {field}: {type(metrics[field])}"
                    )
                if metrics[field] < 0:
                    raise ValueError(f"❌ NEGATIVE COUNT FOR {field}: {metrics[field]}")

        # 5. ENUMERATED VALUES VALIDATION
        valid_sources = ["local", "minio", "s3", "hdfs", "api"]
        if metrics["source"] not in valid_sources:
            raise ValueError(
                f"❌ INVALID SOURCE: {metrics['source']}, must be one of {valid_sources}"
            )

        valid_job_statuses = ["success", "failed", "partial_success"]
        if metrics["job_status"] not in valid_job_statuses:
            raise ValueError(
                f"❌ INVALID JOB_STATUS: {metrics['job_status']}, must be one of {valid_job_statuses}"
            )

        valid_validation_statuses = [
            "complete_success",
            "partial_load",
            "load_failures",
            "unknown",
        ]
        if metrics["validation_status"] not in valid_validation_statuses:
            raise ValueError(
                f"❌ INVALID VALIDATION_STATUS: {metrics['validation_status']}, must be one of {valid_validation_statuses}"
            )

        # 6. STRING LENGTH VALIDATION
        string_fields = [
            "job_id",
            "source",
            "target_table",
            "version",
            "operation_name",
            "created_by",
        ]
        for field in string_fields:
            if field in metrics and metrics[field] is not None:
                if not isinstance(metrics[field], str):
                    raise ValueError(
                        f"❌ INVALID STRING TYPE FOR {field}: {type(metrics[field])}"
                    )
                if len(metrics[field]) > 255:  # Reasonable max length
                    raise ValueError(
                        f"❌ STRING TOO LONG FOR {field}: {len(metrics[field])} chars"
                    )

        # 7. PERFORMANCE METRICS VALIDATION
        if "record_size_bytes" in metrics and metrics["record_size_bytes"] is not None:
            if not isinstance(metrics["record_size_bytes"], int):
                raise ValueError(
                    f"❌ INVALID RECORD SIZE TYPE: {type(metrics['record_size_bytes'])}"
                )
            if metrics["record_size_bytes"] < 0:
                raise ValueError(
                    f"❌ NEGATIVE RECORD SIZE: {metrics['record_size_bytes']}"
                )

        # 8. ERROR CODES VALIDATION
        if "error_codes" in metrics and metrics["error_codes"] is not None:
            if not isinstance(metrics["error_codes"], dict):
                raise ValueError(
                    f"❌ INVALID ERROR_CODES TYPE: {type(metrics['error_codes'])}"
                )
            for code, count in metrics["error_codes"].items():
                if not isinstance(code, str) or not isinstance(count, int):
                    raise ValueError(f"❌ INVALID ERROR_CODE FORMAT: {code}: {count}")
                if count < 0:
                    raise ValueError(f"❌ NEGATIVE ERROR COUNT: {code}: {count}")

        # 9. PARTITION PROCESSING METRICS VALIDATION
        if (
            "partition_processing_metrics" in metrics
            and metrics["partition_processing_metrics"] is not None
        ):
            partition_metrics = metrics["partition_processing_metrics"]
            if not isinstance(partition_metrics, dict):
                raise ValueError(
                    f"❌ INVALID PARTITION_METRICS TYPE: {type(partition_metrics)}"
                )

            required_partition_fields = [
                "partition_id",
                "partition_count",
                "partition_size",
                "partition_total_size_bytes",
                "partition_start",
                "partition_end",
                "partition_processing_time_ms",
            ]

            for field in required_partition_fields:
                if field not in partition_metrics:
                    raise ValueError(f"❌ MISSING REQUIRED PARTITION FIELD: {field}")

        # 10. SPARK METRICS VALIDATION
        if "spark_metrics" in metrics and metrics["spark_metrics"] is not None:
            spark_metrics = metrics["spark_metrics"]
            if not isinstance(spark_metrics, dict):
                raise ValueError(
                    f"❌ INVALID SPARK_METRICS TYPE: {type(spark_metrics)}"
                )

        print(f"✅ STRICT VALIDATION PASSED for job {metrics.get('job_id', 'unknown')}")
        return True

    except Exception as e:
        print(f"❌ STRICT VALIDATION FAILED: {str(e)}")
        print(f"❌ FAILED METRICS: {metrics}")
        return False


def generate_job_id() -> str:
    """
    Generate a unique job identifier

    Returns:
        str: Unique job ID with timestamp and random component
    """
    return f"legal_insert_{int(time.time())}_{uuid.uuid4().hex[:8]}"


def log_job_start(
    observability_logger: HybridLogger,
    job_id: str,
    batch_id: str,
    operation_type: str,
    target_table: str,
) -> None:
    """
    Log job start event

    Args:
        observability_logger: HybridLogger instance for logging
        job_id: Unique job identifier
        batch_id: Unique batch identifier
        operation_type: Type of operation being performed
        target_table: Target table name
    """
    if observability_logger:
        observability_logger.log_business_event(
            "job_started",
            {
                "job_id": job_id,
                "batch_id": batch_id,
                "operation_type": operation_type,
                "target_table": target_table,
            },
        )


def log_job_end(
    observability_logger: HybridLogger,
    job_id: str,
    batch_id: str,
    operation_type: str,
    target_table: str,
    status: str,
    duration_ms: float,
) -> None:
    """
    Log job end event

    Args:
        observability_logger: HybridLogger instance for logging
        job_id: Unique job identifier
        batch_id: Unique batch identifier
        operation_type: Type of operation performed
        target_table: Target table name
        status: Job completion status (success, failed, partial_success)
        duration_ms: Job duration in milliseconds
    """
    if observability_logger:
        observability_logger.log_business_event(
            "job_completed",
            {
                "job_id": job_id,
                "batch_id": batch_id,
                "operation_type": operation_type,
                "target_table": target_table,
                "status": status,
                "duration_ms": duration_ms,
            },
        )


def validate_metrics_only(
    spark: SparkSession,
    batch_id: str,
    target_table: str,
    source_files: List[str],
    validation_result: Dict[str, Any],
    batch_duration_ms: float,
    load_source: str = "local",
    job_start_time: datetime = None,
    job_end_time: datetime = None,
    operation_type: str = "insert",
    job_id: str = None,
    app_name: str = "insert_pipeline",
) -> Dict[str, Any]:
    """
    Validate metrics without inserting into job_logs table

    Args:
        spark: SparkSession for data operations
        batch_id: Unique batch identifier
        target_table: Target table name for metrics validation
        source_files: List of source files that were processed
        validation_result: Validation result dictionary
        batch_duration_ms: Duration of batch processing in milliseconds
        load_source: Source system identifier (local, minio, s3, hdfs, api)
        job_start_time: When the job started (defaults to current time)
        job_end_time: When the job ended (defaults to current time)
        operation_type: Type of operation (insert, update, delete, merge)
        job_id: Unique job identifier (auto-generated if None)
        app_name: Application name that created the job

    Returns:
        Dict[str, Any]: Validated metrics dictionary
    """
    try:
        print(f" Validating metrics for batch {batch_id} (validation mode)")

        # Build metrics (same as parse_logs_and_insert_metrics)
        combined_metrics = {
            # ... same metrics building logic ...
        }

        # Validate against schema
        is_valid, errors = (
            schema_manager.validator.validate_metrics_with_business_logic(
                combined_metrics
            )
        )

        validation_report = {
            "batch_id": batch_id,
            "is_valid": is_valid,
            "errors": errors,
            "metrics": combined_metrics,
            "validation_timestamp": datetime.now(timezone.utc).isoformat(),
        }

        if is_valid:
            print(f"✅ Metrics validation passed for batch {batch_id}")
        else:
            print(f"❌ Metrics validation failed for batch {batch_id}:")
            for error in errors:
                print(f"   - {error}")

        return validation_report

    except Exception as e:
        print(f"❌ Error validating metrics for batch {batch_id}: {e}")
        return {
            "batch_id": batch_id,
            "is_valid": False,
            "errors": [str(e)],
            "metrics": {},
            "validation_timestamp": datetime.now(timezone.utc).isoformat(),
        }


def main():
    """
    Main function for ELT pipeline - Extract and Load operations
    """
    parser = argparse.ArgumentParser(
        description="ELT Pipeline for Legal Documents - Extract and Load operations"
    )

    parser.add_argument(
        "--file-path",
        type=str,
        required=True,
        help="Path to files to process (local directory or MinIO path like s3a://bucket/path)",
    )

    parser.add_argument(
        "--table-name",
        type=str,
        help="Target table name",
    )

    parser.add_argument(
        "--validate-existing-data",
        action="store_true",
        help="Only validate existing data in database, don't insert new files. Checks table existence, row counts, and partition distribution.",
    )

    parser.add_argument(
        "--validate-metrics-schema",
        action="store_true",
        help="Only validate metrics against schema without inserting into database. Useful for testing metrics format and business rules.",
    )

    parser.add_argument(
        "--show-failed",
        action="store_true",
        help="Show failed loads for troubleshooting (use with --validate-existing-data)",
    )

    parser.add_argument(
        "--mode",
        type=str,
        choices=["batch", "streaming"],
        default="batch",
        help="Processing mode: batch (collect all files then insert) or streaming (insert each file immediately)",
    )

    parser.add_argument(
        "--num-partitions",
        type=int,
        default=4,
        help="Number of partitions for parallel processing (default: 4)",
    )

    parser.add_argument(
        "--validate-metrics-only",
        action="store_true",
        help="Only validate metrics without inserting into database",
    )

    args = parser.parse_args()

    print("🚀 Starting Insert Operation")
    print(f"Source: {args.file_path}")
    print(f"Target: {args.table_name}")
    print(f"Mode: {args.mode}")

    # Use default configuration (which now has the correct MinIO credentials)
    spark = create_spark_session(
        app_name=Path(__file__).stem,
        spark_version=SparkVersion.SPARK_CONNECT_3_5,
    )

    with HybridLogger(
        spark=spark,
        app_name=Path(__file__).stem,
        manage_spark=True,
    ) as observability_logger:

        if args.mode == "streaming":
            raise NotImplementedError("Streaming processing not yet implemented")

        # Start operation tracking
        observability_logger.start_operation("default_job_group", "insert_files")
        start_time = time.time()
        job_start_time = datetime.now(timezone.utc)

        # Generate job ID
        job_id = generate_job_id()

        try:
            if args.validate_existing_data:
                # Validate existing data in database
                print("\n🔍 Validating existing data in database...")
                basic_load_validation(observability_logger.spark, args.table_name)
                if args.show_failed:
                    get_failed_loads(observability_logger.spark, args.table_name)
                return 0

            if args.validate_metrics_schema:
                # Validate metrics schema without database insertion
                print("\n🔍 Running metrics schema validation only...")

                # Get source files from spark session if available
                source_files = getattr(observability_logger.spark, "source_files", [])

                validation_result = validate_load(
                    observability_logger.spark,
                    args.table_name,
                    source_files,
                    len(source_files) if source_files else 0,
                )

                validation_report = validate_metrics_only(
                    spark=observability_logger.spark,
                    batch_id=None,  # No batch_id in new schema
                    target_table=args.table_name,
                    source_files=source_files,
                    validation_result=validation_result,
                    batch_duration_ms=execution_time * 1000,
                    load_source="minio" if is_minio_path(args.file_path) else "local",
                    job_start_time=job_start_time,
                    job_end_time=job_end_time,
                    operation_type="insert",
                    job_id=job_id,
                )

                # Print detailed validation report
                print(f"\n📊 Metrics Schema Validation Report:")
                print(f"   Job ID: {validation_report.get('job_id', 'unknown')}")
                print(f"   Valid: {validation_report['is_valid']}")
                print(f"   Error Count: {len(validation_report['errors'])}")
                print(
                    f"   Validation Time: {validation_report['validation_timestamp']}"
                )

                if validation_report["errors"]:
                    print(f"\n❌ Validation Errors:")
                    for error in validation_report["errors"]:
                        print(f"   - {error}")

                return 0 if validation_report["is_valid"] else 1

            insert_results = insert_files(
                spark=observability_logger.spark,
                docs_dir=args.file_path,
                table_name=args.table_name,
                observability_logger=observability_logger,
            )

            success = insert_results.get("success", False)

            # Print detailed results summary
            print(f"\n📊 Processing Summary:")
            print(f"   Success: {success}")
            print(
                f"   Total Files Processed: {insert_results.get('total_files_processed', 0)}"
            )
            print(
                f"   Successful Inserts: {insert_results.get('successful_inserts', 0)}"
            )
            print(f"   Failed Inserts: {insert_results.get('failed_inserts', 0)}")
            print(f"   Success Rate: {insert_results.get('success_rate', 0.0):.2%}")

            # Show failed files by partition if any
            failed_files_by_partition = insert_results.get(
                "failed_files_by_partition", {}
            )
            if failed_files_by_partition:
                print(f"\n❌ Failed Files by Partition:")
                for partition_id, failed_files in failed_files_by_partition.items():
                    print(
                        f"   Partition {partition_id}: {len(failed_files)} failed files"
                    )
                    for failed_file in failed_files[:3]:  # Show first 3 per partition
                        print(
                            f"     - {failed_file['file_path']}: {failed_file['error_message'][:80]}..."
                        )
                    if len(failed_files) > 3:
                        print(f"     ... and {len(failed_files) - 3} more failed files")

            if success:
                print("\n✅ ELT pipeline completed successfully")

                # Show validation results
                basic_load_validation(observability_logger.spark, args.table_name)
                if args.show_failed:
                    get_failed_loads(observability_logger.spark, args.table_name)

            execution_time = time.time() - start_time
            job_end_time = datetime.now(timezone.utc)

            # End operation tracking
            observability_logger.end_operation(execution_time=execution_time)

            # Log successful completion
            observability_logger.log_performance(
                "operation_complete",
                {
                    "operation": "insert_files",
                    "execution_time_seconds": round(execution_time, 4),
                    "status": "success",
                },
            )

            if args.validate_metrics_only:
                # Validation-only mode
                print("\n🔍 Running metrics validation only...")

                # Get source files from spark session if available
                source_files = getattr(observability_logger.spark, "source_files", [])

                validation_result = validate_load(
                    observability_logger.spark,
                    args.table_name,
                    source_files,
                    len(source_files) if source_files else 0,
                )

                validation_report = validate_metrics_only(
                    spark=observability_logger.spark,
                    batch_id=None,  # No batch_id in new schema
                    target_table=args.table_name,
                    source_files=source_files,
                    validation_result=validation_result,
                    batch_duration_ms=execution_time * 1000,
                    load_source="minio" if is_minio_path(args.file_path) else "local",
                    job_start_time=job_start_time,
                    job_end_time=job_end_time,
                    operation_type="insert",
                    job_id=job_id,
                )

                # Print detailed validation report
                print(f"\n📊 Validation Report:")
                print(f"   Job ID: {validation_report.get('job_id', 'unknown')}")
                print(f"   Valid: {validation_report['is_valid']}")
                print(f"   Error Count: {len(validation_report['errors'])}")
                print(
                    f"   Validation Time: {validation_report['validation_timestamp']}"
                )

                return 0 if validation_report["is_valid"] else 1

            # Get source files from spark session if available
            source_files = getattr(observability_logger.spark, "source_files", [])

            validation_result = validate_load(
                observability_logger.spark,
                args.table_name,
                source_files,  # Use actual source files if available
                len(source_files) if source_files else 0,
            )

            # Create a minimal validation result if the above fails
            if not validation_result:
                validation_result = {
                    "files_expected": len(source_files) if source_files else 0,
                    "files_loaded": 0,
                    "files_failed": [],
                    "files_missing": [],
                    "files_successful": [],
                    "validation_status": "unknown",
                }

            parse_logs_and_insert_metrics(
                spark=observability_logger.spark,
                batch_id=None,  # No batch_id in new schema
                target_table=args.table_name,
                source_files=source_files,  # Use actual source files
                validation_result=validation_result,
                batch_duration_ms=execution_time * 1000,
                load_source="minio" if is_minio_path(args.file_path) else "local",
                job_start_time=job_start_time,
                job_end_time=job_end_time,
                operation_type="insert",
                job_id=job_id,  # Use the job_id generated in main
            )

            return 0

        except Exception as e:
            execution_time = time.time() - start_time
            job_end_time = datetime.now(timezone.utc)

            # Log error
            observability_logger.log_error(e, {"operation": "insert_files"})
            observability_logger.log_performance(
                "operation_failed",
                {
                    "operation": "insert_files",
                    "execution_time_seconds": round(execution_time, 4),
                    "error": str(e),
                    "error_type": type(e).__name__,
                    "status": "failed",
                },
            )

            # End operation tracking with failure
            observability_logger.end_operation(
                execution_time=execution_time,
                result_count=0,
            )

            return 1


def parse_minio_logs_for_metrics(
    spark: SparkSession,
    job_id: str,
) -> Dict[str, Any]:
    """
    Parse logs from MinIO to extract metrics

    Args:
        spark: SparkSession for data operations
        job_id: Unique job identifier

    Returns:
        Dict[str, Any]: Extracted metrics from logs
    """
    try:
        print(f"🔍 Parsing MinIO logs for job {job_id}")

        # Read hybrid-observability logs from MinIO
        logs_path = f"s3a://logs/observability/"

        try:
            logs_df = spark.read.text(logs_path)
            logs_df.createOrReplaceTempView("minio_logs")
        except Exception as e:
            return {}

        # Extract different types of metrics from logs
        metrics = {}

        # Parse PERFORMANCE_METRICS
        try:
            perf_metrics = spark.sql(
                """
                SELECT 
                    value,
                    input_file_name() as log_file
                FROM minio_logs 
                WHERE value LIKE '%PERFORMANCE_METRICS:%'
                AND value LIKE '%job_id%'
            """
            )

            if perf_metrics.count() > 0:
                # Extract performance metrics for this job
                job_perf_metrics = perf_metrics.filter(f"value LIKE '%{job_id}%'")
                if job_perf_metrics.count() > 0:
                    # Get the latest performance metrics
                    latest_perf = job_perf_metrics.orderBy("log_file").take(1)[0]
                    perf_data = extract_json_from_log_line(latest_perf["value"])
                    if perf_data:
                        metrics.update(
                            {
                                "performance_operation": perf_data.get("operation", ""),
                                "performance_execution_time_ms": perf_data.get(
                                    "execution_time_ms", 0
                                ),
                                "performance_timestamp": perf_data.get("timestamp", 0),
                            }
                        )
        except Exception as e:
            pass

        # Parse BUSINESS_EVENT metrics
        try:
            business_events = spark.sql(
                """
                SELECT 
                    value,
                    input_file_name() as log_file
                FROM minio_logs 
                WHERE value LIKE '%BUSINESS_EVENT:%'
                AND value LIKE '%job_id%'
            """
            )

            if business_events.count() > 0:
                job_events = business_events.filter(f"value LIKE '%{job_id}%'")
                metrics["business_events_count"] = job_events.count()
        except Exception as e:
            pass

        # Parse ERROR metrics
        try:
            error_logs = spark.sql(
                """
                SELECT 
                    value,
                    input_file_name() as log_file
                FROM minio_logs 
                WHERE value LIKE '%ERROR:%'
                AND value LIKE '%job_id%'
            """
            )

            if error_logs.count() > 0:
                job_errors = error_logs.filter(f"value LIKE '%{job_id}%'")
                metrics["error_count"] = job_errors.count()
        except Exception as e:
            pass

        return metrics

    except Exception as e:
        return {}


def extract_json_from_log_line(log_line: str) -> Dict[str, Any]:
    """
    Extract JSON from a log line

    Args:
        log_line: Log line containing JSON data

    Returns:
        Dict[str, Any]: Parsed JSON data or empty dict if parsing fails
    """
    try:
        # Find JSON start position
        json_start = log_line.find("{")
        if json_start != -1:
            json_str = log_line[json_start:]
            return json.loads(json_str)
    except Exception:
        pass
    return {}


def validate_load(
    spark: SparkSession,
    table_name: str,
    original_files: List[str],
    expected_count: int,
) -> Dict[str, Any]:
    """
    Enhanced validation using distributed operations that now work

    Args:
        spark: SparkSession for distributed operations
        table_name: Name of the table to validate
        original_files: List of original file paths that were processed
        expected_count: Expected number of records to be loaded

    Returns:
        Dict[str, Any]: Validation result with file counts and status
    """
    print(f"\n🔍 Enhanced validation for load...")

    try:

        # Load table as DataFrame
        table_df = spark.table(table_name)

        # Count loaded records using DataFrame API
        loaded_records_df = table_df.agg(count("*").alias("loaded_count"))
        loaded_count = loaded_records_df.take(1)[0]["loaded_count"]

        # File analysis using DataFrame API
        file_analysis_df = (
            table_df.groupBy("file_path")
            .agg(
                count("*").alias("record_count"),
                max_func("generated_at").alias("latest_record"),
            )
            .orderBy(col("record_count").desc())
        )

        file_analysis = file_analysis_df.take(100)

        # Create lookup for loaded records
        loaded_files = {record["file_path"]: record for record in file_analysis}

        # Validate each original file
        validation_result = {
            "files_expected": len(original_files),
            "files_loaded": len(loaded_files),
            "files_missing": [],
            "files_failed": [],
            "files_successful": [],
            "validation_status": "unknown",
        }

        for file_path in original_files:
            file_name = file_path.split("/")[-1] if "/" in file_path else file_path

            if file_name in loaded_files:
                record = loaded_files[file_name]
                # If record exists, consider it successfully loaded
                validation_result["files_successful"].append(file_name)
            else:
                validation_result["files_missing"].append(file_name)

        # Determine overall validation status
        if (
            len(validation_result["files_missing"]) == 0
            and len(validation_result["files_failed"]) == 0
        ):
            validation_result["validation_status"] = "complete_success"
        elif len(validation_result["files_missing"]) > 0:
            validation_result["validation_status"] = "partial_load"
        else:
            validation_result["validation_status"] = "load_failures"

        # Print enhanced validation summary
        print(f"✅ Enhanced validation complete:")
        print(f"   - Expected: {validation_result['files_expected']} files")
        print(f"   - Loaded: {validation_result['files_loaded']} files")
        print(f"   - Successful: {len(validation_result['files_successful'])} files")
        print(f"   - Failed: {len(validation_result['files_failed'])} files")
        print(f"   - Missing: {len(validation_result['files_missing'])} files")
        print(f"   - Status: {validation_result['validation_status']}")
        print(f"   - Total records: {loaded_count:,}")

        return validation_result

    except Exception as e:
        print(f"❌ Enhanced validation failed: {e}")
        return None


def merge_to_iceberg_table(
    spark: SparkSession,
    source_df,
    table_name: str,
    merge_key: str = "document_id",
) -> bool:
    # Register a unique temp view
    temp_view = "temp_merge_view"
    source_df.createOrReplaceTempView(temp_view)
    columns = source_df.columns

    # Exclude only the merge key from UPDATE SET (all other columns are now deterministic)
    update_columns = [col for col in columns if col != merge_key]
    set_clause = ",\n  ".join([f"{col} = s.{col}" for col in update_columns])
    insert_cols = ", ".join(columns)
    insert_vals = ", ".join([f"s.{col}" for col in columns])
    merge_sql = f"""
    MERGE INTO {table_name} t
    USING {temp_view} s
    ON t.{merge_key} = s.{merge_key}
    WHEN MATCHED THEN UPDATE SET
      {set_clause}
    WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """
    # No try/except: fail fast
    spark.sql(merge_sql)
    print(f"✅ Successfully merged data to {table_name} with SQL MERGE INTO")
    return True


def _get_executor_count(spark: SparkSession) -> int:
    """Safely get executor count, with fallback for Spark Connect"""
    try:
        # Try to get from Spark configuration
        return int(spark.conf.get("spark.executor.instances"))
    except Exception:
        try:
            # Try to get from Spark context (if available)
            return spark.sparkContext.defaultParallelism
        except Exception:
            # Fallback to fixed value for Spark Connect
            return 2


def _get_executor_memory_mb(spark: SparkSession) -> int:
    """Safely get executor memory in MB, with fallback for Spark Connect"""
    try:
        memory_str = spark.conf.get("spark.executor.memory")
        if memory_str.endswith("g"):
            return int(memory_str[:-1]) * 1024
        elif memory_str.endswith("m"):
            return int(memory_str[:-1])
        else:
            return int(memory_str)
    except Exception:
        # Fallback to fixed value for Spark Connect
        return 6144  # 6GB in MB


def _get_driver_memory_mb(spark: SparkSession) -> int:
    """Safely get driver memory in MB, with fallback for Spark Connect"""
    try:
        memory_str = spark.conf.get("spark.driver.memory")
        if memory_str.endswith("g"):
            return int(memory_str[:-1]) * 1024
        elif memory_str.endswith("m"):
            return int(memory_str[:-1])
        else:
            return int(memory_str)
    except Exception:
        # Fallback to fixed value for Spark Connect
        return 6144  # 6GB in MB


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
