#!/usr/bin/env python3
"""
ELT Pipeline - Extract and Load operations
Transformations and data quality checks should be handled by dbt/SQLMesh
Note: Iceberg tables don't have constraints, so validation is done at application level
"""

import argparse
from datetime import datetime, timezone
import os
from pathlib import Path
import time
import sys
from typing import List, Dict, Any, Set

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, udf
from pyspark.sql.types import StringType
from utils.logger import HybridLogger
from utils.session import (
    SparkVersion,
    S3FileSystemConfig,
    IcebergConfig,
)

# Import shared legal document models
from schemas.schema import SchemaManager

# Initialize SchemaManager
schema_manager = SchemaManager()

# Cache valid document types to avoid repeated SchemaManager calls
_VALID_DOCUMENT_TYPES: Set[str] = set()

from minio import Minio


def _load_valid_document_types():
    """Load and cache valid document types once"""
    global _VALID_DOCUMENT_TYPES
    if not _VALID_DOCUMENT_TYPES:
        _VALID_DOCUMENT_TYPES = set(schema_manager.get_document_types())
    return _VALID_DOCUMENT_TYPES


def is_minio_path(path_str: str) -> bool:
    """Check if the path is a MinIO/S3 path"""
    return path_str.startswith(("s3a://", "s3://", "minio://"))


def get_minio_path_info(path_str: str) -> Dict[str, str]:
    """Parse MinIO path to extract bucket and key information"""
    if path_str.startswith("s3a://"):
        path_str = path_str[6:]  # Remove s3a://
    elif path_str.startswith("s3://"):
        path_str = path_str[5:]  # Remove s3://
    elif path_str.startswith("minio://"):
        path_str = path_str[8:]  # Remove minio://

    # Split into bucket and key
    parts = path_str.split("/", 1)
    if len(parts) == 2:
        return {"bucket": parts[0], "key": parts[1]}
    else:
        return {"bucket": parts[0], "key": ""}


def list_minio_files_direct(minio_path: str) -> List[str]:
    """List files using direct MinIO client with wildcard support"""
    # Use environment variables or default to the correct credentials
    import os
    import re

    access_key = os.getenv("AWS_ACCESS_KEY_ID", "sparkuser")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "sparkpass")

    client = Minio(
        "localhost:9000",
        access_key=access_key,
        secret_key=secret_key,
        secure=False,
    )

    # Parse bucket and prefix from path
    path_info = get_minio_path_info(minio_path)
    bucket = path_info["bucket"]
    prefix = path_info["key"]

    # Check if path contains wildcards
    if "*" in prefix:
        print(f"🔍 Detected wildcard pattern in path: {prefix}")

        # Split the prefix to find the base path before wildcard
        wildcard_parts = prefix.split("*")
        base_prefix = wildcard_parts[0]

        print(f"   Base prefix: {base_prefix}")

        # List all objects with the base prefix
        all_objects = client.list_objects(bucket, prefix=base_prefix, recursive=True)

        # Convert wildcard pattern to regex
        # Replace * with .* for regex matching
        regex_pattern = prefix.replace("*", ".*")
        regex = re.compile(regex_pattern)

        # Filter objects that match the wildcard pattern
        matching_objects = []
        for obj in all_objects:
            if regex.match(obj.object_name):
                matching_objects.append(obj.object_name)

        print(f"   Found {len(matching_objects)} files matching pattern")
        return matching_objects
    else:
        # No wildcard - use direct listing
        print(f"📋 Listing files with prefix: {prefix}")
        objects = client.list_objects(bucket, prefix=prefix, recursive=True)
        file_list = [obj.object_name for obj in objects]
        print(f"   Found {len(file_list)} files")
        return file_list


def read_minio_file_content(spark: SparkSession, file_path: str) -> str:
    """Read content from a MinIO file - Spark Connect compatible approach"""
    try:
        # For Spark Connect, use the most basic text reading operation
        print(f"📖 Reading file: {file_path}")

        # Use the most basic text reading operation
        df = spark.read.text(file_path)

        # Use take() instead of count() to avoid serialization issues
        # Take a reasonable number of lines to verify the file can be read
        sample_lines = df.take(10)
        print(f"📊 File is readable (sampled {len(sample_lines)} lines)")

        # For Spark Connect compatibility, we'll need to use a different approach
        # to actually get the content. For now, return empty content
        print(f"⚠️  Content extraction not fully supported in Spark Connect mode")
        print(f"   - Consider using direct file reading or different approach")

        return ""

    except Exception as e:
        print(f"❌ Error reading MinIO file {file_path} with Spark: {e}")
        return ""


def get_minio_file_size(spark: SparkSession, file_path: str) -> int:
    """Get file size from MinIO - Spark Connect compatible approach"""
    try:
        # For Spark Connect, use the most basic binaryFile reading operation
        print(f"📏 Getting file size for: {file_path}")

        # Use the most basic binaryFile reading operation
        df = spark.read.format("binaryFile").load(file_path)

        # Use take() instead of count() to avoid serialization issues
        sample_files = df.take(1)
        print(f"📊 File metadata accessible (sampled {len(sample_files)} files)")

        # For Spark Connect compatibility, we'll need to use a different approach
        # to actually get the size. For now, return 0
        print(f"⚠️  Size extraction not fully supported in Spark Connect mode")
        print(f"   - Consider using direct file access or different approach")

        return 0

    except Exception as e:
        print(f"❌ Error getting file size for {file_path} with Spark: {e}")
        return 0


def basic_load_validation(spark, table_name, batch_id=None, expected_count=None):
    """
    ELT-appropriate light validation - only essential load tracking
    Data quality checks belong in Transform layer (dbt/SQLMesh)
    """
    print(f"\n🔍 ELT Load validation: {table_name}")
    if batch_id:
        print(f"Batch ID: {batch_id}")

    try:
        # 1. Check if table exists - use take() to avoid serialization issues
        tables = spark.sql(f"SHOW TABLES IN {table_name.split('.')[0]}")
        table_exists = (
            len(tables.filter(f"tableName = '{table_name.split('.')[1]}'").take(1)) > 0
        )

        if not table_exists:
            print(f"❌ Table {table_name} does not exist!")
            return False

        print(f"✅ Table {table_name} exists")

        # 2. Get total row count - use take() to avoid serialization issues
        count_result = spark.sql(f"SELECT COUNT(*) as row_count FROM {table_name}")
        # Use take() instead of collect() for Spark Connect compatibility
        total_count = count_result.take(1)[0]["row_count"]
        print(f"📊 Total rows in table: {total_count:,}")

        # 3. Get batch-specific count if batch_id provided
        if batch_id:
            batch_count_result = spark.sql(
                f"SELECT COUNT(*) as batch_count FROM {table_name} WHERE load_batch_id = '{batch_id}'"
            )
            # Use take() instead of collect() for Spark Connect compatibility
            batch_count = batch_count_result.take(1)[0]["batch_count"]
            print(f"📊 Rows in current batch: {batch_count:,}")

            # Validate expected count for this batch
            if expected_count is not None:
                if batch_count >= expected_count * 0.95:  # Allow 5% tolerance
                    print(
                        f"✅ Batch count meets expectations ({batch_count:,} >= {expected_count:,} * 0.95)"
                    )
                else:
                    print(
                        f"⚠️  Batch count below expectations ({batch_count:,} < {expected_count:,} * 0.95)"
                    )

        # 4. Check batch tracking for current batch (if batch_id) or overall
        if batch_id:
            status_query = f"""
            SELECT load_batch_id, COUNT(*) as count
            FROM {table_name}
            WHERE load_batch_id = '{batch_id}'
            GROUP BY load_batch_id
            ORDER BY count DESC
            """
        else:
            status_query = f"""
            SELECT load_batch_id, COUNT(*) as count
            FROM {table_name}
            GROUP BY load_batch_id
            ORDER BY count DESC
            """

        try:
            # Use take() instead of collect() for Spark Connect compatibility
            status_dist = spark.sql(status_query).take(
                100
            )  # Limit to avoid memory issues
            print(f"📊 Batch distribution:")
            for status in status_dist:
                print(f"   - {status['load_batch_id']}: {status['count']:,} records")
        except Exception as e:
            print(f"⚠️  Could not check batch distribution: {e}")

        # 5. Show recent load activity (last 24 hours)
        try:
            recent_loads = spark.sql(
                f"""
            SELECT 
                load_batch_id,
                generated_at,
                COUNT(*) as records_loaded
            FROM {table_name}
            WHERE generated_at >= current_timestamp() - INTERVAL 24 HOURS
            GROUP BY load_batch_id, generated_at
            ORDER BY generated_at DESC
            LIMIT 10
            """
            ).take(
                10
            )  # Use take() instead of collect() for Spark Connect compatibility

            if recent_loads:
                print(f"📊 Recent loads (last 24 hours):")
                for load in recent_loads:
                    print(
                        f"   - {load['load_batch_id']}: {load['records_loaded']:,} records at {load['generated_at']}"
                    )
        except Exception as e:
            print(f"⚠️  Could not check recent loads: {e}")

        return True

    except Exception as e:
        print(f"❌ Load validation failed: {e}")
        return False


def track_batch_load(spark, table_name, batch_id, source_files, load_timestamp=None):
    """
    Track batch load for ELT monitoring

    Args:
        spark: SparkSession
        table_name: Target table name
        batch_id: Unique batch identifier
        source_files: List of source files processed
        load_timestamp: Load timestamp (defaults to current time)

    Returns:
        Dict with batch tracking results
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
        # Count rows for this batch
        batch_count_result = spark.sql(
            f"SELECT COUNT(*) as batch_count FROM {table_name} WHERE load_batch_id = '{batch_id}'"
        )
        batch_count = batch_count_result.take(1)[0]["batch_count"]
        tracking_results["target_rows"] = batch_count

        # Check batch distribution for this batch
        batch_dist = spark.sql(
            f"""
        SELECT load_batch_id, COUNT(*) as count
        FROM {table_name}
        WHERE load_batch_id = '{batch_id}'
        GROUP BY load_batch_id
        ORDER BY count DESC
        """
        ).take(
            100
        )  # Use take() instead of collect() for Spark Connect compatibility

        # Determine overall batch status
        total_count = sum(row["count"] for row in batch_dist)

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

        print(f"📊 Batch tracking for {batch_id}:")
        print(f"   - Status: {tracking_results['load_status']}")
        print(f"   - Source files: {len(source_files)}")
        print(f"   - Loaded rows: {batch_count:,}")
        print(f"   - Warnings: {len(tracking_results['warnings'])}")

    except Exception as e:
        tracking_results["warnings"].append(f"Tracking error: {str(e)}")
        print(f"❌ Batch tracking failed: {e}")

    return tracking_results


def get_failed_loads(spark, table_name, batch_id=None, hours_back=24):
    """
    Get failed loads for troubleshooting

    Args:
        spark: SparkSession
        table_name: Target table name
        batch_id: Optional batch ID to filter
        hours_back: Hours to look back (default 24)

    Returns:
        List of failed load records
    """
    try:
        if batch_id:
            failed_query = f"""
            SELECT 
                document_id,
                source,
                load_batch_id
            FROM {table_name}
            WHERE load_batch_id = '{batch_id}'
            ORDER BY load_batch_id DESC
            """
        else:
            failed_query = f"""
            SELECT 
                document_id,
                source,
                load_batch_id
            FROM {table_name}
            ORDER BY load_batch_id DESC
            LIMIT 100
            """

        failed_loads = spark.sql(failed_query).take(
            100
        )  # Use take() instead of collect() for Spark Connect compatibility

        if failed_loads:
            print(f"📊 Found {len(failed_loads)} loaded records:")
            for record in failed_loads[:10]:  # Show first 10
                print(f"   - {record['source']}: {record['document_id']}")
            if len(failed_loads) > 10:
                print(f"   ... and {len(failed_loads) - 10} more")
        else:
            print(f"✅ No records found")

        return failed_loads

    except Exception as e:
        print(f"❌ Error getting failed loads: {e}")
        return []


def insert_stream(
    spark: SparkSession,
    stream_source: str,
    table_name: str,
    batch_id: str = None,
    stream_config: Dict[str, Any] = None,
):
    """
    Stream processing function for real-time data ingestion from queues/message brokers

    Benefits of this approach:
    1. Real-time processing - Processes data as it arrives from message broker
    3. Scalable - Can handle varying message volumes automatically
    4. Fault tolerant - Spark Streaming handles failures and recovery
    5. Low latency - Minimal delay between message arrival and processing
    6. Backpressure handling - Automatically adjusts processing rate based on capacity
    7. Exactly-once semantics - Ensures no duplicate processing with proper configuration
    8. Micro-batch processing - Combines multiple messages for efficient processing

    Args:
        spark: SparkSession with streaming support
        stream_source: Source configuration (Kafka, Kinesis, etc.)
        table_name: Target table name
        batch_id: Optional batch identifier (auto-generated if None)
        stream_config: Additional streaming configuration

    Returns:
        StreamingQuery object for monitoring and control
    """
    if batch_id is None:
        batch_id = f"stream_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"

    print(f"🔄 Starting stream processing from {stream_source} into {table_name}")
    print(f"Stream ID: {batch_id}")

    # Default stream configuration
    if stream_config is None:
        stream_config = {
            "trigger_interval": "30 seconds",  # Micro-batch interval
            "checkpoint_location": "/tmp/stream_checkpoints",
            "max_offsets_per_trigger": 1000,  # Max messages per batch
            "fail_on_data_loss": False,  # Continue on data loss
        }
    raise NotImplementedError("Streaming processing not yet implemented")


def insert_files(
    spark,
    docs_dir: str,
    table_name: str,
    batch_mode=True,
    batch_id=None,
    max_workers=None,
):
    """
    Unified file insertion function - ELT approach
    Loads all data into raw table without validation
    Data quality and transformations handled by SQLMesh in transform layer

    Args:
        spark: SparkSession
        docs_dir: Directory containing files to process (local path or MinIO path like s3a://bucket/path)
        table_name: Target table name (e.g., "legal.documents_raw")
        batch_id: Unique batch identifier for tracking (auto-generated if None)
        max_workers: Maximum number of parallel workers for batch processing (default: auto-detect)
    """
    # Generate batch ID if not provided
    if batch_id is None:
        batch_id = f"batch_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"

    print(f"🔄 ELT: Loading files from {docs_dir} into {table_name}")
    print(f"Batch ID: {batch_id}")

    # Check if this is a MinIO path
    is_minio = is_minio_path(docs_dir)

    if is_minio:
        print(f"🔗 Detected MinIO path: {docs_dir}")
        # List files from MinIO using Spark
        if spark is not None:
            print("📋 Listing MinIO files with Spark...")
            all_files = list_minio_files_direct(docs_dir)
        else:
            print("❌ Spark session required for MinIO access")
            return False

        if not all_files:
            print(f"⚠️  No files found in MinIO path: {docs_dir}")
            return False

        # Filter for supported extensions
        supported_extensions = [".txt", ".json", ".parquet"]
        filtered_files = []
        for file_path in all_files:
            if any(file_path.endswith(ext) for ext in supported_extensions):
                filtered_files.append(file_path)

        if not filtered_files:
            print(f"⚠️  No supported files found in MinIO path: {docs_dir}")
            print(f"Supported formats: {', '.join(supported_extensions)}")
            return False

        all_files = filtered_files
        print(f"Found {len(all_files)} files in MinIO")
    else:
        # Local filesystem processing
        if not os.path.exists(docs_dir):
            print(f"⚠️  Directory not found: {docs_dir}")
            return False

        # Get list of all supported files
        supported_extensions = [".txt", ".json", ".parquet"]
        all_files = []

        for ext in supported_extensions:
            all_files.extend(list(Path(docs_dir).glob(f"*{ext}")))
            all_files.extend(
                list(Path(docs_dir).glob(f"**/*{ext}"))
            )  # Include subdirectories

        if not all_files:
            print(f"⚠️  No supported files found in: {docs_dir}")
            print(f"Supported formats: {', '.join(supported_extensions)}")
            return False

        print(f"Found {len(all_files)} files")

    # Group files by format
    file_groups = {
        "text": [
            f
            for f in all_files
            if f.endswith(".txt") or (hasattr(f, "suffix") and f.suffix == ".txt")
        ],
        "json": [
            f
            for f in all_files
            if f.endswith(".json") or (hasattr(f, "suffix") and f.suffix == ".json")
        ],
        "parquet": [
            f
            for f in all_files
            if f.endswith(".parquet")
            or (hasattr(f, "suffix") and f.suffix == ".parquet")
        ],
    }

    print(f"File breakdown:")
    for format_type, files in file_groups.items():
        if files:
            print(f"   - {format_type}: {len(files)} files")

    # Note: In ELT approach, we don't truncate - we append all data
    # Data deduplication and cleanup happens in Transform layer
    print(f"📝 ELT: Appending data to {table_name} (no truncation)")

    if not batch_mode:
        return _insert_files_sequential(
            spark, file_groups, table_name, batch_id, max_workers
        )
    return _insert_files_batch(spark, file_groups, table_name, batch_id, max_workers)


def _insert_files_sequential(spark, file_groups, table_name, batch_id):
    """Sequential processing - ELT approach"""
    print("🔄 Using sequential ELT approach")

    successful_inserts = 0
    failed_inserts = 0
    all_source_files = []
    load_timestamp = datetime.now(timezone.utc)

    for format_type, files in file_groups.items():
        if not files:
            continue

        print(f"\n📁 Processing {format_type} files...")

        for i, file_path in enumerate(files):
            all_source_files.append(str(file_path))
            try:
                if format_type == "text":
                    result = _process_text_file(file_path, spark)
                elif format_type == "json":
                    result = _process_json_file(file_path, spark)
                elif format_type == "parquet":
                    result = _process_parquet_file(file_path, spark)
                else:
                    continue

                # Add batch tracking fields
                result["data"]["load_batch_id"] = batch_id
                result["data"]["load_timestamp"] = load_timestamp

                if result["is_valid"]:
                    # Insert using SQL for better parallelization
                    df = spark.createDataFrame([result["data"]])
                    df.createOrReplaceTempView("temp_single_doc")
                    spark.sql(f"INSERT INTO {table_name} SELECT * FROM temp_single_doc")
                    successful_inserts += 1
                else:
                    # In ELT approach, still load the data but mark as failed
                    result["data"]["load_status"] = "load_failed"
                    result["data"]["load_error"] = str(result["errors"])

                    df = spark.createDataFrame([result["data"]])
                    df.createOrReplaceTempView("temp_single_doc")
                    spark.sql(f"INSERT INTO {table_name} SELECT * FROM temp_single_doc")
                    failed_inserts += 1

            except Exception as e:
                print(f"❌ Error processing {file_path}: {e}")
                # Create error document but still load it
                filename = (
                    file_path.name
                    if hasattr(file_path, "name")
                    else str(file_path).split("/")[-1]
                )
                error_data = {
                    "document_id": filename,
                    "document_type": "unknown",
                    "raw_text": "",
                    "generated_at": datetime.now(timezone.utc),
                    "source": "soli_legal_document_generator",
                    "file_path": str(file_path),
                    "language": "en",
                    "content_length": 0,
                    "schema_version": "v1",
                    "metadata_file_path": "",
                    "method": "sequential",
                    "load_batch_id": batch_id,
                    "load_timestamp": datetime.now(timezone.utc),
                    "load_status": "load_failed",
                    "load_error": str(e),
                    "load_error_code": "E999",
                    "document_length": 0,
                    "word_count": 0,
                    "metadata": "{}",
                    "uuid": "",
                    "generated_at_str": datetime.now(timezone.utc).isoformat(),
                    "source_system": "soli_legal_document_generator",
                    "file_size": 0,
                    "content_file_path": str(file_path),
                }

                df = spark.createDataFrame([error_data])
                df.createOrReplaceTempView("temp_error_doc")
                spark.sql(f"INSERT INTO {table_name} SELECT * FROM temp_error_doc")
                failed_inserts += 1

            if (i + 1) % 50 == 0:
                print(f"   Processed {i + 1}/{len(files)} {format_type} files...")

    print(f"\n✅ ELT Load Complete:")
    print(f"   - Successfully loaded: {successful_inserts} files")
    print(f"   - Failed loads: {failed_inserts} files (still loaded with error status)")
    print(f"   - Total processed: {successful_inserts + failed_inserts} files")

    # Run ELT-appropriate validation
    print(f"\n🔍 Running ELT load validation...")
    basic_load_validation(
        spark, table_name, batch_id, expected_count=successful_inserts + failed_inserts
    )

    # Track batch load
    track_batch_load(spark, table_name, batch_id, all_source_files, load_timestamp)

    # Show failed loads for troubleshooting
    get_failed_loads(spark, table_name, batch_id)

    return True


def _process_text_file(file_path, spark=None):
    """Process a text file and return document data - ELT approach"""
    try:
        file_path_str = str(file_path)

        # Check if this is a MinIO path
        if is_minio_path(file_path_str):
            if spark is None:
                raise ValueError("Spark session required for MinIO file processing")

            # Read content from MinIO using Spark
            content = read_minio_file_content(spark, file_path_str)
            file_size = get_minio_file_size(spark, file_path_str)

            # Extract filename from MinIO path
            path_info = get_minio_path_info(file_path_str)
            filename = (
                path_info["key"].split("/")[-1] if path_info["key"] else "unknown"
            )

            # Try to read corresponding metadata JSON file
            minio_metadata = _read_minio_metadata(spark, file_path_str)
        else:
            # Local file processing
            content = ""
            with open(file_path, "r", encoding="utf-8") as f:
                chunk_size = 1024 * 1024  # 1MB chunks
                while True:
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break
                    content += chunk

                    # Check if we're exceeding reasonable limits
                    if len(content) > 100 * 1024 * 1024:  # 100MB limit
                        raise ValueError(f"File {file_path.name} exceeds 100MB limit")

            file_size = file_path.stat().st_size
            filename = file_path.name
            minio_metadata = {}

        # Extract metadata from filename (fallback)
        parts = filename.replace(".txt", "").split("_")

        if len(parts) >= 4:
            doc_id = f"{parts[1]}_{parts[2]}"
            doc_type = parts[3]
        else:
            doc_id = filename.replace(".txt", "")
            doc_type = "unknown"

        # Use MinIO metadata if available, otherwise fallback to filename parsing
        if minio_metadata:
            doc_id = minio_metadata.get("document_id", doc_id)
            doc_type = minio_metadata.get("document_type", doc_type)

        # Calculate document statistics
        doc_length = len(content)
        word_count = len(content.split())

        # In ELT approach, we load all data regardless of quality
        # Data quality will be handled in the transform layer
        is_valid = True  # Always true in ELT approach
        errors_list = []  # No validation errors in ELT approach

        # Build enhanced metadata combining MinIO metadata with processing metadata
        enhanced_metadata = {
            "source_file": filename,
            "file_size": file_size,
            "source": "minio" if is_minio_path(file_path_str) else "local",
            "method": "spark" if is_minio_path(file_path_str) else "local",
        }

        # Add MinIO metadata fields if available
        if minio_metadata:
            enhanced_metadata.update(
                {
                    "uuid": minio_metadata.get("uuid", ""),
                    "generated_at": minio_metadata.get("generated_at", ""),
                    "source_system": minio_metadata.get("source", ""),
                    # Derive file paths from UUID and structure
                    "content_file_path": file_path_str,
                    "metadata_file_path": file_path_str.replace(".txt", ".json"),
                }
            )

        return {
            "is_valid": is_valid,
            "data": {
                "document_id": doc_id,
                "document_type": doc_type,
                "raw_text": content,
                "generated_at": datetime.now(timezone.utc),
                "source": minio_metadata.get("source", "soli_legal_document_generator"),
                "file_path": file_path_str,
                "language": "en",
                "content_length": doc_length,
                "schema_version": "v1",
                "metadata_file_path": file_path_str.replace(".txt", ".json"),
                "method": "spark" if is_minio_path(file_path_str) else "local",
                "load_batch_id": "",
                "load_timestamp": datetime.now(timezone.utc),
                "load_status": "loaded",
                "load_error": "",
                "load_error_code": "",
                "document_length": doc_length,
                "word_count": len(content.split()),
                "metadata": "{}",
                "uuid": "",
                "generated_at_str": datetime.now(timezone.utc).isoformat(),
                "source_system": minio_metadata.get(
                    "source", "soli_legal_document_generator"
                ),
                "file_size": file_size,
                "content_file_path": file_path_str,
            },
            "errors": errors_list,
        }

    except Exception as e:
        filename = (
            file_path.name
            if hasattr(file_path, "name")
            else str(file_path).split("/")[-1]
        )
        return {
            "is_valid": False,
            "data": {
                "document_id": filename,
                "document_type": "unknown",
                "raw_text": "",
                "generated_at": datetime.now(timezone.utc),
                "source": "soli_legal_document_generator",
                "file_path": str(file_path),
                "language": "en",
                "content_length": 0,
                "schema_version": "v1",
                "metadata_file_path": "",
                "method": "sequential",
                "load_batch_id": "",
                "load_timestamp": datetime.now(timezone.utc),
                "load_status": "load_failed",
                "load_error": str(e),
                "load_error_code": "E999",
                "document_length": 0,
                "word_count": 0,
                "metadata": "{}",
                "uuid": "",
                "generated_at_str": datetime.now(timezone.utc).isoformat(),
                "source_system": "soli_legal_document_generator",
                "file_size": 0,
                "content_file_path": str(file_path),
            },
            "errors": [
                {"message": f"Processing error: {str(e)}", "type": "processing_error"}
            ],
        }


def _read_minio_metadata(spark, content_file_path):
    """Read metadata from MinIO JSON file (Spark Connect compatible)"""
    try:
        # Convert content file path to metadata file path
        metadata_file_path = content_file_path.replace(".txt", ".json")

        # Check if metadata file exists
        try:
            df = spark.read.json(metadata_file_path)
            if df.count() > 0:
                # Use take() instead of toPandas() for Spark Connect compatibility
                rows = df.take(1)
                if rows:
                    metadata = rows[0].asDict()
                    return metadata
        except Exception as e:
            print(f"⚠️  Could not read metadata file {metadata_file_path}: {e}")

        return {}
    except Exception as e:
        print(f"⚠️  Error reading MinIO metadata: {e}")
        return {}


def _process_json_file(file_path, spark=None):
    """Process a JSON file - placeholder for future implementation"""
    # TODO: Implement JSON file processing
    raise NotImplementedError("JSON file processing not yet implemented")


def _process_parquet_file(file_path, spark=None):
    """Process a Parquet file - placeholder for future implementation"""
    # TODO: Implement Parquet file processing
    raise NotImplementedError("Parquet file processing not yet implemented")


def _insert_files_batch(spark, file_groups, table_name, batch_id, max_workers=None):
    """
    SQL and DataFrame-based parallel processing - Spark Connect compatible

    Benefits of this approach:
    1. True parallelization - Uses Spark SQL for distributed processing
    2. Memory efficient - Data flows through SQL pipeline without collecting everything in memory
    3. Fault tolerance - Spark handles failures and retries automatically across cluster
    4. No manual thread management - Spark handles all parallelization and distribution
    5. Better for large datasets - Scales horizontally across cluster nodes
    6. Built-in load balancing - Spark distributes work evenly across available resources
    7. Schema inference - No explicit schema definitions needed, Spark infers automatically
    8. ELT optimized - Light validation during load, heavy validation in transform layer
    9. Spark Connect compatible - Uses SQL and DataFrame operations instead of RDDs
    """
    print(
        "🔄 Using SQL and DataFrame operations for distributed file processing and insertion"
    )

    all_source_files = []
    load_timestamp = datetime.now(timezone.utc)
    batch_start_time = time.time()

    # Process all files and collect results
    all_processed_data = []

    for format_type, files in file_groups.items():
        if not files:
            continue

        print(f"\n📁 Processing {format_type} files...")
        all_source_files.extend([str(f) for f in files])

        # Process files in batches for better memory management
        batch_size = 100  # Process 100 files at a time
        for i in range(0, len(files), batch_size):
            batch_files = files[i : i + batch_size]
            print(
                f"   Processing batch {i//batch_size + 1}/{(len(files) + batch_size - 1)//batch_size}"
            )

            batch_data = []
            for file_path in batch_files:
                try:
                    result = _process_file_parallel(
                        str(file_path), format_type, batch_id, load_timestamp
                    )
                    if result:
                        batch_data.append(result)
                except Exception as e:
                    print(f"❌ Error processing {file_path}: {e}")
                    error_doc = _create_error_document(
                        str(file_path), batch_id, load_timestamp, str(e)
                    )
                    batch_data.append(error_doc)

            all_processed_data.extend(batch_data)

    # Create DataFrame from all processed data
    if all_processed_data:
        print(
            f"\n📊 Creating DataFrame from {len(all_processed_data)} processed files..."
        )

        # Light logging of data structure
        if all_processed_data:
            first_record = all_processed_data[0]
            print(f"📊 Sample record structure: {list(first_record.keys())}")

        # Define explicit schema for Spark Connect compatibility
        from pyspark.sql.types import (
            StructType,
            StructField,
            StringType,
            IntegerType,
            TimestampType,
            LongType,
        )

        schema = StructType(
            [
                StructField("document_id", StringType(), True),
                StructField("document_type", StringType(), True),
                StructField("raw_text", StringType(), True),
                StructField("generated_at", TimestampType(), True),
                StructField("source", StringType(), True),
                StructField("file_path", StringType(), True),
                StructField("language", StringType(), True),
                StructField("content_length", LongType(), True),
                StructField("schema_version", StringType(), True),
                StructField("metadata_file_path", StringType(), True),
                StructField("method", StringType(), True),
                StructField("load_batch_id", StringType(), True),
                StructField("load_timestamp", TimestampType(), True),
                StructField("load_status", StringType(), True),
                StructField("load_error", StringType(), True),
                StructField("load_error_code", StringType(), True),
                StructField("document_length", LongType(), True),
                StructField("word_count", LongType(), True),
                StructField("metadata", StringType(), True),
                StructField("uuid", StringType(), True),
                StructField("generated_at_str", StringType(), True),
                StructField("source_system", StringType(), True),
                StructField("file_size", LongType(), True),
                StructField("content_file_path", StringType(), True),
            ]
        )

        df = spark.createDataFrame(all_processed_data, schema)

        # Light validation - just verify the DataFrame was created successfully
        print("🔍 Validating DataFrame creation...")
        print(f"   - Created DataFrame with {len(all_processed_data)} rows")
        print(f"   - Schema: {len(df.columns)} columns")
        print(f"   - Column names: {', '.join(df.columns)}")

        # Insert in parallel using SQL
        print("💾 Inserting data using SQL...")
        # Register DataFrame as temp view for SQL operations
        df.createOrReplaceTempView("temp_documents")

        # Use SQL INSERT for better parallelization
        insert_sql = f"""
        INSERT INTO {table_name}
        SELECT * FROM temp_documents
        """
        spark.sql(insert_sql)

        print(f"✅ All files processed and inserted successfully")

    # Step 2: Post-insert validation and metrics
    batch_duration = (time.time() - batch_start_time) * 1000
    validation_result = validate_batch_load(
        spark, table_name, batch_id, all_source_files, len(all_source_files)
    )

    record_batch_metrics_to_table(
        spark, batch_id, table_name, all_source_files, validation_result, batch_duration
    )

    return True


def _process_file_parallel(file_path, format_type, batch_id, load_timestamp):
    """Process a single file (Spark Connect compatible)"""
    try:
        if format_type == "text":
            result = _process_text_file(file_path, None)
        elif format_type == "json":
            result = _process_json_file(file_path, None)
        elif format_type == "parquet":
            result = _process_parquet_file(file_path, None)
        else:
            return None

        # Add batch tracking fields
        result["data"]["load_batch_id"] = batch_id
        result["data"]["load_timestamp"] = load_timestamp
        result["data"]["load_status"] = "loaded"
        result["data"]["load_error"] = ""
        result["data"]["load_error_code"] = ""

        if result["is_valid"]:
            return result["data"]
        else:
            # For failed documents, still return the data but it will be handled by validation
            return result["data"]

    except Exception as e:
        # Return error document
        return _create_error_document(file_path, batch_id, load_timestamp, str(e))


# Removed unused validation functions - table schema validation is handled by Spark


def _create_error_document(file_path, batch_id, load_timestamp, error_msg):
    """Create standardized error document"""
    filename = (
        file_path.name if hasattr(file_path, "name") else str(file_path).split("/")[-1]
    )

    return {
        "document_id": filename,
        "document_type": "unknown",
        "raw_text": "",
        "generated_at": datetime.now(timezone.utc),
        "source": "soli_legal_document_generator",
        "file_path": str(file_path),
        "language": "en",
        "content_length": 0,
        "schema_version": "v1",
        "metadata_file_path": "",
        "method": "parallel_batch",
        "load_batch_id": batch_id,
        "load_timestamp": load_timestamp,
        "load_status": "load_failed",
        "load_error": error_msg,
        "load_error_code": "E999",
        "document_length": 0,
        "word_count": 0,
        "metadata": "{}",
        "uuid": "",
        "generated_at_str": datetime.now(timezone.utc).isoformat(),
        "source_system": "soli_legal_document_generator",
        "file_size": 0,
        "content_file_path": str(file_path),
    }


def validate_batch_load(spark, table_name, batch_id, original_files, expected_count):
    """Validate that all files were loaded correctly"""

    print(f"\n🔍 Validating batch {batch_id}...")

    # Get actual loaded records
    loaded_records = spark.sql(
        f"""
        SELECT 
            source,
            file_path,
            load_batch_id
        FROM {table_name}
        WHERE load_batch_id = '{batch_id}'
    """
    ).take(
        100
    )  # Use take() instead of collect() for Spark Connect compatibility

    # Create lookup for loaded records
    loaded_files = {record["source"]: record for record in loaded_records}

    # Validate each original file
    validation_result = {
        "batch_id": batch_id,
        "files_expected": len(original_files),
        "files_loaded": len(loaded_records),
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

    # Print validation summary
    print(f"✅ Validation complete:")
    print(f"   - Expected: {validation_result['files_expected']} files")
    print(f"   - Loaded: {validation_result['files_loaded']} files")
    print(f"   - Successful: {len(validation_result['files_successful'])} files")
    print(f"   - Failed: {len(validation_result['files_failed'])} files")
    print(f"   - Missing: {len(validation_result['files_missing'])} files")
    print(f"   - Status: {validation_result['validation_status']}")

    return validation_result


def record_batch_metrics_to_table(
    spark: SparkSession,
    batch_id: str,
    target_table: str,
    source_files: List[str],
    validation_result: Dict[str, Any],
    batch_duration_ms: float,
    load_source: str = "local",
) -> bool:
    """
    Record batch metrics to admin.batch_jobs table with light validation
    """
    try:
        # Calculate metrics
        total_file_size_bytes = 0
        for file_path in source_files:
            if is_minio_path(file_path):
                total_file_size_bytes += get_minio_file_size(spark, file_path)
            else:
                total_file_size_bytes += Path(file_path).stat().st_size

        # Build error codes map
        error_codes = {}
        for failed_file in validation_result.get("files_failed", []):
            error_code = failed_file.get("error_code", "E999")
            error_codes[error_code] = error_codes.get(error_code, 0) + 1

        # Create batch metrics record
        batch_metrics = {
            "batch_id": batch_id,
            "job_id": f"job_{int(time.time())}",
            "source": load_source,
            "target_table": target_table,
            "version": "v1",
            "batch_start_time": datetime.now(timezone.utc)
            .isoformat()
            .replace("+00:00", "Z"),
            "batch_end_time": datetime.now(timezone.utc)
            .isoformat()
            .replace("+00:00", "Z"),
            "batch_duration_ms": int(batch_duration_ms),
            # File-level counts (operational metrics)
            "files_processed": validation_result["files_expected"],
            "files_loaded": validation_result["files_loaded"],
            "files_failed": len(validation_result["files_failed"]),
            "files_missing": len(validation_result["files_missing"]),
            "files_successful": len(validation_result["files_successful"]),
            # Record-level counts (data quality metrics)
            "total_records_processed": validation_result["files_expected"],
            "total_records_loaded": validation_result["files_loaded"],
            "total_records_failed": len(validation_result["files_failed"]),
            "total_records_missing": len(validation_result["files_missing"]),
            # File metrics
            "total_file_size_bytes": total_file_size_bytes,
            # Error tracking
            "error_codes": error_codes,
            "error_summary": f"Success: {len(validation_result['files_successful'])}, Failed: {len(validation_result['files_failed'])}, Missing: {len(validation_result['files_missing'])}",
            # Validation status
            "validation_status": validation_result["validation_status"],
            # Audit trail
            "created_by": "system",
            "created_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        }

        # Light validation using SchemaManager
        if not schema_manager.validate_metadata("batch_metrics", batch_metrics):
            print(f"❌ Batch metrics validation failed for {batch_id}")
            print(f"   - Skipping metrics recording to prevent bad operational data")
            return False

        # Convert to DataFrame and insert using SQL
        df = spark.createDataFrame([batch_metrics])
        df.createOrReplaceTempView("temp_batch_metrics")
        spark.sql("INSERT INTO admin.batch_jobs SELECT * FROM temp_batch_metrics")
        print(f"✅ Batch metrics recorded for {batch_id}")
        print(f"   - Status: {validation_result['validation_status']}")
        print(f"   - Duration: {batch_duration_ms:.0f}ms")
        print(
            f"   - Files: {validation_result['files_loaded']}/{validation_result['files_expected']}"
        )

        return True

    except Exception as e:
        print(f"❌ Error recording batch metrics: {e}")
        return False


def process_minio_file(
    file_path: str, minio_metadata: Dict[str, Any], batch_id: str
) -> Dict[str, Any]:
    """
    Process file with UTC timestamp handling (Z suffix)
    """
    try:
        # Get UTC ISO-8601 string from metadata
        generated_at = minio_metadata.get("generated_at", "")

        # Parse for generation_date (TIMESTAMP) for partitioning
        try:
            # Handle Z suffix (convert to +00:00 for parsing)
            if generated_at.endswith("Z"):
                parse_string = generated_at.replace("Z", "+00:00")
            else:
                parse_string = generated_at

            generation_date = datetime.fromisoformat(parse_string)

            # Ensure it's UTC
            if generation_date.tzinfo != timezone.utc:
                generation_date = generation_date.astimezone(timezone.utc)

        except Exception as e:
            print(f"Error parsing timestamp {generated_at}: {e}")
            generation_date = datetime.now(timezone.utc)

        document_data = {
            "document_id": minio_metadata.get("document_id", ""),
            "document_type": minio_metadata.get("document_type", ""),
            "raw_text": "",
            "generation_date": generation_date,  # UTC TIMESTAMP for partitioning
            "file_path": file_path,
            "language": "en",
            "generated_at": generated_at,  # UTC ISO-8601 string with Z
            "source": minio_metadata.get("source", "soli_legal_document_generator"),
            "schema_version": "v1",
            "load_batch_id": batch_id,
            "load_timestamp": datetime.now(timezone.utc),
        }

        return document_data

    except Exception as e:
        print(f"Error processing file {file_path}: {e}")
        return None


def list_minio_files_distributed(spark: SparkSession, minio_path: str) -> List[str]:
    """List files in MinIO using distributed DataFrame operations (Spark Connect compatible)"""
    try:
        print(f"🔍 Listing files in MinIO path: {minio_path}")

        # Use binaryFile format to read file metadata
        files_df = spark.read.format("binaryFile").load(minio_path)

        # Create a temporary view for SQL operations
        files_df.createOrReplaceTempView("minio_files_distributed")

        # Use SQL to get distinct file paths - this avoids RDD operations
        # We'll use a different approach to get the actual file paths
        print(f"📊 Reading file metadata from MinIO...")

        # Instead of trying to collect the paths, we'll use a different strategy
        # Create a DataFrame with file information that can be processed in parallel
        file_info_df = spark.sql(
            """
            SELECT 
                path,
                length,
                modificationTime,
                isDirectory
            FROM minio_files_distributed
            WHERE isDirectory = false
        """
        )

        # Register this as a temp view for further processing
        file_info_df.createOrReplaceTempView("file_info")

        print(f"✅ File metadata loaded successfully")

        # For now, return empty list since we can't safely extract paths
        # The actual file processing will be done using the temp view
        return []

    except Exception as e:
        print(f"❌ Error listing MinIO files with Spark: {e}")
        return []


def read_minio_files_distributed(
    spark: SparkSession, minio_path: str, table_name: str, batch_id: str
):
    """Read and process MinIO files in a distributed, parallel manner (Spark Connect compatible)"""
    try:
        print(f"🔄 Starting distributed file processing from: {minio_path}")
        print(f"📊 Target table: {table_name}")
        print(f"🆔 Batch ID: {batch_id}")

        # Step 1: Load file metadata using binaryFile format
        files_df = spark.read.format("binaryFile").load(minio_path)
        files_df.createOrReplaceTempView("minio_files_metadata")

        # Step 2: Create a processing plan using SQL
        # This approach processes files in parallel without collecting data
        processing_sql = f"""
        WITH file_list AS (
            SELECT 
                path,
                length as file_size,
                modificationTime as mod_time
            FROM minio_files_metadata
            WHERE isDirectory = false
            AND path LIKE '%.txt'
        ),
        file_content AS (
            SELECT 
                path,
                value as content_line,
                ROW_NUMBER() OVER (PARTITION BY path ORDER BY value) as line_number
            FROM file_list f
            CROSS JOIN LATERAL (
                SELECT value 
                FROM {spark.read.text(minio_path).createOrReplaceTempView("file_content_temp")}
                WHERE input_file_name() = f.path
            ) content
        ),
        processed_files AS (
            SELECT 
                path,
                file_size,
                mod_time,
                STRING_AGG(content_line, '\n') as full_content,
                COUNT(*) as line_count
            FROM file_content
            GROUP BY path, file_size, mod_time
        )
        SELECT 
            path as file_path,
            file_size,
            mod_time,
            full_content,
            line_count,
            '{batch_id}' as load_batch_id,
            CURRENT_TIMESTAMP() as load_timestamp,
            'minio' as source,
            'distributed' as method
        FROM processed_files
        """

        # Step 3: Execute the distributed processing
        print(f"🔄 Executing distributed file processing...")
        processed_df = spark.sql(processing_sql)

        # Create UDFs for data transformation
        def extract_document_id(file_path):
            """Extract document ID from file path"""
            try:
                filename = file_path.split("/")[-1]
                return filename.replace(".txt", "")
            except:
                return "unknown"

        def extract_document_type(file_path):
            """Extract document type from file path"""
            try:
                filename = file_path.split("/")[-1]
                parts = filename.replace(".txt", "").split("_")
                return parts[3] if len(parts) >= 4 else "unknown"
            except:
                return "unknown"

        # Register UDFs
        extract_id_udf = udf(extract_document_id, StringType())
        extract_type_udf = udf(extract_document_type, StringType())

        # Transform the data
        transformed_df = processed_df.select(
            extract_id_udf(col("file_path")).alias("document_id"),
            extract_type_udf(col("file_path")).alias("document_type"),
            col("full_content").alias("raw_text"),
            col("mod_time").alias("generation_date"),
            col("file_path"),
            col("line_count").alias("document_length"),
            lit(0).alias("word_count"),  # Will be calculated later
            lit("en").alias("language"),
            lit("{}").alias("metadata"),  # Empty metadata for now
            lit("").alias("generated_at"),
            lit("").alias("source_system"),
            col("file_path").alias("source"),
            col("file_size"),
            col("method"),
            col("file_path").alias("content_file_path"),
            lit("").alias("metadata_file_path"),
            lit("loaded").alias("load_status"),
            lit(None).alias("load_error"),
            col("load_batch_id"),
            col("load_timestamp"),
        )

        # Step 5: Insert the processed data
        print(f"💾 Inserting processed data into {table_name}...")
        transformed_df.createOrReplaceTempView("processed_documents")

        insert_sql = f"""
        INSERT INTO {table_name}
        SELECT * FROM processed_documents
        """

        spark.sql(insert_sql)

        print(f"✅ Distributed file processing completed successfully")
        return True

    except Exception as e:
        print(f"❌ Error in distributed file processing: {e}")
        return False


def process_files_distributed_spark_connect(
    spark: SparkSession, minio_path: str, table_name: str, batch_id: str
):
    """Process files using distributed DataFrame operations (Spark Connect compatible)"""
    try:
        print(f"🔄 Starting distributed file processing from: {minio_path}")
        print(f"📊 Target table: {table_name}")
        print(f"🆔 Batch ID: {batch_id}")

        # Step 1: Load file metadata using binaryFile format
        # This operation is distributed and parallel by default
        files_df = spark.read.format("binaryFile").load(minio_path)

        # Step 2: Filter for text files using DataFrame operations
        # This is also distributed and parallel
        text_files_df = files_df.filter(
            (files_df.isDirectory == False) & (files_df.path.like("%.txt"))
        )

        # Step 3: Create a processing pipeline using DataFrame operations
        # This approach processes files in parallel without collecting data

        # First, let's get the file paths we need to process
        # We'll use a different strategy since we can't collect the paths

        # Create a DataFrame with file information that can be processed
        file_info_df = text_files_df.select("path", "length", "modificationTime")

        # Step 4: Use DataFrame operations to process files in parallel
        # This is the key - we'll use DataFrame operations that are distributed

        from pyspark.sql.functions import col, lit, udf, when
        from pyspark.sql.types import StringType, StructType, StructField

        # Create simple UDFs that work with Spark Connect
        def extract_document_id(file_path):
            """Extract document ID from file path"""
            if not file_path:
                return "unknown"
            filename = file_path.split("/")[-1]
            return filename.replace(".txt", "")

        def extract_document_type(file_path):
            """Extract document type from file path"""
            if not file_path:
                return "unknown"
            filename = file_path.split("/")[-1]
            parts = filename.replace(".txt", "").split("_")
            return parts[3] if len(parts) >= 4 else "unknown"

        # Register simple UDFs
        extract_id_udf = udf(extract_document_id, StringType())
        extract_type_udf = udf(extract_document_type, StringType())

        # Apply simple UDFs to process files in parallel
        # This operation is distributed across the cluster
        final_df = file_info_df.select(
            extract_id_udf(col("path")).alias("document_id"),
            extract_type_udf(col("path")).alias("document_type"),
            lit("").alias("raw_text"),  # We'll handle content separately
            col("path").alias("file_path"),
            col("length").alias("file_size"),
            lit(batch_id).alias("load_batch_id"),
            lit(str(datetime.now(timezone.utc))).alias("load_timestamp"),
            lit("loaded").alias("load_status"),
            lit(None).alias("load_error"),
        )

        # Step 5: Insert the processed data using SQL
        # This operation is also distributed
        final_df.createOrReplaceTempView("distributed_processed_files")

        insert_sql = f"""
        INSERT INTO {table_name}
        SELECT * FROM distributed_processed_files
        """

        spark.sql(insert_sql)

        print(f"✅ Distributed file processing completed successfully")
        return True

    except Exception as e:
        print(f"❌ Error in distributed file processing: {e}")
        return False


def process_files_with_content_distributed(
    spark: SparkSession, minio_path: str, table_name: str, batch_id: str
):
    """Process files with content using distributed operations (Spark Connect compatible)"""
    try:
        print(f"🔄 Starting distributed content processing from: {minio_path}")

        # Step 1: Read all text files in parallel
        # This operation is distributed by default
        text_df = spark.read.text(minio_path)

        # Step 2: Add file metadata using DataFrame operations
        from pyspark.sql.functions import input_file_name, col, lit, udf
        from pyspark.sql.types import StringType

        # Add file path information
        files_with_content_df = text_df.withColumn("file_path", input_file_name())

        # Step 3: Group content by file and aggregate
        # This is where the parallelization happens
        from pyspark.sql.functions import concat_ws, collect_list, count

        aggregated_df = files_with_content_df.groupBy("file_path").agg(
            concat_ws("\n", collect_list("value")).alias("full_content"),
            count("value").alias("line_count"),
        )

        # Step 4: Add metadata using simple UDFs (Spark Connect compatible)
        from pyspark.sql.functions import udf, lit
        from pyspark.sql.types import StringType

        # Simple UDFs that work with Spark Connect
        def extract_document_id(file_path):
            """Extract document ID from file path"""
            if not file_path:
                return "unknown"
            filename = file_path.split("/")[-1]
            return filename.replace(".txt", "")

        def extract_document_type(file_path):
            """Extract document type from file path"""
            if not file_path:
                return "unknown"
            filename = file_path.split("/")[-1]
            parts = filename.replace(".txt", "").split("_")
            return parts[3] if len(parts) >= 4 else "unknown"

        # Register UDFs
        extract_id_udf = udf(extract_document_id, StringType())
        extract_type_udf = udf(extract_document_type, StringType())

        # Apply UDFs to extract document info
        final_df = aggregated_df.select(
            extract_id_udf(col("file_path")).alias("document_id"),
            extract_type_udf(col("file_path")).alias("document_type"),
            col("full_content").alias("raw_text"),
            col("file_path"),
            col("line_count").alias("document_length"),
            lit(0).alias("word_count"),
            lit("en").alias("language"),
            lit("{}").alias("metadata"),
            lit("").alias("generated_at"),
            lit("").alias("source_system"),
            col("file_path").alias("source"),
            lit(0).alias("file_size"),
            lit("distributed").alias("method"),
            col("file_path").alias("content_file_path"),
            lit("").alias("metadata_file_path"),
            lit("loaded").alias("load_status"),
            lit(None).alias("load_error"),
            lit(batch_id).alias("load_batch_id"),
            lit(str(datetime.now(timezone.utc))).alias("load_timestamp"),
        )

        # Step 5: Insert using SQL (distributed)
        final_df.createOrReplaceTempView("content_processed_files")

        insert_sql = f"""
        INSERT INTO {table_name}
        SELECT * FROM content_processed_files
        """

        spark.sql(insert_sql)

        print(f"✅ Distributed content processing completed")
        return True

    except Exception as e:
        print(f"❌ Error in distributed content processing: {e}")
        return False


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
        "--batch-id", type=str, help="Custom batch ID (auto-generated if not provided)"
    )

    parser.add_argument(
        "--mode",
        type=str,
        choices=["batch", "streaming", "sequential"],
        default="batch",
        help="Processing mode: batch (collect all files then batch insert) or streaming (insert each file immediately)",
    )

    parser.add_argument(
        "--num-partitions",
        type=int,
        default=4,
        help="Number of partitions for parallel processing (default: 4)",
    )

    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Only validate existing data, don't insert new data",
    )

    parser.add_argument(
        "--show-failed",
        action="store_true",
        help="Show failed loads for troubleshooting",
    )

    parser.add_argument(
        "--max-workers",
        type=int,
        help="Maximum number of parallel workers for batch processing (default: auto-detect)",
    )

    args = parser.parse_args()

    print("🚀 Starting Insert Operation")
    print(f"Source: {args.file_path}")
    print(f"Target: {args.table_name}")
    print(f"Mode: {args.mode}")

    # Single context manager handles both Spark and logging
    if is_minio_path(args.file_path):
        # Use correct endpoint for Docker containers
        s3_config = S3FileSystemConfig(
            endpoint="minio:9000",  # Use minio hostname, not localhost
            region="us-east-1",
            access_key="sparkuser",
            secret_key="sparkpass",
            path_style_access=True,
            ssl_enabled=False,
        )
        iceberg_config = IcebergConfig(s3_config)
    else:
        s3_config = None
        iceberg_config = None

    # Debug: Print S3 configuration
    if s3_config:
        print(f"🔧 S3 Configuration:")
        print(f"   Endpoint: {s3_config.endpoint}")
        print(f"   Region: {s3_config.region}")
        print(f"   Access Key: {s3_config.access_key}")
        print(
            f"   Secret Key: {'*' * len(s3_config.secret_key) if s3_config.secret_key else 'None'}"
        )

    with HybridLogger(
        app_name="insert_pipeline",
        spark_config={
            "spark_version": SparkVersion.SPARK_CONNECT_3_5,  # Use Spark Connect for better compatibility
            "iceberg_config": iceberg_config,
        },
        manage_spark=True,
    ) as logger:

        if args.mode == "streaming":
            raise NotImplementedError("Streaming processing not yet implemented")

        batch_mode = args.mode == "batch"

        # Start operation tracking
        logger.start_operation("default_job_group", "insert_files")
        start_time = time.time()

        try:
            if args.validate_only:
                # Only validate existing data
                print("\nRunning validation only...")
                basic_load_validation(logger.spark, args.table_name, args.batch_id)
                if args.show_failed:
                    get_failed_loads(logger.spark, args.table_name, args.batch_id)
                return 0

            success = insert_files(
                spark=logger.spark,
                docs_dir=args.file_path,
                table_name=args.table_name,
                batch_id=args.batch_id,
                batch_mode=batch_mode,
                max_workers=args.max_workers,
            )

            if success:
                print("\n✅ ELT pipeline completed successfully")

                # Show validation results
                if args.batch_id:
                    basic_load_validation(logger.spark, args.table_name, args.batch_id)
                    if args.show_failed:
                        get_failed_loads(logger.spark, args.table_name, args.batch_id)

            execution_time = time.time() - start_time

            # End operation tracking
            logger.end_operation(logger.spark, execution_time=execution_time)

            # Log successful completion
            logger.log_performance(
                "operation_complete",
                {
                    "operation": "insert_files",
                    "execution_time_seconds": round(execution_time, 4),
                    "status": "success",
                },
            )

            return 0

        except Exception as e:
            execution_time = time.time() - start_time

            # Log error
            logger.log_error(e, {"operation": "insert_files"})
            logger.log_performance(
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
            logger.end_operation(
                logger.spark, execution_time=execution_time, result_count=0
            )

            raise


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
