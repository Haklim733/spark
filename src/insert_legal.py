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
from src.utils.logger import HybridLogger
from src.utils.session import (
    SparkVersion,
    S3FileSystemConfig,
)

# Import shared legal document models
from src.schemas.schema import SchemaManager

# Initialize SchemaManager
schema_manager = SchemaManager()

# Cache valid document types to avoid repeated SchemaManager calls
_VALID_DOCUMENT_TYPES: Set[str] = set()


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


def list_minio_files(spark: SparkSession, minio_path: str) -> List[str]:
    """List files in MinIO bucket/path using Spark"""
    try:
        # Use Spark to list files in MinIO
        files_df = spark.read.format("binaryFile").load(minio_path)
        file_paths = [row.path for row in files_df.select("path").collect()]
        return file_paths
    except Exception as e:
        print(f"❌ Error listing MinIO files with Spark: {e}")
        return []


def read_minio_file_content(spark: SparkSession, file_path: str) -> str:
    """Read content from a MinIO file using Spark"""
    try:
        # Read as text file using Spark
        df = spark.read.text(file_path)
        content = "\n".join([row.value for row in df.collect()])
        return content
    except Exception as e:
        print(f"❌ Error reading MinIO file {file_path} with Spark: {e}")
        return ""


def get_minio_file_size(spark: SparkSession, file_path: str) -> int:
    """Get file size from MinIO using Spark"""
    try:
        # Use binaryFile format to get file metadata
        df = spark.read.format("binaryFile").load(file_path)
        metadata = df.select("length").collect()
        if metadata:
            return metadata[0]["length"]
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
        # 1. Check if table exists
        tables = spark.sql(f"SHOW TABLES IN {table_name.split('.')[0]}")
        table_exists = (
            tables.filter(f"tableName = '{table_name.split('.')[1]}'").count() > 0
        )

        if not table_exists:
            print(f"❌ Table {table_name} does not exist!")
            return False

        print(f"✅ Table {table_name} exists")

        # 2. Get total row count
        count_result = spark.sql(f"SELECT COUNT(*) as row_count FROM {table_name}")
        total_count = count_result.collect()[0]["row_count"]
        print(f"📊 Total rows in table: {total_count:,}")

        # 3. Get batch-specific count if batch_id provided
        if batch_id:
            batch_count_result = spark.sql(
                f"SELECT COUNT(*) as batch_count FROM {table_name} WHERE load_batch_id = '{batch_id}'"
            )
            batch_count = batch_count_result.collect()[0]["batch_count"]
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

        # 4. Check load status for current batch (if batch_id) or overall
        if batch_id:
            status_query = f"""
            SELECT load_status, COUNT(*) as count
            FROM {table_name}
            WHERE load_batch_id = '{batch_id}'
            GROUP BY load_status
            ORDER BY count DESC
            """
        else:
            status_query = f"""
            SELECT load_status, COUNT(*) as count
            FROM {table_name}
            GROUP BY load_status
            ORDER BY count DESC
            """

        try:
            status_dist = spark.sql(status_query).collect()
            print(f"📊 Load status distribution:")
            for status in status_dist:
                print(f"   - {status['load_status']}: {status['count']:,}")
        except Exception as e:
            print(f"⚠️  Could not check load status distribution: {e}")

        # 5. Show recent load activity (last 24 hours)
        try:
            recent_loads = spark.sql(
                f"""
            SELECT 
                load_batch_id,
                load_timestamp,
                COUNT(*) as records_loaded
            FROM {table_name}
            WHERE load_timestamp >= current_timestamp() - INTERVAL 24 HOURS
            GROUP BY load_batch_id, load_timestamp
            ORDER BY load_timestamp DESC
            LIMIT 10
            """
            ).collect()

            if recent_loads:
                print(f"📊 Recent loads (last 24 hours):")
                for load in recent_loads:
                    print(
                        f"   - {load['load_batch_id']}: {load['records_loaded']:,} records at {load['load_timestamp']}"
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
        batch_count = batch_count_result.collect()[0]["batch_count"]
        tracking_results["target_rows"] = batch_count

        # Check load status distribution for this batch
        status_dist = spark.sql(
            f"""
        SELECT load_status, COUNT(*) as count
        FROM {table_name}
        WHERE load_batch_id = '{batch_id}'
        GROUP BY load_status
        ORDER BY count DESC
        """
        ).collect()

        # Determine overall batch status
        failed_count = sum(
            row["count"] for row in status_dist if row["load_status"] == "load_failed"
        )
        total_count = sum(row["count"] for row in status_dist)

        if failed_count == 0:
            tracking_results["load_status"] = "success"
        elif failed_count < total_count:
            tracking_results["load_status"] = "partial_success"
            tracking_results["warnings"].append(
                f"{failed_count} records failed to load"
            )
        else:
            tracking_results["load_status"] = "failed"
            tracking_results["warnings"].append("All records failed to load")

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
                source_file,
                load_error,
                load_timestamp,
                load_batch_id
            FROM {table_name}
            WHERE load_status = 'load_failed' 
            AND load_batch_id = '{batch_id}'
            ORDER BY load_timestamp DESC
            """
        else:
            failed_query = f"""
            SELECT 
                document_id,
                source_file,
                load_error,
                load_timestamp,
                load_batch_id
            FROM {table_name}
            WHERE load_status = 'load_failed' 
            AND load_timestamp >= current_timestamp() - INTERVAL {hours_back} HOURS
            ORDER BY load_timestamp DESC
            LIMIT 100
            """

        failed_loads = spark.sql(failed_query).collect()

        if failed_loads:
            print(f"❌ Found {len(failed_loads)} failed loads:")
            for failed in failed_loads[:10]:  # Show first 10
                print(f"   - {failed['source_file']}: {failed['load_error']}")
            if len(failed_loads) > 10:
                print(f"   ... and {len(failed_loads) - 10} more")
        else:
            print(f"✅ No failed loads found")

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
            all_files = list_minio_files(spark, docs_dir)
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
                    # Insert using DataFrame for consistency
                    df = spark.createDataFrame([result["data"]])
                    df.writeTo(table_name).append()
                    successful_inserts += 1
                else:
                    # In ELT approach, still load the data but mark as failed
                    result["data"]["load_status"] = "load_failed"
                    result["data"]["load_error"] = str(result["errors"])

                    df = spark.createDataFrame([result["data"]])
                    df.writeTo(table_name).append()
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
                    "generation_date": datetime.now(timezone.utc),
                    "file_path": str(file_path),
                    "document_length": 0,
                    "word_count": 0,
                    "language": "en",
                    "metadata": {},
                    "uuid": "",
                    "generated_at": "",
                    "source_system": "",
                    "source_file": filename,
                    "file_size": 0,
                    "source": "local" if not is_minio_path(str(file_path)) else "minio",
                    "method": "sequential",
                    "content_file_path": str(file_path),
                    "metadata_file_path": "",
                    "load_status": "load_failed",
                    "load_error": f"Processing error: {str(e)}",
                    "load_batch_id": batch_id,
                    "load_timestamp": load_timestamp,
                }

                df = spark.createDataFrame([error_data])
                df.writeTo(table_name).append()
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
                "generation_date": datetime.now(timezone.utc),
                "file_path": file_path_str,
                "document_length": doc_length,
                "word_count": word_count,
                "language": "en",
                "metadata": enhanced_metadata,
                "generated_at": minio_metadata.get("generated_at", ""),
                "source_system": minio_metadata.get("source", ""),
                "source_file": filename,
                "file_size": file_size,
                "source": "minio" if is_minio_path(file_path_str) else "local",
                "method": "spark" if is_minio_path(file_path_str) else "local",
                "content_file_path": file_path_str,
                "metadata_file_path": file_path_str.replace(".txt", ".json"),
                "load_status": "loaded",
                "load_error": None,
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
                "generation_date": datetime.now(timezone.utc),
                "file_path": str(file_path),
                "document_length": 0,
                "word_count": 0,
                "language": "en",
                "metadata": {},
                "uuid": "",
                "generated_at": "",
                "source_system": "",
                "source_file": filename,
                "file_size": 0,
                "source": "local" if not is_minio_path(str(file_path)) else "minio",
                "method": "sequential",
                "content_file_path": str(file_path),
                "metadata_file_path": "",
                "load_status": "load_failed",
                "load_error": f"Processing error: {str(e)}",
            },
            "errors": [
                {"message": f"Processing error: {str(e)}", "type": "processing_error"}
            ],
        }


def _read_minio_metadata(spark, content_file_path):
    """
    Read corresponding metadata JSON file for a MinIO content file

    Args:
        spark: SparkSession
        content_file_path: Path to content file (e.g., s3a://data/docs/legal/contract/20240115/uuid.txt)

    Returns:
        dict: Metadata from JSON file or empty dict if not found
    """
    try:
        # Convert content file path to metadata file path
        metadata_file_path = content_file_path.replace(".txt", ".json")

        # Check if metadata file exists
        try:
            df = spark.read.json(metadata_file_path)
            if df.count() > 0:
                metadata = df.toPandas().to_dict("records")[0]
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
    True PySpark parallel processing - both reading and inserting distributed

    Benefits of this approach:
    1. True parallelization - Both file processing and insertion happen in distributed pipeline
    2. Memory efficient - Data flows through pipeline without collecting everything in memory
    3. Fault tolerance - Spark handles failures and retries automatically across cluster
    4. No manual thread management - Spark handles all parallelization and distribution
    5. Better for large datasets - Scales horizontally across cluster nodes
    6. Built-in load balancing - Spark distributes work evenly across available resources
    7. Schema inference - No explicit schema definitions needed, Spark infers automatically
    8. ELT optimized - Light validation during load, heavy validation in transform layer
    """
    print("🔄 Using PySpark RDD for distributed file processing and insertion")

    all_source_files = []
    load_timestamp = datetime.now(timezone.utc)
    batch_start_time = time.time()

    # Step 1: Create RDD of file paths for distributed processing
    for format_type, files in file_groups.items():
        if not files:
            continue

        print(f"\n📁 Creating RDD for {format_type} files...")

        # Convert file paths to RDD - Spark distributes these across cluster
        file_paths_rdd = spark.sparkContext.parallelize([str(f) for f in files])
        all_source_files.extend([str(f) for f in files])

        # Process files in parallel using RDD map
        processed_rdd = file_paths_rdd.map(
            lambda file_path: _process_file_parallel(
                file_path, format_type, batch_id, load_timestamp
            )
        )

        # Add schema validation in parallel
        validated_rdd = processed_rdd.map(
            lambda data: _validate_and_mark_status(data, schema_manager)
        )

        # Convert to DataFrame and insert in parallel
        df = validated_rdd.toDF()
        df.writeTo(table_name).append()

        print(f"✅ {format_type} files processed and inserted in parallel")

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
    """Process a single file (distributed by Spark RDD)"""
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

        if result["is_valid"]:
            return result["data"]
        else:
            # Mark as failed but still include in batch
            result["data"]["load_status"] = "load_failed"
            result["data"]["load_error"] = str(result["errors"])
            return result["data"]

    except Exception as e:
        # Return error document
        return _create_error_document(file_path, batch_id, load_timestamp, str(e))


def _validate_and_mark_status(data, schema_manager):
    """Validate data and mark status (distributed by Spark RDD)"""
    if not schema_manager.validate_metadata("legal_doc_metadata", data):
        data["load_status"] = "load_failed"
        data["load_error"] = "Schema validation failed"
        data["load_error_code"] = "E001"
    else:
        data["load_status"] = "loaded"
        data["load_error"] = None

    return data


def _create_error_document(file_path, batch_id, load_timestamp, error_msg):
    """Create standardized error document"""
    filename = (
        file_path.name if hasattr(file_path, "name") else str(file_path).split("/")[-1]
    )

    return {
        "document_id": filename,
        "document_type": "unknown",
        "raw_text": "",
        "generation_date": datetime.now(timezone.utc),
        "file_path": str(file_path),
        "document_length": 0,
        "word_count": 0,
        "language": "en",
        "metadata": {},
        "uuid": "",
        "generated_at": "",
        "source_system": "",
        "source_file": filename,
        "file_size": 0,
        "source": "local" if not is_minio_path(str(file_path)) else "minio",
        "method": "parallel_batch",
        "content_file_path": str(file_path),
        "metadata_file_path": "",
        "load_status": "load_failed",
        "load_error": f"Processing error: {error_msg}",
        "load_batch_id": batch_id,
        "load_timestamp": load_timestamp,
    }


def validate_batch_load(spark, table_name, batch_id, original_files, expected_count):
    """Validate that all files were loaded correctly"""

    print(f"\n🔍 Validating batch {batch_id}...")

    # Get actual loaded records
    loaded_records = spark.sql(
        f"""
        SELECT 
            source_file,
            file_path,
            load_status,
            load_error,
            load_error_code
        FROM {table_name}
        WHERE load_batch_id = '{batch_id}'
    """
    ).collect()

    # Create lookup for loaded records
    loaded_files = {record["source_file"]: record for record in loaded_records}

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
            if record["load_status"] == "loaded":
                validation_result["files_successful"].append(file_name)
            else:
                validation_result["files_failed"].append(
                    {
                        "file": file_name,
                        "error": record["load_error"],
                        "error_code": record.get("load_error_code"),
                    }
                )
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
    Record batch metrics to admin.batch_metrics table with light validation
    """
    try:
        # Calculate metrics
        total_file_size_bytes = 0
        for file_path in source_files:
            if is_minio_path(file_path):
                total_file_size_bytes += get_minio_file_size(spark, file_path)
            else:
                total_file_size_bytes += Path(file_path).stat().st_size

        avg_processing_rate = (
            len(source_files) / (batch_duration_ms / 1000)
            if batch_duration_ms > 0
            else 0
        )

        # Build error codes map
        error_codes = {}
        for failed_file in validation_result.get("files_failed", []):
            error_code = failed_file.get("error_code", "E999")
            error_codes[error_code] = error_codes.get(error_code, 0) + 1

        # Create batch metrics record
        batch_metrics = {
            "load_batch_id": batch_id,
            "target_table": target_table,
            "load_timestamp": datetime.now(timezone.utc)
            .isoformat()
            .replace("+00:00", "Z"),
            "load_source": load_source,
            "load_job_id": f"job_{int(time.time())}",
            "load_version": "1.0",
            "metadata_schema_version": "v1",
            # File-level counts
            "files_processed": validation_result["files_expected"],
            "files_loaded": validation_result["files_loaded"],
            "files_failed": len(validation_result["files_failed"]),
            "files_missing": len(validation_result["files_missing"]),
            "files_successful": len(validation_result["files_successful"]),
            # Record-level counts
            "total_records_processed": validation_result["files_expected"],
            "total_records_loaded": validation_result["files_loaded"],
            "total_records_failed": len(validation_result["files_failed"]),
            "total_records_missing": len(validation_result["files_missing"]),
            # Performance metrics
            "load_duration_ms": int(batch_duration_ms),
            "total_file_size_bytes": total_file_size_bytes,
            "avg_processing_rate": round(avg_processing_rate, 2),
            # Error tracking
            "error_codes": error_codes,
            "error_summary": f"Success: {len(validation_result['files_successful'])}, Failed: {len(validation_result['files_failed'])}, Missing: {len(validation_result['files_missing'])}",
            # Validation status
            "validation_status": validation_result["validation_status"],
            # Audit trail
            "created_by": "system",
        }

        # Light validation using SchemaManager
        if not schema_manager.validate_metadata("batch_metrics", batch_metrics):
            print(f"❌ Batch metrics validation failed for {batch_id}")
            print(f"   - Skipping metrics recording to prevent bad operational data")
            return False

        # Convert to DataFrame and insert
        df = spark.createDataFrame([batch_metrics])
        df.writeTo("admin.ppend()
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
            "content_length": minio_metadata.get("content_length", 0),
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
    with HybridLogger(
        app_name="insert_pipeline",
        spark_config={
            "spark_version": SparkVersion.SPARK_3_5,
            "s3_config": (
                S3FileSystemConfig() if is_minio_path(args.file_path) else None
            ),
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
