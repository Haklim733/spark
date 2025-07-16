#!/usr/bin/env python3
"""
ELT Pipeline - Extract and Load operations
Transformations and data quality checks should be handled in transformation stage
Note: Iceberg tables don't have constraints, so validation is done at application level
"""

import argparse
from pathlib import Path
import time
import sys
from typing import List

from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.functions import (
    col,
    concat_ws,
    current_timestamp,
    date_format,
    element_at,
    get_json_object,
    regexp_extract,
    lit,
    split,
    sum,
    sha2,
    when,
    count,
)
from pyspark.sql.types import StringType, LongType, TimestampType
from src.utils.logger import HybridLogger
from src.utils.session import create_spark_session, SparkVersion
from src.schemas.schema import SchemaManager
from src.utils.process import (
    generate_job_id,
    basic_load_validation,
    insert_records,
    process_pipeline_results,
)

# Initialize SchemaManager and get schema version
schema_manager = SchemaManager()
schema_dict = schema_manager.get_schema("legal_doc_metadata")
SCHEMA_VERSION = schema_dict["version"]


def is_minio_path(path_str: str) -> bool:
    """Check if the path is a MinIO/S3 path"""
    return path_str.startswith(("s3a://", "s3://", "minio://"))


def list_files_distributed(spark: SparkSession, file_dir: str) -> DataFrame:
    """List files in MinIO with comprehensive metadata"""
    files_df = spark.read.format("binaryFile").load(file_dir)

    files_metadata_df = (
        files_df.withColumn("file_path", col("path"))
        .withColumn(
            "document_type",
            regexp_extract(
                col("file_path"), r"s3a:\/\/raw\/docs\/legal\/([^\/]+)\/", 1
            ),
        )
        .withColumn(
            "document_id",
            regexp_extract(
                element_at(split(col("file_path"), "/"), -1), r"^(.+?)(?:\.[^.]*$|$)", 1
            ),
        )
        .withColumn(
            "file_extension",
            regexp_extract(
                element_at(split(col("file_path"), "/"), -1), r"(?<=\.)([^.]*)$", 1
            ),
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
            "document_id",
            "file_extension",
            "is_content_file",
            "is_metadata_file",
            col("modificationTime").alias("modification_time_utc"),
            col("length").alias("file_size_bytes"),
            col("content"),
        )
    )

    return files_metadata_df


def process_records(
    spark: SparkSession,
    files_metadata_df: DataFrame,
    job_id: str,
    observability_logger: HybridLogger,
) -> List[DataFrame]:
    """
    Process files using native Spark distributed operations.
    Optimized to minimize transformations, efficiently handle content/metadata,
    and trigger actions once for metrics.
    """
    print(f"📄 Processing files using distributed operations.")
    observability_logger.start_operation("file_processing", "process_records")
    processing_start = time.time()

    # Capture job batch timestamp once for consistency
    job_batch_timestamp = date_format(current_timestamp(), "yyyyMMddHHmmssSSS")

    # --- Optimization 1: Cache files_metadata_df strategically ---
    # Cache the initial DataFrame if it's the result of an expensive
    # upstream operation and will be scanned multiple times (which it is here for filter).
    # Consider .persist(StorageLevel.DISK_ONLY) if memory is a concern for very large DFs.
    # files_metadata_df.cache()

    # --- Optimization 2: Process content and metadata files, dropping 'content' early ---
    # This is already good in your code. Casting to StringType and dropping binary 'content'
    # immediately reduces data size in memory and during shuffles.
    content_df_raw = (
        files_metadata_df.filter(col("is_content_file") == True)
        .select(
            col("document_id").alias("content_document_id"),
            col("document_type").alias("content_document_type"),
            col("file_path").alias("content_file_path_raw"),
            col("content").cast(StringType()).alias("raw_text_content"),
            col("file_size_bytes").alias("content_file_size_bytes"),
        )
        .drop("content")
    )

    metadata_df_raw = (
        files_metadata_df.filter(col("is_metadata_file") == True)
        .select(
            col("document_id").alias("meta_document_id"),
            col("file_path").alias("metadata_file_path_raw"),
            col("content").cast(StringType()).alias("metadata_json_string"),
        )
        .drop("content")
    )

    # --- Step 3: Join Processed Content and Metadata ---
    # Optimization 3: Consider Broadcast Join Hint
    # If metadata_df_raw is significantly smaller than content_df_raw (and fits in executor memory),
    # broadcasting it can make the join much faster by avoiding a shuffle on the larger DF.
    # Spark might do this automatically if spark.sql.autoBroadcastJoinThreshold is met,
    # but an explicit hint can ensure it.
    # If both are large, Spark's default Sort-Merge Join is generally efficient.
    joined_df = content_df_raw.join(
        # Use broadcast() if metadata_df_raw is small. Remove if both are large.
        metadata_df_raw,  # Example: broadcast(metadata_df_raw),
        on=col("content_document_id") == col("meta_document_id"),
        how="inner",
    ).withColumnRenamed("content_document_id", "document_id")

    # --- Step 4: Final Transformations and Type Casting (to match TARGET_ICEBERG_SCHEMA) ---
    # Optimization 4: Consolidate transformations into a single .select() for efficiency
    # Spark's Catalyst optimizer is good at chaining these, but explicit consolidation
    # can sometimes lead to clearer DAGs and fewer intermediate DataFrame objects.
    final_df = joined_df.select(
        col("document_id"),
        col("content_document_type").alias("document_type").cast(StringType()),
        current_timestamp().cast(TimestampType()).alias("generated_at"),
        # Extract and cast JSON fields
        get_json_object(col("metadata_json_string"), "$.source")
        .cast(StringType())
        .alias("source"),
        get_json_object(col("metadata_json_string"), "$.language")
        .cast(StringType())
        .alias("language"),
        get_json_object(col("metadata_json_string"), "$.file_size")
        .cast(LongType())
        .alias("file_size"),
        get_json_object(col("metadata_json_string"), "$.method")
        .cast(StringType())
        .alias("method"),
        lit(SCHEMA_VERSION).cast(StringType()).alias("schema_version"),
        col("metadata_file_path_raw").cast(StringType()).alias("metadata_file_path"),
        col("raw_text_content").cast(StringType()).alias("raw_text"),
        col("content_file_path_raw").cast(StringType()).alias("file_path"),
        # Optimization 5: Generate deterministic batch_id using file_path hash
        # This makes the batch_id stable and unique per source file and job run.
        concat_ws(
            "_",
            lit(f"batch_{job_id}_{job_batch_timestamp}"),
            sha2(col("content_file_path_raw"), 256),  # Hash the content file path
        )
        .cast(StringType())
        .alias("batch_id"),
        lit(job_id).cast(StringType()).alias("job_id"),
    )

    # CRITICAL: Filter out any records where document_id is NULL.
    # This ensures a clean primary key for MERGE INTO.
    final_df_filtered = final_df.filter(col("document_id").isNotNull())
    if final_df_filtered.isEmpty():
        raise Exception("No valid records found after processing")

    # --- Optimization 6: Trigger Actions Once for Metrics and Debugging ---
    # Cache the final, cleaned DataFrame *before* the first action
    # to avoid recomputation if multiple actions follow.
    final_df_filtered.cache()

    # Perform all necessary aggregations in a single .agg() call.
    # This ensures the DataFrame DAG is executed only once to compute these metrics.
    metrics_row = final_df_filtered.agg(
        count("*").alias("total_records"),
        sum("file_size").alias("total_bytes_processed"),
    ).collect()[
        0
    ]  # .collect() triggers the action

    total_records_processed = int(metrics_row["total_records"])
    total_bytes_processed = int(metrics_row["total_bytes_processed"] or 0)

    processing_time = time.time() - processing_start

    # Debugging prints (keep these, they are helpful!)
    print(f"DEBUG: Count of final_df: {total_records_processed}")
    print(
        f"DEBUG: Null document_id count in final_df (should be 0): {final_df_filtered.filter(col('document_id').isNull()).count()}"
    )

    # Log metrics using the collected values
    observability_logger.log_performance(
        "file_processing_complete",
        {
            "records_processed": total_records_processed,
            "bytes_transferred": total_bytes_processed,
            "processing_time_ms": int(processing_time * 1000),
            "total_files": total_records_processed,  # Renamed for clarity in the log payload
        },
    )

    observability_logger.end_operation(
        execution_time=processing_time, result_count=total_records_processed
    )

    # Optimization 7: Unpersist initial DataFrame if no longer needed
    # Frees up memory if files_metadata_df won't be used again after this function.
    files_metadata_df.unpersist()

    return [final_df_filtered]


def insert_dummy_data(spark: SparkSession, table_name: str) -> bool:
    """insert dummy data if main branch has no snapshot
    'Cannot complete replace branch operation on legal.documents_snapshot, main has no snapshot'
    """
    dummy_data = [
        {
            "document_id": "doc_000",
            "document_type": "dummy",
            "generated_at": "2024-01-01 10:00:00",
            "source": "soli_legal_document_generator",
            "language": "en",
            "file_size": 1024,
            "method": "spark",
            "schema_version": "1.0",
            "metadata_file_path": "s3a://raw/docs/legal/contract/20240115/metadata/doc_001.json",
            "raw_text": "This is a sample legal contract text for testing purposes.",
            "file_path": "s3a://raw/docs/legal/contract/20240115/content/doc_001.txt",
            "batch_id": "batch_20240115_001",
            "job_id": "test_job_001",
        },
    ]

    # Create DataFrame using v2 API approach
    # df = spark.createDataFrame(dummy_data)
    # df = df.withColumn("generated_at", to_timestamp("generated_at"))
    # df.writeTo(table_name).createOrReplace()
    return True


def main():
    """Main function for ELT pipeline"""
    parser = argparse.ArgumentParser(description="ELT Pipeline for Legal Documents")
    parser.add_argument(
        "--file-dir", type=str, required=True, help="Path to files to process"
    )
    parser.add_argument(
        "--table-name",
        type=str,
        help="Target table name. must include catalog name (ex: iceberg.namespace.table_name)",
    )
    parser.add_argument(
        "--validate-existing-data",
        action="store_true",
        help="Only validate existing data in database",
    )
    parser.add_argument(
        "--show-failed",
        action="store_true",
        help="Show failed loads for troubleshooting",
    )
    parser.add_argument(
        "--mode",
        type=str,
        choices=["batch", "streaming"],
        default="batch",
        help="Processing mode",
    )
    parser.add_argument(
        "--num-partitions",
        type=int,
        default=4,
        help="Number of partitions for parallel processing",
    )

    parser.add_argument(
        "--branch",
        type=str,
        default="staging",
        help="Branch name",
    )

    args = parser.parse_args()

    print("🚀 Starting Insert Operation")
    print(f"Source: {args.file_dir}")
    print(f"Target: {args.table_name}")
    print(f"Mode: {args.mode}")

    spark = create_spark_session(
        app_name=Path(__file__).stem,
        spark_version=SparkVersion.SPARK_CONNECT_3_5,
        additional_configs={"spark.wap.branch": "staging"},
    )

    with HybridLogger(
        spark=spark, app_name=Path(__file__).stem, manage_spark=True
    ) as observability_logger:
        if args.mode == "streaming":
            raise NotImplementedError("Streaming processing not yet implemented")

        observability_logger.start_operation("default_job_group", "insert_files")
        job_start_time = time.time()
        job_id = generate_job_id()

        print(args.file_dir)

        if args.validate_existing_data:
            print("\n🔍 Validating existing data in database...")
            basic_load_validation(observability_logger.spark or spark, args.table_name)
            if args.show_failed:
                # Simplified - just show basic validation
                print("No failed loads to show")
            return 0

        print("📋 Listing MinIO files with comprehensive metadata...")
        files_metadata_df = list_files_distributed(spark, args.file_dir)
        if files_metadata_df.count() == 0:
            print(f"❌ No files found in path {args.file_dir}")
            observability_logger.log(
                Exception(f"No files found in MinIO path: {args.file_dir}"),
                {"error_type": "NO_FILES_FOUND"},
            )
            raise Exception("No files found")

        print(f"Found {files_metadata_df.count()} files with metadata")

        processed_dataframes = process_records(
            spark=observability_logger.spark or spark,
            files_metadata_df=files_metadata_df,
            job_id=job_id,
            observability_logger=observability_logger,
        )

        insert_results = insert_records(
            spark=observability_logger.spark or spark,
            docs_dir=args.file_dir,
            table_name=args.table_name,
            processed_dataframes=processed_dataframes,
            observability_logger=observability_logger,
        )

        process_pipeline_results(
            insert_results=insert_results,
            job_start_time=job_start_time,
            files_metadata_df=files_metadata_df,
            content_filter_column="is_content_file",
            observability_logger=observability_logger,
            spark_session=observability_logger.spark or spark,
            table_name=args.table_name,
            show_failed=args.show_failed,
        )
        return 0


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
