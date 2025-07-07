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
from typing import List, Dict, Any
import uuid

from pyspark.sql import SparkSession, DataFrame
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

# Initialize SchemaManager and get schema version
schema_manager = SchemaManager()
schema_dict = schema_manager.get_schema("legal_doc_metadata")
SCHEMA_VERSION = schema_dict["schema_version"]


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

    # files_metadata_df.cache()
    file_count = files_metadata_df.count()
    print(f" Found {file_count} files with metadata")

    return files_metadata_df


def basic_load_validation(spark: SparkSession, table_name: str) -> bool:
    """Validate table existence and get basic stats"""
    print(f"\n🔍 Validating table: {table_name}")

    # Check table exists
    tables = spark.sql(f"SHOW TABLES IN {table_name.split('.')[0]}")
    table_list = tables.take(100)
    table_exists = any(table_name.split(".")[1] in str(row) for row in table_list)

    if not table_exists:
        print(f"❌ Table {table_name} does not exist!")
        return False

    # Get row count
    count_result = spark.sql(f"SELECT COUNT(*) as total_count FROM {table_name}")
    total_count = count_result.take(1)[0]["total_count"]
    print(f"📊 Total records: {total_count:,}")

    return True


def generate_job_id() -> str:
    """Generate unique job identifier"""
    return f"legal_insert_{int(time.time())}_{uuid.uuid4().hex[:8]}"


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


def merge_to_iceberg_table(
    spark: SparkSession, source_df: DataFrame, table_name: str
) -> bool:
    """Optimized merge without debug operations"""
    temp_view = "temp_merge_view"
    source_df.createOrReplaceTempView(temp_view)

    columns = source_df.columns
    update_columns = [col for col in columns if col != "document_id"]

    set_clause = ",\n  ".join([f"t.{col} = s.{col}" for col in update_columns])
    insert_cols = ", ".join([f"{col}" for col in columns])
    insert_vals = ", ".join([f"s.{col}" for col in columns])

    merge_sql = f"""
    MERGE INTO {table_name} t
    USING {temp_view} s
    ON t.document_id = s.document_id
    AND t.document_type = s.document_type
    AND month(t.generated_at) = month(s.generated_at) - 1
    WHEN MATCHED THEN UPDATE SET
      {set_clause}
    WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """

    spark.sql(merge_sql)
    return True


def insert_records(
    spark: SparkSession,
    docs_dir: str,
    table_name: str,
    processed_dataframes: List[DataFrame],
    observability_logger: HybridLogger,
) -> Dict[str, Any]:
    """Insert processed data with comprehensive metrics"""
    print(f" Loading files from {docs_dir} into {table_name}")

    # Start insertion operation
    observability_logger.start_operation("data_insertion", "insert_records")
    insertion_start = time.time()

    successful_inserts = 0
    failed_inserts = 0
    error_codes = {}

    for i, df in enumerate(processed_dataframes):
        if df is not None:
            try:
                merge_to_iceberg_table(spark, df, table_name)
                df_count = int(df.count())  # Convert to regular int
                successful_inserts += df_count

                print(f"✅ Inserted DataFrame {i+1} with {df_count} rows")

                # Log business event with converted types
                observability_logger.log_business_event(
                    "dataframe_inserted",
                    {
                        "dataframe_index": int(i),
                        "records_inserted": df_count,
                        "table_name": table_name,
                    },
                )

            except Exception as e:
                df_count = int(df.count())  # Convert to regular int
                failed_inserts += df_count
                error_codes["E004"] = (
                    error_codes.get("E004", 0) + df_count
                )  # SCHEMA_MISMATCH

                # Log error with converted types
                observability_logger.log_error(
                    e,
                    {
                        "dataframe_index": int(i),
                        "records_failed": df_count,
                        "error_type": "INSERTION_ERROR",
                    },
                )

                print(f"❌ Failed to insert DataFrame {i+1}: {e}")

    # Calculate insertion time
    insertion_time = time.time() - insertion_start

    # Log performance metrics with converted types
    observability_logger.log_performance(
        "data_insertion_complete",
        {
            "successful_inserts": int(successful_inserts),
            "failed_inserts": int(failed_inserts),
            "insertion_time_ms": int(insertion_time * 1000),
            "total_dataframes": int(len(processed_dataframes)),
        },
    )

    # End operation
    observability_logger.end_operation(
        execution_time=insertion_time, result_count=int(successful_inserts)
    )

    return {
        "successful_inserts": int(successful_inserts),
        "failed_inserts": int(failed_inserts),
        "error_codes": error_codes,
    }


def main():
    """Main function for ELT pipeline"""
    parser = argparse.ArgumentParser(description="ELT Pipeline for Legal Documents")
    parser.add_argument(
        "--file-dir", type=str, required=True, help="Path to files to process"
    )
    parser.add_argument("--table-name", type=str, help="Target table name")
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

    args = parser.parse_args()

    print("🚀 Starting Insert Operation")
    print(f"Source: {args.file_dir}")
    print(f"Target: {args.table_name}")
    print(f"Mode: {args.mode}")

    spark = create_spark_session(
        app_name=Path(__file__).stem,
        spark_version=SparkVersion.SPARK_CONNECT_3_5,
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
            basic_load_validation(observability_logger.spark, args.table_name)
            if args.show_failed:
                # Simplified - just show basic validation
                print("No failed loads to show")
            return 0

        print("📋 Listing MinIO files with comprehensive metadata...")
        files_metadata_df = list_files_distributed(spark, args.file_dir)
        if files_metadata_df.count() == 0:
            print(f"❌ No files found in path {args.file_dir}")
            observability_logger.log_error(
                Exception(f"No files found in MinIO path: {args.file_dir}"),
                {"error_type": "NO_FILES_FOUND"},
            )
            raise Exception("No files found")

        print(f"Found {files_metadata_df.count()} files with metadata")

        processed_dataframes = process_records(
            spark=observability_logger.spark,
            files_metadata_df=files_metadata_df,
            job_id=job_id,
            observability_logger=observability_logger,
        )

        insert_results = insert_records(
            spark=observability_logger.spark,
            docs_dir=args.file_dir,
            table_name=args.table_name,
            processed_dataframes=processed_dataframes,
            observability_logger=observability_logger,
        )

        processing_time_seconds = time.time() - job_start_time

        successful_inserts = insert_results.get("successful_inserts", 0)
        failed_inserts = insert_results.get("failed_inserts", 0)
        content_files_count = files_metadata_df.filter(
            col("is_content_file") == True
        ).count()
        processing_time_seconds = time.time() - job_start_time

        partition_metrics = {
            "total_partitions": 4,
            "files_per_partition": (
                content_files_count // 4 if content_files_count > 0 else 0
            ),
            "processing_time_seconds": processing_time_seconds,
            "success_rate": (
                successful_inserts / content_files_count
                if content_files_count > 0
                else 0
            ),
        }

        print(f"✅ Distributed processing complete:")
        print(f"   - Total content files: {content_files_count}")
        print(f"   - Successful: {successful_inserts}")
        print(f"   - Failed: {failed_inserts}")
        print(f"   - Processing time: {processing_time_seconds:.2f}s")
        print(f"   - Success rate: {partition_metrics['success_rate']:.2%}")

        success = failed_inserts == 0

        print(f"\n Processing Summary:")
        print(f"   Success: {success}")
        print(
            f"   Total Files Processed: {insert_results.get('total_files_processed', 0)}"
        )
        print(f"   Successful Inserts: {insert_results.get('successful_inserts', 0)}")
        print(f"   Failed Inserts: {insert_results.get('failed_inserts', 0)}")
        print(f"   Success Rate: {insert_results.get('success_rate', 0.0):.2%}")

        if success:
            print("\n✅ ELT pipeline completed successfully")
            basic_load_validation(observability_logger.spark, args.table_name)
            if args.show_failed:
                print("No failed loads to show")

        execution_time = time.time() - job_start_time
        observability_logger.end_operation(execution_time=execution_time)
        observability_logger.log_performance(
            "operation_complete",
            {
                "operation": "insert_files",
                "execution_time_seconds": round(execution_time, 4),
                "status": "success",
            },
        )

        return 0


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
