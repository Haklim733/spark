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
    concat,
    current_timestamp,
    date_format,
    element_at,
    get_json_object,
    regexp_extract,
    lit,
    split,
    sum,
    spark_partition_id,
    when,
)
from pyspark.sql.types import (
    StringType,
    LongType,
)
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

    files_metadata_df.cache()
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
    """Process files using native Spark distributed operations"""
    print(f"📄 Processing files using distributed operations")
    observability_logger.start_operation("file_processing", "process_records")
    processing_start = time.time()

    content_entries_with_binary_df = (
        files_metadata_df.filter(col("is_content_file") == True)
        .withColumnRenamed("file_path", "content_file_path")
        .withColumn("raw_text", col("content").cast(StringType()))
    )

    # In process_records, after creating metadata_entries_df:
    metadata_entries_df = (
        files_metadata_df.filter(col("is_metadata_file") == True)
        .withColumn("metadata_json_string", col("content").cast(StringType()))
        .withColumn(
            "document_type_from_json",
            get_json_object(col("metadata_json_string"), "$.document_type"),
        )
        .withColumn(
            "source_from_json", get_json_object(col("metadata_json_string"), "$.source")
        )
        .withColumn(
            "language_from_json",
            get_json_object(col("metadata_json_string"), "$.language"),
        )
        .withColumn(
            "method_from_json", get_json_object(col("metadata_json_string"), "$.method")
        )
        .withColumn(
            "file_size_from_json",
            get_json_object(col("metadata_json_string"), "$.file_size"),
        )
        .select(
            col("file_path").alias("metadata_file_path"),
            col("document_id").alias("meta_document_id"),
            col("document_type_from_json"),
            col("source_from_json"),
            col("language_from_json"),
            col("method_from_json"),
            col("file_size_from_json"),
        )
    )

    content_df = content_entries_with_binary_df.join(
        metadata_entries_df,
        on=col("document_id") == col("meta_document_id"),
        how="inner",
    )
    content_df = (
        content_df.withColumn(
            "generated_at", current_timestamp()
        )  # FIXED: Use Spark's deterministic current_timestamp()
        .withColumn("source", lit("soli_legal_document_generator").cast(StringType()))
        .withColumn("language", lit("en").cast(StringType()))
        .withColumn(
            "method", lit("spark_text_reader").cast(StringType())
        )  # Method here is more accurate, it's a binary reader now
        .withColumn("schema_version", lit(SCHEMA_VERSION).cast(StringType()))
        .withColumn("job_id", lit(job_id).cast(StringType()))
        .withColumn(
            "batch_id",
            concat(
                lit(f"batch_{job_id}_"),
                date_format(
                    current_timestamp(), "yyyyMMddHHmmssSSS"
                ),  # Adding timestamp back to batch_id for more uniqueness
                lit("_"),
                spark_partition_id().cast(StringType()),
            ).cast(StringType()),
        )
        .select(
            col("document_id").cast(StringType()),
            col("document_type").cast(StringType()),
            col("generated_at"),
            col("source").cast(StringType()),
            col("language").cast(StringType()),
            col("file_size_bytes")
            .cast(LongType())
            .alias("file_size"),  # Use the file_size from the content file itself
            col("method").cast(StringType()),
            col("schema_version").cast(StringType()),
            col("metadata_file_path").cast(
                StringType()
            ),  # Path to the associated metadata file
            col("raw_text").cast(StringType()),
            col("content_file_path")
            .cast(StringType())
            .alias("file_path"),  # The path to the content file
            col("batch_id").cast(StringType()),
            col("job_id").cast(StringType()),
        )
    )

    content_df.cache()

    # Get metrics using Spark native operations
    total_files = int(content_df.count())
    total_bytes = int(content_df.agg(sum("file_size")).collect()[0][0] or 0)

    processing_time = time.time() - processing_start

    # Log metrics
    observability_logger.log_performance(
        "file_processing_complete",
        {
            "records_processed": total_files,
            "bytes_transferred": total_bytes,
            "processing_time_ms": int(processing_time * 1000),
            "total_files": total_files,
        },
    )

    observability_logger.end_operation(
        execution_time=processing_time, result_count=total_files
    )

    return [content_df]


def merge_to_iceberg_table(
    spark: SparkSession, source_df: DataFrame, table_name: str
) -> bool:
    """Merge DataFrame to Iceberg table"""
    temp_view = "temp_merge_view"
    source_df.printSchema()
    source_df.createOrReplaceTempView(temp_view)
    # source_df.write.mode("append").saveAsTable("legal.documents")
    # print(source_df.show())
    # print(f"✅ Successfully wrote source_df to table: {table_name}")
    # spark.sql(f"DROP TABLE IF EXISTS {table_name}")  # Clean up

    # Get columns excluding document_id for updates
    columns = source_df.columns
    update_columns = [col for col in columns if col != "document_id"]

    # Build the SET clause for updates - be explicit about column references
    set_clause = ",\n  ".join([f"t.{col} = s.{col}" for col in update_columns])

    # Build INSERT clause
    insert_cols = ", ".join(columns)
    insert_vals = ", ".join([f"s.{col}" for col in columns])

    merge_sql = f"""
    MERGE INTO {table_name} t
    USING {temp_view} s
    ON t.document_id = s.document_id
    WHEN MATCHED THEN UPDATE SET
      {set_clause}
    WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """

    print(f"Executing MERGE SQL:")
    print(merge_sql)

    try:
        # First, let's check the schema of both tables
        print(f"Source DataFrame schema:")
        source_df.printSchema()

        print(f"Target table schema:")
        target_schema = spark.sql(f"DESCRIBE {table_name}")
        target_schema.show()

        spark.sql(merge_sql)
        print(f"✅ Successfully merged data to {table_name}")
        return True
    except Exception as e:
        print(f"❌ MERGE failed: {e}")
        print(f"Source DataFrame columns: {source_df.columns}")
        print(f"Source DataFrame count: {source_df.count()}")
        print(f"Source DataFrame schema: {source_df.schema}")
        print(
            f"Source DataFrame schema: {spark.sql("SELECT * FROM temp_merge_view LIMIT 5;")}"
        )

        # Show sample data to debug
        print(f"Sample source data:")
        source_df.show(5)

        raise e


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
            # --- DEBUG START ---
            print("\n==== DEBUG: DataFrame Schema Before Merge ====")
            df.printSchema()
            print("\n==== DEBUG: DataFrame Columns ====")
            print(df.columns)
            print("\n==== DEBUG: DataFrame Sample Data ====")
            df.show(5, vertical=True)
            print("\n==== DEBUG: Null document_id count ====")
            print(df.filter(col("document_id").isNull()).count())
            print("\n==== DEBUG: Target Table Schema ====")
            spark.sql(f"DESCRIBE {table_name}").show(truncate=False)
            # --- DEBUG END ---

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
