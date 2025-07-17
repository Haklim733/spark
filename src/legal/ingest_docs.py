#!/usr/bin/env python3
"""
Legal Documents Content Processing Pipeline
Processes legal document content files from S3 into legal.docs table
"""
import time
from pathlib import Path
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, LongType
from pyspark.sql.functions import (
    col,
    current_timestamp,
    length,
    lit,
)
from src.utils.session import create_spark_session, SparkVersion
from src.utils.logger import HybridLogger
from src.utils.metrics import ErrorCode, Operation, OperationStatus
from src.utils.ingest import (
    InsertMode,
    calculate_job_metrics,
    check_table_exists,
    generate_job_id,
    insert_records,
    list_files,
    load_validation,
    read_files,
    _safe_add_distributed_columns,
)


def process_content_files(content_df, job_id: str, logger):
    """Process legal content files for legal.docs table"""

    processing_start = time.time()

    # Safely add distributed processing columns
    content_df = _safe_add_distributed_columns(content_df)

    # Process content files to match legal.docs schema
    processed_df = content_df.select(
        col("document_id").cast(StringType()),
        col("document_type").cast(StringType()),
        col("value").cast(StringType()).alias("raw_text"),
        col("file_path").cast(StringType()),
        lit(job_id).cast(StringType()).alias("job_id"),
        length(col("value")).cast(LongType()).alias("text_length"),
        current_timestamp().alias("loaded_at"),
        current_timestamp().alias("updated_at"),
        # Use the safely added distributed processing columns
        col("spark_partition_id"),
        col("task_execution_id"),
    )

    # Filter out null document_ids
    processed_df = processed_df.filter(col("document_id").isNotNull())

    record_count = int(processed_df.count())
    processing_time = time.time() - processing_start

    logger.log(
        Operation.DATAFRAME_OPERATION.value,
        OperationStatus.SUCCESS.value,
        {
            "operation": "process_content_files_complete",
            "processing_time_ms": int(processing_time * 1000),
            "records_processed": record_count,
        },
    )

    return processed_df


def main(
    namespace: str,
    table_name: str,
    file_path_pattern: str,
    limit: Optional[int] = None,
    spark_session: Optional[SparkSession] = None,
):
    """Main function to process legal documents"""
    app_name = Path(__file__).stem

    # Use provided Spark session or create a new one
    if spark_session is not None:
        spark = spark_session
        should_stop_spark = False  # Don't stop session we didn't create
    else:
        spark = create_spark_session(
            spark_version=SparkVersion.SPARK_CONNECT_3_5,
            app_name=app_name,
            catalog="iceberg",
            additional_configs={"spark.wap.branch": "staging"},
        )
        should_stop_spark = True  # Stop session we created

    try:
        job_id = generate_job_id()
        job_start_time = time.time()

        with HybridLogger(spark=spark, app_name=app_name, manage_spark=False) as logger:

            print(f"📄 Starting legal documents content processing...")
            print(f"📁 Source: s3://raw/docs/legal (content files only)")
            print(f"🎯 Target: {namespace}.{table_name}")
            print(f"📋 Job ID: {job_id}")

            try:
                print("📋 Loading legal document files...")

                # Define extraction rules for metadata from file paths
                extraction_rules = {
                    "document_type": r"s3a:\/\/raw\/docs\/legal\/[^\/]+\/([^\/]+)\/",  # Extract doc type
                    "document_id": r"\/([^\/]+?)(?:\.[^.]*)?$",  # Extract filename without extension at end of path
                    "year": r"s3a:\/\/raw\/docs\/legal\/(\d{4})\d{4}\/",  # Extract year from date folder
                    "month": r"s3a:\/\/raw\/docs\/legal\/\d{4}(\d{2})\d{2}\/",  # Extract month from date folder
                    "day": r"s3a:\/\/raw\/docs\/legal\/\d{6}(\d{2})\/",  # Extract day from date folder
                }

                # List all files first
                all_files, files_count = list_files(
                    spark,
                    file_path_pattern,
                    logger,
                    limit=limit,
                )

                # DEBUG: Show what files were found
                print(f"📋 DEBUG: Found {len(all_files)} total files")
                if all_files:
                    print("📋 DEBUG: Sample file paths:")
                    for i, file_path in enumerate(
                        all_files[:10]
                    ):  # Show first 10 files
                        print(f"  {i+1}. {file_path}")

                content_files = [f for f in all_files if f.endswith(".json")]
                print(f"📄 Found {len(content_files)} content files (.json files)")

                # If no .txt files, let's check what file extensions we have
                if not content_files:
                    print("📋 DEBUG: No .txt files found. Checking file extensions:")
                    extensions = {}
                    for file_path in all_files:
                        ext = (
                            file_path.split(".")[-1]
                            if "." in file_path
                            else "no_extension"
                        )
                        extensions[ext] = extensions.get(ext, 0) + 1

                    for ext, count in extensions.items():
                        print(f"  .{ext}: {count} files")

                    # If we have other text-like files, include them
                    text_extensions = [".txt", ".text", ".md"]
                    content_files = [
                        f
                        for f in all_files
                        if any(f.endswith(ext) for ext in text_extensions)
                    ]
                    print(
                        f"📄 Expanded search: Found {len(content_files)} content files with text extensions"
                    )

                if not content_files:
                    print(
                        "❌ No content files found. Check the file path pattern and file structure."
                    )
                    return {
                        "job_status": "failed",
                        "records_inserted": 0,
                        "error": "No content files found",
                    }

                # Read content files (text)
                content_df = read_files(
                    spark=spark,
                    file_paths=content_files,
                    mode="text",
                    extraction_rules=extraction_rules,
                    logger=logger,
                )

                processed_df = process_content_files(content_df, job_id, logger)

                check_table_exists(spark, namespace, table_name, logger)

                insert_results = insert_records(
                    namespace=namespace,
                    table_name=table_name,
                    data=processed_df,
                    hybrid_logger=logger,
                    mode=InsertMode.OVERWRITE,
                )

                load_validation(
                    spark,
                    namespace,
                    table_name,
                    insert_results["total_records"],
                    logger,
                )

                job_metrics = calculate_job_metrics(
                    insert_results=insert_results,
                    job_start_time=job_start_time,
                    hybrid_logger=logger,
                    files_count=int(files_count),
                )

                print(f"✅ Legal documents content processing completed successfully")
                print(
                    f"📊 Job metrics: {job_metrics['job_status']} - {job_metrics['records_inserted']} records"
                )

                return job_metrics

            except FileNotFoundError as e:
                logger.log(
                    Operation.FILE_READ.value,
                    OperationStatus.FAILURE.value,
                    {
                        "error_code": ErrorCode.FILE_NOT_FOUND.value,
                        "source_path": file_path_pattern,
                    },
                )
                raise
            except PermissionError as e:
                logger.log(
                    Operation.FILE_READ.value,
                    OperationStatus.FAILURE.value,
                    {
                        "error_code": ErrorCode.PERMISSION_DENIED.value,
                        "source_path": file_path_pattern,
                    },
                )
                raise
            except Exception as e:
                logger.log(
                    Operation.FILE_READ.value,
                    OperationStatus.ERROR.value,
                    {
                        "error_code": ErrorCode.PROCESSING_ERROR.value,
                        "context": "legal_documents_content_processing",
                    },
                )
                raise

    except Exception as e:
        print(f"❌ Error in legal documents content processing: {e}")
        raise
    finally:
        if should_stop_spark:
            spark.stop()


if __name__ == "__main__":
    main(
        namespace="legal",
        table_name="docs",
        file_path_pattern="s3a://raw/docs/legal/**/**/*.txt",
    )
