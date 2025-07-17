#!/usr/bin/env python3
"""
Legal Documents Metadata Processing Pipeline
Processes legal document metadata files from S3 into legal.docs_metadata table
"""
import time
from pathlib import Path
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, LongType, TimestampType
from pyspark.sql.functions import (
    col,
    coalesce,
    current_timestamp,
    get_json_object,
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
    safe_cast_columns,
    _safe_add_distributed_columns,
)
from src.schemas.schema import SchemaManager

# Initialize SchemaManager and get schema version
schema_manager = SchemaManager()
schema_dict = schema_manager.get_schema("legal_doc_metadata")
SCHEMA_VERSION = schema_dict["schema_version"] if schema_dict else "1.0"


def process_metadata_files(metadata_df, job_id: str, logger):
    """Process legal metadata files for legal.docs_metadata table"""

    processing_start = time.time()

    # Safely add distributed processing columns first
    metadata_df = _safe_add_distributed_columns(metadata_df)

    # Process metadata files to match legal.docs_metadata schema (exact column order)
    processed_df = metadata_df.select(
        # Match table schema column order exactly
        col("document_id").cast(StringType()),
        col("document_type").cast(StringType()),
        # Extract from JSON with fallback to schema defaults
        coalesce(
            get_json_object(col("value"), "$.source"),
            lit("soli_legal_document_generator"),
        )
        .cast(StringType())
        .alias("source"),
        coalesce(get_json_object(col("value"), "$.language"), lit("en"))
        .cast(StringType())
        .alias("language"),
        coalesce(get_json_object(col("value"), "$.file_size"), lit(0))
        .cast(LongType())
        .alias("file_size"),
        coalesce(get_json_object(col("value"), "$.method"), lit("spark"))
        .cast(StringType())
        .alias("method"),
        lit(SCHEMA_VERSION).cast(StringType()).alias("schema_version"),
        col("file_path").cast(StringType()).alias("metadata_file_path"),
        # REMOVED batch_id - table doesn't expect it
        lit(job_id).cast(StringType()).alias("job_id"),
        coalesce(
            get_json_object(col("value"), "$.generated_at"), current_timestamp()
        ).alias("generated_at"),
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
            "operation": "process_metadata_files_complete",
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
    """Main function to process legal document metadata"""
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

            print(f"📋 Starting legal documents metadata processing...")
            print(f"📁 Source: s3://raw/docs/legal (metadata files only)")
            print(f"🎯 Target: {namespace}.{table_name}")
            print(f"📋 Job ID: {job_id}")

            try:
                print("📋 Loading legal document metadata files...")

                # Define extraction rules for metadata from file paths
                extraction_rules = {
                    "document_type": r"s3a:\/\/raw\/docs\/legal\/[^\/]+\/([^\/]+)\/",  # Extract doc type
                    "document_id": r"\/([^\/]+?)(?:\.[^.]*)?$",  # Extract filename without extension at end of path
                    "year": r"s3a:\/\/raw\/docs\/legal\/(\d{4})\d{4}\/",  # Extract year from date folder
                    "month": r"s3a:\/\/raw\/docs\/legal\/\d{4}(\d{2})\d{2}\/",  # Extract month from date folder
                    "day": r"s3a:\/\/raw\/docs\/legal\/\d{6}(\d{2})\/",  # Extract day from date folder
                }

                # List metadata files only
                all_files, files_count = list_files(
                    spark,
                    file_path_pattern,
                    logger,
                    limit=limit,
                )

                # Filter for metadata files only
                metadata_files = [f for f in all_files if "/metadata/" in f]
                print(f"📋 Found {len(metadata_files)} metadata files")

                # Read metadata files as TEXT (not JSON) so we can use get_json_object
                metadata_df = read_files(
                    spark=spark,
                    file_paths=metadata_files,
                    mode="text",  # Back to text mode for get_json_object
                    extraction_rules=extraction_rules,
                    logger=logger,
                )

                # Process metadata files for legal.docs_metadata table
                processed_df = process_metadata_files(metadata_df, job_id, logger)
                processed_df = safe_cast_columns(
                    df=processed_df,
                    columns=[
                        "generated_at",
                    ],
                    col_type=TimestampType,
                    logger=logger,
                )

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
                    files_count=len(metadata_files),
                )

                print(f"✅ Legal documents metadata processing completed successfully")
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
                        "context": "legal_documents_metadata_processing",
                    },
                )
                raise

    except Exception as e:
        print(f"❌ Error in legal documents metadata processing: {e}")
        raise
    finally:
        if should_stop_spark:
            spark.stop()


if __name__ == "__main__":
    main(
        namespace="legal",
        table_name="docs_metadata",
        file_path_pattern="s3a://raw/docs/legal/**/**/metadata/*.json",
    )
