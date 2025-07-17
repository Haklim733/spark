from datetime import datetime
from enum import Enum
import time
from typing import Any, Optional, Dict, Callable
import uuid

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    input_file_name,
    regexp_extract,
    when,
    trim,
    spark_partition_id,
    monotonically_increasing_id,
    lit,
)
from pyspark.sql.types import IntegerType
from src.utils.logger import HybridLogger
from src.utils.metrics import (
    Operation,
    OperationMetric,
    OperationStatus,
    ErrorCode,
)


class InsertMode(Enum):
    """Insert mode types"""

    # Data operations
    OVERWRITE = "overwrite"
    APPEND = "append"
    MERGE = "merge"


def _safe_add_partition_info(df: DataFrame, logger: Optional[HybridLogger] = None):
    """Safely add partition info for Spark Connect compatibility"""
    try:
        # Try to add partition information for debugging
        df_with_partition = df.withColumn("spark_partition_id", spark_partition_id())
        partition_stats = (
            df_with_partition.groupBy("spark_partition_id")
            .count()
            .withColumnRenamed("count", "record_count")  # Rename to avoid confusion
            .collect()
        )

        return [
            {
                "partition_id": row.spark_partition_id,
                "record_count": row.record_count,  # Now uses the renamed column
            }
            for row in partition_stats
        ]
    except Exception as e:
        if logger:
            print(
                f"Warning: Could not add partition info (Spark Connect limitation): {e}"
            )
        # Return empty list if partition functions not available
        return []


def _safe_add_distributed_columns(df: DataFrame):
    """Safely add distributed processing columns for Spark Connect compatibility"""
    try:
        # Try to add distributed processing columns
        return df.withColumn("spark_partition_id", spark_partition_id()).withColumn(
            "task_execution_id", monotonically_increasing_id()
        )
    except Exception:
        # Fallback: add literal values if functions not available
        return df.withColumn("spark_partition_id", lit(-1)).withColumn(
            "task_execution_id", lit(-1)
        )


def list_files(
    spark: SparkSession,
    path_pattern: str,
    logger: Optional[HybridLogger] = None,
    limit: Optional[int] = None,
    method: str = "binaryFile",
    metadata: list[str] = ["path"],
) -> tuple[list[str], int]:
    """Count input files without reading data - for inputs_discovered metric"""
    try:
        files_df = spark.read.format(method).load(path_pattern).select(*metadata)

        if limit:
            files_df = files_df.limit(limit)

        file_count = int(files_df.count())
        file_paths = [row.path for row in files_df.collect()]

        if logger:
            # Safely get partition distribution for debugging
            partition_distribution = _safe_add_partition_info(files_df, logger)

            logger.log(
                Operation.FILE_LIST.value,
                OperationStatus.SUCCESS.value,
                {
                    "path_pattern": path_pattern,
                    "limit": limit,
                    "file_count": file_count,
                    "partition_distribution": partition_distribution,
                },
            )

        return file_paths, file_count

    except Exception as e:
        if logger:
            # Get distributed context for error debugging
            try:
                spark_context = spark.sparkContext
                distributed_context = {
                    "spark_app_id": spark_context.applicationId,
                    "default_parallelism": spark_context.defaultParallelism,
                }
            except:
                distributed_context = {}

            logger.log(
                Operation.FILE_LIST.value,
                OperationStatus.ERROR.value,
                {
                    "error_code": ErrorCode.FILE_LISTING_ERROR.value,
                    "error_message": str(e),
                    **distributed_context,
                },
            )
        raise e


def read_files(
    spark: SparkSession,
    file_paths: list[str],
    mode: str = "parquet",
    extraction_rules: dict[str, str] = {},
    logger: Optional[HybridLogger] = None,
) -> DataFrame:
    """Optimized: Read files and extract metadata in single operation"""
    try:
        data_df = spark.read.format(mode).load(file_paths)

        # Add source file path for tracking and extraction
        data_df = data_df.withColumn("file_path", input_file_name())

        # Extract metadata from file paths in same operation
        if extraction_rules:
            print(f"   Extracting metadata: {list(extraction_rules.keys())}")

            for column_name, regex_pattern in extraction_rules.items():
                data_df = data_df.withColumn(
                    column_name,
                    regexp_extract(col("file_path"), regex_pattern, 1),
                )

        print(f"✅ Data loaded with columns: {data_df.columns}")

        if logger:
            # Safely get partition distribution for debugging
            partition_distribution = _safe_add_partition_info(data_df, logger)

            logger.log(
                Operation.FILE_READ.value,
                OperationStatus.SUCCESS.value,
                {
                    "operation": "read_files_with_extraction",
                    "mode": mode,
                    "extracted_columns": (
                        list(extraction_rules.keys()) if extraction_rules else []
                    ),
                    "total_records": data_df.count(),
                    "partition_distribution": partition_distribution,
                },
            )

        return data_df

    except Exception as e:
        if logger:
            # Try to get some distributed context even in error case
            try:
                spark_context = spark.sparkContext
                app_id = spark_context.applicationId
                default_parallelism = spark_context.defaultParallelism
                distributed_context = {
                    "spark_app_id": app_id,
                    "default_parallelism": default_parallelism,
                }
            except:
                distributed_context = {}

            logger.log(
                Operation.FILE_READ.value,
                OperationStatus.ERROR.value,
                {
                    "error_code": ErrorCode.FILE_LISTING_ERROR.value,
                    "error_message": str(e),
                    "file_paths": file_paths,
                    "mode": mode,
                    "extracted_columns": (
                        list(extraction_rules.keys()) if extraction_rules else []
                    ),
                    **distributed_context,
                },
            )
        raise


def safe_cast_columns(
    df: DataFrame,
    columns: list[str],
    col_type: Any,
    logger: Optional[HybridLogger] = None,
):
    """Lazy transformation: safely cast string columns to double"""
    expressions = []

    for col_name in df.columns:
        if col_name in columns:
            expressions.append(
                when(
                    trim(col(col_name)).rlike(r"^-?\d+\.?\d*$"),
                    col(col_name).cast(col_type()),
                )
                .otherwise(None)
                .alias(col_name)
            )
        else:
            expressions.append(col(col_name))

    if logger:
        # Safely get partition distribution for debugging
        partition_distribution = _safe_add_partition_info(df, logger)

        logger.log(
            Operation.DATAFRAME_OPERATION.value,
            OperationStatus.SUCCESS.value,
            {
                "columns_cast": columns,
                "target_type": str(col_type()),
                "partition_distribution": partition_distribution,
            },
        )

    return df.select(*expressions)  # lazy


def insert_records(
    namespace: str,
    table_name: str,
    data: DataFrame,
    hybrid_logger: HybridLogger,
    mode: InsertMode = InsertMode.OVERWRITE,
) -> dict[str, Any]:
    """Insert processed data with distributed processing"""

    insertion_start = time.time()

    print(f"Inserting records into {namespace}.{table_name}")

    try:

        print(f"📝 Inserting records into {namespace}.{table_name}")

        # Single distributed insert operation
        if mode == InsertMode.OVERWRITE:
            data.writeTo(f"{namespace}.{table_name}").overwritePartitions()
        elif mode == InsertMode.APPEND:
            data.writeTo(f"{namespace}.{table_name}").append()
        elif mode == InsertMode.MERGE:
            raise NotImplementedError("Merge mode not implemented: use merge function")

        insertion_time = time.time() - insertion_start
        records_processed = int(data.count())

        # Safely get partition distribution for debugging
        partition_distribution = _safe_add_partition_info(data, hybrid_logger)

        # Log successful insertion
        hybrid_logger.log(
            Operation.TABLE_INSERT.value,
            OperationStatus.SUCCESS.value,
            {
                OperationMetric.RECORDS_PROCESSED.value: records_processed,
                OperationMetric.EXECUTION_TIME_MS.value: int(insertion_time * 1000),
                "table_name": table_name,
                "insert_mode": mode.value,
                "partition_distribution": partition_distribution,
            },
        )

        return {
            "successful_inserts": records_processed,
            "failed_inserts": 0,
            "error_codes": {},
            "total_records": records_processed,
            "insertion_time_ms": int(insertion_time * 1000),
        }

    except Exception as e:
        insertion_time = time.time() - insertion_start

        # Try to get partition info even in error case for debugging
        partition_distribution = _safe_add_partition_info(data, hybrid_logger)

        hybrid_logger.log(
            Operation.TABLE_INSERT.value,
            OperationStatus.ERROR.value,
            {
                "error_code": ErrorCode.INSERTION_ERROR.value,
                "table_name": table_name,
                OperationMetric.EXECUTION_TIME_MS.value: int(insertion_time * 1000),
                "error_message": str(e),
                "insert_mode": mode.value,
                "partition_distribution": partition_distribution,
            },
        )

        return {
            "successful_inserts": 0,
            "failed_inserts": 0,
            "error_codes": {"E033": 1},  # INSERTION_ERROR
            "total_records": 0,
            "insertion_time_ms": int(insertion_time * 1000),
        }


def check_table_exists(
    spark: SparkSession, namespace: str, table_name: str, hybrid_logger: HybridLogger
) -> None:
    """Check if a table exists in the specified namespace"""
    full_table_name = f"{namespace}.{table_name}"

    try:
        print(f"📋 Checking table existence: {full_table_name}")

        tables = spark.sql(f"SHOW TABLES IN {namespace}")
        table_list = [row.tableName for row in tables.collect()]

        # Get distributed context for debugging
        try:
            spark_context = spark.sparkContext
            distributed_context = {
                "spark_app_id": spark_context.applicationId,
                "default_parallelism": spark_context.defaultParallelism,
            }
        except:
            distributed_context = {}

        if table_name in table_list:
            print(f"✅ Table exists: {full_table_name}")
            hybrid_logger.log(
                Operation.TABLE_EXISTS_CHECK.value,
                OperationStatus.SUCCESS.value,
                {"table": full_table_name, "exists": True, **distributed_context},
            )
        else:
            print(f"❌ Table does not exist: {full_table_name}")
            hybrid_logger.log(
                Operation.TABLE_EXISTS_CHECK.value,
                OperationStatus.ERROR.value,
                {
                    "table": full_table_name,
                    "exists": False,
                    "available_tables": table_list,
                    **distributed_context,
                },
            )

    except Exception as e:
        print(f"❌ Error checking table existence for {full_table_name}: {e}")
        hybrid_logger.log(
            Operation.TABLE_EXISTS_CHECK.value,
            OperationStatus.ERROR.value,
            {
                "error_code": ErrorCode.TABLE_NOT_EXISTS.value,
                "error_message": str(e),
            },
        )
        raise e


def load_validation(
    spark: SparkSession,
    namespace: str,
    table_name: str,
    count: int,
    hybrid_logger: HybridLogger,
) -> bool:
    """Validate table existence and get basic stats"""

    try:
        count_result = spark.sql(
            f"SELECT COUNT(*) as total_count FROM {namespace}.{table_name}"
        )
        total_count = count_result.take(1)[0]["total_count"]
        print(f"📊 Total records: {total_count:,}")

        assert total_count == count, "Total count does not match"

        # Get distributed context for debugging
        try:
            spark_context = spark.sparkContext
            distributed_context = {
                "spark_app_id": spark_context.applicationId,
                "default_parallelism": spark_context.defaultParallelism,
            }
        except:
            distributed_context = {}

        hybrid_logger.log(
            Operation.TABLE_VALIDATION.value,
            OperationStatus.SUCCESS.value,
            {
                "table": f"{namespace}.{table_name}",
                OperationMetric.ACTUAL_COUNT.value: total_count,
                OperationMetric.EXPECTED_COUNT.value: count,
                **distributed_context,
            },
        )
        return True

    except Exception as e:
        # Get distributed context for error case too
        try:
            spark_context = spark.sparkContext
            distributed_context = {
                "spark_app_id": spark_context.applicationId,
                "default_parallelism": spark_context.defaultParallelism,
            }
        except:
            distributed_context = {}

        hybrid_logger.log(
            Operation.TABLE_VALIDATION.value,
            OperationStatus.ERROR.value,
            {
                "error_code": ErrorCode.LOAD_COUNT_MISMATCH.value,
                "error_message": str(e),
                **distributed_context,
            },
        )
        return False


def generate_job_id(prefix="job"):
    """Generate a job ID with timestamp and UUID"""
    return f"{prefix}_{int(time.time())}_{uuid.uuid4().hex[:8]}"


def merge(
    spark: SparkSession, source_df: DataFrame, table_name: str, key_ids: list[str]
) -> bool:
    """Optimized merge without debug operations"""
    temp_view = "temp_merge_view"
    source_df.createOrReplaceTempView(temp_view)

    columns = source_df.columns
    update_columns = [col for col in columns if col != "document_id"]

    set_clause = ",\n  ".join([f"t.{col} = s.{col}" for col in update_columns])
    insert_cols = ", ".join([f"{col}" for col in columns])
    insert_vals = ", ".join([f"s.{col}" for col in columns])
    on_clause_primary = " ON ".join([f"t.{col} = s.{col}" for col in key_ids[0]])
    on_clause_additional = " AND ".join([f"t.{col} = s.{col}" for col in key_ids[1:]])
    on_clause = (
        f"{on_clause_primary} {on_clause_additional}"
        if len(key_ids) > 1
        else on_clause_primary
    )

    merge_sql = f"""
    MERGE INTO {table_name} t
    USING {temp_view} s
    {on_clause}
    WHEN MATCHED THEN UPDATE SET
      {set_clause}
    WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """

    spark.sql(merge_sql)
    return True


def calculate_job_metrics(
    insert_results: Dict[str, Any],
    job_start_time: float,
    hybrid_logger: Optional[HybridLogger] = None,
    files_count: int = 0,
    source_system: str = "unknown",
    job_type: str = "data_processing",
) -> Dict[str, Any]:
    """
    Calculate comprehensive job-level metrics for business reporting and observability
    Designed as separate Dagster task for job completion tracking

    Args:
        insert_results: Results from insert_records operation
        job_start_time: When the entire job started
        observability_logger: Logger for metrics collection
        files_count: Total files discovered in source
        source_system: Source system identifier
        job_type: Type of job being executed

    Returns:
        Dictionary with comprehensive job metrics for business reporting
    """

    job_end_time = time.time()
    total_job_duration = job_end_time - job_start_time

    # Extract core metrics from insert results
    records_inserted = insert_results.get("total_records", 0)
    records_failed = insert_results.get("failed_inserts", 0)
    records_processed = records_inserted + records_failed

    # Determine job status
    if records_failed == 0 and records_inserted > 0:
        job_status = OperationStatus.SUCCESS.value
    elif records_inserted > 0 and records_failed > 0:
        job_status = OperationStatus.PARTIAL_SUCCESS.value
    elif records_failed > 0:
        job_status = OperationStatus.FAILURE.value
    else:
        job_status = OperationStatus.ERROR.value

    # Build comprehensive job metrics
    job_metrics = {
        # Job identification
        "job_id": hybrid_logger.job_id if hybrid_logger else "unknown",
        "source": source_system,
        "job_type": job_type,
        "job_status": job_status,
        # Timing metrics
        "job_start": datetime.fromtimestamp(job_start_time).isoformat(),
        "job_end": datetime.fromtimestamp(job_end_time).isoformat(),
        "execution_time_ms": int(total_job_duration * 1000),
        # Core business metrics
        "files_discovered": files_count,
        "files_processed": files_count,  # Assuming all discovered files were processed
        "records_processed": records_processed,
        "records_inserted": records_inserted,
        "records_failed": records_failed,
        "records_missing": max(0, files_count - records_processed),
        # Calculated KPIs
        "average_processing_time_per_file_ms": (
            int((total_job_duration * 1000) / files_count) if files_count > 0 else 0
        ),
        # Data quality metrics
        "completeness_ratio": (
            round(records_processed / files_count, 4) if files_count > 0 else 1.0
        ),
        # Operational metrics
        "error_count": records_failed,
        "bytes_transferred": insert_results.get("total_size_bytes", 0),
        # Metadata
        "created_by": "spark_pipeline",
        "created_at": datetime.utcnow().isoformat(),
    }

    # Log job completion metrics
    if hybrid_logger:
        hybrid_logger.log(
            Operation.JOB_COMPLETE.value,
            OperationStatus.SUCCESS.value,
            {
                "execution_time_ms": job_metrics["execution_time_ms"],
                "job_status": job_status,
            },
        )

    return job_metrics
