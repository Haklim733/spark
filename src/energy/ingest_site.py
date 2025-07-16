#!/usr/bin/env python3
"""
Site Data Processing Pipeline
Processes site reference data and inserts into Iceberg site table
"""
import time
from pathlib import Path
from typing import Optional

from pyspark.sql.types import DoubleType
from pyspark.sql.functions import lit
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
)


def main(namespace: str, table_name: str, limit: Optional[int] = None):
    """Main function to process site data"""
    app_name = Path(__file__).stem

    # Create Spark session for Iceberg operations
    spark = create_spark_session(
        spark_version=SparkVersion.SPARK_CONNECT_3_5,
        app_name=app_name,
        catalog="iceberg",
    )

    try:
        job_id = generate_job_id()
        job_start_time = time.time()

        with HybridLogger(spark=spark, app_name=app_name, manage_spark=False) as logger:

            print(f"🏢 Starting site data processing...")
            print(f"📁 Source: s3://raw/site")
            print(f"🎯 Target: energy.pv_site")
            print(f"📋 Job ID: {job_id}")

            try:
                print("📋 Loading site data files...")
                target_path_pattern = f"s3a://raw/site/*.parquet"
                files, file_count = list_files(
                    spark, target_path_pattern, logger, limit
                )

                print(f"✅ Found {file_count} site records")

                check_table_exists(spark, namespace, table_name, logger)

                files_df = read_files(spark, files, logger)

                processed_df = safe_cast_columns(
                    df=files_df,
                    columns=[
                        "av_pressure",
                        "av_temp",
                        "elevation",
                        "latitude",
                        "longitude",
                    ],
                    col_type=DoubleType,
                    logger=logger,
                )

                processed_df = processed_df.withColumn("job_id", lit(job_id))
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
                    files_count=files_df.count(),
                )

                print(f"✅ Site data processing completed successfully")
                print(
                    f"📊 Job metrics: {job_metrics['job_status']} - {job_metrics['records_inserted']} records"
                )

                # Return job metrics for potential Dagster asset dependency
                return job_metrics

            except FileNotFoundError as e:
                logger.log(
                    Operation.FILE_READ.value,
                    OperationStatus.FAILURE.value,
                    {
                        "error_code": ErrorCode.FILE_NOT_FOUND.value,
                        "source_path": target_path_pattern,
                    },
                )
                raise
            except PermissionError as e:
                logger.log(
                    Operation.FILE_READ.value,
                    OperationStatus.FAILURE.value,
                    {
                        "error_code": ErrorCode.PERMISSION_DENIED.value,
                        "source_path": target_path_pattern,
                    },
                )
                raise
            except Exception as e:
                logger.log(
                    Operation.FILE_READ.value,
                    OperationStatus.ERROR.value,
                    {
                        "error_code": ErrorCode.PROCESSING_ERROR.value,
                        "context": "site_data_processing",
                    },
                )
                raise

    except Exception as e:
        print(f"❌ Error in site data processing: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main(namespace="energy", table_name="pv_site")
