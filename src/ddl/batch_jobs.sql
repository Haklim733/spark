-- Universal Batch Metrics Table for Data Lake
-- Tracks batch-level metrics across all tables in the data lake

CREATE TABLE IF NOT EXISTS admin.batch_jobs (
    -- Core batch identification
    batch_id STRING COMMENT 'Unique batch identifier for tracking and reprocessing',
    job_id STRING COMMENT 'Job/process identifier for operational tracking',
    source STRING COMMENT 'Source system identifier (local, minio, s3, hdfs, api)',
    target_table STRING COMMENT 'Target table name for the batch load (e.g., legal.documents)',
    version STRING COMMENT 'Schema/process version for compatibility tracking',
    
    -- Timing metrics
    batch_start_time TIMESTAMP COMMENT 'When batch processing started',
    batch_end_time TIMESTAMP COMMENT 'When batch processing completed',
    batch_duration_ms BIGINT COMMENT 'Batch processing duration in milliseconds',
    
    -- File-level counts (operational metrics)
    files_processed INT COMMENT 'Total number of files attempted to process in this batch',
    files_loaded INT COMMENT 'Number of files successfully loaded into the target table',
    files_failed INT COMMENT 'Number of files that failed to load due to file system, format, or access issues',
    files_missing INT COMMENT 'Number of files that were expected but not found or accessible',
    files_successful INT COMMENT 'Number of files that loaded successfully (same as files_loaded)',
    
    -- Record-level counts (data quality metrics)
    total_records_processed INT COMMENT 'Total number of individual records/data rows processed across all files',
    total_records_loaded INT COMMENT 'Total number of individual records successfully loaded into the target table',
    total_records_failed INT COMMENT 'Total number of individual records that failed to load due to schema validation, data quality, or business rule violations',
    total_records_missing INT COMMENT 'Total number of individual records that were expected but not found or were empty/null',
    
    -- File metrics
    total_file_size_bytes BIGINT COMMENT 'Total size of all files processed in bytes',
    
    -- Error tracking
    error_codes MAP<STRING, INT> COMMENT 'Distribution of error codes encountered during load (error_code -> count)',
    error_summary STRING COMMENT 'Human-readable summary of load results',
    
    -- Validation status
    validation_status STRING COMMENT 'Overall validation status: complete_success, partial_load, load_failures, unknown',
    
    -- Audit trail
    created_by STRING COMMENT 'User/system that initiated the load',
    created_at TIMESTAMP COMMENT 'When the batch record was inserted into this table'
)
USING iceberg
PARTITIONED BY (target_table, DATE(batch_start_time))
TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.merge.isolation-level' = 'snapshot',
    'comment' = 'Universal batch metrics for data lake ELT pipeline loads with comprehensive operational and data quality tracking'
);
