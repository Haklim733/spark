-- Job Logs Fact Table - Single table with hierarchical structure
-- Captures job-level, batch-level, and operation-level metrics in one table
-- Uses NULL batch_id for job-level records, non-NULL for batch-level records
CREATE NAMESPACE IF NOT EXISTS dataops;

CREATE TABLE IF NOT EXISTS dataops.job_logs (
    -- Core identification
    log_id STRING COMMENT 'Unique log entry identifier',
    job_id STRING COMMENT 'Job identifier (always present)',
    batch_id STRING COMMENT 'Batch identifier (NULL for job-level records)',
    operation_name STRING COMMENT 'Operation name (NULL for job-level records)',
    
    -- Hierarchy level
    log_level STRING COMMENT 'Level of this log entry: job, batch, operation',
    
    -- Source and target
    source STRING COMMENT 'Source system identifier (local, minio, s3, hdfs, api)',
    target_table STRING COMMENT 'Target table name (NULL for job-level records)',
    app_name STRING COMMENT 'Application name that created the job',
    job_type STRING COMMENT 'Type of job (insert, transform, validate, etc.)',
    operation_type STRING COMMENT 'Type of operation (insert, update, delete, merge)',
    
    -- Timing (appropriate for the level)
    start_time TIMESTAMP COMMENT 'Start time for this level',
    end_time TIMESTAMP COMMENT 'End time for this level',
    duration_ms BIGINT COMMENT 'Duration in milliseconds for this level',
    
    -- Record-level metrics (batch and operation level only)
    records_listed INT COMMENT 'Total number of records/files discovered/listed in source directory',
    records_processed INT COMMENT 'Number of records/files that were actually attempted to be processed',
    records_inserted INT COMMENT 'Number of records/files successfully inserted into table',
    records_failed INT COMMENT 'Number of records/files that failed to load',
    records_missing INT COMMENT 'Number of records/files that were expected but not found',
    records_updated INT COMMENT 'Total number of records updated',
    records_deleted INT COMMENT 'Total number of records deleted',
    
    -- Performance metrics
    total_file_size_bytes BIGINT COMMENT 'Total size of files processed in bytes',
    bytes_transferred BIGINT COMMENT 'Total bytes transferred/processed',
    
    -- Error tracking
    error_count INT COMMENT 'Number of errors logged',
    error_codes MAP<STRING, INT> COMMENT 'Distribution of error codes',
    error_summary STRING COMMENT 'Human-readable summary of results',
    error_types ARRAY<STRING> COMMENT 'Types of errors encountered',
    
    -- Detailed failure tracking by partition
    failed_files_by_partition MAP<STRING, ARRAY<STRUCT<
        file_path: STRING,
        document_id: STRING,
        document_type: STRING,
        error_message: STRING,
        error_code: STRING,
        record_size_bytes: BIGINT,
        partition_id: INT,
        failure_timestamp: TIMESTAMP
    >>> COMMENT 'Detailed mapping of partition IDs to lists of failed files with their specific error information',
    
    -- Business events
    business_events_count INT COMMENT 'Number of business events logged',
    business_event_types ARRAY<STRING> COMMENT 'Types of business events encountered',
    
    -- Custom metrics
    custom_metrics_count INT COMMENT 'Number of custom metrics logged',
    custom_operation_names ARRAY<STRING> COMMENT 'Names of custom operations tracked',
    
    -- Logging system metrics
    total_logs_generated INT COMMENT 'Total number of log entries generated',
    async_logging_enabled BOOLEAN COMMENT 'Whether async logging was enabled',
    
    -- Status and validation
    validation_status STRING COMMENT 'Validation status: complete_success, partial_load, load_failures, unknown',
    status STRING COMMENT 'Overall status: success, failed, partial_success',
    
    -- System and environment (job level only)
    spark_version STRING COMMENT 'Spark version used',
    executor_count INT COMMENT 'Number of executors used',
    executor_memory_mb INT COMMENT 'Executor memory in MB',
    driver_memory_mb INT COMMENT 'Driver memory in MB',
    
    -- Audit trail
    created_by STRING COMMENT 'User/system that initiated the operation',
    created_at TIMESTAMP COMMENT 'When the record was created',
    updated_at TIMESTAMP COMMENT 'When the record was last updated'    
)
USING iceberg
PARTITIONED BY (date(start_time), app_name)
TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'snappy',
    'write.merge.isolation-level' = 'snapshot',
    'comment' = 'Job logs fact table with hierarchical structure: job-level, batch-level, and operation-level metrics'
); 