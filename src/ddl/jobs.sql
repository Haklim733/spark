-- Universal Batch Metrics Table for Data Lake
-- Tracks batch-level metrics across all tables in the data lake

CREATE OR REPLACE TABLE admin.batch_metrics (
    -- Core batch identification
    load_batch_id STRING,
    target_table STRING,  -- Which table was loaded (e.g., 'legal.documents', 'nyc.taxi_data')
    load_timestamp TIMESTAMP,
    load_source STRING,
    load_job_id STRING,
    load_version STRING,
    
    -- Schema version tracking
    metadata_schema_version STRING,  -- Version of metadata schema used
    
    -- File-level counts
    files_processed INT,
    files_loaded INT,
    files_failed INT,
    files_missing INT,
    files_successful INT,
    
    -- Record-level counts
    total_records_processed INT,
    total_records_loaded INT,
    total_records_failed INT,
    total_records_missing INT,
    
    -- Performance metrics
    load_duration_ms BIGINT,
    total_file_size_bytes BIGINT,
    avg_processing_rate DECIMAL(10,2),
    
    -- Error tracking
    error_codes MAP<STRING, INT>,  -- Error code -> count distribution
    error_summary STRING,
    
    -- Validation status
    validation_status STRING,  -- complete_success, partial_load, load_failures, unknown
    
    -- Audit trail
    created_by STRING
)
USING iceberg
PARTITIONED BY (target_table, DATE(load_timestamp))
TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.merge.isolation-level' = 'snapshot',
    'comment' = 'Universal batch metrics for data lake ELT pipeline loads with schema version tracking'
);
