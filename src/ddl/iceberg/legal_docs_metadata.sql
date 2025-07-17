-- Legal Documents Metadata Table
-- Stores metadata information for legal documents processed from S3
-- Partitioned by document_type and source for optimal query performance

CREATE OR REPLACE TABLE legal.docs_metadata (
    -- Primary identification
    document_id STRING COMMENT 'Unique identifier for the legal document',
    document_type STRING COMMENT 'Type of legal document (contract, agreement, etc.)',
    
    -- Document metadata
    source STRING COMMENT 'Source system or organization that provided the document',
    language STRING COMMENT 'Language of the document (e.g., en, es, fr)',
    file_size BIGINT COMMENT 'Size of the original file in bytes',
    method STRING COMMENT 'Processing method used to extract the document',
    
    -- Schema and versioning
    schema_version STRING COMMENT 'Version of the schema used for processing',
    
    -- File tracking
    metadata_file_path STRING COMMENT 'S3 path to the original metadata file',
    
    -- Processing metadata
    job_id STRING COMMENT 'Job identifier that processed this document',
    generated_at TIMESTAMP COMMENT 'Timestamp when metadata was generated',
    
    -- Audit fields
    loaded_at TIMESTAMP COMMENT 'Record loaded timestamp',
    updated_at TIMESTAMP COMMENT 'Record last update timestamp',
    
    -- Distributed processing metadata (for debugging/lineage)
    spark_partition_id INTEGER COMMENT 'Spark partition that processed this record',
    task_execution_id BIGINT COMMENT 'Unique task execution identifier for distributed debugging'
)
USING ICEBERG
PARTITIONED BY (document_type, source)
TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'snappy',
    'write.target-file-size-bytes' = '134217728',
    'write.metadata.compression-codec' = 'gzip',
    'write.metadata.metrics.default' = 'truncate(16)',
    'write.metadata.metrics.column.document_id' = 'full',
    'write.metadata.metrics.column.batch_id' = 'full',
    'write.metadata.metrics.column.job_id' = 'full'
)
COMMENT 'Metadata table for legal documents containing document properties, processing information, and file tracking data'; 

ALTER TABLE legal.docs_metadata CREATE BRANCH IF NOT EXISTS main;