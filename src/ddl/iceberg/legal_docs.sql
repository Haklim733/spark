-- Legal Documents Content Table
-- Stores the actual text content of legal documents processed from S3
-- Partitioned by document_type and batch_id for optimal storage and retrieval

CREATE OR REPLACE TABLE legal.docs (
    -- Primary identification
    document_id STRING COMMENT 'Unique identifier for the legal document, matches legal_docs_metadata.document_id',
    document_type STRING COMMENT 'Type of legal document (contract, agreement, etc.)',
    
    -- Content data
    raw_text STRING COMMENT 'Full text content of the legal document',
    
    -- File tracking
    file_path STRING COMMENT 'S3 path to the original content file',
    
    -- Processing metadata
    job_id STRING COMMENT 'Job identifier that processed this document',
    
    -- Content metrics
    text_length BIGINT COMMENT 'Length of the raw text content in characters',
    
    -- Audit fields
    loaded_at TIMESTAMP COMMENT 'Record loaded timestamp',
    updated_at TIMESTAMP COMMENT 'Record last update timestamp',
    
    -- Distributed processing metadata (for debugging/lineage)
    spark_partition_id INTEGER COMMENT 'Spark partition that processed this record',
    task_execution_id BIGINT COMMENT 'Unique task execution identifier for distributed debugging'
)
USING ICEBERG
PARTITIONED BY (loaded_at)
TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'snappy',
    'write.target-file-size-bytes' = '268435456',
    'write.metadata.compression-codec' = 'gzip',
    'write.metadata.metrics.default' = 'truncate(16)',
    'write.metadata.metrics.column.document_id' = 'full',
    'write.metadata.metrics.column.job_id' = 'full',
    'write.metadata.metrics.column.text_length' = 'full'
)
COMMENT 'Content table for legal documents containing the actual text data and processing information'; 

ALTER TABLE legal.docs CREATE BRANCH IF NOT EXISTS main;