-- Legal Documents Table DDL for ELT approach
-- This table stores raw legal documents with metadata as individual columns

CREATE TABLE IF NOT EXISTS legal.documents (
    -- Core document fields
    document_id STRING,
    document_type STRING,
    raw_text STRING,
    generated_at TIMESTAMP,
    source STRING,
    file_path STRING,
    language STRING,  
    content_length BIGINT,
    
    -- Schema version tracking
    schema_version STRING,
    
    -- ELT Load tracking fields
    load_batch_id STRING,
    load_timestamp TIMESTAMP
)
USING iceberg
PARTITIONED BY (document_type, month(generated_at))
TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.merge.isolation-level' = 'snapshot',
    'comment' = 'Raw legal documents with metadata as individual columns for ELT processing'
); 