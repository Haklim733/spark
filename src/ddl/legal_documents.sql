-- Legal Documents Table DDL for ELT approach
-- This table stores raw legal documents before transformation

CREATE OR REPLACE TABLE legal.documents_raw (
    document_id STRING,
    document_type STRING,
    raw_text STRING,
    raw_metadata STRING,  -- JSON string for ELT approach
    file_path STRING,
    metadata_file_path STRING,  -- Path to the separate metadata JSON file
    document_length BIGINT,
    word_count BIGINT,
    language STRING,
    load_timestamp TIMESTAMP,
    source_system STRING,
    load_status STRING,
    load_error STRING,
    -- Additional metadata fields from document generation
    uuid STRING,  -- UUID used for filename
    content_filename STRING,  -- The .txt filename
    metadata_filename STRING,  -- The .json filename
    processing_timestamp STRING,  -- When the document was processed
    metadata_version STRING  -- Version of metadata schema
)
USING iceberg
PARTITIONED BY (document_type, month(load_timestamp))
TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.merge.isolation-level' = 'snapshot',
    'comment' = 'Raw legal documents for ELT processing - stores both content and metadata'
); 