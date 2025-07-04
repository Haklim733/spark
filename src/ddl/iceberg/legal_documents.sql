-- Legal Documents Table DDL for ELT approach
-- This table stores raw legal documents with metadata as individual columns

CREATE OR REPLACE TABLE legal.documents (
    -- metadata
    document_id STRING COMMENT 'Unique identifier for the document',
    document_type STRING COMMENT 'Type of legal document (contract, legal_memo, court_filing, policy_document, legal_opinion)',
    generated_at TIMESTAMP COMMENT 'When the document was generated (UTC ISO-8601)',
    source STRING COMMENT 'Source system identifier (default: soli_legal_document_generator)',
    language STRING COMMENT 'Language of the document content (en, es, fr, de, it, pt)',
    file_size BIGINT COMMENT 'Size of the file in bytes, retrieved from MinIO metadata',
    method STRING COMMENT 'Processing method used for document ingestion (sequential, spark, local, parallel_batch, distributed)',
    schema_version STRING COMMENT 'Version of the metadata schema used',
    metadata_file_path STRING COMMENT 'Path to the metadata file in MinIO storage',
    
    -- content
    raw_text STRING COMMENT 'Raw text content of the legal document',
    file_path STRING COMMENT 'Path to the document file in MinIO storage',
    
    -- ELT Load tracking fields
    batch_id STRING COMMENT 'Links to batch_jobs table for operational tracking',
    job_id STRING COMMENT 'Job identifier for operational tracking and debugging'    
)
USING iceberg
PARTITIONED BY (document_type, month(generated_at))
TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.merge.isolation-level' = 'snapshot',
    'comment' = 'Raw legal documents with metadata as individual columns for ELT processing'
); 