-- Legal Documents Table DDL
-- This table stores legal documents with metadata and LLM-extracted information

CREATE TABLE IF NOT EXISTS legal.documents (
    document_id STRING,
    document_type STRING,
    raw_text STRING,
    generation_date TIMESTAMP,
    file_path STRING,
    document_length INT,
    word_count INT,
    language STRING,
    metadata MAP<STRING, STRING>
)
USING iceberg
PARTITIONED BY (document_type, months(generation_date))
TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.merge.isolation-level' = 'snapshot',
    'comment' = 'Legal documents extracted raw'
); 