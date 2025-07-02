MODEL (
    name legal.documents_cleaned,
    kind INCREMENTAL_BY_TIME_RANGE (
        time_column load_timestamp
    ),
    grain [document_id, load_timestamp],
);

-- Reference existing table without schema management
SELECT 
    document_id,
    document_type,
    raw_text,
    generated_at,
    file_path,
    content_length as document_length,
    word_count,
    language,
    load_batch_id,
    load_timestamp,
    -- Replace JSON_VALID with Spark-compatible logic
    CASE 
        WHEN document_length > 0 AND raw_text IS NOT NULL THEN 'valid'
        ELSE 'invalid'
    END as data_quality_status
FROM iceberg.legal.documents
WHERE load_timestamp >= @start_date
  AND load_timestamp < @end_date; 