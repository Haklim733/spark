MODEL (
    name legal.documents_cleaned,
    kind INCREMENTAL_BY_TIME_RANGE (
        time_column generated_at
    ),
    grain [document_id, generated_at],
);

-- Reference existing table without schema management
SELECT 
    document_id,
    document_type,
    raw_text,
    generated_at,
    file_path,
    source,
    language,
    file_size,
    method,
    schema_version,
    metadata_file_path,
    batch_id,
    job_id,
    -- Calculate content length from raw_text
    CASE 
        WHEN raw_text IS NOT NULL THEN LENGTH(raw_text)
        ELSE 0
    END as content_length,
    -- Calculate word count from raw_text
    CASE 
        WHEN raw_text IS NOT NULL THEN SIZE(SPLIT(raw_text, ' '))
        ELSE 0
    END as word_count,
    -- Data quality assessment
    CASE 
        WHEN raw_text IS NOT NULL AND LENGTH(raw_text) > 0 
             AND document_id IS NOT NULL 
             AND generated_at IS NOT NULL THEN 'valid'
        ELSE 'invalid'
    END as data_quality_status
FROM iceberg.legal.documents_snapshot.branch_staging
WHERE generated_at >= @start_date
  AND generated_at < @end_date; 