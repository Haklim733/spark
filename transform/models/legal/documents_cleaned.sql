MODEL (
    name legal.documents_cleaned,
    kind INCREMENTAL_BY_TIME_RANGE (
        time_column load_timestamp
    ),
    grain [document_id, load_timestamp],
);

SELECT 
    document_id,
    document_type,
    raw_text,
    generated_at,
    file_path,
    document_length,
    word_count,
    language,
    load_batch_id,
    load_timestamp,
    -- Add data quality flags
    CASE 
        WHEN document_length > 0 THEN 'valid'
        ELSE 'invalid'
    END as quality_status,
    -- Add business logic categories
    CASE 
        WHEN document_type IN ('contract', 'agreement', 'lease') THEN 'legal_agreement'
        WHEN document_type IN ('policy', 'regulation', 'guideline') THEN 'policy_document'
        WHEN document_type IN ('report', 'analysis', 'summary') THEN 'analytical_document'
        ELSE 'other'
    END as business_category,
    CURRENT_TIMESTAMP as processed_at
FROM iceberg.legal.documents

WHERE load_timestamp >= @start_date
  AND load_timestamp < @end_date
  AND document_id IS NOT NULL
  AND raw_text IS NOT NULL; 