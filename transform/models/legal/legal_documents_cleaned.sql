MODEL (
    name legal.legal_documents_cleaned,
    kind INCREMENTAL_BY_TIME_RANGE (
        time_column generation_date,
    ),
    grain [document_id, generation_date],
    description "Cleaned legal documents that works with both DuckDB and Spark gateways"
);

SELECT 
    document_id,
    document_type,
    TRIM(raw_text) as cleaned_text,  -- Remove extra whitespace
    document_length,
    word_count,
    generation_date,
    -- Add data quality flags
    CASE WHEN document_length > 0 THEN true ELSE false END as is_valid_length,
    CASE WHEN word_count > 0 THEN true ELSE false END as is_valid_word_count,
    -- Add business logic categories
    CASE 
        WHEN document_length < 500 THEN 'short'
        WHEN document_length < 1500 THEN 'medium'
        ELSE 'long'
    END as length_category,
    CASE 
        WHEN LOWER(raw_text) LIKE '%contract%' THEN 'contract_related'
        WHEN LOWER(raw_text) LIKE '%agreement%' THEN 'agreement_related'
        WHEN LOWER(raw_text) LIKE '%legal%' THEN 'legal_related'
        ELSE 'other'
    END as content_category,
    CURRENT_TIMESTAMP as processed_at
FROM iceberg.legal.documents
WHERE generation_date >= @start_date
  AND generation_date < @end_date
  AND document_id IS NOT NULL  -- Filter out invalid records
  AND document_type IS NOT NULL
  AND raw_text IS NOT NULL 