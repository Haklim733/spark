MODEL (
    name legal.legal_documents_error,
    kind INCREMENTAL_BY_TIME_RANGE (
        time_column quarantined_at,
    ),
    grain [original_document_id, error_sequence],
    audits [assert_error_data_integrity],
    description "Legal documents error data that works with both DuckDB and Spark gateways"
);

SELECT
    original_document_id,
    document_type,
    raw_text,
    generation_date,
    file_path,
    document_length,
    word_count,
    language,
    metadata,
    error_message,
    error_type,
    quarantined_at,
    error_sequence,
    field_name,
    field_value
FROM iceberg.legal.documents_error
WHERE quarantined_at >= @start_date
  AND quarantined_at < @end_date 