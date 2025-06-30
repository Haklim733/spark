MODEL (
    name legal.data_quality_metrics,
    kind FULL,
    description "Data quality metrics for legal documents using ELT approach"
);

WITH raw_documents AS (
    SELECT * FROM iceberg.legal.documents_raw
),

load_quality AS (
    SELECT 
        'legal_documents_raw' as table_name,
        COUNT(*) as total_records,
        COUNT(CASE WHEN load_status = 'loaded' THEN 1 END) as successful_loads,
        COUNT(CASE WHEN load_status = 'load_failed' THEN 1 END) as load_failures,
        ROUND((COUNT(CASE WHEN load_status = 'loaded' THEN 1 END) * 100.0 / COUNT(*)), 2) as load_success_rate,
        COUNT(CASE WHEN document_id IS NULL THEN 1 END) as null_document_ids,
        COUNT(CASE WHEN document_type IS NULL THEN 1 END) as null_document_types,
        COUNT(CASE WHEN raw_text IS NULL THEN 1 END) as null_raw_text,
        COUNT(DISTINCT document_id) as unique_document_ids,
        COUNT(*) - COUNT(DISTINCT document_id) as duplicate_document_ids,
        AVG(document_length) as avg_document_length,
        MIN(document_length) as min_document_length,
        MAX(document_length) as max_document_length,
        AVG(word_count) as avg_word_count,
        MIN(word_count) as min_word_count,
        MAX(word_count) as max_word_count
    FROM raw_documents
),

metadata_quality AS (
    SELECT 
        'legal_documents_metadata' as table_name,
        COUNT(*) as total_records,
        COUNT(CASE WHEN JSON_VALID(raw_metadata) THEN 1 END) as valid_json_metadata,
        COUNT(CASE WHEN NOT JSON_VALID(raw_metadata) THEN 1 END) as invalid_json_metadata,
        ROUND((COUNT(CASE WHEN JSON_VALID(raw_metadata) THEN 1 END) * 100.0 / COUNT(*)), 2) as metadata_validity_rate,
        COUNT(CASE WHEN JSON_EXTRACT(raw_metadata, '$.source_file') IS NOT NULL THEN 1 END) as has_source_file,
        COUNT(CASE WHEN JSON_EXTRACT(raw_metadata, '$.file_size') IS NOT NULL THEN 1 END) as has_file_size,
        COUNT(CASE WHEN JSON_EXTRACT(raw_metadata, '$.format') IS NOT NULL THEN 1 END) as has_format
    FROM raw_documents
    WHERE load_status = 'loaded'
),

document_type_quality AS (
    SELECT 
        'legal_documents_by_type' as table_name,
        document_type,
        COUNT(*) as total_records,
        COUNT(CASE WHEN load_status = 'loaded' THEN 1 END) as successful_loads,
        COUNT(CASE WHEN load_status = 'load_failed' THEN 1 END) as load_failures,
        ROUND((COUNT(CASE WHEN load_status = 'loaded' THEN 1 END) * 100.0 / COUNT(*)), 2) as load_success_rate,
        AVG(document_length) as avg_document_length,
        AVG(word_count) as avg_word_count,
        COUNT(CASE WHEN JSON_VALID(raw_metadata) THEN 1 END) as valid_metadata_count,
        ROUND((COUNT(CASE WHEN JSON_VALID(raw_metadata) THEN 1 END) * 100.0 / COUNT(*)), 2) as metadata_validity_rate
    FROM raw_documents
    GROUP BY document_type
),

validation_quality AS (
    SELECT 
        'legal_documents_validation' as table_name,
        COUNT(*) as total_records,
        COUNT(CASE WHEN document_length > 0 AND word_count > 0 THEN 1 END) as valid_content,
        COUNT(CASE WHEN document_length <= 0 OR word_count <= 0 THEN 1 END) as invalid_content,
        COUNT(CASE WHEN JSON_VALID(raw_metadata) THEN 1 END) as valid_metadata,
        COUNT(CASE WHEN NOT JSON_VALID(raw_metadata) THEN 1 END) as invalid_metadata,
        COUNT(CASE WHEN 
            document_length > 0 
            AND word_count > 0 
            AND JSON_VALID(raw_metadata)
            AND (
                (document_type = 'email' AND 
                 JSON_EXTRACT(raw_metadata, '$.sender') IS NOT NULL AND 
                 JSON_EXTRACT(raw_metadata, '$.subject') IS NOT NULL)
                OR
                (document_type = 'contract' AND 
                 JSON_EXTRACT(raw_metadata, '$.parties') IS NOT NULL AND 
                 JSON_EXTRACT(raw_metadata, '$.contract_value') IS NOT NULL)
                OR
                (document_type = 'legal_memo' AND 
                 JSON_EXTRACT(raw_metadata, '$.author') IS NOT NULL AND 
                 JSON_EXTRACT(raw_metadata, '$.subject') IS NOT NULL)
                OR
                (document_type NOT IN ('email', 'contract', 'legal_memo'))
            )
        THEN 1 END) as fully_valid_documents,
        ROUND((COUNT(CASE WHEN 
            document_length > 0 
            AND word_count > 0 
            AND JSON_VALID(raw_metadata)
            AND (
                (document_type = 'email' AND 
                 JSON_EXTRACT(raw_metadata, '$.sender') IS NOT NULL AND 
                 JSON_EXTRACT(raw_metadata, '$.subject') IS NOT NULL)
                OR
                (document_type = 'contract' AND 
                 JSON_EXTRACT(raw_metadata, '$.parties') IS NOT NULL AND 
                 JSON_EXTRACT(raw_metadata, '$.contract_value') IS NOT NULL)
                OR
                (document_type = 'legal_memo' AND 
                 JSON_EXTRACT(raw_metadata, '$.author') IS NOT NULL AND 
                 JSON_EXTRACT(raw_metadata, '$.subject') IS NOT NULL)
                OR
                (document_type NOT IN ('email', 'contract', 'legal_memo'))
            )
        THEN 1 END) * 100.0 / COUNT(*)), 2) as overall_quality_rate
    FROM raw_documents
    WHERE load_status = 'loaded'
),

daily_quality_trends AS (
    SELECT 
        'legal_documents_daily_trends' as table_name,
        DATE(load_timestamp) as load_date,
        COUNT(*) as total_records,
        COUNT(CASE WHEN load_status = 'loaded' THEN 1 END) as successful_loads,
        COUNT(CASE WHEN load_status = 'load_failed' THEN 1 END) as load_failures,
        ROUND((COUNT(CASE WHEN load_status = 'loaded' THEN 1 END) * 100.0 / COUNT(*)), 2) as load_success_rate,
        COUNT(CASE WHEN JSON_VALID(raw_metadata) THEN 1 END) as valid_metadata,
        ROUND((COUNT(CASE WHEN JSON_VALID(raw_metadata) THEN 1 END) * 100.0 / COUNT(*)), 2) as metadata_validity_rate
    FROM raw_documents
    GROUP BY DATE(load_timestamp)
    ORDER BY load_date DESC
    LIMIT 30  -- Last 30 days
)

SELECT * FROM load_quality
UNION ALL
SELECT * FROM metadata_quality
UNION ALL
SELECT * FROM document_type_quality
UNION ALL
SELECT * FROM validation_quality
UNION ALL
SELECT * FROM daily_quality_trends 