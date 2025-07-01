MODEL (
    name legal.documents_error_cleaned,
    kind INCREMENTAL_BY_TIME_RANGE (
        time_column load_timestamp,
    ),
    grain [document_id, error_type],
    description "Legal documents error data using ELT approach - identifies issues from raw data"
);

WITH raw_documents AS (
    SELECT * FROM legal.documents
    WHERE load_timestamp >= @start_date
      AND load_timestamp < @end_date
),

load_errors AS (
    -- Documents that failed to load (critical errors)
    SELECT 
        document_id,
        document_type,
        raw_text,
        raw_metadata,
        file_path,
        load_timestamp,
        source_system,
        'load_error' as error_type,
        load_error as error_message,
        'Critical error during file loading prevented successful ingestion' as error_description,
        'high' as error_severity,
        CURRENT_TIMESTAMP as error_detected_at
    FROM raw_documents
    WHERE load_status = 'load_failed'
),

validation_errors AS (
    -- Documents that loaded but failed validation
    SELECT 
        document_id,
        document_type,
        raw_text,
        raw_metadata,
        file_path,
        load_timestamp,
        source_system,
        'validation_error' as error_type,
        CASE 
            WHEN NOT JSON_VALID(raw_metadata) THEN 'Invalid JSON metadata structure'
            WHEN document_length <= 0 OR word_count <= 0 THEN 'Empty or invalid document content'
            WHEN document_type = 'email' AND (
                JSON_EXTRACT(raw_metadata, '$.sender') IS NULL 
                OR JSON_EXTRACT(raw_metadata, '$.subject') IS NULL
            ) THEN 'Email document missing required fields (sender, subject)'
            WHEN document_type = 'contract' AND (
                JSON_EXTRACT(raw_metadata, '$.parties') IS NULL 
                OR JSON_EXTRACT(raw_metadata, '$.contract_value') IS NULL
            ) THEN 'Contract document missing required fields (parties, contract_value)'
            WHEN document_type = 'legal_memo' AND (
                JSON_EXTRACT(raw_metadata, '$.author') IS NULL 
                OR JSON_EXTRACT(raw_metadata, '$.subject') IS NULL
            ) THEN 'Legal memo missing required fields (author, subject)'
            WHEN document_type = 'court_filing' AND (
                JSON_EXTRACT(raw_metadata, '$.case_number') IS NULL 
                OR JSON_EXTRACT(raw_metadata, '$.filing_date') IS NULL
            ) THEN 'Court filing missing required fields (case_number, filing_date)'
            WHEN document_type = 'policy_document' AND (
                JSON_EXTRACT(raw_metadata, '$.policy_number') IS NULL 
                OR JSON_EXTRACT(raw_metadata, '$.effective_date') IS NULL
            ) THEN 'Policy document missing required fields (policy_number, effective_date)'
            WHEN document_type = 'legal_opinion' AND (
                JSON_EXTRACT(raw_metadata, '$.author') IS NULL 
                OR JSON_EXTRACT(raw_metadata, '$.subject') IS NULL
            ) THEN 'Legal opinion missing required fields (author, subject)'
            ELSE 'Unknown validation error'
        END as error_message,
        CASE 
            WHEN NOT JSON_VALID(raw_metadata) THEN 'Metadata is not valid JSON format'
            WHEN document_length <= 0 OR word_count <= 0 THEN 'Document has no meaningful content'
            WHEN document_type = 'email' THEN 'Email document missing required metadata fields for processing'
            WHEN document_type = 'contract' THEN 'Contract document missing required metadata fields for processing'
            WHEN document_type = 'legal_memo' THEN 'Legal memo missing required metadata fields for processing'
            WHEN document_type = 'court_filing' THEN 'Court filing missing required metadata fields for processing'
            WHEN document_type = 'policy_document' THEN 'Policy document missing required metadata fields for processing'
            WHEN document_type = 'legal_opinion' THEN 'Legal opinion missing required metadata fields for processing'
            ELSE 'Document failed type-specific validation requirements'
        END as error_description,
        CASE 
            WHEN NOT JSON_VALID(raw_metadata) THEN 'high'
            WHEN document_length <= 0 OR word_count <= 0 THEN 'high'
            ELSE 'medium'
        END as error_severity,
        CURRENT_TIMESTAMP as error_detected_at
    FROM raw_documents
    WHERE load_status = 'loaded'
    AND (
        NOT JSON_VALID(raw_metadata)
        OR document_length <= 0 
        OR word_count <= 0
        OR (document_type = 'email' AND (
            JSON_EXTRACT(raw_metadata, '$.sender') IS NULL 
            OR JSON_EXTRACT(raw_metadata, '$.subject') IS NULL
        ))
        OR (document_type = 'contract' AND (
            JSON_EXTRACT(raw_metadata, '$.parties') IS NULL 
            OR JSON_EXTRACT(raw_metadata, '$.contract_value') IS NULL
        ))
        OR (document_type = 'legal_memo' AND (
            JSON_EXTRACT(raw_metadata, '$.author') IS NULL 
            OR JSON_EXTRACT(raw_metadata, '$.subject') IS NULL
        ))
        OR (document_type = 'court_filing' AND (
            JSON_EXTRACT(raw_metadata, '$.case_number') IS NULL 
            OR JSON_EXTRACT(raw_metadata, '$.filing_date') IS NULL
        ))
        OR (document_type = 'policy_document' AND (
            JSON_EXTRACT(raw_metadata, '$.policy_number') IS NULL 
            OR JSON_EXTRACT(raw_metadata, '$.effective_date') IS NULL
        ))
        OR (document_type = 'legal_opinion' AND (
            JSON_EXTRACT(raw_metadata, '$.author') IS NULL 
            OR JSON_EXTRACT(raw_metadata, '$.subject') IS NULL
        ))
    )
),

data_quality_errors AS (
    -- Additional data quality issues
    SELECT 
        document_id,
        document_type,
        raw_text,
        raw_metadata,
        file_path,
        load_timestamp,
        source_system,
        'data_quality_error' as error_type,
        CASE 
            WHEN document_length > 100000 THEN 'Document exceeds size limit (100KB)'
            WHEN word_count > 50000 THEN 'Document exceeds word count limit (50K words)'
            WHEN document_type NOT IN ('email', 'contract', 'legal_memo', 'court_filing', 'policy_document', 'legal_opinion') THEN 'Unknown document type'
            ELSE 'Data quality issue detected'
        END as error_message,
        CASE 
            WHEN document_length > 100000 THEN 'Document size exceeds processing limits'
            WHEN word_count > 50000 THEN 'Document word count exceeds processing limits'
            WHEN document_type NOT IN ('email', 'contract', 'legal_memo', 'court_filing', 'policy_document', 'legal_opinion') THEN 'Document type not recognized in current schema'
            ELSE 'Document failed data quality checks'
        END as error_description,
        'low' as error_severity,
        CURRENT_TIMESTAMP as error_detected_at
    FROM raw_documents
    WHERE load_status = 'loaded'
    AND (
        document_length > 100000
        OR word_count > 50000
        OR document_type NOT IN ('email', 'contract', 'legal_memo', 'court_filing', 'policy_document', 'legal_opinion')
    )
)

SELECT * FROM load_errors
UNION ALL
SELECT * FROM validation_errors
UNION ALL
SELECT * FROM data_quality_errors 