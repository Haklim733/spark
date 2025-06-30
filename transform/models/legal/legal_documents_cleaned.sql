MODEL (
    name legal.legal_documents_cleaned,
    kind INCREMENTAL_BY_TIME_RANGE (
        time_column load_timestamp,
    ),
    grain [document_id, load_timestamp],
    description "Cleaned legal documents from raw data using ELT approach"
);

WITH raw_documents AS (
    SELECT * FROM iceberg.legal.documents_raw
    WHERE load_status = 'loaded'  -- Only process successfully loaded documents
      AND load_timestamp >= @start_date
      AND load_timestamp < @end_date
),

validated_documents AS (
    SELECT 
        document_id,
        document_type,
        raw_text,
        raw_metadata,
        file_path,
        document_length,
        word_count,
        language,
        load_timestamp,
        source_system,
        
        -- Parse and validate metadata
        CASE 
            WHEN JSON_VALID(raw_metadata) THEN raw_metadata
            ELSE NULL 
        END as parsed_metadata,
        
        -- Validation flags
        CASE 
            WHEN JSON_VALID(raw_metadata) THEN true
            ELSE false 
        END as metadata_is_valid_json,
        
        CASE 
            WHEN document_length > 0 AND word_count > 0 THEN true
            ELSE false 
        END as content_is_valid,
        
        -- Document type specific validation
        CASE 
            WHEN document_type = 'email' THEN
                JSON_EXTRACT(raw_metadata, '$.sender') IS NOT NULL
                AND JSON_EXTRACT(raw_metadata, '$.subject') IS NOT NULL
            WHEN document_type = 'contract' THEN
                JSON_EXTRACT(raw_metadata, '$.parties') IS NOT NULL
                AND JSON_EXTRACT(raw_metadata, '$.contract_value') IS NOT NULL
            WHEN document_type = 'legal_memo' THEN
                JSON_EXTRACT(raw_metadata, '$.author') IS NOT NULL
                AND JSON_EXTRACT(raw_metadata, '$.subject') IS NOT NULL
            WHEN document_type = 'court_filing' THEN
                JSON_EXTRACT(raw_metadata, '$.case_number') IS NOT NULL
                AND JSON_EXTRACT(raw_metadata, '$.filing_date') IS NOT NULL
            WHEN document_type = 'policy_document' THEN
                JSON_EXTRACT(raw_metadata, '$.policy_number') IS NOT NULL
                AND JSON_EXTRACT(raw_metadata, '$.effective_date') IS NOT NULL
            WHEN document_type = 'legal_opinion' THEN
                JSON_EXTRACT(raw_metadata, '$.author') IS NOT NULL
                AND JSON_EXTRACT(raw_metadata, '$.subject') IS NOT NULL
            ELSE true
        END as type_specific_validation,
        
        -- Extract common metadata fields
        JSON_EXTRACT(raw_metadata, '$.source_file') as source_file,
        JSON_EXTRACT(raw_metadata, '$.file_size') as file_size,
        JSON_EXTRACT(raw_metadata, '$.format') as format_type,
        
        -- Extract document type specific fields
        CASE 
            WHEN document_type = 'email' THEN JSON_EXTRACT(raw_metadata, '$.sender')
            ELSE NULL 
        END as email_sender,
        
        CASE 
            WHEN document_type = 'email' THEN JSON_EXTRACT(raw_metadata, '$.recipients')
            ELSE NULL 
        END as email_recipients,
        
        CASE 
            WHEN document_type = 'contract' THEN JSON_EXTRACT(raw_metadata, '$.parties')
            ELSE NULL 
        END as contract_parties,
        
        CASE 
            WHEN document_type = 'contract' THEN JSON_EXTRACT(raw_metadata, '$.contract_value')
            ELSE NULL 
        END as contract_value,
        
        CASE 
            WHEN document_type = 'legal_memo' THEN JSON_EXTRACT(raw_metadata, '$.author')
            ELSE NULL 
        END as memo_author
        
    FROM raw_documents
),

final_cleaned_documents AS (
    SELECT 
        document_id,
        document_type,
        TRIM(raw_text) as cleaned_text,  -- Remove extra whitespace
        document_length,
        word_count,
        language,
        load_timestamp as generation_date,  -- Use load_timestamp for consistency
        source_system,
        
        -- Metadata fields
        parsed_metadata,
        source_file,
        file_size,
        format_type,
        email_sender,
        email_recipients,
        contract_parties,
        contract_value,
        memo_author,
        
        -- Validation status
        CASE 
            WHEN metadata_is_valid_json 
                AND content_is_valid 
                AND type_specific_validation 
            THEN 'valid'
            ELSE 'invalid'
        END as validation_status,
        
        -- Data quality flags
        metadata_is_valid_json as is_valid_metadata,
        content_is_valid as is_valid_content,
        type_specific_validation as is_valid_type_specific,
        
        -- Business logic categories
        CASE 
            WHEN document_length < 500 THEN 'short'
            WHEN document_length < 1500 THEN 'medium'
            ELSE 'long'
        END as length_category,
        
        CASE 
            WHEN LOWER(raw_text) LIKE '%contract%' THEN 'contract_related'
            WHEN LOWER(raw_text) LIKE '%agreement%' THEN 'agreement_related'
            WHEN LOWER(raw_text) LIKE '%legal%' THEN 'legal_related'
            WHEN LOWER(raw_text) LIKE '%email%' THEN 'email_related'
            WHEN LOWER(raw_text) LIKE '%memo%' THEN 'memo_related'
            ELSE 'other'
        END as content_category,
        
        CURRENT_TIMESTAMP as processed_at
        
    FROM validated_documents
)

SELECT * FROM final_cleaned_documents
WHERE validation_status = 'valid'  -- Only return valid documents
  AND document_id IS NOT NULL
  AND document_type IS NOT NULL
  AND cleaned_text IS NOT NULL 