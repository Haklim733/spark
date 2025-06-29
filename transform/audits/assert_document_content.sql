AUDIT (
    name assert_document_content
);

SELECT 
    COUNT(*) as invalid_content
FROM @this_model
WHERE raw_text IS NULL 
   OR raw_text = ''
   OR LENGTH(TRIM(raw_text)) = 0
   OR document_length <= 0
   OR word_count <= 0 