AUDIT (
    name assert_valid_document_id
);

SELECT 
    COUNT(*) as invalid_document_ids
FROM @this_model
WHERE document_id IS NULL 
   OR document_id = ''
   OR LENGTH(TRIM(document_id)) = 0 