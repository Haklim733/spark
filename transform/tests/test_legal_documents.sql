-- Test for null values in critical columns
SELECT COUNT(*) as null_count
FROM legal.documents
WHERE document_id IS NULL 
   OR document_type IS NULL 
   OR raw_text IS NULL;

-- Test for duplicate document IDs
SELECT document_id, COUNT(*) as duplicate_count
FROM legal.documents
GROUP BY document_id
HAVING COUNT(*) > 1;

-- Test for document length quality
SELECT COUNT(*) as short_docs
FROM legal.documents
WHERE document_length < 100;  -- Documents too short

-- Test for valid document types
SELECT document_type, COUNT(*) as count
FROM legal.documents
GROUP BY document_type
ORDER BY count DESC; 