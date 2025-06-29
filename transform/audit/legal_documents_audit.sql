-- Legal Documents Data Quality Audit using SQLMesh AUDIT syntax

-- 1. Legal Documents Null Check
AUDIT (
  name assert_legal_docs_not_null,
  description "Ensure critical legal document fields are not null"
);

SELECT COUNT(*) as null_count
FROM legal.documents
WHERE document_id IS NULL 
   OR document_type IS NULL 
   OR raw_text IS NULL;

-- 2. Legal Documents Duplicate Check
AUDIT (
  name assert_legal_docs_unique,
  description "Ensure document IDs are unique"
);

SELECT document_id, COUNT(*) as duplicate_count
FROM legal.documents
GROUP BY document_id
HAVING COUNT(*) > 1;

-- 3. Legal Documents Length Validation
AUDIT (
  name assert_legal_docs_valid_length,
  description "Ensure documents have reasonable length"
);

SELECT COUNT(*) as short_docs
FROM legal.documents
WHERE document_length < 100;

-- 4. Legal Documents Word Count Validation
AUDIT (
  name assert_legal_docs_valid_word_count,
  description "Ensure documents have reasonable word count"
);

SELECT COUNT(*) as invalid_word_count
FROM legal.documents
WHERE word_count <= 0 OR word_count > 100000;

-- 5. Legal Documents Type Validation
AUDIT (
  name assert_legal_docs_valid_type,
  description "Ensure document types are valid"
);

SELECT document_type, COUNT(*) as count
FROM legal.documents
GROUP BY document_type
HAVING document_type NOT IN ('contract', 'legal_memo', 'court_filing', 'policy_document', 'legal_opinion');

-- 6. Legal Documents Content Quality
AUDIT (
  name assert_legal_docs_content_quality,
  description "Ensure document content is not empty or too short"
);

SELECT COUNT(*) as poor_quality_docs
FROM legal.documents
WHERE LENGTH(TRIM(raw_text)) < 50 OR raw_text IS NULL;

-- 7. Legal Documents Metadata Validation
AUDIT (
  name assert_legal_docs_metadata_valid,
  description "Ensure document metadata is properly set"
);

SELECT COUNT(*) as invalid_metadata
FROM legal.documents
WHERE generation_date IS NULL 
   OR file_path IS NULL 
   OR language IS NULL; 