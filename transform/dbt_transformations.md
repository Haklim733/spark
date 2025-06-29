# ELT Pipeline - Transformation Layer Operations

## What Should Be Moved to dbt/SQLMesh

### ❌ **Removed from ELT Layer (insert_tables.py)**
- Data quality checks (null values, duplicates)
- Data distribution analysis
- Business logic validations
- Complex aggregations
- Data profiling

### ✅ **Kept in ELT Layer (insert_tables.py)**
- Basic table existence checks
- Row count validation
- Sample record display (for debugging)
- Load operation success/failure tracking

## dbt/SQLMesh Transformation Examples

### 1. **Data Quality Tests (dbt)**

```sql
-- models/tests/legal_documents_quality.sql
-- Test for null values in critical columns
SELECT COUNT(*) as null_count
FROM {{ ref('legal_documents') }}
WHERE document_id IS NULL 
   OR document_type IS NULL 
   OR raw_text IS NULL;

-- Test for duplicate document IDs
SELECT document_id, COUNT(*) as duplicate_count
FROM {{ ref('legal_documents') }}
GROUP BY document_id
HAVING COUNT(*) > 1;

-- Test for document length quality
SELECT COUNT(*) as short_docs
FROM {{ ref('legal_documents') }}
WHERE document_length < 100;  -- Documents too short
```

### 2. **Data Profiling (dbt)**

```sql
-- models/analysis/legal_documents_profile.sql
SELECT 
    document_type,
    COUNT(*) as doc_count,
    AVG(document_length) as avg_length,
    MIN(document_length) as min_length,
    MAX(document_length) as max_length,
    AVG(word_count) as avg_words,
    COUNT(DISTINCT document_id) as unique_docs
FROM {{ ref('legal_documents') }}
GROUP BY document_type
ORDER BY doc_count DESC;
```

### 3. **Business Logic Transformations (dbt)**

```sql
-- models/marts/legal_documents_enriched.sql
SELECT 
    document_id,
    document_type,
    raw_text,
    document_length,
    word_count,
    -- Business logic: categorize by length
    CASE 
        WHEN document_length < 500 THEN 'short'
        WHEN document_length < 1500 THEN 'medium'
        ELSE 'long'
    END as length_category,
    -- Business logic: extract key terms
    CASE 
        WHEN LOWER(raw_text) LIKE '%contract%' THEN 'contract_related'
        WHEN LOWER(raw_text) LIKE '%agreement%' THEN 'agreement_related'
        ELSE 'other'
    END as content_category,
    generation_date
FROM {{ ref('legal_documents') }};
```

### 4. **Data Quality Monitoring (dbt)**

```sql
-- models/monitoring/data_quality_metrics.sql
WITH quality_metrics AS (
    SELECT 
        'legal_documents' as table_name,
        COUNT(*) as total_records,
        COUNT(CASE WHEN document_id IS NULL THEN 1 END) as null_document_ids,
        COUNT(CASE WHEN document_type IS NULL THEN 1 END) as null_document_types,
        COUNT(CASE WHEN raw_text IS NULL THEN 1 END) as null_raw_text,
        COUNT(DISTINCT document_id) as unique_document_ids,
        COUNT(*) - COUNT(DISTINCT document_id) as duplicate_document_ids
    FROM {{ ref('legal_documents') }}
)
SELECT 
    *,
    ROUND((total_records - null_document_ids) * 100.0 / total_records, 2) as document_id_completeness_pct,
    ROUND((total_records - null_document_types) * 100.0 / total_records, 2) as document_type_completeness_pct,
    ROUND((total_records - null_raw_text) * 100.0 / total_records, 2) as raw_text_completeness_pct
FROM quality_metrics;
```

### 5. **dbt Schema Tests (schema.yml)**

```yaml
version: 2

models:
  - name: legal_documents
    description: "Raw legal documents from ELT pipeline"
    columns:
      - name: document_id
        description: "Unique identifier for each document"
        tests:
          - not_null
          - unique
      - name: document_type
        description: "Type of legal document"
        tests:
          - not_null
          - accepted_values:
              values: ['contract', 'legal_memo', 'court_filing', 'policy_document', 'legal_opinion']
      - name: document_length
        description: "Length of document in characters"
        tests:
          - not_null
          - positive_values
      - name: word_count
        description: "Number of words in document"
        tests:
          - not_null
          - positive_values
```

### 6. **SQLMesh Transformations**

```sql
-- models/legal_documents_cleaned.sql
MODEL (
    name legal_documents_cleaned,
    kind incremental,
    grain [document_id, generation_date]
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
    CURRENT_TIMESTAMP() as processed_at
FROM legal.documents
WHERE document_id IS NOT NULL  -- Filter out invalid records
```

## ELT Pipeline Workflow

### 1. **Extract & Load (insert_tables.py)**
```bash
# Run ELT operations
python src/insert_tables.py
```

### 2. **Transform & Test (dbt/SQLMesh)**
```bash
# Run dbt tests and transformations
dbt run
dbt test
dbt docs generate

# Or with SQLMesh
sqlmesh run
sqlmesh test
```

### 3. **Monitor & Alert**
```bash
# Run data quality monitoring
dbt run --select monitoring
```

## Benefits of This Separation

1. **Clear Responsibilities**: ELT focuses on data movement, dbt/SQLMesh on business logic
2. **Reusability**: Transformations can be reused across different data sources
3. **Testing**: Comprehensive data quality testing in transformation layer
4. **Documentation**: Self-documenting transformations with dbt docs
5. **Version Control**: All transformations tracked in git
6. **Lineage**: Clear data lineage from source to final models
7. **Performance**: Optimized transformations with incremental models 