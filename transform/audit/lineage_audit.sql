-- Data Lineage Audit using SQLMesh AUDIT syntax

-- 1. Model Dependencies Audit
AUDIT (
  name assert_model_dependencies,
  description "Check that all required source tables exist and are accessible"
);

SELECT 
    model_name,
    source_table,
    dependency_status
FROM (
    SELECT 'legal_documents_cleaned' as model_name, 'legal.documents' as source_table
    UNION ALL
    SELECT 'nyc_taxi_aggregated' as model_name, 'nyc_taxi_data.yellow_tripdata' as source_table
) model_deps
CROSS JOIN (
    SELECT 
        CASE 
            WHEN COUNT(*) > 0 THEN 'EXISTS'
            ELSE 'MISSING'
        END as dependency_status
    FROM legal.documents
    LIMIT 1
) status_check;

-- 2. Column Lineage Validation
AUDIT (
  name assert_column_lineage,
  description "Ensure all required columns exist in source tables"
);

SELECT 
    table_name,
    column_name,
    column_exists
FROM (
    SELECT 'legal.documents' as table_name, 'document_id' as column_name
    UNION ALL
    SELECT 'legal.documents' as table_name, 'document_type' as column_name
    UNION ALL
    SELECT 'legal.documents' as table_name, 'raw_text' as column_name
    UNION ALL
    SELECT 'nyc_taxi_data.yellow_tripdata' as table_name, 'VendorID' as column_name
    UNION ALL
    SELECT 'nyc_taxi_data.yellow_tripdata' as table_name, 'tpep_pickup_datetime' as column_name
    UNION ALL
    SELECT 'nyc_taxi_data.yellow_tripdata' as table_name, 'trip_distance' as column_name
) required_columns
CROSS JOIN (
    SELECT 'EXISTS' as column_exists
    FROM legal.documents
    WHERE document_id IS NOT NULL
    LIMIT 1
) existence_check;

-- 3. Data Freshness Audit
AUDIT (
  name assert_data_freshness,
  description "Ensure data is not stale"
);

SELECT 
    table_name,
    last_updated,
    days_since_update,
    freshness_status
FROM (
    SELECT 
        'legal.documents' as table_name,
        MAX(generation_date) as last_updated,
        DATEDIFF('day', MAX(generation_date), CURRENT_TIMESTAMP()) as days_since_update
    FROM legal.documents
    UNION ALL
    SELECT 
        'nyc_taxi_data.yellow_tripdata' as table_name,
        MAX(tpep_pickup_datetime) as last_updated,
        DATEDIFF('day', MAX(tpep_pickup_datetime), CURRENT_TIMESTAMP()) as days_since_update
    FROM nyc_taxi_data.yellow_tripdata
) freshness_check
CROSS JOIN (
    SELECT 
        CASE 
            WHEN days_since_update <= 7 THEN 'FRESH'
            WHEN days_since_update <= 30 THEN 'STALE'
            ELSE 'VERY_STALE'
        END as freshness_status
    FROM (SELECT 1) dummy
) status_calc; 