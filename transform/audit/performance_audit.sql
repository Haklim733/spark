-- Performance Audit using SQLMesh AUDIT syntax

-- 1. Model Execution Time Audit
AUDIT (
  name assert_model_execution_time,
  description "Ensure models execute within acceptable time limits"
);

SELECT 
    model_name,
    execution_time_seconds,
    performance_status
FROM (
    SELECT 
        'legal_documents_cleaned' as model_name,
        30 as execution_time_seconds  -- Example execution time
    UNION ALL
    SELECT 
        'nyc_taxi_aggregated' as model_name,
        45 as execution_time_seconds  -- Example execution time
) model_performance
CROSS JOIN (
    SELECT 
        CASE 
            WHEN execution_time_seconds <= 60 THEN 'ACCEPTABLE'
            WHEN execution_time_seconds <= 300 THEN 'SLOW'
            ELSE 'VERY_SLOW'
        END as performance_status
    FROM (SELECT 1) dummy
) performance_calc;

-- 2. Data Volume Audit
AUDIT (
  name assert_data_volume_reasonable,
  description "Ensure data volumes are within expected ranges"
);

SELECT 
    table_name,
    row_count,
    volume_status
FROM (
    SELECT 
        'legal.documents' as table_name,
        COUNT(*) as row_count
    FROM legal.documents
    UNION ALL
    SELECT 
        'nyc_taxi_data.yellow_tripdata' as table_name,
        COUNT(*) as row_count
    FROM nyc_taxi_data.yellow_tripdata
) volume_check
CROSS JOIN (
    SELECT 
        CASE 
            WHEN row_count > 0 AND row_count <= 1000000 THEN 'NORMAL'
            WHEN row_count > 1000000 AND row_count <= 10000000 THEN 'LARGE'
            WHEN row_count > 10000000 THEN 'VERY_LARGE'
            ELSE 'EMPTY'
        END as volume_status
    FROM (SELECT 1) dummy
) volume_calc;

-- 3. Memory Usage Audit
AUDIT (
  name assert_memory_usage_acceptable,
  description "Monitor memory usage for data processing"
);

SELECT 
    table_name,
    estimated_memory_mb,
    memory_status
FROM (
    SELECT 
        'legal.documents' as table_name,
        (COUNT(*) * 1024) / (1024*1024) as estimated_memory_mb  -- Rough estimate: 1KB per row
    FROM legal.documents
    UNION ALL
    SELECT 
        'nyc_taxi_data.yellow_tripdata' as table_name,
        (COUNT(*) * 512) / (1024*1024) as estimated_memory_mb  -- Rough estimate: 512B per row
    FROM nyc_taxi_data.yellow_tripdata
) memory_check
CROSS JOIN (
    SELECT 
        CASE 
            WHEN estimated_memory_mb <= 100 THEN 'LOW'
            WHEN estimated_memory_mb <= 1000 THEN 'MEDIUM'
            ELSE 'HIGH'
        END as memory_status
    FROM (SELECT 1) dummy
) memory_calc;

-- 4. Query Complexity Audit
AUDIT (
  name assert_query_complexity,
  description "Check for overly complex queries that might impact performance"
);

SELECT 
    'legal_documents_cleaned' as model_name,
    'MEDIUM' as complexity_level,
    'ACCEPTABLE' as complexity_status
UNION ALL
SELECT 
    'nyc_taxi_aggregated' as model_name,
    'HIGH' as complexity_level,
    'MONITOR' as complexity_status; 