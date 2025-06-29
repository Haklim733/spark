MODEL (
    name legal.data_quality_metrics,
    kind FULL,
    description "Data quality metrics for legal documents and NYC taxi data that works with both gateways"
);

WITH legal_docs_quality AS (
    SELECT 
        'legal_documents' as table_name,
        COUNT(*) as total_records,
        COUNT(CASE WHEN document_id IS NULL THEN 1 END) as null_document_ids,
        COUNT(CASE WHEN document_type IS NULL THEN 1 END) as null_document_types,
        COUNT(CASE WHEN raw_text IS NULL THEN 1 END) as null_raw_text,
        COUNT(DISTINCT document_id) as unique_document_ids,
        COUNT(*) - COUNT(DISTINCT document_id) as duplicate_document_ids,
        AVG(document_length) as avg_document_length,
        MIN(document_length) as min_document_length,
        MAX(document_length) as max_document_length
    FROM iceberg.legal.documents
),
nyc_taxi_quality AS (
    SELECT 
        'nyc_taxi_data' as table_name,
        COUNT(*) as total_records,
        COUNT(CASE WHEN VendorID IS NULL THEN 1 END) as null_vendor_ids,
        COUNT(CASE WHEN tpep_pickup_datetime IS NULL THEN 1 END) as null_pickup_times,
        COUNT(CASE WHEN trip_distance <= 0 THEN 1 END) as invalid_distances,
        COUNT(CASE WHEN fare_amount <= 0 THEN 1 END) as invalid_fares,
        AVG(trip_distance) as avg_trip_distance,
        AVG(fare_amount) as avg_fare_amount,
        MIN(tpep_pickup_datetime) as earliest_trip,
        MAX(tpep_pickup_datetime) as latest_trip
    FROM iceberg.nyc_taxi_data.yellow_tripdata
)
SELECT * FROM legal_docs_quality
UNION ALL
SELECT * FROM nyc_taxi_quality 