-- SQLMesh Macros for Table Access
-- These macros provide gateway-specific table access patterns

-- Legal documents table access macro
@IF(gateway == 'local')
    iceberg_scan('s3://data/wh/legal/documents')
@ELSE
    iceberg.legal.documents
@ENDIF

-- NYC taxi table access macro  
@IF(gateway == 'local')
    iceberg_scan('s3://data/wh/nyc_taxi_data/yellow_tripdata')
@ELSE
    iceberg.nyc_taxi_data.yellow_tripdata
@ENDIF

-- Batch metrics table access macro
@IF(gateway == 'local')
    iceberg_scan('s3://data/wh/admin/batch_metrics')
@ELSE
    iceberg.admin.batch_metrics
@ENDIF 