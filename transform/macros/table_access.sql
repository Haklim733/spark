-- SQLMesh Macros for Table Access
-- These macros provide gateway-specific table access patterns

-- Legal documents table access macro
@IF(gateway == 'local')
    iceberg_scan('s3://data/wh/legal/documents_snapshot')
@ELSE
    iceberg.legal.documents
@ENDIF

-- Batch metrics table access macro
@IF(gateway == 'local')
    iceberg_scan('s3://data/wh/dataops/job_logs')
@ELSE
    iceberg.dataops.job_logs
@ENDIF 