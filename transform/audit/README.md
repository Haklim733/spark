# SQLMesh Audit Folder

This folder contains audit-related configurations and scripts for SQLMesh data quality monitoring and lineage tracking.

## Purpose

The audit folder is used for:
- **Data Quality Audits** - Monitoring data quality metrics over time
- **Lineage Tracking** - Understanding data dependencies and transformations
- **Compliance Reporting** - Generating audit reports for regulatory requirements
- **Performance Monitoring** - Tracking model performance and execution times

## Structure

```
audit/
├── README.md                    # This file
├── legal_documents_audit.sql    # Legal documents specific audits
├── nyc_taxi_audit.sql          # NYC taxi data specific audits
├── lineage_audit.sql           # Data lineage audit queries
└── performance_audit.sql       # Performance monitoring queries
```

## Audit Files

### **Legal Documents Audits** (`legal_documents_audit.sql`)
- `assert_legal_docs_not_null` - Checks for null critical fields
- `assert_legal_docs_unique` - Ensures document IDs are unique
- `assert_legal_docs_valid_length` - Validates document lengths
- `assert_legal_docs_valid_word_count` - Validates word counts
- `assert_legal_docs_valid_type` - Validates document types
- `assert_legal_docs_content_quality` - Checks content quality
- `assert_legal_docs_metadata_valid` - Validates metadata

### **NYC Taxi Audits** (`nyc_taxi_audit.sql`)
- `assert_nyc_taxi_not_null` - Checks for null critical fields
- `assert_nyc_taxi_valid_distance` - Validates trip distances
- `assert_nyc_taxi_valid_fare` - Validates fare amounts
- `assert_nyc_taxi_valid_dates` - Ensures no future trips
- `assert_nyc_taxi_valid_passengers` - Validates passenger counts
- `assert_nyc_taxi_valid_duration` - Validates trip durations
- `assert_nyc_taxi_valid_locations` - Validates pickup/dropoff locations
- `assert_nyc_taxi_valid_payment` - Validates payment types
- `assert_nyc_taxi_valid_rate_code` - Validates rate codes
- `assert_nyc_taxi_valid_store_fwd` - Validates store and forward flag

### **Lineage Audits** (`lineage_audit.sql`)
- `assert_model_dependencies` - Checks source table existence
- `assert_column_lineage` - Validates required columns exist
- `assert_data_freshness` - Monitors data staleness

### **Performance Audits** (`performance_audit.sql`)
- `assert_model_execution_time` - Monitors execution times
- `assert_data_volume_reasonable` - Checks data volumes
- `assert_memory_usage_acceptable` - Monitors memory usage
- `assert_query_complexity` - Tracks query complexity

## Usage

### **Run Specific Audits:**
```bash
# Legal documents only
sqlmesh audit --audit-file audit/legal_documents_audit.sql

# NYC taxi data only
sqlmesh audit --audit-file audit/nyc_taxi_audit.sql

# Lineage tracking
sqlmesh audit --audit-file audit/lineage_audit.sql

# Performance monitoring
sqlmesh audit --audit-file audit/performance_audit.sql
```

### **Run Multiple Audits:**
```bash
# Run both data quality audits
sqlmesh audit --audit-file audit/legal_documents_audit.sql --audit-file audit/nyc_taxi_audit.sql

# Run all audits
sqlmesh audit --audit-file audit/legal_documents_audit.sql --audit-file audit/nyc_taxi_audit.sql --audit-file audit/lineage_audit.sql --audit-file audit/performance_audit.sql
```

### **Run with Specific Gateway:**
```bash
# Local development
sqlmesh audit --gateway local --audit-file audit/legal_documents_audit.sql

# Spark cluster
sqlmesh audit --gateway spark --audit-file audit/legal_documents_audit.sql

# Production
sqlmesh audit --gateway prod --audit-file audit/legal_documents_audit.sql
```

## Integration

These audit files work with all three gateways:
- **Local** (DuckDB) - Fast local auditing
- **Spark** (Iceberg) - Production data auditing
- **Production** (PostgreSQL) - Final audit reports

## Best Practices

1. **Run Legal Audits First** - Legal documents are typically smaller and faster
2. **Run NYC Taxi Audits Separately** - Larger dataset, may take longer
3. **Schedule Regular Audits** - Set up automated audit runs
4. **Monitor Performance** - Track audit execution times
5. **Alert on Failures** - Configure alerts for quality issues
6. **Document Results** - Keep audit results for compliance
7. **Version Control** - Track audit configurations in git

## Audit Failure Conditions

Audits fail when they return rows, indicating:
- **Data quality issues** (null values, duplicates, invalid data)
- **Performance problems** (slow queries, high memory usage)
- **Lineage issues** (missing dependencies, stale data)
- **Compliance violations** (data not meeting requirements) 