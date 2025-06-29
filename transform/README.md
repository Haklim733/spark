# SQLMesh Transform Project

This directory contains SQLMesh transformations for the Spark/Iceberg data pipeline.

## Project Structure

```
transform/
├── sqlmesh.yaml          # SQLMesh configuration
├── requirements.txt      # Python dependencies
├── models/              # SQLMesh models
│   ├── legal_documents_cleaned.sql
│   ├── nyc_taxi_aggregated.sql
│   └── data_quality_metrics.sql
├── tests/               # Data quality tests
│   ├── test_legal_documents.sql
│   └── test_nyc_taxi_data.sql
└── README.md           # This file
```

## Setup

1. **Install SQLMesh:**
   ```bash
   pip install sqlmesh
   ```

2. **Initialize SQLMesh project:**
   ```bash
   sqlmesh init
   ```

3. **Plan and run transformations:**
   ```bash
   sqlmesh plan
   sqlmesh run
   ```

## Models

### 1. `legal_documents_cleaned`
- Cleans and enriches legal documents data
- Adds data quality flags and business logic categories
- Incremental model for efficient processing

### 2. `nyc_taxi_aggregated`
- Aggregates NYC taxi trip data by date and location
- Calculates metrics like trip counts, average fares, etc.
- Incremental model for daily processing

### 3. `data_quality_metrics`
- Monitors data quality across all tables
- Provides comprehensive quality metrics
- Full refresh model for monitoring

## Tests

### Legal Documents Tests
- Null value checks for critical columns
- Duplicate document ID detection
- Document length quality validation
- Document type validation

### NYC Taxi Data Tests
- Null value checks for critical columns
- Trip distance and fare amount validation
- Date range validation
- Passenger count validation

## Usage

### Development
```bash
# Set target to dev
sqlmesh plan --target dev

# Run models
sqlmesh run --target dev

# Run tests
sqlmesh test --target dev
```

### Production
```bash
# Set target to prod
sqlmesh plan --target prod

# Run models
sqlmesh run --target prod

# Run tests
sqlmesh test --target prod
```

## Integration with Spark/Iceberg

This SQLMesh project is configured to work with:
- **Spark 3.5** with Iceberg 1.9.1
- **REST Catalog** for metadata management
- **MinIO** for data storage
- **PostgreSQL** for catalog metadata

## Data Flow

1. **ELT Layer** (`src/insert_tables.py`) - Extracts and loads raw data
2. **Transform Layer** (SQLMesh) - Applies business logic and data quality
3. **Monitoring** - Continuous data quality monitoring

## Benefits

- **Incremental Processing**: Only process new/changed data
- **Data Quality**: Comprehensive testing and validation
- **Business Logic**: Centralized transformation logic
- **Monitoring**: Real-time data quality metrics
- **Version Control**: All transformations tracked in git 