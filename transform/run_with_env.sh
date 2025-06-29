#!/bin/bash

# Set up environment variables for DuckDB to connect to MinIO and Iceberg
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export AWS_DEFAULT_REGION=us-east-1
export AWS_ENDPOINT_URL=http://localhost:9000
export AWS_S3_FORCE_PATH_STYLE=true

# Set DuckDB-specific environment variables
export DUCKDB_S3_REGION=us-east-1
export DUCKDB_S3_ACCESS_KEY_ID=minioadmin
export DUCKDB_S3_SECRET_ACCESS_KEY=minioadmin
export DUCKDB_S3_ENDPOINT=http://localhost:9000
export DUCKDB_S3_URL_STYLE=path

echo "Environment variables set for DuckDB MinIO connection:"
echo "AWS_ENDPOINT_URL: $AWS_ENDPOINT_URL"
echo "DUCKDB_S3_ENDPOINT: $DUCKDB_S3_ENDPOINT"

# Run SQLMesh with the configured environment
echo "Running SQLMesh..."
sqlmesh "$@" 