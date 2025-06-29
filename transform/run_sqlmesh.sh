#!/bin/bash

# Set up environment variables for DuckDB to connect to MinIO and Iceberg
export S3_REGION=us-east-1
export S3_ACCESS_KEY=minioadmin
export S3_SECRET_KEY=minioadmin
export S3_ENDPOINT=http://localhost:9000

echo "Environment variables set for DuckDB MinIO connection:"
echo "S3_ENDPOINT: $S3_ENDPOINT"
echo "S3_REGION: $S3_REGION"

# Run SQLMesh with the configured environment
echo "Running SQLMesh..."
cd /home/llee/projects/spark/transform
sqlmesh "$@" 