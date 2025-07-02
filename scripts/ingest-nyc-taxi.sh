# Set Minio endpoint, access key, and secret key
MINIO_ENDPOINT="http://localhost:9000"
MINIO_ACCESS_KEY="admin"
MINIO_SECRET_KEY="password"
MINIO_BUCKET="warehouse"

DATA_DIR="data"

# Copy all .parquet files to Minio
for file in data/*.parquet; do
    mc cp "$file" spark-minio/data/nyc/taxi/
done

uv run scripts/submit.py --file-path=src/insert_nyc_taxi.py --args="--file-path=s3a://data/nyc/taxi/* --table-name=nyc_taxi_data.yellow_tripdata"