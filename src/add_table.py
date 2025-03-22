import pyarrow.parquet as pq
from pyarrow import fs
from pyiceberg.catalog.sql import RestCatalog

catalog = RestCatalog(
    "default",
    **{
        "uri": "postgresql+psycopg2://postgres:postgres@localhost:5432/postgres",
        "warehouse": "s3://warehouse/iceberg",
        "s3.endpoint": "http://localhost:9000",
        "s3.access-key-id": "admin",
        "s3.secret-access-key": "password"
    }
)

# https://binayakd.tech/posts/2024-12-25-register-parquet-files-to-iceberg-without-rewrites/

minio = fs.S3FileSystem(
    endpoint_override='localhost:9000',
    access_key="admin",
    secret_key="password",
    scheme="http"
)

df = pq.read_table(
    "warehouse/data/yellow_tripdata_2024-01.parquet",
    filesystem=minio
)

catalog.create_namespace("nyc_taxi_data")
table = catalog.create_table(
    "nyc_taxi_data.yellow_tripdata",
    schema=df.schema
)

table = catalog.load_table("nyc_taxi_data.yellow_tripdata")
table.add_files(["s3://warehouse/data/yellow_tripdata_2024-01.parquet"])