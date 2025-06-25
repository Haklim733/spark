import pyarrow.parquet as pq
from pyarrow import fs
from pyiceberg.catalog.rest import RestCatalog

catalog = RestCatalog(
    "default",
    **{
        "uri": "http://spark-rest:8181",
        "warehouse": "s3://data/wh",
        "s3.endpoint": "http://minio:9000",
        "s3.access-key-id": "admin",
        "s3.secret-access-key": "password",
    },
)

# https://binayakd.tech/posts/2024-12-25-register-parquet-files-to-iceberg-without-rewrites/

minio = fs.S3FileSystem(
    endpoint_override="minio:9000",
    access_key="admin",
    secret_key="password",
    scheme="http",
)

df = pq.read_table("data/yellow_tripdata_2022-01.parquet", filesystem=minio)

catalog.create_namespace("nyc_taxi_data")
table = catalog.create_table("nyc_taxi_data.yellow_tripdata", schema=df.schema)

table = catalog.load_table("nyc_taxi_data.yellow_tripdata")
table.add_files(["s3://data/yellow_tripdata_2022-01.parquet"])
