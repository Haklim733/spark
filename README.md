# Spark Project with Iceberg and MinIO

This project provides a complete local environment to run a ELT pipeline to generate and extract text into apache iceberg tables using Python, Spark, MinIO object storage, and SQLMesh for transformations.  The iceberg rest fixture uses SQLite in-memory to service the catalog.

## Architecture

- **Spark Master/Workers**: Spark 3.5.6 cluster
- **Iceberg REST API**: Catalog service using SQLite in-memory
- **MinIO**: S3-compatible object storage for data files
- **Spark History Server**: Job history and monitoring

## Services

- `spark-master`: Spark master node (port 8080, 7077)
- `spark-worker`: Spark worker nodes (scaled to 2 instances)
- `spark-history`: Spark history server (port 18080)
- `spark-rest`: Iceberg REST API (port 8181)
- `minio`: S3-compatible storage (port 9000, console 9001)
- `mc`: MinIO client for bucket setup

# requirements
- uv 
- docker compose

``` bash
uv sync --all-groups
```
``` bash
docker compose up -d
```

## workflow
The data is stored in minio and the iceberg tables are created with Spark. A pyspark job is used to insert the data into the iceberg tables.  The sqlmesh is used to transform the data into the desired format.

1. generate_legal_docs.py: generate legal documents (python, minio)
2. create_tables.py: create iceberg tables (pyspark)
3. insert_tables.py: insert legal documents into iceberg table (pyspark)
4. transformations: sqlmesh 