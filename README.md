# Spark Project with Iceberg and MinIO

This project provides a complete Spark environment with Apache Iceberg table format and MinIO object storage, using SQLite in-memory for catalog metadata.

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