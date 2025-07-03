# Spark Session Creation Guide

This guide explains how to use the new enum-based Spark session creation system that supports both Spark Connect (4.0+) and regular PySpark (3.5).

## Overview

The new system provides:
- **Enum-based version selection**: Choose between Spark Connect 4.0+ and PySpark 3.5/4.0
- **Dependency injection for Iceberg**: Configure Iceberg settings through a dedicated config class
- **Unified API**: Single function for all session types with optional configurations
- **Convenience functions**: Pre-configured sessions for common use cases

## Core Components

### SparkVersion Enum

```python
from utils import SparkVersion

# Available options:
SparkVersion.SPARK_CONNECT_3_5  # Spark Connect (3.5)
SparkVersion.SPARK_CONNECT_4_0  # Spark Connect (4.0+)
SparkVersion.SPARK_3_5          # Regular PySpark (3.5)
SparkVersion.SPARK_4_0          # Regular PySpark (4.0)
```

### IcebergConfig Class

```python
from utils import IcebergConfig

# Default configuration (uses environment variables)
iceberg_config = IcebergConfig()

# Custom configuration
custom_config = IcebergConfig(
    catalog_uri="http://localhost:8181",
    warehouse="s3://data/warehouse",
    s3_endpoint="http://localhost:9000",
    s3_access_key="my_key",
    s3_secret_key="my_secret"
)
```

## Main Function: `create_spark_session`

### Basic Usage

```python
from utils import create_spark_session, SparkVersion

# Create PySpark 3.5 session
spark = create_spark_session(
    spark_version=SparkVersion.SPARK_3_5,
    app_name="MyApp"
)

# Create Spark Connect 4.0 session
spark = create_spark_session(
    spark_version=SparkVersion.SPARK_CONNECT_4_0,
    app_name="MyApp"
)

# Create Spark Connect 3.5 session
spark = create_spark_session(
    spark_version=SparkVersion.SPARK_CONNECT_3_5,
    app_name="MyApp"
)
```

### With Iceberg Configuration
Default creates the necessary s3a and iceberg configuration in utils/sessions.py. You can alter the settings by adding different config patterns.

```python
from utils import create_spark_session, SparkVersion, IcebergConfig, S3FileSystemConfig

# Create S3 config
s3_config = S3FileSystemConfig() 

# Create Iceberg config
iceberg_config = IcebergConfig(
    catalog_uri="http://spark-rest:8181",
    warehouse="s3://data/wh",
    s3_config=s3_config
)

# Create session with Iceberg
spark = create_spark_session(
    spark_version=SparkVersion.SPARK_3_5,
    app_name="IcebergApp",
    iceberg_config=iceberg_config
)
```

### With Additional Configurations

```python
# Additional Spark configurations
additional_configs = {
    "spark.sql.shuffle.partitions": "200",
    "spark.executor.memory": "2g",
    "spark.sql.adaptive.enabled": "true"
}

spark = create_spark_session(
    spark_version=SparkVersion.SPARK_3_5,
    app_name="OptimizedApp",
    iceberg_config=iceberg_config,
    **additional_configs
)
```

## Convenience Functions

### `create_iceberg_spark_session`

Creates a session with default Iceberg configuration:

```python
from utils import create_iceberg_spark_session, SparkVersion

# PySpark with Iceberg
spark = create_iceberg_spark_session(
    spark_version=SparkVersion.SPARK_3_5,
    app_name="IcebergApp"
)

# Spark Connect with Iceberg
spark = create_iceberg_spark_session(
    spark_version=SparkVersion.SPARK_CONNECT_4_0,
    app_name="IcebergConnectApp"
)

# Spark Connect 3.5 with Iceberg
spark = create_iceberg_spark_session(
    spark_version=SparkVersion.SPARK_CONNECT_3_5,
    app_name="IcebergConnect35App"
)
```

## Configuration Examples

### Basic PySpark Session

```python
spark = create_spark_session(
    spark_version=SparkVersion.SPARK_3_5,
    app_name="BasicApp"
)
```

### Spark Connect with Custom Config

```python
spark = create_spark_session(
    spark_version=SparkVersion.SPARK_CONNECT_4_0,
    app_name="ConnectApp",
    spark_executor_memory="4g",
    spark_driver_memory="2g"
)
```

### Spark Connect 3.5 with Custom Config

```python
spark = create_spark_session(
    spark_version=SparkVersion.SPARK_CONNECT_3_5,
    app_name="Connect35App",
    spark_executor_memory="4g",
    spark_driver_memory="2g"
)
```

### PySpark with Iceberg and Performance Tuning

```python
iceberg_config = IcebergConfig(
    catalog_uri="http://spark-rest:8181",
    warehouse="s3://data/warehouse"
)

performance_configs = {
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.shuffle.partitions": "200",
    "spark.executor.memory": "4g",
    "spark.driver.memory": "2g",
    "spark.sql.adaptive.coalescePartitions.enabled": "true"
}

spark = create_spark_session(
    spark_version=SparkVersion.SPARK_3_5,
    app_name="PerformanceApp",
    iceberg_config=iceberg_config,
    **performance_configs
)
```

## Environment Variables

The system uses these environment variables for default Iceberg configuration:

- `AWS_ACCESS_KEY_ID`: S3 access key (default: "admin")
- `AWS_SECRET_ACCESS_KEY`: S3 secret key (default: "password")

## Testing

Run the examples to test the new system:

```bash
python src/examples/session_examples.py
```

## Benefits

1. **Type Safety**: Enum prevents invalid version selections
2. **Flexibility**: Easy to switch between Spark versions
3. **Maintainability**: Centralized configuration management
4. **Extensibility**: Easy to add new Spark versions or configurations
5. **Dependency Injection**: Clean separation of concerns for Iceberg config
6. **Backward Compatibility**: Old functions still work with deprecation warnings 

## S3A vs Iceberg Catalog Configuration

When working with S3/MinIO storage, it's important to understand the difference between S3A filesystem configuration and Iceberg catalog configuration, as they serve different purposes and use different property names.

### S3A Filesystem Configuration (for direct S3 operations)

Used for direct S3/MinIO filesystem operations like `spark.read.text("s3a://bucket/file.txt")`:

```conf
# S3A Filesystem configuration for direct S3/MinIO access
spark.hadoop.fs.s3.impl org.apache.hadoop.fs.s3a.S3AFileSystem
spark.hadoop.fs.s3a.aws.credentials.provider org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider
spark.hadoop.fs.s3a.access.key sparkuser
spark.hadoop.fs.s3a.secret.key sparkpass
spark.hadoop.fs.s3a.endpoint http://minio:9000
spark.hadoop.fs.s3a.region us-east-1
spark.hadoop.fs.s3a.path.style.access true
spark.hadoop.fs.s3a.connection.ssl.enabled false
```

### Iceberg Catalog Configuration (for Iceberg operations)

Used for Iceberg catalog operations like table creation and metadata management:

```conf
# Iceberg catalog configuration
spark.sql.catalog.iceberg.s3.access-key-id sparkuser
spark.sql.catalog.iceberg.s3.secret-access-key sparkpass
spark.sql.catalog.iceberg.s3.endpoint http://minio:9000
spark.sql.catalog.iceberg.s3.region us-east-1
spark.sql.catalog.iceberg.s3.path-style-access true
spark.sql.catalog.iceberg.s3.ssl-enabled false
```

### Key Differences

| Aspect | S3A Filesystem | Iceberg Catalog |
|--------|----------------|-----------------|
| **Purpose** | Direct S3/MinIO file operations | Iceberg table metadata and catalog operations |
| **Property prefix** | `spark.hadoop.fs.s3a.*` | `spark.sql.catalog.iceberg.s3.*` |
| **Access key** | `access.key` | `access-key-id` |
| **Secret key** | `secret.key` | `secret-access-key` |
| **Implementation** | Hadoop S3A filesystem | Iceberg S3FileIO |
| **Use cases** | `spark.read.text("s3a://bucket/file.txt")` | `spark.sql("CREATE TABLE ...")` |

### Common Issue: Missing S3A Configuration

If you encounter 403 Forbidden errors when trying to read files directly from S3/MinIO (e.g., `spark.read.text("s3a://data/test.txt")`), ensure that:

1. **Both configurations are present**: S3A filesystem config for direct file access, Iceberg catalog config for table operations
2. **Credentials match**: Both configurations should use the same access credentials
3. **Spark Connect server has S3A config**: The Spark Connect server needs S3A configuration in `spark-defaults.conf`

### Example: Complete Configuration

For a setup that supports both direct S3 operations and Iceberg catalog operations:

```conf
# Iceberg catalog configuration
spark.sql.catalog.iceberg org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.iceberg.type rest
spark.sql.catalog.iceberg.uri http://iceberg-rest:8181
spark.sql.catalog.iceberg.io-impl org.apache.iceberg.aws.s3.S3FileIO
spark.sql.catalog.iceberg.warehouse s3://data/wh/
spark.sql.catalog.iceberg.s3.access-key-id sparkuser
spark.sql.catalog.iceberg.s3.secret-access-key sparkpass
spark.sql.catalog.iceberg.s3.endpoint http://minio:9000
spark.sql.catalog.iceberg.s3.region us-east-1
spark.sql.catalog.iceberg.s3.path-style-access true
spark.sql.catalog.iceberg.s3.ssl-enabled false
spark.sql.defaultCatalog iceberg

# S3A Filesystem configuration for direct S3/MinIO access
spark.hadoop.fs.s3.impl org.apache.hadoop.fs.s3a.S3AFileSystem
spark.hadoop.fs.s3a.aws.credentials.provider org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider
spark.hadoop.fs.s3a.access.key sparkuser
spark.hadoop.fs.s3a.secret.key sparkpass
spark.hadoop.fs.s3a.endpoint http://minio:9000
spark.hadoop.fs.s3a.region us-east-1
spark.hadoop.fs.s3a.path.style.access true
spark.hadoop.fs.s3a.connection.ssl.enabled false
```

## Limitations: Unsupported and Supported Functions in Spark Connect

Spark Connect (as of your current environment) supports more features than earlier versions, but some limitations remain. Based on comprehensive atomic testing in `tests/integ/local/test_spark_connect_functions.py`, here is what is and isn't supported:

### Test Structure
The test suite is organized into classes that test specific categories of operations:
- `TestDataFrameOperations`: Basic DataFrame creation and transformations
- `TestSQLOperations`: SQL queries, temp views, and GROUP BY operations
- `TestAggregationFunctions`: Aggregation functions like count, collect_list, concat_ws
- `TestFileOperations`: File reading and input_file_name function
- `TestPythonUDFs`: Python user-defined functions
- `TestActionOperations`: DataFrame actions (expected to fail)
- `TestComplexOperations`: Complex operations that depend on basic ones
- `TestUnsupportedOperations`: Operations known to be unsupported
- `TestIcebergOperations`: Iceberg catalog operations
- `TestPerformanceAndLimitations`: Performance considerations

### ✅ Fully Supported (work without issues)
- **Python UDFs**: User-defined functions written in Python are fully supported
- **DataFrame actions**: `collect()`, `toPandas()`, and `take()` work as expected
- **DataFrame transformations**: `select()`, `distinct()`, `groupBy()`, `agg()` work normally
- **SQL operations**: `spark.sql()` queries work, including DDL operations
- **Temp views**: `createOrReplaceTempView()` works for SQL queries
- **Aggregation functions**: `collect_list()`, `concat_ws()`, `sum()`, `max()` work in aggregations
- **Iceberg catalog DDL**: Namespace creation and table operations work
- **Local file access**: Reading local files from the Python process works
- **S3/MinIO file operations**: `binaryFile` format and S3A operations work
- **File listing**: Reading file metadata from S3/MinIO works
- **File system functions**: `input_file_name()` works for local files

### ❌ Not Supported (raise exceptions)
- **DataFrame `count()`**: Not supported as an action in this Spark Connect environment (raises `SparkConnectGrpcException`)
- **DataFrame `foreach()`**: Not supported; raises serialization errors
- **RDD operations**: The RDD API is not available in Spark Connect
- **Custom serialization**: Operations like `map()`, `flatMap()` are not supported
- **DataFrame transformations with Python lambdas**: e.g., `df.filter(lambda row: ...)` is not supported
- **Direct SparkContext access**: Not available in Spark Connect architecture

### ⚠️ Partially Supported (work but with limitations)
- **File system functions**: `input_file_name()` works but may fail on certain file operations
- **Complex aggregations**: Multi-step aggregations work but may be slow

**Note:**
- `DataFrame.count()` is not supported as an action in this Spark Connect environment. Attempting to use it will raise a `SparkConnectGrpcException`. Use SQL `COUNT(*)` in queries or aggregations instead, or use other supported actions like `collect()` or `toPandas()` for result retrieval.

### 🔄 Performance Considerations
- **Remote execution**: All Spark operations run on the remote Spark Connect server
- **Network overhead**: Operations that require data transfer can be slow
- **Job scheduling**: Each action triggers a remote job, which has overhead
- **Memory usage**: Large result sets may cause memory issues during transfer

### 📋 Best Practices for Spark Connect
1. **Use DataFrame API**: Prefer DataFrame operations over RDD operations
2. **Avoid Python lambdas**: Use SQL expressions or UDFs instead of lambda functions
3. **Limit result sizes**: Use `take()` or `limit()` instead of `collect()` for large datasets
4. **Batch operations**: Group multiple operations to reduce network round trips
5. **Use SQL when possible**: SQL operations are often more efficient than DataFrame API

### Notes
- Support for features may vary by Spark version, deployment, and backend configuration
- The Spark Connect architecture means Python code runs locally while Spark operations run remotely
- For the latest list of supported and unsupported features, see the [official Spark Connect documentation](https://spark.apache.org/docs/latest/connect/index.html#limitations)

## Common Issues and Quirks

### Environment Variables vs Configuration Files

**Issue**: Setting environment variables or configuration keys directly in Python code doesn't work for Spark Connect.

**Why**: Spark Connect is a client-server architecture where:
- Python code runs locally (your script)
- Spark operations run remotely (on Spark Connect server)
- Configuration must be on the server side, not client side

**Example of what doesn't work**:
```python
# ❌ This only sets env vars in YOUR local Python process
import os
os.environ['AWS_ACCESS_KEY_ID'] = 'sparkuser'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'sparkpass'

# ❌ This only configures the CLIENT session, not the SERVER
spark = (
    SparkSession.builder
    .remote("sc://localhost:15002")
    .config("spark.hadoop.fs.s3a.access.key", "sparkuser")  # Client config only
    .getOrCreate()
)
```

**Solution**: Configure the Spark Connect server via `spark-defaults.conf` or Docker environment variables.

### S3A Authentication Chain

S3A authentication follows this chain:
```
Your Python Code → Spark Connect Server → S3A Filesystem → MinIO
```

The authentication happens at the **Spark Connect server level**, not the client level.

### Environment Variables Alone Aren't Enough

**Issue**: Setting environment variables in `docker-compose.yaml` isn't sufficient for S3A authentication.

**Why**: Spark S3A needs explicit configuration to know:
- Which credentials provider to use
- What the endpoint is
- Whether to use path-style access
- SSL settings

**What you need**:
```conf
# Required: Tell Spark to use environment variables
spark.hadoop.fs.s3a.aws.credentials.provider org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider

# Required: Tell Spark where MinIO is
spark.hadoop.fs.s3a.endpoint http://minio:9000

# Optional: Explicit credentials (if not using env vars)
spark.hadoop.fs.s3a.access.key sparkuser
spark.hadoop.fs.s3a.secret.key sparkpass
```

### MinIO User Management

**Issue**: The `admin` user (root user) can't be created via MinIO admin API.

**Why**: The `admin` user is the root user and can't be created through the admin API.

**Solution**: Create a separate user for Spark operations:
```bash
# Create a dedicated user for Spark
mc admin user add minio sparkuser sparkpass
mc admin policy attach minio readwrite --user sparkuser
```

### Configuration Precedence

**Important**: Explicit configuration in `spark-defaults.conf` takes precedence over environment variables.

**Example**:
```conf
# This will be used (explicit config)
spark.hadoop.fs.s3a.access.key sparkuser

# This will be ignored (env var)
AWS_ACCESS_KEY_ID=admin
```

### Docker Compose Environment Variables

**Issue**: Environment variables in `docker-compose.yaml` only take effect when containers are created, not on restart.

**Solution**: Use `docker-compose down && docker-compose up -d` to recreate containers with new environment variables.

### Endpoint Configuration

**Issue**: Using wrong endpoint hostname causes connection failures.

**Context matters**:
- **Inside Docker network**: Use `http://minio:9000`
- **From host machine**: Use `http://localhost:9000`
- **From Spark Connect server**: Use `http://minio:9000`

### Credentials Provider Configuration

**Critical**: Always include the credentials provider configuration:
```conf
spark.hadoop.fs.s3a.aws.credentials.provider org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider
```

Without this, Spark S3A doesn't know how to authenticate, even with environment variables set.

### Complete Working Example

Here's a complete working configuration that uses environment variables:

**docker-compose.yaml**:
```yaml
spark-connect:
  environment:
    - AWS_ACCESS_KEY_ID=sparkuser
    - AWS_SECRET_ACCESS_KEY=sparkpass
```

**spark-defaults.conf**:
```conf
# S3A Filesystem configuration using environment variables
spark.hadoop.fs.s3.impl org.apache.hadoop.fs.s3a.S3AFileSystem
spark.hadoop.fs.s3a.aws.credentials.provider org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider
spark.hadoop.fs.s3a.endpoint http://minio:9000
spark.hadoop.fs.s3a.region us-east-1
spark.hadoop.fs.s3a.path.style.access true
spark.hadoop.fs.s3a.connection.ssl.enabled false
```

The `SimpleAWSCredentialsProvider` will automatically read `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` from environment variables.

### Troubleshooting Checklist

When S3A operations fail with 403 Forbidden:

1. ✅ **Check if user exists in MinIO**: `mc admin user list minio`
2. ✅ **Verify user permissions**: `mc admin policy list minio --user sparkuser`
3. ✅ **Check Spark Connect environment**: `docker exec spark-connect env | grep AWS`
4. ✅ **Verify spark-defaults.conf**: `docker exec spark-connect cat /opt/bitnami/spark/conf/spark-defaults.conf`
5. ✅ **Check endpoint configuration**: Ensure correct hostname for context
6. ✅ **Verify credentials provider**: Must include `SimpleAWSCredentialsProvider`
7. ✅ **Restart Spark Connect**: `docker-compose restart spark-connect`
8. ✅ **Recreate containers if needed**: `docker-compose down && docker-compose up -d` 