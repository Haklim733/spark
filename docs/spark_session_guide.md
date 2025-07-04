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

Creates a session (default includes Iceberg config):

```python
from utils import create_iceberg_spark_session, SparkVersion

# PySpark with Iceberg
spark = create_spark_session(
    spark_version=SparkVersion.SPARK_3_5,
    app_name="IcebergApp"
)

# Spark Connect with Iceberg 
# 4.0 is not supported yet
spark = create_spark_session(
    spark_version=SparkVersion.SPARK_CONNECT_3_5,
    app_name="IcebergConnectApp"
)

```
## configuration
Configuration of catalog and related settings can be found in src/utils/session.py

### Spark Connect with Custom Config

```python
spark = create_spark_session(
    spark_version=SparkVersion.SPARK_CONNECT_4_0,
    app_name="ConnectApp",
    path_style_access=False,
    spark_driver_memory="2g"
)
```

### Spark Connect 3.5 with Custom Config
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
uv run -m src.examples.session_examples
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

**Server-side configuration (spark-defaults.conf)**:
See the current `spark-defaults.conf` file for S3A filesystem registration and other server-side settings.

**Application-side configuration (session.py)**:
```python
from src.utils import S3FileSystemConfig

s3_config = S3FileSystemConfig(
    endpoint="minio:9000",
    access_key="admin",
    secret_key="password",
    region="us-east-1"
)
```

### Iceberg Catalog Configuration (for Iceberg operations)

Used for Iceberg catalog operations like table creation and metadata management:

**Server-side configuration (spark-defaults.conf)**:
See the current `spark-defaults.conf` file for Iceberg catalog registration and other server-side settings.

**Application-side configuration (session.py)**:
```python
from src.utils import IcebergConfig, S3FileSystemConfig

s3_config = S3FileSystemConfig(
    endpoint="minio:9000",
    access_key="admin",
    secret_key="password",
    region="us-east-1"
)

iceberg_config = IcebergConfig(s3_config)
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
3. **Server-side registration is present**: The Spark Connect server needs S3A registration in `spark-defaults.conf`
4. **Application-side credentials are set**: Use `S3FileSystemConfig` in your session configuration

### Example: Complete Configuration

For a setup that supports both direct S3 operations and Iceberg catalog operations:

**Server-side (spark-defaults.conf)**:
See the current `spark-defaults.conf` file for complete server-side configuration including:
- Core Iceberg catalog registration
- Core S3A filesystem registration
- Spark Connect settings
- Performance and resource configurations

**Application-side (session.py)**:
```python
from src.utils import S3FileSystemConfig, IcebergConfig, create_spark_session, SparkVersion

# S3 configuration for direct file operations
s3_config = S3FileSystemConfig(
    endpoint="minio:9000",
    access_key="admin",
    secret_key="password",
    region="us-east-1"
)

# Iceberg configuration for catalog operations
iceberg_config = IcebergConfig(s3_config)

# Create session with both configurations
spark = create_spark_session(
    spark_version=SparkVersion.SPARK_CONNECT_3_5,
    app_name="MyApp",
    s3_config=s3_config,
    iceberg_config=iceberg_config
)
```

**⚠️ UPDATE**: The new flexible configuration system allows you to set client-side parameters like credentials, endpoints, SSL settings, and catalog configurations per application in `session.py`, providing better multi-user support and application-specific customization.

## Limitations: Unsupported and Supported Functions in Spark Connect

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
- **Distributed operations**: `count()`, `groupBy().count()`, `ORDER BY`, `JOIN` operations now work
- **Window functions**: `ROW_NUMBER()`, `RANK()`, `DENSE_RANK()` now work
- **Complex aggregations**: `SUM()`, `AVG()`, `MAX()`, `MIN()` with `GROUP BY` now work
- **Subqueries**: Correlated and non-correlated subqueries now work
- **CTEs (Common Table Expressions)**: Complex CTEs with multiple subqueries work
- **String aggregation**: `concat_ws()` with `collect_list()` works

### ❌ Not Supported (raise exceptions)
- **DataFrame `foreach()`**: Not supported; raises serialization errors
- **RDD operations**: The RDD API is not available in Spark Connect
- **Custom serialization**: Operations like `map()`, `flatMap()` are not supported
- **DataFrame transformations with Python lambdas**: e.g., `df.filter(lambda row: ...)` is not supported
- **Direct SparkContext access**: Not available in Spark Connect architecture

### 📋 Best Practices for Spark Connect
1. **Use DataFrame API**: Prefer DataFrame operations over RDD operations
2. **Avoid Python lambdas**: Use SQL expressions or UDFs instead of lambda functions
3. **Limit result sizes**: Use `take()` or `limit()` instead of `collect()` for large datasets
4. **Batch operations**: Group multiple operations to reduce network round trips
5. **Use SQL when possible**: SQL operations are often more efficient than DataFrame API
6. **Leverage distributed operations**: Now that distributed operations work, use them for better performance
7. **Use window functions**: For complex aggregations and ranking operations
8. **Utilize CTEs**: For complex queries that need intermediate results

### Notes
- Support for features may vary by Spark version, deployment, and backend configuration
- The Spark Connect architecture means Python code runs locally while Spark operations run remotely
- **Major Update**: Distributed operations now work in Spark Connect with proper worker configuration
- For the latest list of supported and unsupported features, see the [official Spark Connect documentation](https://spark.apache.org/docs/latest/connect/index.html#limitations)

## Spark Connect Requirements: JARs vs Python Files

### JAR Requirements for Spark Connect

**Spark Connect requires JARs to be available in the Spark Connect container** for operations that need Java classes (like Iceberg catalog operations).

#### Required JARs for Iceberg
- **`iceberg-spark-runtime-3.5_2.12-1.9.1.jar`** - Core Iceberg Spark integration
- **`spark-avro_2.12-3.5.6.jar`** - Avro format support for Iceberg  
- **`iceberg-aws-bundle-1.9.1.jar`** - AWS S3 integration for Iceberg
- **`postgresql-42.7.3.jar`** - PostgreSQL JDBC driver for Iceberg catalog

#### Where JARs Must Be Installed
- **Spark Connect container**: JARs must be in `/opt/bitnami/spark/jars/`
- **Spark Workers**: JARs must be available for task execution
- **Spark Master**: No JARs needed (only coordinates, doesn't execute tasks)
- **Client side**: No JARs needed (client only sends requests)

#### Common JAR-Related Error
```
Cannot find catalog plugin class for catalog 'iceberg': org.apache.iceberg.spark.SparkCatalog
```
This error occurs when Iceberg JARs are missing from the Spark Connect container.

### Python Files in Spark Connect

**Spark Connect does NOT require Python files to be built into the container** for most operations.

#### When Python Files Are NOT Required
- Standard PySpark operations
- Built-in Spark functions  
- Standard Python libraries (pandas, numpy, etc.)
- Direct SQL operations
- UDFs using standard libraries

#### When Python Files ARE Required
- Importing custom modules from your `src/` directory
- Using custom Python utilities in UDFs or operations

#### Two Approaches for Custom Python Files

**1. Dynamic Artifacts (Recommended)**
```python
from src.utils.session import create_spark_connect_session
spark = create_spark_connect_session("MyApp", add_artifacts=True)
```
This automatically uploads your `src/` directory to Spark Connect at runtime.

**2. Built into Container**
Copy `src/` directory to the Spark Connect container during build (less flexible).

#### Python Library Requirements

**Client Side (Your Local Machine)**
- Libraries you import: `import pandas as pd`
- Libraries for function definitions
- Your custom Python modules

**Server Side (Spark Connect Container)**  
- Libraries used in UDFs: `@udf def my_function(x): return pd.Series([x]).mean()`
- Libraries for operations that execute on the Spark cluster
- Standard libraries (pandas, numpy, pyspark) are already installed

### Summary

| Component | JARs Required | Python Files Required |
|-----------|---------------|----------------------|
| **Spark Connect Container** | ✅ Yes (for Iceberg, etc.) | ❌ No (use artifacts) |
| **Spark Workers** | ✅ Yes (for task execution) | ❌ No |
| **Spark Master** | ❌ No (only coordinates) | ❌ No |
| **Client Side** | ❌ No | ✅ Yes (for imports) |

**Key Takeaway**: Spark Connect needs JARs for Java-based operations but can handle Python files dynamically through artifacts.

## Common Issues and Solutions

### Critical Issue: Spark Connect Must Be in Workers

**Problem**: Distributed operations (COUNT, GROUP BY, JOIN) failed with `ClassCastException` errors.

**Root Cause**: Spark Connect was only running on the master, not on the workers. Distributed operations require Spark Connect to be available on worker nodes.

**Solution**: Add Spark Connect to worker containers in `docker-compose.yaml`:
```yaml
spark-worker:
  image: bitnami/spark:3.5
  environment:
    - SPARK_MODE=worker
    - SPARK_MASTER_URL=spark://spark-master:7077
    - SPARK_WORKER_MEMORY=1G
    - SPARK_WORKER_CORES=1
    # Add Spark Connect to workers
    - SPARK_CONNECT_ENABLED=true
    - SPARK_CONNECT_PORT=15002
```

**Result**: After adding Spark Connect to workers, distributed operations now work:
- ✅ `df.count()` works
- ✅ `spark.sql("SELECT COUNT(*) FROM table")` works  
- ✅ `GROUP BY`, `ORDER BY`, `JOIN` operations work
- ✅ Window functions work

### Critical Issue: spark-defaults.conf Required for Certain Configs

**Problem**: Setting configurations in Python code (via `session.py`) didn't work for Spark Connect.

**Root Cause**: Spark Connect is a client-server architecture. Some configurations must be set on the server side (in `spark-defaults.conf`) rather than the client side (Python code).

**Examples of what doesn't work in Python**:
```python
# ❌ These only configure the CLIENT, not the SERVER
spark = SparkSession.builder.remote("sc://localhost:15002")
    .config("spark.hadoop.fs.s3a.access.key", "admin")  # Client only
    .config("spark.sql.catalog.iceberg.type", "rest")    # Client only
    .getOrCreate()
```

**Solution**: Put critical configurations in `spark-defaults.conf`:
```conf
# Iceberg catalog configuration (server-side)
spark.sql.catalog.iceberg org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.iceberg.type rest
spark.sql.catalog.iceberg.uri http://iceberg-rest:8181
spark.sql.catalog.iceberg.io-impl org.apache.iceberg.aws.s3.S3FileIO
spark.sql.catalog.iceberg.warehouse s3://data/wh/
spark.sql.catalog.iceberg.s3.access-key-id admin
spark.sql.catalog.iceberg.s3.secret-access-key password
spark.sql.catalog.iceberg.s3.endpoint http://minio:9000
spark.sql.catalog.iceberg.s3.region us-east-1
spark.sql.catalog.iceberg.s3.path-style-access true
spark.sql.catalog.iceberg.s3.ssl-enabled false
spark.sql.defaultCatalog iceberg

# S3A Filesystem configuration (server-side)
spark.hadoop.fs.s3.impl org.apache.hadoop.fs.s3a.S3AFileSystem
spark.hadoop.fs.s3a.aws.credentials.provider org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider
spark.hadoop.fs.s3a.access.key admin
spark.hadoop.fs.s3a.secret.key password
spark.hadoop.fs.s3a.endpoint http://minio:9000
spark.hadoop.fs.s3a.region us-east-1
spark.hadoop.fs.s3a.path.style.access true
spark.hadoop.fs.s3a.connection.ssl.enabled false
```

**⚠️ UPDATE**: With the new flexible configuration system, client-side parameters like S3A and Iceberg catalog configurations (credentials, endpoints, SSL settings) can now be set in `session.py` for per-application customization. The server-side `spark-defaults.conf` still needs the core catalog registration and extensions, but client-side parameters can be overridden per session.

### Issue: Environment Variables vs Configuration Files

**Problem**: Setting environment variables in `docker-compose.yaml` wasn't sufficient for S3A authentication.

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
spark.hadoop.fs.s3a.access.key admin
spark.hadoop.fs.s3a.secret.key password
```

### Issue: S3A vs Iceberg Catalog Configuration

**Problem**: Confusion between S3A filesystem config and Iceberg catalog config.

**Key Differences**:

| Aspect | S3A Filesystem | Iceberg Catalog |
|--------|----------------|-----------------|
| **Purpose** | Direct S3/MinIO file operations | Iceberg table metadata and catalog operations |
| **Property prefix** | `spark.hadoop.fs.s3a.*` | `spark.sql.catalog.iceberg.s3.*` |
| **Access key** | `access.key` | `access-key-id` |
| **Secret key** | `secret.key` | `secret-access-key` |
| **Use cases** | `spark.read.text("s3a://bucket/file.txt")` | `spark.sql("CREATE TABLE ...")` |

**Solution**: Configure both for complete functionality. You can now set these flexibly:

**Option 1: Server-side configuration (spark-defaults.conf)**
```conf
# For direct S3 operations
spark.hadoop.fs.s3a.access.key admin
spark.hadoop.fs.s3a.secret.key password

# For Iceberg catalog operations  
spark.sql.catalog.iceberg.s3.access-key-id admin
spark.sql.catalog.iceberg.s3.secret-access-key password
```

**Option 2: Application-side configuration (session.py)**
```python
from src.utils import S3FileSystemConfig, IcebergConfig, create_spark_session

# S3 configuration for direct file operations
s3_config = S3FileSystemConfig(
    endpoint="minio:9000",
    access_key="admin",
    secret_key="password",
    region="us-east-1"
)

# Iceberg configuration for catalog operations
iceberg_config = IcebergConfig(s3_config)

# Create session with both configurations
spark = create_spark_session(
    spark_version=SparkVersion.SPARK_CONNECT_3_5,
    app_name="MyApp",
    s3_config=s3_config,
    iceberg_config=iceberg_config
)
```

**⚠️ UPDATE**: The new flexible configuration system allows you to set S3A and Iceberg catalog configurations per application in `session.py`, providing better multi-user support and application-specific customization.

### Issue: JAR Requirements for Spark Connect

**Problem**: `Cannot find catalog plugin class for catalog 'iceberg'` errors.

**Root Cause**: Spark Connect requires JARs to be available in the Spark Connect container for Java-based operations.

**Required JARs**:
- `iceberg-spark-runtime-3.5_2.12-1.9.1.jar`
- `spark-avro_2.12-3.5.6.jar`
- `iceberg-aws-bundle-1.9.1.jar`
- `postgresql-42.7.3.jar`

**Solution**: Install JARs in `/opt/bitnami/spark/jars/` in the Spark Connect container.

### Issue: Python Files vs JARs in Spark Connect

**Misconception**: Python files need to be built into the Spark Connect container.

**Reality**: 
- **JARs**: Must be in Spark Connect container for Java operations
- **Python files**: Can be uploaded dynamically via artifacts

**Solution**: Use dynamic artifacts for Python files:
```python
spark = create_spark_connect_session("MyApp", add_artifacts=True)
```

### Issue: Network Configuration

**Problem**: Connection failures due to wrong endpoint hostnames.

**Context matters**:
- **Inside Docker network**: Use `http://minio:9000`
- **From host machine**: Use `http://localhost:9000`
- **From Spark Connect server**: Use `http://minio:9000`

### Issue: Credentials Provider Configuration

**Critical**: Always include the credentials provider configuration:
```conf
spark.hadoop.fs.s3a.aws.credentials.provider org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider
```

Without this, Spark S3A doesn't know how to authenticate, even with environment variables set.

### Issue: Configuration Precedence

**Important**: Explicit configuration in `spark-defaults.conf` takes precedence over environment variables.

**Example**:
```conf
# This will be used (explicit config)
spark.hadoop.fs.s3a.access.key admin

# This will be ignored (env var)
AWS_ACCESS_KEY_ID=admin
```

### Issue: Docker Compose Environment Variables

**Problem**: Environment variables in `docker-compose.yaml` only take effect when containers are created, not on restart.

**Solution**: Use `docker-compose down && docker-compose up -d` to recreate containers with new environment variables.

### Complete Working Example

Here's a complete working configuration with the new flexible system:

**docker-compose.yaml**:
```yaml
spark-connect:
  image: bitnami/spark:3.5
  environment:
    - SPARK_MODE=connect
    - SPARK_CONNECT_PORT=15002
    - AWS_ACCESS_KEY_ID=admin
    - AWS_SECRET_ACCESS_KEY=password
  volumes:
    - ./spark-defaults.conf:/opt/bitnami/spark/conf/spark-defaults.conf
```

**spark-defaults.conf** (server-side configuration):
See the current `spark-defaults.conf` file for the complete server-side configuration including:
- Core Iceberg catalog registration
- Core S3A filesystem registration  
- Spark Connect settings
- Performance and resource configurations

**Python application (session.py)** (flexible application-side configuration):
```python
from src.utils import S3FileSystemConfig, IcebergConfig, create_spark_session, SparkVersion

# S3 configuration for direct file operations
s3_config = S3FileSystemConfig(
    endpoint="minio:9000",
    access_key="admin",
    secret_key="password",
    region="us-east-1"
)

# Iceberg configuration for catalog operations
iceberg_config = IcebergConfig(s3_config)

# Create session with both configurations
spark = create_spark_session(
    spark_version=SparkVersion.SPARK_CONNECT_3_5,
    app_name="MyApp",
    s3_config=s3_config,
    iceberg_config=iceberg_config
)
```

**⚠️ UPDATE**: The new system provides flexibility where:
- **Server-side** (`spark-defaults.conf`): Core catalog registration, extensions, and infrastructure settings
- **Application-side** (`session.py`): Client-side parameters like credentials, endpoints, SSL settings, and per-application customization

### Troubleshooting Checklist

When Spark Connect operations fail:

1. ✅ **Check if Spark Connect is in workers**: Verify `SPARK_CONNECT_ENABLED=true` in worker containers
2. ✅ **Verify spark-defaults.conf**: Check that core catalog registration and extensions are in the file
3. ✅ **Check application-side configs**: Verify S3A and Iceberg credentials/endpoints are set in `session.py`
4. ✅ **Check JAR installation**: Ensure Iceberg JARs are in `/opt/bitnami/spark/jars/`
5. ✅ **Verify endpoint configuration**: Ensure correct hostname for context
6. ✅ **Check credentials provider**: Must include `SimpleAWSCredentialsProvider` in server-side config
7. ✅ **Restart Spark Connect**: `docker-compose restart spark-connect`
8. ✅ **Recreate containers if needed**: `docker-compose down && docker-compose up -d`
9. ✅ **Check MinIO user permissions**: Verify user exists and has proper permissions

**⚠️ UPDATE**: With the new flexible configuration system, troubleshooting now involves checking both server-side (`spark-defaults.conf`) and application-side (`session.py`) configurations, where `session.py` handles client-side parameters like credentials, endpoints, SSL settings, and catalog configurations. 

### Issue: S3A SSL Configuration

**Problem**: S3A operations failed due to SSL configuration issues with MinIO.

**Root Cause**: MinIO uses HTTP by default, but Spark S3A might try to use HTTPS, causing connection failures.

**Solution**: Explicitly configure SSL settings for S3A:
```conf
# Disable SSL for MinIO (which uses HTTP)
spark.hadoop.fs.s3a.connection.ssl.enabled false
spark.hadoop.fs.s3a.ssl.enabled false

# For Iceberg catalog operations
spark.sql.catalog.iceberg.s3.ssl-enabled false
```

**Important**: Both S3A filesystem and Iceberg catalog need SSL disabled for MinIO:
```conf
# S3A Filesystem SSL settings
spark.hadoop.fs.s3a.connection.ssl.enabled false
spark.hadoop.fs.s3a.ssl.enabled false

# Iceberg catalog SSL settings  
spark.sql.catalog.iceberg.s3.ssl-enabled false
```

**Alternative**: If using HTTPS MinIO, enable SSL:
```conf
# For HTTPS MinIO
spark.hadoop.fs.s3a.connection.ssl.enabled true
spark.hadoop.fs.s3a.ssl.enabled true
spark.sql.catalog.iceberg.s3.ssl-enabled true
```

### Issue: Environment Variables vs Configuration Files

**Problem**: Setting environment variables in `docker-compose.yaml` wasn't sufficient for S3A authentication.

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
spark.hadoop.fs.s3a.access.key admin
spark.hadoop.fs.s3a.secret.key password
```

### Issue: Configuration Precedence

**Important**: Explicit configuration in `spark-defaults.conf` takes precedence over environment variables.

**Example**:
```conf
# This will be used (explicit config)
spark.hadoop.fs.s3a.access.key admin

# This will be ignored (env var)
AWS_ACCESS_KEY_ID=admin
```

### Issue: Docker Compose Environment Variables

**Problem**: Environment variables in `docker-compose.yaml` only take effect when containers are created, not on restart.

**Solution**: Use `docker-compose down && docker-compose up -d` to recreate containers with new environment variables.

### Complete Working Example

Here's a complete working configuration:

**docker-compose.yaml**:
```yaml
spark-connect:
  image: bitnami/spark:3.5
  environment:
    - SPARK_MODE=connect
    - SPARK_CONNECT_PORT=15002
    - AWS_ACCESS_KEY_ID=admin
    - AWS_SECRET_ACCESS_KEY=password
  volumes:
    - ./spark-defaults.conf:/opt/bitnami/spark/conf/spark-defaults.conf
```

**spark-defaults.conf**:
```conf
# Iceberg catalog configuration
spark.sql.catalog.iceberg org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.iceberg.type rest
spark.sql.catalog.iceberg.uri http://iceberg-rest:8181
spark.sql.catalog.iceberg.io-impl org.apache.iceberg.aws.s3.S3FileIO
spark.sql.catalog.iceberg.warehouse s3://data/wh/
spark.sql.catalog.iceberg.s3.access-key-id admin
spark.sql.catalog.iceberg.s3.secret-access-key password
spark.sql.catalog.iceberg.s3.endpoint http://minio:9000
spark.sql.catalog.iceberg.s3.region us-east-1
spark.sql.catalog.iceberg.s3.path-style-access true
spark.sql.catalog.iceberg.s3.ssl-enabled false
spark.sql.defaultCatalog iceberg

# S3A Filesystem configuration
spark.hadoop.fs.s3.impl org.apache.hadoop.fs.s3a.S3AFileSystem
spark.hadoop.fs.s3a.aws.credentials.provider org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider
spark.hadoop.fs.s3a.access.key admin
spark.hadoop.fs.s3a.secret.key password
spark.hadoop.fs.s3a.endpoint http://minio:9000
spark.hadoop.fs.s3a.region us-east-1
spark.hadoop.fs.s3a.path.style.access true
spark.hadoop.fs.s3a.connection.ssl.enabled false
```

### Troubleshooting Checklist

When Spark Connect operations fail:

1. ✅ **Check if Spark Connect is in workers**: Verify `SPARK_CONNECT_ENABLED=true` in worker containers
2. ✅ **Verify spark-defaults.conf**: Check that critical configs are in the file, not just Python code
3. ✅ **Check JAR installation**: Ensure Iceberg JARs are in `/opt/bitnami/spark/jars/`
4. ✅ **Verify endpoint configuration**: Ensure correct hostname for context
5. ✅ **Check credentials provider**: Must include `SimpleAWSCredentialsProvider`
6. ✅ **Restart Spark Connect**: `docker-compose restart spark-connect`
7. ✅ **Recreate containers if needed**: `docker-compose down && docker-compose up -d`
8. ✅ **Check MinIO user permissions**: Verify user exists and has proper permissions 