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
```

### With Iceberg Configuration

```python
from utils import create_spark_session, SparkVersion, IcebergConfig

# Create Iceberg config
iceberg_config = IcebergConfig(
    catalog_uri="http://spark-rest:8181",
    warehouse="s3://data/wh"
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