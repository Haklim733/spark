# MinIO Integration with insert_tables.py

This document explains how to use the updated `insert_tables.py` script to read files from MinIO storage using Spark.

## Overview

The `insert_tables.py` script now supports reading files from both local filesystem and MinIO/S3-compatible storage using Spark's built-in S3 filesystem support.

## Prerequisites

1. **Spark Setup**: Ensure Spark is properly configured with S3 filesystem support
2. **MinIO Setup**: Ensure MinIO is running and accessible (already configured in your docker-compose.yaml)

## Configuration

Spark is configured with MinIO credentials in your `spark-defaults.conf`:

```properties
spark.hadoop.fs.s3a.access.key=admin
spark.hadoop.fs.s3a.secret.key=password
spark.hadoop.fs.s3a.endpoint=http://minio:9000
spark.hadoop.fs.s3a.path.style.access=true
spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
```

## Usage Examples

### 1. Insert Files from MinIO

```python
from utils.session import create_spark_session
from insert_tables import insert_files

# Create Spark session
spark = create_spark_session()

# Insert files from MinIO
success = insert_files(
    spark=spark,
    docs_dir="s3a://data/docs/legal",  # MinIO path
    table_name="legal.documents",
    use_parallel=True,
    num_partitions=4
)
```

### 2. Command Line Usage

```bash
# Insert files from MinIO
python src/insert_tables.py s3a://data/docs/legal legal.documents

# Force parallel processing
python src/insert_tables.py s3a://data/docs/legal legal.documents --parallel

# Specify partitions
python src/insert_tables.py s3a://data/docs/legal legal.documents --partitions 8
```

### 3. Supported File Formats

The script supports these file formats from MinIO:
- **Text files** (`.txt`): Read using Spark's text format
- **JSON files** (`.json`): Read using Spark's JSON format
- **Parquet files** (`.parquet`): Read using Spark's parquet format

### 4. Path Formats

The script accepts these MinIO path formats:
- `s3a://bucket-name/path/to/files`
- `s3://bucket-name/path/to/files`
- `minio://bucket-name/path/to/files`

## How It Works

### 1. File Discovery

When a MinIO path is detected, the script:
1. Uses Spark's `binaryFile` format to list objects in the specified bucket/path
2. Filters for supported file extensions
3. Returns a list of file paths

### 2. File Processing

For each file:
1. **Text files**: Uses Spark's `text` format to read content
2. **JSON files**: Uses Spark's `json` format to parse JSON
3. **Parquet files**: Uses Spark's `parquet` format to read and summarize

### 3. Data Validation

All files go through the same validation process:
- Document quality checks
- Size and content validation
- Invalid records are sent to quarantine table

## Testing MinIO Connection

Use the test script to verify MinIO connectivity:

```bash
python src/test_minio_connection.py
```

This will:
- Test Spark session creation
- List available files in MinIO
- Test path parsing functions

## Example Workflow

1. **Upload files to MinIO**:
   ```bash
   # Using the mc client
   docker exec -it spark-mc mc cp local_file.txt minio/data/docs/legal/
   ```

2. **Insert files into Spark tables**:
   ```bash
   python src/insert_tables.py s3a://data/docs/legal legal.documents
   ```

3. **Verify results**:
   ```sql
   SELECT COUNT(*) FROM legal.documents;
   SELECT * FROM legal.documents LIMIT 5;
   ```

## Error Handling

The script includes comprehensive error handling:

- **Connection errors**: Clear error messages for MinIO connectivity issues
- **File processing errors**: Invalid files are quarantined
- **Validation errors**: Records with quality issues are quarantined
- **Missing files**: Clear error messages for missing paths

## Performance Considerations

- **Spark distributed processing**: Leverages Spark's distributed computing capabilities
- **Parallel processing**: Automatically enabled for large file sets
- **Partitioning**: Configurable based on file count and size
- **Memory management**: Spark handles memory allocation and garbage collection

## Troubleshooting

### Common Issues

1. **Connection refused**:
   - Check if MinIO is running: `docker ps | grep minio`
   - Verify Spark configuration in `spark-defaults.conf`

2. **Authentication errors**:
   - Verify access key and secret in Spark configuration
   - Check if MinIO credentials are correct

3. **No files found**:
   - Verify the bucket and path exist
   - Check file extensions are supported
   - Ensure files are actually uploaded to MinIO

4. **Spark configuration issues**:
   - Check `spark-defaults.conf` for S3A configuration
   - Verify Spark session is created with proper configuration

### Debug Mode

Enable debug output by setting environment variables:
```bash
export SPARK_LOG_LEVEL=DEBUG
```

## Integration with Existing Workflow

The updated script maintains backward compatibility:
- Local file processing works exactly as before
- Existing functions continue to work
- New MinIO functionality is additive

You can seamlessly switch between local and MinIO paths without changing your code structure.

## Advantages of Spark-based Approach

- **Unified processing**: Same code path for local and MinIO files
- **Distributed processing**: Leverages Spark's distributed computing
- **Built-in support**: No additional dependencies required
- **Consistent with existing workflow**: Uses the same Spark session for all operations 