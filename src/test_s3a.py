import sys
from src.utils.session import create_spark_session, SparkVersion

# Define your S3 warehouse path
S3_WAREHOUSE_PATH = "s3a://iceberg/"  # Adjust to your actual warehouse root

# Initialize SparkSession
# spark = SparkSession.builder.appName("S3AConnectivityTest").getOrCreate()
spark = create_spark_session(
    spark_version=SparkVersion.SPARK_CONNECT_3_5,
    app_name="S3AConnectivityTest",
)

try:
    print(f"Attempting to list files at: {S3_WAREHOUSE_PATH}")
    # Try to list the directory content
    # This operation uses the configured S3AFileSystem
    files = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark._jvm.java.net.URI.create(S3_WAREHOUSE_PATH),
        spark._jvm.org.apache.hadoop.conf.Configuration(),
    ).listStatus(spark._jvm.org.apache.hadoop.fs.Path(S3_WAREHOUSE_PATH))

    print(f"Successfully listed {len(files)} items in {S3_WAREHOUSE_PATH}")
    for f in files:
        print(f"  - {f.getPath()}")

    # Try to read a dummy Parquet file if you have one, or just a sample operation
    # This will test the full read path, similar to what count() does
    try:
        df = spark.read.format("parquet").load(
            f"{S3_WAREHOUSE_PATH}some_existing_parquet_file_or_table_path"
        )
        df.printSchema()
        print(f"Successfully read schema of a Parquet file/table.")
        print(f"Attempting count() on the test DataFrame...")
        count_result = df.count()
        print(f"Count successful: {count_result}")
    except Exception as e:
        print(f"Error during Parquet read/count: {e}")
        sys.exit(1)

except Exception as e:
    print(f"S3A connectivity test failed: {e}")
    sys.exit(1)
finally:
    spark.stop()

sys.exit(0)
