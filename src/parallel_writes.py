import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql import Row
import time
from datetime import datetime
import random
from datetime import datetime

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "admin")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "password")

if "spark" in locals():
    locals["spark"].stop()


def get_spark_session() -> SparkSession:
    spark = (SparkSession.builder.appName("CreateIcebergTable")
        .config("spark.sql.streaming.schemaInference", "true")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.defaultCatalog", "iceberg")
        .config("spark.sql.catalog.iceberg.type", "rest")
        .config("spark.sql.catalog.iceberg.uri", "http://spark-rest:8181")
        .config("spark.sql.catalog.iceberg.s3.endpoint", "http://minio:9000")
        .config("spark.sql.catalog.iceberg.warehouse", "s3://data/wh")
        .config("spark.sql.catalog.iceberg.s3.access-key", AWS_ACCESS_KEY_ID)
        .config("spark.sql.catalog.iceberg.s3.secret-key", AWS_SECRET_ACCESS_KEY)
        .config("spark.sql.catalog.iceberg.s3.region", 'us-east-1')
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.region", "us-east-1")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        #parallelization
        .config("spark.default.parallelism", 2)  # Set based on total cores
        .config("spark.sql.shuffle.partitions", 2)  # For SQL operations
        .config("spark.executor.instances", 2)  # Number of workers
        .config("spark.executor.cores", 1)  # Cores per executor
        .config("spark.executor.memory", "1g")  # Memory per executor
        .config("spark.driver.memory", "1g")  # Memory per executor

        .config("spark.eventLog.dir", "file:/opt/bitnami/spark/logs/spark-events")
        .config("spark.history.fs.logDirectory", "file:/opt/bitnami/spark/logs/spark-events")
        .config("spark.executor.logs.rolling.strategy", "time")
        .config("spark.executor.logs.rolling.time.interval", "daily")
        .config("spark.executor.logs.rolling.maxRetainedFiles", "7")
        
        .master("spark://spark-master:7077")
        .getOrCreate()
            )
    
    spark.sparkContext.setLogLevel("INFO")
    return spark

def create_batch(batch_id: int, batch_size: int, partition_id: int) -> list:

    batch_data = []
    for i in range(batch_size):
        record_id = f"id-{batch_id}-{partition_id}-{i}"
        value = random.randint(1, 100)
        timestamp = datetime.now()
        batch_data.append((record_id, value,timestamp))
    return batch_data

def main():
    spark  = get_spark_session()
    db_name = 'test'
    table_name = 'small_writes'
    table_identifier = f"{db_name}.{table_name}"
    spark.sql("SHOW NAMESPACES;").show()
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
    spark.sql("SHOW DATABASES;").show()
    spark.sql(f"DROP TABLE IF EXISTS {table_identifier}")

    # schema
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("value", IntegerType(), False),
        StructField("timestamp", TimestampType(), False)
    ])
    
    # Create an empty dataframe with the schema to initialize the table
    empty_df = spark.createDataFrame([], schema)
    print("Creating Iceberg table...")
    empty_df.writeTo(f"iceberg.{db_name}.{table_name}") \
        .using("iceberg") \
        .tableProperty("write.merge.isolation-level", "snapshot") \
        .tableProperty("write.distribution-mode", "hash") \
        .tableProperty("write.format.default", "avro") \
        .create()
    

    num_partitions = 2
    records_per_partition = 100 
    total_records = num_partitions * records_per_partition
    batch_size = 1
    num_batches = total_records // batch_size
    total_records = 0
    
    print(f"Starting insertion test - {total_records} total records in {num_batches} batches")
    start_time = time.time()
    
    for batch in range(num_batches):
        # Generate a batch of data
        batch_start = time.time()
        # Create RDD with specified partitions for better parallelism
        rdd = spark.sparkContext.parallelize(range(num_partitions), num_partitions) \
                .flatMap(lambda partition_id: create_batch(batch, batch_size, partition_id))
        df = spark.createDataFrame(rdd, schema=schema)
        records_in_batch = df.count()
        total_records += records_in_batch
        write_start = time.time()
        df.writeTo(f"iceberg.{db_name}.{table_name}").using("iceberg").\
            tableProperty("write.merge.isolation-level", "snapshot").\
            tableProperty("write.format.default", "avro").append()
        write_end = time.time()


        batch_end = time.time()
        batch_duration = batch_end - batch_start
        write_duration = write_end - write_start
        rps = batch_size / batch_duration if batch_duration > 0 else 0
        print(f"Batch {batch+1}/{num_batches}:")
        print(f"  Records: {records_in_batch}")
        print(f"  Total time: {batch_duration:.2f}s (Write time: {write_duration:.2f}s)")
        print(f"  Performance: {rps:.2f} records/sec")
    
    end_time = time.time()
    total_duration = end_time - start_time
    avg_records_per_second = total_records / total_duration if total_duration > 0 else 0
    
    print("\n=== Performance Test Results ===")
    print(f"Total records: {total_records}")
    print(f"Total time: {total_duration:.2f} seconds")
    print(f"Average throughput: {avg_records_per_second:.2f} records/second")

    actual_count = spark.sql(f"SELECT COUNT(*) FROM {full_table_name}").collect()[0][0]
    print(f"Verified record count in table: {actual_count}")
    

if __name__ == "__main__":
    main()