import os
from pyspark.sql import SparkSession

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "admin")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "password")

if "spark" in locals():
    locals["spark"].stop()


def get_spark_session() -> SparkSession:
    # jars = [
    #     "/opt/bitnami/spark/jars/iceberg-spark-runtime-3.5_2.12-1.8.1.jar",
    #     "/opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar",
    #     "/opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.262.jar"
    # ]
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
        .getOrCreate()
            )
    
    # for jar in jars:
    #     spark.sparkContext.addFile(jar)
    spark.sparkContext.setLogLevel("INFO")
    return spark

def main():
    spark  = get_spark_session()
    db_name = 'nyc'
    table_name = 'taxis'
    table_identifier = f"{db_name}.{table_name}"
    spark.sql("SHOW NAMESPACES;").show()
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
    spark.sql("SHOW DATABASES;").show()
    spark.sql(f"DROP TABLE IF EXISTS {table_identifier}")

    spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW parquet_files AS
        SELECT input_file_name() as file_path
        FROM parquet.`s3a://data/*.parquet`
        GROUP BY input_file_name()
    """)
    
    # Get the list of files
    parquet_files = [row.file_path for row in spark.sql("SELECT file_path FROM parquet_files").collect()]
    print(parquet_files)
    file_path = 's3://data/yellow_tripdata_2021-04.parquet' 
    df_first = spark.read.parquet(file_path)
    df_first.writeTo(f"iceberg.{db_name}.{table_name}").using("iceberg").tableProperty("write.merge.isolation-level", "snapshot").create()
    df_first.createOrReplaceTempView("temp_data")
    first_file = parquet_files[0]

    
    # Process the first file to create the table
    # if parquet_files:
    #     first_file = parquet_files[0]
    #     print(f"Creating table with first file: {first_file}")
    #     create_table_sql = f"""
    #         CREATE TABLE IF NOT EXISTS {table_identifier}
    #         USING iceberg
    #         AS SELECT * FROM '{first_file}'
    #         """
    #     spark.sql(create_table_sql)
        
        # Create the initial table 
        # Process remaining files as separate snapshots
        # for file_path in parquet_files[1:]:
        #     print(file_path)
        #     try:
        #         print(f"Processing file: {file_path}")
        #         df = spark.read.parquet(file_path)
                
        #         # Write as a new snapshot
        #         df.writeTo(table_identifier).using("iceberg").append()
                
        #         print(f"Successfully added snapshot from {file_path}")
        #     except Exception as e:
        #         print(f"Error processing {file_path}: {str(e)}")
    # else:
    #     print("No Parquet files found in the bucket")
    
    # Verify the table and show snapshots
    # count = spark.table(table_identifier).count()
    # print(f"Total records in {table_identifier}: {count}")
    
    # # Show table history
    # print("Table history (snapshots):")
    # spark.sql(f"SELECT * FROM {table_identifier}.history").show(truncate=False)



if __name__ == "__main__":
    main()