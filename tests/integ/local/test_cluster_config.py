import pytest
from pyspark.sql import SparkSession
from pyspark.errors.exceptions.connect import SparkConnectGrpcException


@pytest.fixture(scope="module")
def spark_connect_session():
    """Create Spark Connect session for testing"""
    spark = (
        SparkSession.builder.appName("ClusterConfigTest")
        .remote("sc://localhost:15002")
        .getOrCreate()
    )
    yield spark
    spark.stop()


def test_cluster_info(spark_connect_session):
    """Test basic cluster information"""
    try:
        # Check if we can get cluster info
        spark = spark_connect_session

        # Test basic DataFrame creation
        df = spark.createDataFrame([(1, "test")], ["id", "name"])
        print(f"✅ DataFrame creation works")

        # Test simple count (this should work according to docs)
        count = df.count()
        print(f"✅ Count works: {count}")

        # Test groupBy count
        df2 = spark.createDataFrame(
            [("A", 1), ("A", 2), ("B", 3)], ["category", "value"]
        )
        grouped = df2.groupBy("category").count()
        result = grouped.collect()
        print(f"✅ GroupBy count works: {result}")

        # Test SQL count
        df2.createOrReplaceTempView("test_table")
        sql_result = spark.sql("SELECT COUNT(*) as count FROM test_table").collect()
        print(f"✅ SQL count works: {sql_result}")

        # Test ORDER BY
        order_result = spark.sql("SELECT * FROM test_table ORDER BY value").collect()
        print(f"✅ ORDER BY works: {len(order_result)} rows")

        # Test JOIN
        df3 = spark.createDataFrame(
            [("A", "desc1"), ("B", "desc2")], ["category", "description"]
        )
        df3.createOrReplaceTempView("categories")
        join_result = spark.sql(
            """
            SELECT t.category, t.value, c.description 
            FROM test_table t 
            JOIN categories c ON t.category = c.category
        """
        ).collect()
        print(f"✅ JOIN works: {len(join_result)} rows")

    except SparkConnectGrpcException as e:
        print(f"❌ Spark Connect error: {e}")
        raise
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        raise


def test_worker_configuration(spark_connect_session):
    """Test worker configuration"""
    try:
        # Check Spark configuration
        spark = spark_connect_session

        # Get configuration
        config = spark.conf.get("spark.executor.instances", "Not set")
        print(f"Executor instances: {config}")

        config = spark.conf.get("spark.executor.cores", "Not set")
        print(f"Executor cores: {config}")

        config = spark.conf.get("spark.executor.memory", "Not set")
        print(f"Executor memory: {config}")

        # Test with larger dataset
        data = [(i, f"item_{i}") for i in range(1000)]
        df = spark.createDataFrame(data, ["id", "name"])

        # This should work if workers are properly configured
        count = df.count()
        print(f"✅ Large dataset count works: {count}")

    except Exception as e:
        print(f"❌ Configuration test failed: {e}")
        raise
