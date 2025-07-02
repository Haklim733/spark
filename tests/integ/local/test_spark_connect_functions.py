import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType


@pytest.fixture(scope="module")
def spark_connect_session():
    spark = (
        SparkSession.builder.appName("UnsupportedFeaturesTest")
        .remote("sc://localhost:15002")
        .getOrCreate()
    )
    yield spark
    spark.stop()


# 1. Python UDFs are supported
from pyspark.sql.functions import udf


def test_python_udf_supported(spark_connect_session):
    df = spark_connect_session.createDataFrame([(1,)], ["x"])
    my_udf = udf(lambda x: x + 1, IntegerType())
    # Should work without raising
    result = df.withColumn("y", my_udf(df["x"]))
    assert "y" in result.columns


# 2. DataFrame actions
@pytest.mark.parametrize("action", ["collect", "toPandas"])
def test_dataframe_actions_supported(spark_connect_session, action):
    df = spark_connect_session.createDataFrame([(1,)], ["x"])
    # Should work without raising
    result = getattr(df, action)()
    assert result is not None


# count() is unsupported
@pytest.mark.parametrize("action", ["count"])
def test_dataframe_count_not_supported(spark_connect_session, action):
    df = spark_connect_session.createDataFrame([(1,)], ["x"])
    with pytest.raises(Exception):
        getattr(df, action)()


def test_foreach_not_supported(spark_connect_session):
    df = spark_connect_session.createDataFrame([(1,)], ["x"])
    with pytest.raises(Exception):
        df.foreach(lambda row: print(row))


# 3. RDD operations are not available


def test_rdd_not_supported(spark_connect_session):
    df = spark_connect_session.createDataFrame([(1,)], ["x"])
    with pytest.raises(Exception):
        _ = df.rdd.collect()


# 4. Custom serialization (map, flatMap) is unsupported
def test_map_flatmap_not_supported(spark_connect_session):
    df = spark_connect_session.createDataFrame([(1,)], ["x"])
    with pytest.raises(Exception):
        df.map(lambda row: row).collect()
    with pytest.raises(Exception):
        df.flatMap(lambda row: [row]).collect()


# 5. DataFrame transformations with Python lambdas are unsupported
def test_lambda_filter_not_supported(spark_connect_session):
    df = spark_connect_session.createDataFrame([(1,), (2,)], ["x"])
    with pytest.raises(Exception):
        df.filter(lambda row: row.x > 1).collect()


# 6. Iceberg catalog DDL (namespace creation) is supported
def test_iceberg_catalog_ddl_supported(spark_connect_session):
    # This assumes Iceberg catalog is configured as 'spark_catalog'
    # Should work without raising
    spark_connect_session.sql(
        "CREATE NAMESPACE IF NOT EXISTS spark_catalog.unsupported_ns"
    )


# 7. Direct file system access is supported
def test_local_file_access_supported(spark_connect_session, tmp_path):
    # Create a local file
    file_path = tmp_path / "test.txt"
    file_path.write_text("hello")
    # Should work without raising
    df = spark_connect_session.read.text(str(file_path))
    assert df is not None
