#!/usr/bin/env python3
"""
Integration tests for PV Data processing pipeline using real MinIO data
"""

import pytest
from src.utils.session import create_spark_session, SparkVersion
from src.utils.ddl import DDLLoader
from src.legal.ingest_docs import main as docs_main
from src.legal.ingest_docs_metadata import main as docs_metadata_main
from src.utils.ingest import list_files


@pytest.fixture(scope="session")
def spark_session():
    """Create Spark session for testing"""
    spark = create_spark_session(
        app_name="test_process_pvdata_integration",
        spark_version=SparkVersion.SPARK_CONNECT_3_5,
        catalog="iceberg",
    )
    yield spark
    spark.stop()


@pytest.fixture(scope="session")
def default_spark_session():
    """Create Spark session for testing"""
    spark = create_spark_session(
        app_name="test_process_pvdata_integration",
        spark_version=SparkVersion.SPARK_CONNECT_3_5,
    )
    yield spark
    spark.stop()


@pytest.fixture(scope="session")
def ddl_loader():
    """Create DDL loader instance"""
    return DDLLoader()


@pytest.fixture(scope="session")
def test_namespace(spark_session):
    """Create test namespace"""
    spark_session.sql("CREATE NAMESPACE IF NOT EXISTS test")
    return "test"


@pytest.fixture(scope="session")
def docs_table(spark_session, ddl_loader, test_namespace):
    """Create pv_data table using DDL loader"""
    ddl_content = ddl_loader.load_ddl("legal_docs.sql")
    print(f"Original DDL:\n{ddl_content}")

    ddl = ddl_loader.modify_ddl(
        ddl_content=ddl_content,
        table_name="legal.docs",
        new_table_name="test.docs",
    )
    ddl_loader.execute_iceberg_ddl(ddl, spark_session)
    return "test.docs"


@pytest.fixture(scope="session")
def docs_metadata(spark_session, ddl_loader, test_namespace):
    """Create pv_data table using DDL loader"""
    ddl_content = ddl_loader.load_ddl("legal_docs_metadata.sql")
    print(f"Original DDL:\n{ddl_content}")

    ddl = ddl_loader.modify_ddl(
        ddl_content=ddl_content,
        table_name="legal.docs_metadata",
        new_table_name="test.docs_metadata",
    )
    ddl_loader.execute_iceberg_ddl(ddl, spark_session)
    return "test.docs_metadata"


def test_legal_docs_pipeline(spark_session, test_namespace, docs_table):
    """Test site pipeline main function"""

    docs_main(
        namespace="test",
        table_name="docs",
        file_path_pattern="s3a://raw/docs/legal/**/**/content/*.txt",
        limit=50,
        spark_session=spark_session,
    )

    # Verify data was processed
    result_df = spark_session.sql(f"SELECT COUNT(*) as count FROM {docs_table}")
    count = result_df.collect()[0]["count"]
    assert count > 0, "Legal pipeline should process some records"

    print(f"✅ Site pipeline processed {count} records")


def test_legal_docs_metadata_pipeline(spark_session, test_namespace, docs_metadata):
    """Test site pipeline main function"""

    docs_metadata_main(
        namespace="test",
        table_name="docs_metadata",
        file_path_pattern="s3a://raw/docs/legal/**/**/metadata/*.json",
        limit=50,
        spark_session=spark_session,
    )

    # Verify data was processed
    result_df = spark_session.sql(f"SELECT COUNT(*) as count FROM {docs_metadata}")
    count = result_df.collect()[0]["count"]
    assert count > 0, "Legal pipeline should process some records"

    print(f"✅ Site pipeline processed {count} records")
