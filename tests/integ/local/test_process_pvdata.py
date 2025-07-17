#!/usr/bin/env python3
"""
Integration tests for PV Data processing pipeline using real MinIO data
"""

import pytest
from src.utils.session import create_spark_session, SparkVersion
from src.utils.ddl import DDLLoader
from src.energy.ingest_site import main as site_main
from src.energy.ingest_system import main as system_main
from src.energy.ingest_pvdata import main as pvdata_main


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
def ddl_loader():
    """Create DDL loader instance"""
    return DDLLoader()


@pytest.fixture(scope="session")
def test_namespace(spark_session):
    """Create test namespace"""
    spark_session.sql("CREATE NAMESPACE IF NOT EXISTS test")
    return "test"


@pytest.fixture(scope="session")
def pv_data_table(spark_session, ddl_loader, test_namespace):
    """Create pv_data table using DDL loader"""
    ddl_content = ddl_loader.load_ddl("pvdata.sql")
    print(f"Original DDL:\n{ddl_content}")

    ddl = ddl_loader.modify_ddl(
        ddl_content=ddl_content,
        table_name="energy.pv_data",
        new_table_name="test.pv_data",
    )
    print(f"Modified DDL:\n{ddl}")

    ddl_loader.execute_iceberg_ddl(ddl, spark_session)
    return "test.pv_data"


@pytest.fixture(scope="session")
def pv_system_table(spark_session, ddl_loader, test_namespace):
    """Create pv_system table using DDL loader"""
    ddl_content = ddl_loader.load_ddl("system.sql")
    ddl = ddl_loader.modify_ddl(
        ddl_content=ddl_content,
        table_name="energy.pv_system",
        new_table_name="test.pv_system",
    )
    ddl_loader.execute_iceberg_ddl(ddl, spark_session)
    return "test.pv_system"


@pytest.fixture(scope="session")
def pv_site_table(spark_session, ddl_loader, test_namespace):
    """Create pv_site table using DDL loader"""
    ddl_content = ddl_loader.load_ddl("site.sql")
    ddl = ddl_loader.modify_ddl(
        ddl_content=ddl_content,
        table_name="energy.pv_site",
        new_table_name="test.pv_site",
    )
    ddl_loader.execute_iceberg_ddl(ddl, spark_session)
    return "test.pv_site"


class TestSiteIntegration:
    """Integration test for site data processing pipeline"""

    def test_real_site_pipeline(self, spark_session, test_namespace, pv_site_table):
        """Test site pipeline main function"""

        site_main(
            namespace="test",
            table_name="pv_site",
            file_path_pattern="s3a://raw/site/*.parquet",
            limit=10,
            spark_session=spark_session,
        )  # Limit records for faster testing

        # Verify data was processed
        result_df = spark_session.sql(f"SELECT COUNT(*) as count FROM {pv_site_table}")
        count = result_df.collect()[0]["count"]
        assert count > 0, "Site pipeline should process some records"

        print(f"✅ Site pipeline processed {count} records")


class TestSystemIntegration:
    """Integration test for system data processing pipeline"""

    def test_real_system_pipeline(self, spark_session, test_namespace, pv_system_table):
        """Test system pipeline main function"""

        system_main(
            namespace="test",
            table_name="pv_system",
            file_path_pattern="s3a://raw/system/*.parquet",
            limit=10,
            spark_session=spark_session,
        )

        # Verify data was processed
        result_df = spark_session.sql("SELECT COUNT(*) as count FROM test.pv_system")
        count = result_df.collect()[0]["count"]
        assert count > 0, "System pipeline should process some records"

        print(f"✅ System pipeline processed {count} records")


class TestPVDataIntegration:
    """Integration test for PV data processing pipeline"""

    def test_real_pvdata_pipeline(self, spark_session, test_namespace, pv_data_table):
        """Test PV data pipeline main function"""

        pvdata_main(
            namespace="test",
            table_name="pv_data",
            file_path_pattern="s3a://raw/pvdata/system_id=3/",
            limit=10,
            spark_session=spark_session,
        )

        # Verify data was processed
        result_df = spark_session.sql(f"SELECT COUNT(*) as count FROM {pv_data_table}")
        count = result_df.collect()[0]["count"]
        assert count > 0, "PV data pipeline should process some records"

        print(f"✅ PV data pipeline processed {count} records")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
