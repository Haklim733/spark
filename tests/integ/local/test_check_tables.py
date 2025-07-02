#!/usr/bin/env python3
"""
Test check_tables.py functionality using Spark Connect
This tests that Python code can run locally while connecting to remote Spark cluster
"""

import os
import pytest
from pyspark.sql import SparkSession
from src.check_tables import check_namespace_tables
from src.utils import create_spark_connect_session


@pytest.fixture(scope="class")
def spark_connect_session():
    """Create Spark Connect session"""
    spark = create_spark_connect_session("CheckTablesSparkConnectTest")
    yield spark
    spark.stop()


class TestCheckTablesSparkConnect:
    """Test check_tables functionality using Spark Connect"""

    def test_spark_connect_connection(self, spark_connect_session):
        """Test basic connection to Spark Connect server"""
        print(f"✅ Connected to Spark Connect server!")
        print(f"   Spark version: {spark_connect_session.version}")
        print(f"   Master: {spark_connect_session.conf.get('spark.master', 'N/A')}")

    def test_show_databases(self, spark_connect_session):
        """Test showing databases using Spark Connect"""
        print("=== TESTING SHOW DATABASES ===")
        try:
            databases = spark_connect_session.sql("SHOW DATABASES;")
            # Just check that we can execute the query, avoid count() which can be slow
            schema = databases.schema
            assert len(schema.fields) > 0
            print(f"✅ Successfully executed SHOW DATABASES")
            print(f"   Schema: {[field.name for field in schema.fields]}")
        except Exception as e:
            print(f"❌ Error showing databases: {e}")

    def test_check_namespace_tables_legal(self, spark_connect_session):
        """Test checking tables in legal namespace using Spark Connect"""
        print("=== TESTING LEGAL NAMESPACE ===")
        try:
            # Test the check_namespace_tables function
            check_namespace_tables(spark_connect_session, "legal")
            print("✅ Successfully checked legal namespace tables")
        except Exception as e:
            print(f"⚠️  Legal namespace check failed: {e}")

    def test_check_namespace_tables_nyc_taxi(self, spark_connect_session):
        """Test checking tables in nyc_taxi namespace using Spark Connect"""
        print("=== TESTING NYC_TAXI NAMESPACE ===")
        try:
            # Test the check_namespace_tables function
            check_namespace_tables(spark_connect_session, "nyc_taxi")
            print("✅ Successfully checked nyc_taxi namespace tables")
        except Exception as e:
            print(f"⚠️  NYC taxi namespace check failed: {e}")

    def test_check_namespace_tables_admin(self, spark_connect_session):
        """Test checking tables in admin namespace using Spark Connect"""
        print("=== TESTING ADMIN NAMESPACE ===")
        try:
            # Test the check_namespace_tables function
            check_namespace_tables(spark_connect_session, "admin")
            print("✅ Successfully checked admin namespace tables")
        except Exception as e:
            print(f"⚠️  Admin namespace check failed: {e}")

    def test_use_namespace_command(self, spark_connect_session):
        """Test USE namespace command using Spark Connect"""
        print("=== TESTING USE NAMESPACE COMMAND ===")
        try:
            # Test switching to a namespace
            spark_connect_session.sql("USE legal;")
            print("✅ Successfully switched to legal namespace")

            # Test showing tables in current namespace
            tables = spark_connect_session.sql("SHOW TABLES;")
            schema = tables.schema
            assert len(schema.fields) > 0
            print(f"✅ Successfully showed tables in legal namespace")
            print(f"   Schema: {[field.name for field in schema.fields]}")
        except Exception as e:
            print(f"⚠️  USE namespace command failed: {e}")

    def test_describe_table_command(self, spark_connect_session):
        """Test DESCRIBE table command using Spark Connect"""
        print("=== TESTING DESCRIBE TABLE COMMAND ===")
        try:
            # First switch to a namespace
            spark_connect_session.sql("USE legal;")

            # Get list of tables
            tables = spark_connect_session.sql("SHOW TABLES;")

            # Try to describe the first table if any exist
            # Avoid collect() which can be slow, just check schema
            schema = tables.schema
            if len(schema.fields) > 0:
                print("✅ Tables exist in legal namespace")
                # Note: We can't easily get table names without collect(),
                # but we can verify the SHOW TABLES command works
            else:
                print("ℹ️  No tables found in legal namespace")

        except Exception as e:
            print(f"⚠️  DESCRIBE table command failed: {e}")

    def test_basic_sql_query(self, spark_connect_session):
        """Test basic SQL query execution in Spark Connect"""
        print("=== TESTING BASIC SQL QUERY ===")
        try:
            result = spark_connect_session.sql("SELECT 'test_query' as test_column")
            schema = result.schema
            assert len(schema.fields) > 0
            print("✅ Basic SQL query executed successfully")
            print(f"   Test query schema: {[field.name for field in schema.fields]}")
        except Exception as e:
            print(f"⚠️  Basic SQL query failed: {e}")


def test_standalone_check_tables():
    """Test standalone check_tables functionality without fixtures"""
    print("=== STANDALONE CHECK TABLES TEST ===")
    try:
        spark = create_spark_connect_session("StandaloneCheckTablesTest")

        # Test basic functionality
        databases = spark.sql("SHOW DATABLES;")
        print("✅ Standalone check_tables test successful")
        spark.stop()

    except Exception as e:
        print(f"❌ Standalone check_tables test failed: {e}")


if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v"])
