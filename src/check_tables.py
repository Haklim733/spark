import os
import re
from pathlib import Path
from src.utils import create_spark_connect_session

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "admin")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "password")


def extract_namespaces_and_tables_from_ddl(ddl_dir):
    """Extract namespaces and table names from DDL files"""
    namespaces_and_tables = {}

    if not ddl_dir.exists():
        print(f"DDL directory not found: {ddl_dir}")
        return namespaces_and_tables

    for ddl_file in ddl_dir.glob("*.sql"):
        try:
            with open(ddl_file, "r") as f:
                content = f.read()

            print(f"DEBUG: Processing DDL file: {ddl_file}")

            # Look for CREATE TABLE statements with namespace.table format
            # Handle both CREATE TABLE IF NOT EXISTS and CREATE OR REPLACE TABLE
            matches = re.findall(
                r"CREATE (?:TABLE IF NOT EXISTS|OR REPLACE TABLE) (\w+)\.(\w+)",
                content,
                re.IGNORECASE,
            )

            for namespace, table_name in matches:
                if namespace not in namespaces_and_tables:
                    namespaces_and_tables[namespace] = []
                namespaces_and_tables[namespace].append(table_name)
                print(f"DEBUG: Found {namespace}.{table_name}")

        except Exception as e:
            print(f"⚠️  Error reading {ddl_file}: {e}")

    return namespaces_and_tables


def check_namespace_tables(spark, namespace, expected_tables=None):
    """Check tables in a specific namespace"""
    print(f"\n=== SHOWING TABLES IN {namespace.upper()} DATABASE ===")
    try:
        # Try direct table access instead of USE namespace
        table_name = f"iceberg.{namespace}.documents"  # Assuming 'documents' table

        print(f"Trying to access table: {table_name}")

        # Test if we can access the table directly
        try:
            desc_result = spark.sql(f"DESCRIBE {table_name}")
            desc_result.show(truncate=False)
            print(f"✅ Successfully accessed table: {table_name}")

            # Show sample data
            print(f"\n--- Sample data from {table_name} ---")
            sample_result = spark.sql(f"SELECT * FROM {table_name} LIMIT 3")
            sample_result.show(truncate=False)

        except Exception as table_error:
            print(f"❌ Error accessing table {table_name}: {table_error}")

            # Try alternative approaches
            print(f"\nTrying alternative table names...")
            alternatives = [
                f"{namespace}.documents",
                "documents",
                f"iceberg.{namespace}_documents",
            ]

            for alt_name in alternatives:
                try:
                    test_result = spark.sql(f"SELECT COUNT(*) FROM {alt_name}")
                    print(f"✅ Alternative {alt_name} works")
                    break
                except Exception as alt_error:
                    print(f"❌ Alternative {alt_name} failed: {alt_error}")

    except Exception as e:
        print(f"❌ Error accessing namespace '{namespace}': {e}")
        print(f"Namespace '{namespace}' may not exist yet.")


def get_table_row_counts(spark, namespace):
    """Get row counts for all tables in a namespace using metadata only"""
    print(f"\n=== ROW COUNTS FOR {namespace.upper()} TABLES ===")
    try:
        # Use the Iceberg catalog explicitly
        spark.sql(f"USE iceberg.{namespace};")
        tables = spark.sql("SHOW TABLES;")

        for row in tables.collect():
            table_name = row.tableName
            full_table_name = f"iceberg.{namespace}.{table_name}"
            try:
                # Try to get table statistics from metadata only
                try:
                    # Use DESCRIBE EXTENDED to get metadata without scanning data
                    desc_result = spark.sql(f"DESCRIBE EXTENDED {full_table_name}")
                    desc_rows = desc_result.take(50)

                    # Look for various metadata indicators
                    has_data = False
                    row_count = None
                    table_type = "unknown"

                    for desc_row in desc_rows:
                        desc_str = str(desc_row)

                        # Check for table type
                        if "Table Type" in desc_str:
                            if "EXTERNAL" in desc_str:
                                table_type = "external"
                            elif "MANAGED" in desc_str:
                                table_type = "managed"

                        # Check for statistics
                        if "Statistics" in desc_str:
                            if "numRows" in desc_str:
                                import re

                                match = re.search(r"numRows=(\d+)", desc_str)
                                if match:
                                    row_count = int(match.group(1))
                                    has_data = row_count > 0
                            elif "numFiles" in desc_str:
                                # If there are files, there's likely data
                                has_data = True

                        # Check for location (indicates data exists)
                        if "Location" in desc_str and "s3a://" in desc_str:
                            has_data = True

                    # Report findings
                    if row_count is not None:
                        print(f"📊 {table_name}: {row_count:,} rows (from metadata)")
                    elif has_data:
                        print(
                            f"📊 {table_name}: data exists (metadata indicates files present)"
                        )
                    else:
                        print(f"📊 {table_name}: no data indicators found in metadata")

                except Exception as stats_error:
                    print(f"📊 {table_name}: metadata scan failed ({stats_error})")

            except Exception as e:
                print(f"📊 {table_name}: error accessing table ({e})")

    except Exception as e:
        print(f"❌ Error getting row counts for namespace '{namespace}': {e}")


def main():
    # Use Iceberg configuration to connect to the same catalog as create_tables.py
    spark = create_spark_connect_session("CheckTables")

    try:
        print("=== SHOWING DATABASES ===")
        try:
            databases = spark.sql("SHOW DATABASES;")
            databases.show(truncate=False)
        except Exception as e:
            print(f"❌ Error showing databases: {e}")

        # Extract namespaces and tables from DDL files
        ddl_dir = Path("src/ddl/iceberg")
        namespaces_and_tables = extract_namespaces_and_tables_from_ddl(ddl_dir)

        print(f"\n=== EXPECTED NAMESPACES AND TABLES FROM DDL ===")
        for namespace, tables in namespaces_and_tables.items():
            print(f"{namespace}: {tables}")

        # Check tables in each namespace found in DDL files
        for namespace, expected_tables in namespaces_and_tables.items():
            check_namespace_tables(spark, namespace, expected_tables)

            # Get row counts for all tables in this namespace
            get_table_row_counts(spark, namespace)

        print(f"\n{'='*50}")
        print("Table check complete!")
        print(f"{'='*50}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
