#!/usr/bin/env python3
"""
DDL Executor for Iceberg Tables using Spark SQL
Executes SQL DDL statements to create Iceberg tables
Supports both traditional DDL files and SchemaManager-based table creation
"""

from pathlib import Path
from utils.session import (
    create_spark_session,
    SparkVersion,
    IcebergConfig,
    S3FileSystemConfig,
)


def execute_ddl_file(ddl_file_path, spark):
    """Execute DDL from a SQL file using Spark SQL"""
    print(f"Executing DDL from: {ddl_file_path}")

    # Read SQL file
    with open(ddl_file_path, "r") as f:
        sql_content = f.read()

    # Process SQL content to handle comments properly
    lines = []
    for line in sql_content.split("\n"):
        line = line.strip()
        if line and not line.startswith("--"):
            # Remove inline comments (everything after --)
            if "--" in line:
                line = line.split("--")[0].strip()
            if line:  # Only add non-empty lines
                lines.append(line)

    sql_statement = " ".join(lines)

    # Execute the SQL statement
    try:
        spark.sql(sql_statement)
        print(f"✅ Successfully executed SQL statement")
        return True
    except Exception as e:
        print(f"❌ Error executing SQL: {e}")
        return False


def create_namespace(spark, namespace):
    """Create a namespace if it doesn't exist"""
    print(f"Creating namespace '{namespace}'...")
    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
        print(f"✅ Successfully created namespace '{namespace}'")
        return True
    except Exception as e:
        print(f"❌ Error creating namespace '{namespace}': {e}")
        return False


def extract_namespaces_from_ddl(ddl_dir):
    """Extract all namespaces referenced in DDL files"""
    namespaces = set()

    for ddl_file in ddl_dir.glob("*.sql"):
        try:
            with open(ddl_file, "r") as f:
                content = f.read()

            print(f"DEBUG: Processing file: {ddl_file}")
            print(f"DEBUG: File content preview: {content[:200]}...")

            # Look for CREATE TABLE statements with namespace.table format
            import re

            # Handle both CREATE TABLE IF NOT EXISTS and CREATE OR REPLACE TABLE
            matches = re.findall(
                r"CREATE (?:TABLE IF NOT EXISTS|OR REPLACE TABLE) (\w+)\.", content
            )
            print(f"DEBUG: Found matches: {matches}")
            namespaces.update(matches)

        except Exception as e:
            print(f"⚠️  Error reading {ddl_file}: {e}")

    print(f"DEBUG: Final namespaces found: {list(namespaces)}")
    return list(namespaces)


def main():
    """Main function to execute DDL statements and create tables from schemas"""
    app_name = Path(__file__).stem

    spark = create_spark_session(
        spark_version=SparkVersion.SPARK_CONNECT_3_5,
        app_name=app_name,
    )

    # DDL files directory
    ddl_dir = Path("src/ddl")

    if not ddl_dir.exists():
        print(f"DDL directory not found: {ddl_dir}")
        return

    # Extract and create all required namespaces
    print(f"\n{'='*50}")
    print("Processing traditional DDL files...")
    print(f"{'='*50}")

    print("Detecting required namespaces...")
    required_namespaces = extract_namespaces_from_ddl(ddl_dir)

    if required_namespaces:
        print(f"Found namespaces: {required_namespaces}")
        print("Creating namespaces...")

        for namespace in required_namespaces:
            namespace_success = create_namespace(spark, namespace)
            if not namespace_success:
                print(
                    f"⚠️  Failed to create namespace '{namespace}'. Continuing with other namespaces..."
                )
    else:
        print("No namespaces found in DDL files")

    # Execute all DDL files
    for ddl_file in ddl_dir.glob("*.sql"):
        print(f"\n{'='*50}")
        print(f"Processing: {ddl_file}")
        print(f"{'='*50}")

        try:
            success = execute_ddl_file(ddl_file, spark)
            if success:
                print(f"✅ Successfully executed: {ddl_file}")
            else:
                print(f"❌ Failed to execute: {ddl_file}")
        except Exception as e:
            print(f"❌ Error executing {ddl_file}: {e}")

    print(f"\n{'='*50}")
    print("Table creation complete!")
    print(f"{'='*50}")

    spark.stop()


if __name__ == "__main__":
    main()
