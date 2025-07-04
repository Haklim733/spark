#!/usr/bin/env python3
"""
DDL Executor for Iceberg and PostgreSQL Tables
Executes SQL DDL statements to create tables in both Iceberg and PostgreSQL
Supports separate directory structure for different database types
"""

from pathlib import Path
import os
import psycopg2
from src.utils.session import (
    create_spark_session,
    SparkVersion,
    IcebergConfig,
    S3FileSystemConfig,
)


def execute_iceberg_ddl(ddl_file_path, spark):
    """Execute DDL from a SQL file using Spark SQL for Iceberg tables"""
    print(f"Executing Iceberg DDL from: {ddl_file_path}")

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
        print(f"✅ Successfully executed Iceberg SQL statement")
        return True
    except Exception as e:
        print(f"❌ Error executing Iceberg SQL: {e}")
        return False


def execute_postgres_ddl(ddl_file_path, pg_conn):
    """Execute DDL from a SQL file using PostgreSQL connection"""
    print(f"Executing PostgreSQL DDL from: {ddl_file_path}")

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
        with pg_conn.cursor() as cursor:
            cursor.execute(sql_statement)
            pg_conn.commit()
        print(f"✅ Successfully executed PostgreSQL SQL statement")
        return True
    except Exception as e:
        print(f"❌ Error executing PostgreSQL SQL: {e}")
        pg_conn.rollback()
        return False


def create_iceberg_namespace(spark, namespace):
    """Create a namespace if it doesn't exist in Iceberg"""
    print(f"Creating Iceberg namespace '{namespace}'...")
    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
        print(f"✅ Successfully created Iceberg namespace '{namespace}'")
        return True
    except Exception as e:
        print(f"❌ Error creating Iceberg namespace '{namespace}': {e}")
        return False


def extract_iceberg_namespaces_from_ddl(iceberg_ddl_dir):
    """Extract all namespaces referenced in Iceberg DDL files"""
    namespaces = set()

    for ddl_file in iceberg_ddl_dir.glob("*.sql"):
        try:
            with open(ddl_file, "r") as f:
                content = f.read()

            print(f"DEBUG: Processing Iceberg file: {ddl_file}")
            print(f"DEBUG: File content preview: {content[:200]}...")

            # Look for CREATE TABLE statements with namespace.table format
            import re

            # Handle both CREATE TABLE IF NOT EXISTS and CREATE OR REPLACE TABLE
            matches = re.findall(
                r"CREATE (?:TABLE IF NOT EXISTS|OR REPLACE TABLE) (\w+)\.", content
            )
            print(f"DEBUG: Found Iceberg matches: {matches}")
            namespaces.update(matches)

        except Exception as e:
            print(f"⚠️  Error reading {ddl_file}: {e}")

    print(f"DEBUG: Final Iceberg namespaces found: {list(namespaces)}")
    return list(namespaces)


def get_postgres_connection():
    """Get PostgreSQL connection using environment variables"""
    try:
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=os.getenv("POSTGRES_PORT", "5432"),
            database=os.getenv("POSTGRES_DB", "iceberg"),
            user=os.getenv("POSTGRES_USER", "admin"),
            password=os.getenv("POSTGRES_PASSWORD", "password"),
        )
        return conn
    except Exception as e:
        print(f"❌ Error connecting to PostgreSQL: {e}")
        return None


def main():
    """Main function to execute DDL statements for both Iceberg and PostgreSQL"""
    app_name = Path(__file__).stem

    # Initialize Spark session for Iceberg
    spark = create_spark_session(
        spark_version=SparkVersion.SPARK_CONNECT_3_5,
        app_name=app_name,
    )

    # DDL directories
    ddl_dir = Path("src/ddl")
    iceberg_ddl_dir = ddl_dir / "iceberg"
    postgres_ddl_dir = ddl_dir / "postgres"

    if not ddl_dir.exists():
        print(f"DDL directory not found: {ddl_dir}")
        return

    # Process Iceberg DDL files
    if iceberg_ddl_dir.exists():
        print(f"\n{'='*50}")
        print("Processing Iceberg DDL files...")
        print(f"{'='*50}")

        print("Detecting required Iceberg namespaces...")
        required_namespaces = extract_iceberg_namespaces_from_ddl(iceberg_ddl_dir)

        if required_namespaces:
            print(f"Found Iceberg namespaces: {required_namespaces}")
            print("Creating Iceberg namespaces...")

            for namespace in required_namespaces:
                namespace_success = create_iceberg_namespace(spark, namespace)
                if not namespace_success:
                    print(
                        f"⚠️  Failed to create Iceberg namespace '{namespace}'. Continuing with other namespaces..."
                    )
        else:
            print("No Iceberg namespaces found in DDL files")

        # Execute all Iceberg DDL files
        for ddl_file in iceberg_ddl_dir.glob("*.sql"):
            print(f"\n{'='*50}")
            print(f"Processing Iceberg: {ddl_file}")
            print(f"{'='*50}")

            try:
                success = execute_iceberg_ddl(ddl_file, spark)
                if success:
                    print(f"✅ Successfully executed Iceberg: {ddl_file}")
                else:
                    print(f"❌ Failed to execute Iceberg: {ddl_file}")
            except Exception as e:
                print(f"❌ Error executing Iceberg {ddl_file}: {e}")
    else:
        print(f"Iceberg DDL directory not found: {iceberg_ddl_dir}")

    # Process PostgreSQL DDL files
    if postgres_ddl_dir.exists():
        print(f"\n{'='*50}")
        print("Processing PostgreSQL DDL files...")
        print(f"{'='*50}")

        # Get PostgreSQL connection
        pg_conn = get_postgres_connection()
        if pg_conn is None:
            print("❌ Cannot process PostgreSQL DDL files - no connection available")
        else:
            try:
                # Execute all PostgreSQL DDL files
                for ddl_file in postgres_ddl_dir.glob("*.sql"):
                    print(f"\n{'='*50}")
                    print(f"Processing PostgreSQL: {ddl_file}")
                    print(f"{'='*50}")

                    try:
                        success = execute_postgres_ddl(ddl_file, pg_conn)
                        if success:
                            print(f"✅ Successfully executed PostgreSQL: {ddl_file}")
                        else:
                            print(f"❌ Failed to execute PostgreSQL: {ddl_file}")
                    except Exception as e:
                        print(f"❌ Error executing PostgreSQL {ddl_file}: {e}")

            finally:
                pg_conn.close()
                print("✅ PostgreSQL connection closed")
    else:
        print(f"PostgreSQL DDL directory not found: {postgres_ddl_dir}")

    print(f"\n{'='*50}")
    print("Table creation complete!")
    print(f"{'='*50}")

    spark.stop()


if __name__ == "__main__":
    main()
