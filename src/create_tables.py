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
)


def extract_table_names_from_ddl(iceberg_ddl_dir):
    """Extract all table names referenced in Iceberg DDL files"""
    table_names = set()

    for ddl_file in iceberg_ddl_dir.glob("*.sql"):
        try:
            with open(ddl_file, "r") as f:
                content = f.read()

            print(f"DEBUG: Processing Iceberg file: {ddl_file}")
            print(f"DEBUG: File content preview: {content[:200]}...")

            # Look for CREATE TABLE statements with namespace.table format
            import re

            # Handle both CREATE TABLE IF NOT EXISTS and CREATE OR REPLACE TABLE
            # This regex captures the full table name (namespace.table)
            matches = re.findall(
                r"CREATE (?:TABLE IF NOT EXISTS|OR REPLACE TABLE) ([^.]+(?:\.[^.]+)*\.\w+)\s*\(",
                content,
            )
            print(f"DEBUG: Found table matches: {matches}")
            table_names.update(matches)

        except Exception as e:
            print(f"⚠️  Error reading {ddl_file}: {e}")

    print(f"DEBUG: Final table names found: {list(table_names)}")
    return list(table_names)


def execute_iceberg_ddl(ddl_file_path, spark):
    """Execute DDL from a SQL file using Spark SQL for Iceberg tables"""
    print(f"Executing Iceberg DDL from: {ddl_file_path}")

    # Read SQL file
    with open(ddl_file_path, "r") as f:
        sql_content = f.read()

    # Split by semicolon to handle multiple commands
    statements = []
    for statement in sql_content.split(";"):
        # Process SQL content to handle comments properly
        lines = []
        for line in statement.split("\n"):
            line = line.strip()
            if line and not line.startswith("--"):
                # Remove inline comments (everything after --)
                if "--" in line:
                    line = line.split("--")[0].strip()
                if line:  # Only add non-empty lines
                    lines.append(line)

        # Join lines and clean up
        if lines:
            sql_statement = " ".join(lines).strip()
            if sql_statement:  # Only add non-empty statements
                statements.append(sql_statement)

    # Execute each statement
    success_count = 0
    total_statements = len(statements)

    for i, sql_statement in enumerate(statements, 1):
        print(
            f"📝 Executing statement {i}/{total_statements}: {sql_statement[:100]}..."
        )

        try:
            spark.sql(sql_statement)
            print(f"✅ Successfully executed statement {i}")
            success_count += 1
        except Exception as e:
            print(f"❌ Error executing statement {i}: {e}")
            print(f"   Statement: {sql_statement}")

    print(
        f"✅ Successfully executed {success_count}/{total_statements} statements from {ddl_file_path}"
    )
    return success_count == total_statements


def execute_postgres_ddl(ddl_file_path, pg_conn):
    """Execute DDL from a SQL file using PostgreSQL connection"""
    print(f"Executing PostgreSQL DDL from: {ddl_file_path}")

    # Read SQL file
    with open(ddl_file_path, "r") as f:
        sql_content = f.read()

    # Split by semicolon to handle multiple commands
    statements = []
    for statement in sql_content.split(";"):
        # Process SQL content to handle comments properly
        lines = []
        for line in statement.split("\n"):
            line = line.strip()
            if line and not line.startswith("--"):
                # Remove inline comments (everything after --)
                if "--" in line:
                    line = line.split("--")[0].strip()
                if line:  # Only add non-empty lines
                    lines.append(line)

        # Join lines and clean up
        if lines:
            sql_statement = " ".join(lines).strip()
            if sql_statement:  # Only add non-empty statements
                statements.append(sql_statement)

    # Execute each statement
    success_count = 0
    total_statements = len(statements)

    for i, sql_statement in enumerate(statements, 1):
        print(
            f"📝 Executing statement {i}/{total_statements}: {sql_statement[:100]}..."
        )

        try:
            with pg_conn.cursor() as cursor:
                cursor.execute(sql_statement)
                pg_conn.commit()
            print(f"✅ Successfully executed statement {i}")
            success_count += 1
        except Exception as e:
            print(f"❌ Error executing statement {i}: {e}")
            print(f"   Statement: {sql_statement}")
            pg_conn.rollback()

    print(
        f"✅ Successfully executed {success_count}/{total_statements} statements from {ddl_file_path}"
    )
    return success_count == total_statements


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

        # Extract table names for branch cleanup
        print("Detecting Iceberg tables for branch cleanup...")
        table_names = extract_table_names_from_ddl(iceberg_ddl_dir)
        print(f"Found tables to process: {table_names}")

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
