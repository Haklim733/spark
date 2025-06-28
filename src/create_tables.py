#!/usr/bin/env python3
"""
DDL Executor for Iceberg Tables using Spark SQL
Executes SQL DDL statements to create Iceberg tables
"""

from pathlib import Path
from utils.session import create_spark_session, SparkVersion


def execute_ddl_file(ddl_file_path, spark):
    """Execute DDL from a SQL file using Spark SQL"""
    print(f"Executing DDL from: {ddl_file_path}")

    # Read SQL file
    with open(ddl_file_path, "r") as f:
        sql_content = f.read()

    # Remove comments and clean up SQL
    lines = []
    for line in sql_content.split("\n"):
        line = line.strip()
        if line and not line.startswith("--"):
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


def extract_tables_from_ddl(ddl_dir):
    """Extract all table names referenced in DDL files"""
    tables = []

    for ddl_file in ddl_dir.glob("*.sql"):
        try:
            with open(ddl_file, "r") as f:
                content = f.read()

            # Look for CREATE TABLE statements with namespace.table format
            import re

            # Handle both CREATE TABLE IF NOT EXISTS and CREATE OR REPLACE TABLE
            matches = re.findall(
                r"CREATE (?:TABLE IF NOT EXISTS|OR REPLACE TABLE) (\w+)\.(\w+)", content
            )

            for namespace, table_name in matches:
                tables.append((namespace, table_name))

        except Exception as e:
            print(f"⚠️  Error reading {ddl_file}: {e}")

    return tables


def create_error_table(spark, namespace, table_name):
    """Create an error table for the given table"""
    error_table_name = f"{table_name}_error"
    print(f"Creating error table: {namespace}.{error_table_name}")

    # Generic error table schema that can handle most cases
    error_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {namespace}.{error_table_name} (
        original_record_id STRING,
        original_data MAP<STRING, STRING>,  -- Changed from STRING to MAP for better queryability
        error_message STRING,
        error_type STRING,
        quarantined_at TIMESTAMP,
        source_table STRING,
        processing_step STRING,
        error_sequence INT,  -- For multiple errors per record (1, 2, 3, etc.)
        metadata MAP<STRING, STRING>
    )
    USING iceberg
    PARTITIONED BY (error_type, month(quarantined_at))
    TBLPROPERTIES (
        'write.format.default' = 'parquet',
        'write.parquet.compression-codec' = 'zstd',
        'write.merge.isolation-level' = 'snapshot',
        'comment' = 'Error/quarantine table for {namespace}.{table_name} - supports multiple errors per record'
    )
    """

    try:
        spark.sql(error_table_sql)
        print(f"✅ Successfully created error table: {namespace}.{error_table_name}")
        return True
    except Exception as e:
        print(f"❌ Error creating error table {namespace}.{error_table_name}: {e}")
        return False


def create_specific_error_table_for_legal_documents(spark):
    """Create a specific error table for legal documents with detailed schema"""
    print("Creating specific error table for legal documents...")

    legal_error_sql = """
    CREATE TABLE IF NOT EXISTS legal.documents_error (
        original_document_id STRING,
        document_type STRING,
        raw_text STRING,
        generation_date TIMESTAMP,
        file_path STRING,
        document_length INT,
        word_count INT,
        language STRING,
        metadata MAP<STRING, STRING>,
        error_message STRING,
        error_type STRING,
        quarantined_at TIMESTAMP,
        error_sequence INT,  -- For multiple errors per document (1, 2, 3, etc.)
        field_name STRING,   -- Which specific field caused the error
        field_value STRING   -- The problematic value
    )
    USING iceberg
    PARTITIONED BY (error_type, month(quarantined_at))
    TBLPROPERTIES (
        'write.format.default' = 'parquet',
        'write.parquet.compression-codec' = 'zstd',
        'write.merge.isolation-level' = 'snapshot',
        'comment' = 'Error/quarantine table for legal documents with detailed schema - supports multiple errors per document'
    )
    """

    try:
        spark.sql(legal_error_sql)
        print("✅ Successfully created legal documents error table")
        return True
    except Exception as e:
        print(f"❌ Error creating legal documents error table: {e}")
        return False


def main():
    """Main function to execute DDL statements"""
    app_name = Path(__file__).stem

    # Create Spark session with Iceberg
    spark = create_spark_session(
        spark_version=SparkVersion.SPARK_3_5,
        app_name=app_name,
    )

    # DDL files directory
    ddl_dir = Path("src/ddl")

    if not ddl_dir.exists():
        print(f"DDL directory not found: {ddl_dir}")
        return

    # Extract and create all required namespaces
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

    # Create error tables for all tables
    print(f"\n{'='*50}")
    print("CREATING ERROR TABLES")
    print(f"{'='*50}")

    tables = extract_tables_from_ddl(ddl_dir)

    if tables:
        print(f"Found tables: {tables}")
        print("Creating error tables...")

        for namespace, table_name in tables:
            # Create specific error table for legal documents
            if namespace == "legal" and table_name == "documents":
                create_specific_error_table_for_legal_documents(spark)
            else:
                # Create generic error table for other tables
                create_error_table(spark, namespace, table_name)
    else:
        print("No tables found in DDL files")

    print(f"\n{'='*50}")
    print("DDL execution complete!")
    print(f"{'='*50}")

    spark.stop()


if __name__ == "__main__":
    main()
