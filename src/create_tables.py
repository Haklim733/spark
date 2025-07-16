#!/usr/bin/env python3
"""
DDL Executor for Iceberg and PostgreSQL Tables
Simplified script that uses DDLLoader for all DDL operations
"""

from pathlib import Path
from src.utils.session import create_spark_session, SparkVersion
from src.utils.ddl import DDLLoader


def main():
    """Main function to execute DDL statements for both Iceberg and PostgreSQL"""
    app_name = Path(__file__).stem

    # Initialize Spark session for Iceberg
    spark = create_spark_session(
        spark_version=SparkVersion.SPARK_CONNECT_3_5,
        app_name=app_name,
        catalog="iceberg",
    )

    try:
        # Initialize DDL loader
        ddl_loader = DDLLoader()

        # DDL directories
        ddl_dir = Path("src/ddl")
        iceberg_ddl_dir = ddl_dir / "iceberg"
        postgres_ddl_dir = ddl_dir / "postgres"

        if not ddl_dir.exists():
            print(f"❌ DDL directory not found: {ddl_dir}")
            return

        # Process Iceberg DDL files
        if iceberg_ddl_dir.exists():
            print(f"\n{'='*50}")
            print("🗃️  Processing Iceberg DDL files...")
            print(f"{'='*50}")

            # Extract table names for reference
            print("🔍 Detecting Iceberg tables...")
            table_names = ddl_loader.extract_table_names_from_ddl(iceberg_ddl_dir)
            print(f"📋 Found tables to process: {table_names}")

            # Execute all Iceberg DDL files with enhanced table name handling
            iceberg_success_count = 0
            iceberg_files = list(iceberg_ddl_dir.glob("*.sql"))

            for ddl_file in iceberg_files:
                print(f"\n🔧 Processing: {ddl_file.name}")

                try:
                    content = ddl_loader.load_ddl(ddl_file.name)
                    success = ddl_loader.execute_iceberg_ddl(content, spark)

                    if success:
                        print(f"✅ Successfully executed: {ddl_file.name}")
                        iceberg_success_count += 1
                    else:
                        print(f"❌ Failed to execute: {ddl_file.name}")

                except Exception as e:
                    print(f"❌ Error executing {ddl_file.name}: {e}")

            print(
                f"\n📊 Iceberg Summary: {iceberg_success_count}/{len(iceberg_files)} files executed successfully"
            )

        else:
            print(f"❌ Iceberg DDL directory not found: {iceberg_ddl_dir}")

        # Process PostgreSQL DDL files
        if postgres_ddl_dir.exists():
            print(f"\n{'='*50}")
            print("🐘 Processing PostgreSQL DDL files...")
            print(f"{'='*50}")

            # Get PostgreSQL connection
            pg_conn = ddl_loader.get_postgres_connection()
            if pg_conn is None:
                print(
                    "❌ Cannot process PostgreSQL DDL files - no connection available"
                )
            else:
                try:
                    postgres_success_count = 0
                    postgres_files = list(postgres_ddl_dir.glob("*.sql"))

                    for ddl_file in postgres_files:
                        print(f"\n🔧 Processing: {ddl_file.name}")

                        try:
                            success = ddl_loader.execute_postgres_ddl(ddl_file, pg_conn)
                            if success:
                                print(f"✅ Successfully executed: {ddl_file.name}")
                                postgres_success_count += 1
                            else:
                                print(f"❌ Failed to execute: {ddl_file.name}")
                        except Exception as e:
                            print(f"❌ Error executing {ddl_file.name}: {e}")

                    print(
                        f"\n📊 PostgreSQL Summary: {postgres_success_count}/{len(postgres_files)} files executed successfully"
                    )

                finally:
                    pg_conn.close()
                    print("✅ PostgreSQL connection closed")
        else:
            print(f"❌ PostgreSQL DDL directory not found: {postgres_ddl_dir}")

        print(f"\n{'='*60}")
        print("🎉 Table creation process complete!")
        print(f"{'='*60}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
