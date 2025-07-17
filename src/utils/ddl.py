"""
DDL Loader utility for loading and customizing DDL files
"""

import os
import re
from pathlib import Path
from typing import Optional
import psycopg2
from pyspark.sql import SparkSession


class DDLLoader:
    """Load and customize DDL files for testing and execution"""

    def __init__(self, ddl_base_path: str = "src/ddl/iceberg"):
        self.ddl_path = Path(ddl_base_path)

    def load_ddl(
        self,
        ddl_file: str,
    ) -> str:
        """
        Load DDL file

        Args:
            ddl_file: Path to the DDL file (e.g., 'pvdata.sql')

        Returns:
            DDL string
        """

        ddl_file_path = self.ddl_path / Path(ddl_file)

        if not ddl_file_path.exists():
            raise FileNotFoundError(f"DDL file not found: {ddl_file_path}")

        # Read DDL content
        with open(ddl_file_path, "r") as f:
            ddl_content = f.read()

        return ddl_content

    def modify_ddl(
        self,
        ddl_content: str,
        table_name: str,
        new_table_name: Optional[str] = None,
    ) -> str:
        """
        Create table using DDL file with optional table name replacement

        Args:
            spark: Spark session
            ddl_file: DDL file name (e.g., 'pvdata.sql')
            table_name: Original table name to replace (e.g., 'energy.pv_data')
            new_table_name: New table name to use (e.g., 'test.pv_data.branch_test')

        Returns:
            Table name created
        """

        # If new table name provided, replace all occurrences
        if new_table_name:
            # Replace CREATE TABLE statements
            ddl_content = ddl_content.replace(table_name, new_table_name)

        return ddl_content

    def extract_table_names_from_ddl(self, iceberg_ddl_dir: Path) -> list[str]:
        """Extract all table names referenced in Iceberg DDL files"""
        table_names = set()

        for ddl_file in iceberg_ddl_dir.glob("*.sql"):
            try:
                with open(ddl_file, "r") as f:
                    content = f.read()

                matches = re.findall(
                    r"CREATE (?:TABLE IF NOT EXISTS|OR REPLACE TABLE) ([^.]+\.[^.\s]+)",
                    content,
                    re.IGNORECASE,
                )
                table_names.update(matches)

            except Exception as e:
                print(f"⚠️  Error reading {ddl_file}: {e}")

        return list(table_names)

    def _clean_sql_statements(self, sql_content: str) -> list[str]:
        """Clean and split SQL content into individual statements"""
        statements = []

        for statement in sql_content.split(";"):
            # Remove comments and empty lines
            lines = []
            for line in statement.split("\n"):
                line = line.strip()
                if line and not line.startswith("--"):
                    # Remove inline comments
                    if "--" in line:
                        line = line.split("--")[0].strip()
                    if line:
                        lines.append(line)

            # Join lines and clean up
            if lines:
                sql_statement = " ".join(lines).strip()
                if sql_statement:
                    statements.append(sql_statement)

        return statements

    def execute_iceberg_ddl(self, ddl_content: str, spark: SparkSession) -> bool:
        """Execute DDL from a SQL file using Spark SQL for Iceberg tables"""
        print(f"📝 Executing Iceberg DDL")

        # Validate session upfront
        try:
            spark.sql("SELECT 1").collect()
        except Exception as e:
            raise RuntimeError(f"Spark session is not active: {e}")

        # Clean and split statements
        statements = self._clean_sql_statements(ddl_content)

        if not statements:
            print("⚠️  No DDL statements found")
            return True

        # Execute each statement with validation
        success_count = 0
        total_statements = len(statements)

        for i, sql_statement in enumerate(statements, 1):
            print(
                f"📝 Executing statement {i}/{total_statements}: {sql_statement[:100]}..."
            )

            try:
                # Validate session before each statement
                spark.sql("SELECT 1").collect()

                # Execute the DDL statement
                spark.sql(sql_statement)

                print(f"✅ Successfully executed statement {i}")
                success_count += 1

            except Exception as e:
                print(f"❌ Error executing statement {i}: {e}")
                print(f"   Statement: {sql_statement}")
                # Log but continue with remaining statements
                continue

        print(f"✅ Successfully executed {success_count}/{total_statements} statements")
        return success_count == total_statements

    def execute_postgres_ddl(self, ddl_file_path: Path, pg_conn) -> bool:
        """Execute DDL from a SQL file using PostgreSQL connection"""
        print(f"📝 Executing PostgreSQL DDL from: {ddl_file_path}")

        # Update DDL path for PostgreSQL files
        original_path = self.ddl_path
        self.ddl_path = ddl_file_path.parent

        try:
            # Load DDL content
            sql_content = self.load_ddl(ddl_file_path.name)
        except FileNotFoundError as e:
            print(f"❌ DDL file not found: {e}")
            return False
        finally:
            # Restore original path
            self.ddl_path = original_path

        # Clean and split statements
        statements = self._clean_sql_statements(sql_content)

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

        print(f"✅ Successfully executed {success_count}/{total_statements} statements")
        return success_count == total_statements

    def get_postgres_connection(self):
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
