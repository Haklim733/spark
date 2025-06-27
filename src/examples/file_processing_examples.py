#!/usr/bin/env python3
"""
File Processing Examples for Large Files

This script demonstrates two approaches for handling large files in Spark:
1. Original approach: Simple but memory-intensive for small datasets
2. SQL Parallel approach: SQL-based parallelization using UDFs for large datasets
"""

import os
import sys
from pathlib import Path

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent))

from utils.session import create_spark_session, SparkVersion, IcebergConfig
from insert_tables import (
    insert_legal_documents_from_files,
    insert_legal_documents_from_files_sql_parallel,
    choose_file_processing_approach,
)


def demonstrate_file_processing_approaches():
    """Demonstrate different file processing approaches"""

    # Create Spark session
    iceberg_config = IcebergConfig()
    spark = create_spark_session(
        spark_version=SparkVersion.SPARK_3_5,
        app_name="file_processing_examples",
        iceberg_config=iceberg_config,
    )

    docs_dir = "data/docs/legal"

    print("=" * 60)
    print("FILE PROCESSING APPROACHES DEMONSTRATION")
    print("=" * 60)

    if not os.path.exists(docs_dir):
        print(f"❌ Documents directory not found: {docs_dir}")
        print("Please run generate_legal_docs.py first to create documents")
        return

    doc_files = list(Path(docs_dir).glob("*.txt"))
    if not doc_files:
        print(f"❌ No document files found in: {docs_dir}")
        return

    # Analyze files
    total_size = sum(f.stat().st_size for f in doc_files)
    avg_size_mb = (total_size / len(doc_files)) / (1024 * 1024)

    print(f"\n📊 File Analysis:")
    print(f"   - Number of files: {len(doc_files)}")
    print(f"   - Total size: {total_size / (1024*1024):.1f} MB")
    print(f"   - Average file size: {avg_size_mb:.1f} MB")
    print(
        f"   - Largest file: {max(f.stat().st_size for f in doc_files) / (1024*1024):.1f} MB"
    )

    # Get recommended approach
    recommended = choose_file_processing_approach(len(doc_files), avg_size_mb)

    print(f"\n🎯 Recommended approach: {recommended}")

    # Demonstrate each approach
    approaches = {
        "original": {
            "name": "Original Approach",
            "description": "Simple file reading - good for small datasets (<100 files)",
            "function": insert_legal_documents_from_files,
            "params": {},
        },
        "sql_parallel": {
            "name": "SQL Parallel Approach",
            "description": "SQL-based parallelization with UDFs - best for large datasets (>100 files)",
            "function": insert_legal_documents_from_files_sql_parallel,
            "params": {"num_partitions": 4},
        },
    }

    print(f"\n🔄 Demonstrating approaches:")

    for approach_key, approach_info in approaches.items():
        print(f"\n{'='*40}")
        print(f"Testing: {approach_info['name']}")
        print(f"Description: {approach_info['description']}")
        print(f"{'='*40}")

        try:
            # Clear table first
            spark.sql("TRUNCATE TABLE legal.documents")

            # Run the approach
            result = approach_info["function"](spark, **approach_info["params"])

            if result:
                print(f"✅ {approach_info['name']} completed successfully")

                # Show results
                count = spark.sql(
                    "SELECT COUNT(*) as count FROM legal.documents"
                ).collect()[0]["count"]
                print(f"   - Documents inserted: {count}")

                # Show sample data
                print(f"   - Sample document:")
                spark.sql(
                    "SELECT document_id, document_type, document_length FROM legal.documents LIMIT 1"
                ).show(truncate=False)

            else:
                print(f"❌ {approach_info['name']} failed")

        except Exception as e:
            print(f"❌ Error with {approach_info['name']}: {e}")

    print(f"\n{'='*60}")
    print("RECOMMENDATIONS")
    print(f"{'='*60}")
    print(f"✅ For your dataset ({len(doc_files)} files, {avg_size_mb:.1f}MB avg):")
    print(f"   - Primary: Use '{recommended}' approach")

    if recommended == "original":
        print(f"   - Alternative: Use 'sql_parallel' if files get more numerous")
    elif recommended == "sql_parallel":
        print(f"   - Alternative: Use 'original' only for very small datasets")

    print(f"\n💡 Performance Tips:")
    print(f"   - Monitor memory usage during processing")
    print(f"   - Adjust partitions based on available resources")
    print(f"   - Use Spark UI to monitor job progress")
    print(f"   - SQL parallel leverages Spark's SQL optimizer")

    spark.stop()


def create_test_files():
    """Create test files of different sizes for demonstration"""
    docs_dir = Path("data/docs/legal")
    docs_dir.mkdir(parents=True, exist_ok=True)

    print("Creating test files of different sizes...")

    # Small files (1-10KB) - for original approach
    for i in range(10):
        content = f"This is a small legal document {i}.\n" * 50
        with open(docs_dir / f"legal_doc_{i:04d}_contract.txt", "w") as f:
            f.write(content)

    # Medium files (100KB-1MB) - for SQL parallel approach
    for i in range(5):
        content = f"This is a medium legal document {i}.\n" * 5000
        with open(docs_dir / f"legal_doc_{i+10:04d}_memo.txt", "w") as f:
            f.write(content)

    # Large files (1-5MB) - for SQL parallel approach
    for i in range(2):
        content = f"This is a large legal document {i}.\n" * 25000
        with open(docs_dir / f"legal_doc_{i+15:04d}_filing.txt", "w") as f:
            f.write(content)

    print(f"✅ Created test files in {docs_dir}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="File Processing Examples")
    parser.add_argument(
        "--create-test-files",
        action="store_true",
        help="Create test files of different sizes",
    )
    parser.add_argument(
        "--demo",
        action="store_true",
        help="Demonstrate different processing approaches",
    )

    args = parser.parse_args()

    if args.create_test_files:
        create_test_files()

    if args.demo:
        demonstrate_file_processing_approaches()

    if not args.create_test_files and not args.demo:
        print("Please specify --create-test-files or --demo")
        print("Example: python file_processing_examples.py --create-test-files --demo")
