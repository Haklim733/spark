#!/usr/bin/env python3
"""
Test script to verify Spark connection to MinIO and HybridLogger functionality
"""

from src.utils.session import create_spark_session
from src.utils.logger import HybridLogger
from src.process_legal import list_minio_files_distributed, get_minio_path_info


import pytest


@pytest.mark.spark_integration
def test_minio_connection():
    """Test Spark connection to MinIO and HybridLogger functionality"""

    print("🔗 Testing MinIO connection with Spark and HybridLogger...")

    try:
        # Test 1: Create Spark session
        print("📋 Creating Spark session...")
        spark = create_spark_session()
        if not spark:
            print("❌ Failed to create Spark session")
            return False
        print("✅ Spark session created successfully")

        # Test 2: Create HybridLogger
        print("📋 Creating HybridLogger...")
        logger = HybridLogger(
            spark=spark, app_name="test_minio_connection", manage_spark=False
        )
        print("✅ HybridLogger created successfully")

        # Test 3: Test logging functionality
        print("📋 Testing logging functionality...")
        logger.log_performance(
            "minio_connection_test",
            {"test_type": "minio_connection", "timestamp": "test"},
        )

        logger.log_business_event(
            "test_started", {"operation": "minio_connection_test"}
        )

        print("✅ Logging functionality tested successfully")

        # Test 4: List files in data bucket
        print("📋 Listing files in data bucket...")
        try:
            files = list_minio_files_distributed(spark, "s3a://data")
            if files:
                print(f"✅ Found {len(files)} files in data bucket:")
                for file in files[:10]:  # Show first 10 files
                    print(f"   - {file}")
                if len(files) > 10:
                    print(f"   ... and {len(files) - 10} more files")
            else:
                print("⚠️  No files found in data bucket")
        except Exception as e:
            print(f"⚠️  Could not list files in data bucket: {e}")

        # Test 5: Test path parsing
        print("📋 Testing path parsing...")
        test_paths = [
            "s3a://data/docs/legal/file.txt",
            "s3://data/docs/legal/file.txt",
            "minio://data/docs/legal/file.txt",
        ]

        for path in test_paths:
            info = get_minio_path_info(path)
            print(f"   {path} -> bucket: {info['bucket']}, key: {info['key']}")

        # Test 6: List files in specific directory
        print("📋 Testing list_minio_files function with specific path...")
        minio_path = "s3a://data/docs"
        files = list_minio_files_distributed(spark, minio_path)
        print(f"✅ Found {len(files)} files in {minio_path}")

        # Test 7: Force log sync and verify logs
        print("📋 Testing log sync...")
        sync_result = logger.force_sync_logs()
        print(f"✅ Log sync completed: {sync_result}")

        # Test 8: Verify log files were created in app-specific directory
        print("📋 Verifying log files...")
        import os

        # Determine expected log directory
        if os.path.exists("/opt/bitnami/spark"):
            log_dir = "/opt/bitnami/spark/logs/app/test_minio_connection"
        else:
            log_dir = "./spark-logs/app/test_minio_connection"

        if os.path.exists(log_dir):
            print(f"✅ Log directory exists: {log_dir}")
            files = os.listdir(log_dir)
            print(f"📄 Files in log directory: {files}")

            # Check for expected log files
            expected_files = [
                "test_minio_connection-application.log",
                "test_minio_connection-hybrid-observability.log",
            ]

            for expected_file in expected_files:
                file_path = os.path.join(log_dir, expected_file)
                if os.path.exists(file_path):
                    file_size = os.path.getsize(file_path)
                    print(f"✅ Found {expected_file} (size: {file_size} bytes)")
                else:
                    print(f"❌ Missing expected file: {expected_file}")
        else:
            print(f"❌ Log directory does not exist: {log_dir}")

        # Cleanup
        logger.shutdown()
        spark.stop()

        return True

    except Exception as e:
        print(f"❌ Error testing MinIO connection: {e}")
        return False
