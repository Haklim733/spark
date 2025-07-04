#!/usr/bin/env python3
"""
Local integration test for HybridLogger app-specific logging fix
"""

import os
import time
import json
import pytest
from src.utils.logger import HybridLogger
from src.utils.session import create_spark_session, SparkVersion


def test_hybrid_logger_app_specific_logging():
    """Test that HybridLogger writes to app-specific directories"""

    print("🔗 Testing HybridLogger app-specific logging...")

    app_name = "test_logging::test_hybrid_logger_local"

    try:
        # Test 1: Create Spark session
        print("📋 Creating Spark session...")
        spark = create_spark_session(
            app_name=app_name, spark_version=SparkVersion.SPARK_CONNECT_3_5
        )
        if not spark:
            print("❌ Failed to create Spark session")
            return False
        print("✅ Spark session created successfully")

        # Test 2: Create HybridLogger
        print("📋 Creating HybridLogger...")
        logger = HybridLogger(spark=spark, app_name=app_name, manage_spark=False)
        print("✅ HybridLogger created successfully")

        # Test 3: Test logging functionality
        print("📋 Testing logging operations...")

        # Log performance metrics
        logger.log_performance(
            "test_operation",
            {
                "test_metric": 123,
                "test_string": "hello world",
                "timestamp": int(time.time() * 1000),
            },
        )

        # Log business events
        logger.log_business_event(
            "test_event",
            {"event_type": "test", "data": {"key": "value"}, "user_id": "test_user"},
        )

        # Log custom metrics
        logger.log_custom_metrics(
            "test_group",
            "test_operation",
            {"custom_metric": 456, "processing_time_ms": 150},
        )

        # Log operation completion
        logger.log_operation_completion("test_group", "test_operation", 1.5, 100)

        # Log batch metrics
        logger.log_batch_metrics(1, 1000, 2.5)

        # Test 4: Force sync logs
        print("📋 Forcing log sync...")
        sync_result = logger.force_sync_logs()
        print(f"✅ Log sync completed: {sync_result}")

        # Test 5: Check if log files were created
        print("📋 Checking for log files...")

        # Determine expected log directory
        if os.path.exists("/opt/bitnami/spark"):
            log_dir = f"/opt/bitnami/spark/logs/app/{app_name}"
        else:
            log_dir = f"./spark-logs/app/{app_name}"

        print(f"📁 Expected log directory: {log_dir}")

        if os.path.exists(log_dir):
            print("✅ Log directory exists")

            # List files in the directory
            files = os.listdir(log_dir)
            print(f"📄 Files in log directory: {files}")

            # Check for specific log files
            expected_files = [
                f"{app_name}-application.log",
                f"{app_name}-hybrid-observability.log",
            ]

            log_files_found = 0
            for expected_file in expected_files:
                file_path = os.path.join(log_dir, expected_file)
                if os.path.exists(file_path):
                    file_size = os.path.getsize(file_path)
                    print(f"✅ Found {expected_file} (size: {file_size} bytes)")
                    log_files_found += 1

                    # Read a few lines to verify content
                    try:
                        with open(file_path, "r") as f:
                            lines = f.readlines()
                            print(f"   📝 File contains {len(lines)} lines")
                            if lines:
                                print(f"   📝 First line: {lines[0].strip()}")

                                # Check for structured log content
                                for line in lines[:5]:  # Check first 5 lines
                                    if any(
                                        keyword in line
                                        for keyword in [
                                            "PERFORMANCE_METRICS",
                                            "BUSINESS_EVENT",
                                            "CUSTOM_METRICS",
                                        ]
                                    ):
                                        print(
                                            f"   ✅ Found structured log: {line.strip()}"
                                        )
                    except Exception as e:
                        print(f"   ⚠️  Could not read file: {e}")
                else:
                    print(f"❌ Missing expected file: {expected_file}")

            # Verify we found at least one log file
            assert log_files_found > 0, f"No log files found in {log_dir}"
            print(f"✅ Found {log_files_found} log files")

        else:
            print(f"❌ Log directory does not exist: {log_dir}")
            assert False, f"Log directory {log_dir} was not created"

        # Test 6: Test job and batch tracking
        print("📋 Testing job and batch tracking...")

        job_id = "test_job_123"
        batch_id = "test_batch_456"

        # Start job tracking
        logger.start_job_tracking(
            job_id,
            {
                "source_path": "/test/path",
                "target_table": "test_table",
                "mode": "batch",
            },
        )

        # Start batch tracking
        logger.start_batch_tracking(
            job_id,
            batch_id,
            {"source_path": "/test/path", "target_table": "test_table"},
        )

        # Log file success
        logger.log_file_success(
            batch_id=batch_id,
            file_path="/test/file.txt",
            file_size=1024,
            records_loaded=100,
            processing_time_ms=500,
        )

        # End batch tracking
        logger.end_batch_tracking(job_id, batch_id, expected_files=1)

        # End job tracking
        logger.end_job_tracking(job_id, expected_files=1)

        print("✅ Job and batch tracking completed")

        # Test 7: Get performance summary
        print("📋 Getting performance summary...")
        summary = logger.get_performance_summary()
        print(f"✅ Performance summary: {summary}")

        # Test 8: Cleanup
        print("📋 Cleaning up...")
        logger.shutdown()
        spark.stop()

        print("✅ Test completed successfully")
        return True

    except Exception as e:
        print(f"❌ Error testing HybridLogger: {e}")
        import traceback

        traceback.print_exc()
        return False


def test_hybrid_logger_without_spark():
    """Test HybridLogger without Spark session (fallback mode)"""

    print("🔗 Testing HybridLogger without Spark session...")

    try:
        # Create HybridLogger without Spark
        logger = HybridLogger(spark=None, app_name="test_no_spark", manage_spark=False)

        # Test logging operations
        logger.log_performance("test_operation", {"metric": 123})
        logger.log_business_event("test_event", {"data": "value"})
        logger.log_custom_metrics("group", "operation", {"custom": 456})

        # Get performance summary
        summary = logger.get_performance_summary()
        print(f"✅ Performance summary: {summary}")

        # Cleanup
        logger.shutdown()

        print("✅ Test completed successfully")
        return True

    except Exception as e:
        print(f"❌ Error testing HybridLogger without Spark: {e}")
        import traceback

        traceback.print_exc()
        return False


def test_hybrid_logger_managed_spark():
    """Test HybridLogger with managed Spark session"""

    print("🔗 Testing HybridLogger with managed Spark session...")

    app_name = "test_managed_spark"

    try:
        # Create HybridLogger with managed Spark
        logger = HybridLogger(
            app_name=app_name,
            manage_spark=True,
            spark_version=SparkVersion.SPARK_CONNECT_3_5,
        )

        # Test logging operations
        logger.log_performance("managed_test", {"metric": 789})
        logger.log_business_event("managed_event", {"data": "managed"})

        # Force sync logs
        sync_result = logger.force_sync_logs()
        print(f"✅ Log sync completed: {sync_result}")

        # Check log directory
        if os.path.exists("/opt/bitnami/spark"):
            log_dir = f"/opt/bitnami/spark/logs/app/{app_name}"
        else:
            log_dir = f"./spark-logs/app/{app_name}"

        if os.path.exists(log_dir):
            files = os.listdir(log_dir)
            print(f"✅ Found log files: {files}")
        else:
            print(f"⚠️  Log directory not found: {log_dir}")

        # Cleanup
        logger.shutdown()

        print("✅ Test completed successfully")
        return True

    except Exception as e:
        print(f"❌ Error testing HybridLogger with managed Spark: {e}")
        import traceback

        traceback.print_exc()
        return False


if __name__ == "__main__":
    # Run all tests
    test_hybrid_logger_app_specific_logging()
    test_hybrid_logger_without_spark()
    test_hybrid_logger_managed_spark()
