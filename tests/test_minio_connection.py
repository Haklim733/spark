#!/usr/bin/env python3
"""
Test script to verify Spark connection to MinIO
"""

from utils.session import create_spark_session
from insert_legal import list_minio_files, get_minio_path_info


def test_minio_connection():
    """Test Spark connection to MinIO"""

    print("🔗 Testing MinIO connection with Spark...")

    try:
        # Test 1: Create Spark session
        print("📋 Creating Spark session...")
        spark = create_spark_session()
        if not spark:
            print("❌ Failed to create Spark session")
            return False
        print("✅ Spark session created successfully")

        # Test 2: List files in data bucket
        print("📋 Listing files in data bucket...")
        try:
            files = list_minio_files(spark, "s3a://data")
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

        # Test 3: Test path parsing
        print("📋 Testing path parsing...")
        test_paths = [
            "s3a://data/docs/legal/file.txt",
            "s3://data/docs/legal/file.txt",
            "minio://data/docs/legal/file.txt",
        ]

        for path in test_paths:
            info = get_minio_path_info(path)
            print(f"   {path} -> bucket: {info['bucket']}, key: {info['key']}")

        # Test 4: List files in specific directory
        print("📋 Testing list_minio_files function with specific path...")
        minio_path = "s3a://data/docs"
        files = list_minio_files(spark, minio_path)
        print(f"✅ Found {len(files)} files in {minio_path}")

        return True

    except Exception as e:
        print(f"❌ Error testing MinIO connection: {e}")
        return False


if __name__ == "__main__":
    success = test_minio_connection()
    if success:
        print("\n🎉 MinIO connection test completed successfully!")
    else:
        print("\n💥 MinIO connection test failed!")
