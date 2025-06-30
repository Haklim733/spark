#!/usr/bin/env python3
"""
Test script to demonstrate MinIO data loading validation
"""

from utils.session import (
    create_spark_session,
    SparkVersion,
    IcebergConfig,
    S3FileSystemConfig,
)
from insert import (
    validate_minio_data_loading,
    insert_minio_legal_documents,
    list_minio_files,
    is_minio_path,
)


def test_minio_validation():
    """Test MinIO data loading validation"""

    print("🧪 Testing MinIO Data Loading Validation")
    print("=" * 50)

    # Test MinIO path detection
    test_paths = [
        "s3a://data/docs/legal",
        "s3://data/docs/legal",
        "minio://data/docs/legal",
        "/local/path/docs",
        "data/docs/legal",
    ]

    print("🔍 Testing MinIO path detection:")
    for path in test_paths:
        is_minio = is_minio_path(path)
        print(f"   {path} -> {'MinIO' if is_minio else 'Local'}")

    # Create Spark session
    print("\n🚀 Creating Spark session...")
    with create_spark_session(
        spark_version=SparkVersion.SPARK_3_5,
        app_name="test_minio_validation",
        iceberg_config=IcebergConfig(
            s3_config=S3FileSystemConfig(),
        ),
    ) as spark:

        # Test MinIO file listing
        minio_path = "s3a://data/docs/legal"
        print(f"\n📋 Testing MinIO file listing: {minio_path}")

        try:
            files = list_minio_files(spark, minio_path)
            print(f"✅ Found {len(files)} files in MinIO")

            if files:
                print("📄 Sample files:")
                for file in files[:5]:
                    print(f"   - {file}")
                if len(files) > 5:
                    print(f"   ... and {len(files) - 5} more files")

            # Test validation function
            print(f"\n🔍 Testing comprehensive validation...")
            validation_results = validate_minio_data_loading(
                spark=spark,
                minio_path=minio_path,
                table_name="legal.documents",
                expected_file_count=len(files),
            )

            print(f"\n📊 Validation Results Summary:")
            print(f"   - Validation passed: {validation_results['validation_passed']}")
            print(f"   - Source files: {validation_results['source_files']}")
            print(f"   - Target records: {validation_results['target_records']}")
            print(
                f"   - Quarantine records: {validation_results['quarantine_records']}"
            )
            print(f"   - Errors: {len(validation_results['errors'])}")
            print(f"   - Warnings: {len(validation_results['warnings'])}")

            if validation_results["errors"]:
                print(f"\n❌ Errors found:")
                for error in validation_results["errors"]:
                    print(f"   - {error}")

            if validation_results["warnings"]:
                print(f"\n⚠️  Warnings found:")
                for warning in validation_results["warnings"]:
                    print(f"   - {warning}")

        except Exception as e:
            print(f"❌ Error testing MinIO validation: {e}")
            return False

        return True


def test_minio_insertion():
    """Test complete MinIO insertion with validation"""

    print("\n🧪 Testing Complete MinIO Insertion")
    print("=" * 50)

    with create_spark_session(
        spark_version=SparkVersion.SPARK_3_5,
        app_name="test_minio_insertion",
        iceberg_config=IcebergConfig(
            s3_config=S3FileSystemConfig(),
        ),
    ) as spark:

        minio_path = "s3a://data/docs/legal"
        table_name = "legal.documents"

        print(f"🚀 Testing insertion from {minio_path} to {table_name}")

        try:
            success = insert_minio_legal_documents(
                spark=spark, minio_path=minio_path, table_name=table_name
            )

            if success:
                print("✅ MinIO insertion completed successfully!")
            else:
                print("❌ MinIO insertion failed or validation issues detected")

            return success

        except Exception as e:
            print(f"❌ Error during MinIO insertion: {e}")
            return False


if __name__ == "__main__":
    print("🧪 MinIO Validation Test Suite")
    print("=" * 50)

    # Test 1: Basic validation
    test1_success = test_minio_validation()

    # Test 2: Complete insertion (uncomment to test full insertion)
    # test2_success = test_minio_insertion()

    print(f"\n📊 Test Results:")
    print(f"   - Path detection test: {'✅ PASS' if test1_success else '❌ FAIL'}")
    # print(f"   - Insertion test: {'✅ PASS' if test2_success else '❌ FAIL'}")

    print("\n✨ Test completed!")
