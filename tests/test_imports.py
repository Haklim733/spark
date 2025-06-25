#!/usr/bin/env python3
"""
Test imports from the utils package
"""

import pytest


def test_utils_imports():
    """Test that all public APIs can be imported from utils package"""
    try:
        # Test session management imports
        from src.utils import SparkVersion, IcebergConfig, create_spark_session

        # Test logging imports
        from src.utils import HybridLogger, MetricsTracker

        # Test log parsing imports
        from src.utils import log_parser

        print("✅ All imports successful!")

    except ImportError as e:
        pytest.fail(f"Import failed: {e}")


def test_utils_all_attribute():
    """Test that __all__ is properly defined"""
    try:
        from src.utils import __all__

        expected_exports = [
            "SparkVersion",
            "IcebergConfig",
            "create_spark_session",
            "HybridLogger",
            "MetricsTracker",
            "log_parser",
        ]

        for export in expected_exports:
            assert export in __all__, f"Missing export: {export}"

        print("✅ __all__ properly defined!")

    except ImportError as e:
        pytest.fail(f"Import failed: {e}")


def test_import_from_all():
    """Test importing using from utils import *"""
    try:
        # Import the module and check __all__
        import src.utils as utils

        # Verify we can access the expected names through the module
        assert hasattr(utils, "SparkVersion")
        assert hasattr(utils, "create_spark_session")
        assert hasattr(utils, "HybridLogger")
        assert hasattr(utils, "MetricsTracker")
        assert hasattr(utils, "log_parser")

        print("✅ All expected attributes available!")

    except ImportError as e:
        pytest.fail(f"Import failed: {e}")


if __name__ == "__main__":
    test_utils_imports()
    test_utils_all_attribute()
    test_import_from_all()
    print("All import tests passed!")
