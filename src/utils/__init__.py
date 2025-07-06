"""
Spark utilities package

This package provides utilities for Spark session management, logging, and performance monitoring.
"""

# Session management
from .session import (
    SparkVersion,
    IcebergConfig,
    S3FileSystemConfig,
    create_spark_session,
)

# Logging utilities
from .logger import HybridLogger, MetricsTracker

# Log parsing utilities
from .log_parser import main as parse_logs

__all__ = [
    # Session management
    "SparkVersion",
    "IcebergConfig",
    "S3FileSystemConfig",
    "create_spark_session",
    # Logging
    "HybridLogger",
    "MetricsTracker",
    # Log parsing
    "log_parser",
]
