#!/usr/bin/env python3
"""
Enums for Logging System
"""

from enum import Enum

class Operation(Enum):
    """Core operation types tracked in the logging system"""

    # Data operations
    DATAFRAME_OPERATION = "dataframe_operation"

    # Table operations
    TABLE_EXISTS_CHECK = "table_exists_check"
    TABLE_CREATION = "table_creation"
    TABLE_VALIDATION = "table_validation"
    TABLE_MERGE = "table_merge"
    TABLE_INSERT = "table_insert"

    # I/O operations
    FILE_LIST = "file_list"
    FILE_READ = "file_read"
    FILE_WRITE = "file_write"
    FILE_SAVE = "file_save"

    # System operations
    SQL_QUERY = "sql_query"

    # Job operations
    JOB_START = "job_start"
    JOB_COMPLETE = "job_complete"

    # Validation operations
    LOAD_VALIDATION = "load_validation"
    SCHEMA_VALIDATION = "schema_validation"


class OperationMetric(Enum):
    # Source-agnostic input metrics
    INPUTS_DISCOVERED = "inputs_discovered"  # files, tables, API calls, etc.
    INPUTS_PROCESSED = "inputs_processed"
    INPUTS_INSERTED = "inputs_inserted"
    INPUTS_FAILED = "inputs_failed"
    INPUTS_MISSING = "inputs_missing"

    # Record/Row-level metrics (always relevant)
    RECORDS_DISCOVERED = "records_discovered"
    RECORDS_PROCESSED = "records_processed"
    RECORDS_INSERTED = "records_inserted"
    RECORDS_FAILED = "records_failed"
    RECORDS_MISSING = "records_missing"

    # Size metrics
    INPUT_SIZE_BYTES = "input_size_bytes"  # file size, result set size, etc.
    RECORD_SIZE_BYTES = "record_size_bytes"

    # Time metrics (standardized to milliseconds)
    EXECUTION_TIME_MS = "execution_time_ms"
    INSERTION_TIME_MS = "insertion_time_ms"
    VALIDATION_TIME_MS = "validation_time_ms"

    # System metrics
    PARTITION_COUNT = "partition_count"
    EXECUTOR_COUNT = "executor_count"
    MEMORY_USAGE_MB = "memory_usage_mb"

    # Validation metrics
    EXPECTED_COUNT = "expected_count"
    ACTUAL_COUNT = "actual_count"
    SCHEMA_VALID = "schema_valid"
    DATA_QUALITY_VALID = "data_quality_valid"

    # Status indicators
    TABLE_EXISTS = "table_exists"
    STATUS = "status"


class OperationStatus(Enum):
    """Operation status values"""

    SUCCESS = "success"
    FAILURE = "failure"
    PARTIAL_SUCCESS = "partial_success"
    IN_PROGRESS = "in_progress"
    ERROR = "error"


class ErrorCode(Enum):
    """Standardized error codes"""

    # File/IO errors (E001-E010)
    FILE_NOT_FOUND = "E001"
    PERMISSION_DENIED = "E002"
    INVALID_FORMAT = "E003"
    NETWORK_ERROR = "E004"
    TIMEOUT = "E005"
    FILE_LISTING_ERROR = "E006"

    # Data/Schema errors (E011-E020)
    SCHEMA_MISMATCH = "E011"
    DATA_QUALITY_FAILURE = "E012"
    VALIDATION_ERROR = "E013"
    NULL_VALUES_ERROR = "E014"
    EMPTY_DATAFRAME = "E015"

    # Table/Database errors (E021-E030)
    TABLE_NOT_EXISTS = "E021"
    NAMESPACE_NOT_FOUND = "E022"
    LOAD_COUNT_MISMATCH = "E023"
    CONNECTION_ERROR = "E024"

    # Processing errors (E031-E040)
    PROCESSING_ERROR = "E031"
    TRANSFORMATION_ERROR = "E032"
    INSERTION_ERROR = "E033"

    # System errors (E041-E050)
    MEMORY_ERROR = "E041"
    RESOURCE_EXHAUSTED = "E042"
    CONFIGURATION_ERROR = "E043"

    # Unknown
    UNKNOWN_ERROR = "E999"


class SourceSystem(Enum):
    """Source system identifiers"""

    LOCAL = "local"
    S3 = "s3"
    DATABASE = "database"
    API = "api"


class LogLevel(Enum):
    """Log severity levels"""

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARN = "WARN"
    ERROR = "ERROR"
    FATAL = "FATAL"


# Utility functions
def calculate_success_rate(records_inserted: int, records_processed: int) -> float:
    """Calculate success rate from core metrics"""
    return records_inserted / records_processed if records_processed > 0 else 0.0


def calculate_throughput(records_processed: int, execution_time_ms: int) -> float:
    """Calculate records per second from core metrics"""
    return (
        (records_processed * 1000) / execution_time_ms if execution_time_ms > 0 else 0.0
    )


def get_error_description(error_code: ErrorCode) -> str:
    """Get human-readable error descriptions"""
    descriptions = {
        ErrorCode.FILE_NOT_FOUND: "File or directory not found",
        ErrorCode.SCHEMA_MISMATCH: "Schema validation failed",
        ErrorCode.TABLE_NOT_EXISTS: "Table does not exist",
        ErrorCode.LOAD_COUNT_MISMATCH: "Record count validation failed",
        ErrorCode.UNKNOWN_ERROR: "Unknown error occurred",
    }
    return descriptions.get(error_code, "No description available")
