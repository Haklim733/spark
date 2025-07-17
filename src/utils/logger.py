#!/usr/bin/env python3
"""
Simplified HybridLogger for Spark Connect environments
Focuses on file-based logging with consolidated Operation enum
"""

import json
import logging
import time
import os
from typing import Dict, Any, Optional
from datetime import datetime
from pyspark.sql import SparkSession


def _safe_json_serialize(obj):
    """Safely serialize objects for JSON logging, handling non-serializable types"""
    if hasattr(obj, "__call__"):
        # Handle function objects
        return f"<function {obj.__name__}>"
    elif hasattr(obj, "item"):
        # Handle numpy/pandas scalars that have .item() method
        try:
            return obj.item()
        except:
            return str(obj)
    elif str(type(obj)).startswith("<class 'numpy.") or str(type(obj)).startswith(
        "<class 'pandas."
    ):
        # Handle numpy/pandas types by string conversion and parsing
        try:
            return obj.item() if hasattr(obj, "item") else str(obj)
        except:
            return str(obj)
    elif (
        "int64" in str(type(obj))
        or "int32" in str(type(obj))
        or "int16" in str(type(obj))
    ):
        # Handle any int64/int32/int16 regardless of source
        return int(obj)
    elif "float64" in str(type(obj)) or "float32" in str(type(obj)):
        # Handle any float64/float32 regardless of source
        return float(obj)
    elif "bool_" in str(type(obj)):
        # Handle any boolean types
        return bool(obj)
    elif hasattr(obj, "__dict__"):
        # Handle objects with attributes
        return str(obj)
    else:
        return obj


class HybridLogger:
    """Simplified logger for Spark Connect with file-based logging"""

    def __init__(
        self,
        spark: Optional[SparkSession] = None,
        app_name: str = "App",
        manage_spark: bool = False,
    ):
        self.app_name = app_name
        self.spark = spark
        self.manage_spark = manage_spark

        # Generate job ID
        self.job_id = f"{app_name}_{int(time.time())}"
        if spark is not None:
            try:
                self.job_id = spark.sparkContext.applicationId
            except Exception:
                pass  # Keep generated job_id for Spark Connect

        # Setup log directory
        self.log_dir = os.path.join("./spark-logs", "app", app_name)
        os.makedirs(self.log_dir, exist_ok=True)

        # Setup Python logger
        self.logger = self._setup_logger()

        # Log storage for file export
        self.log_entries = []

    def _setup_logger(self) -> logging.Logger:
        """Setup simple file and console logging"""
        logger = logging.getLogger(f"{self.app_name}.{self.job_id}")
        logger.setLevel(logging.INFO)

        if not logger.handlers:
            # Console handler
            console_handler = logging.StreamHandler()
            console_formatter = logging.Formatter(
                "%(asctime)s - %(levelname)s - %(message)s"
            )
            console_handler.setFormatter(console_formatter)
            logger.addHandler(console_handler)

            # File handler
            log_file = os.path.join(self.log_dir, f"{self.app_name}-operations.log")
            file_handler = logging.FileHandler(log_file)
            file_formatter = logging.Formatter("%(message)s")  # JSON only
            file_handler.setFormatter(file_formatter)
            logger.addHandler(file_handler)

            logger.propagate = False

        return logger

    def _safe_serialize_metrics(self, metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Recursively serialize metrics to ensure JSON compatibility"""
        serialized = {}
        for key, value in metrics.items():
            if isinstance(value, dict):
                serialized[key] = self._safe_serialize_metrics(value)
            elif isinstance(value, list):
                serialized[key] = [
                    (
                        self._safe_serialize_metrics(item)
                        if isinstance(item, dict)
                        else _safe_json_serialize(item)
                    )
                    for item in value
                ]
            else:
                serialized[key] = _safe_json_serialize(value)
        return serialized

    def log(
        self,
        operation: str,
        status: str,
        metrics: Dict[str, Any],
    ) -> None:
        """Consolidated logging for both operations and errors"""
        # Safely serialize metrics to prevent JSON errors
        safe_metrics = self._safe_serialize_metrics(metrics)

        log_entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "job_id": self.job_id,
            "operation": operation,
            "status": status,
            **safe_metrics,
        }
        # Store for later export
        self.log_entries.append(log_entry)

        # Log to file as JSON
        if status == "error":
            self.logger.error(json.dumps(log_entry))
        else:
            self.logger.info(json.dumps(log_entry))

    def start_operation(self, job_group: str, operation: str):
        """Simple operation tracking"""
        self.current_operation_start = time.time()

    def get_log_entries(self) -> list:
        """Get all logged entries for export"""
        return self.log_entries

    def shutdown(self):
        """Simple cleanup"""
        print(
            f"📝 Logger shutdown for {self.app_name} - {len(self.log_entries)} entries logged"
        )

        # Close handlers
        for handler in self.logger.handlers:
            handler.close()
            self.logger.removeHandler(handler)

    def __enter__(self):
        """Context manager entry"""
        print(f"🎯 Starting {self.app_name} with job_id: {self.job_id}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        if exc_type is not None:
            self.log(
                "exception",
                "error",
                {"context": "main_block_exception", "exception": str(exc_val)},
            )
        self.shutdown()
