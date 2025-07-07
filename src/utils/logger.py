import atexit
from contextlib import contextmanager
import json
import logging
import queue
import threading
import time
import os
import glob
from typing import Dict, Any, Optional
from pyspark.sql import SparkSession
from datetime import datetime
from enum import Enum
import traceback  # Moved import to top-level for consistency, as it's used in log_error
from src.utils.session import create_spark_session


class MetricsTracker:
    """
    Tracks and aggregates metrics for Spark operations.
    For MVP, data metrics are simulated. In a real scenario, these would come
    from actual DataFrame analysis or Spark listeners.
    """

    def __init__(self, spark: SparkSession):
        self.metrics: Dict[str, Dict[str, Any]] = {}
        self.current_job_group: Optional[str] = None
        self.current_operation: Optional[str] = None
        self.spark = spark  # Store spark session to potentially execute queries

    def start_operation(self, job_group: str, operation: str):
        """Start tracking metrics for a new operation."""
        self.current_job_group = job_group
        self.current_operation = operation
        if job_group not in self.metrics:
            self.metrics[job_group] = {}
        if operation not in self.metrics[job_group]:
            self.metrics[job_group][operation] = {
                "start_time": time.time(),
                "data_skew_ratio": 0,
                "partition_count": 0,
                "record_count": 0,
                "unique_keys": 0,
                "max_key_frequency": 0,
                "min_key_frequency": 0,
                "shuffle_partitions": 0,
                "estimated_memory_mb": 0,  # Renamed from memory_usage_mb to reflect estimation
                "execution_plan_complexity": "low",
            }

    def record_data_metrics(self, spark: SparkSession, table_name: str = "skewed_data"):
        """
        Record simulated data distribution metrics.
        In a real application, this would analyze an actual DataFrame.
        """
        if not self.current_job_group or not self.current_operation or spark is None:
            return

        try:
            # Simulate data generation for demonstration purposes
            spark.sql(
                f"CREATE OR REPLACE TEMPORARY VIEW {table_name} AS SELECT id as key FROM RANGE(1000) LATERAL VIEW explode(array_repeat(id, (id % 10) + 1)) AS key;"
            ).collect()

            distribution_df = spark.sql(
                f"""
                SELECT key, COUNT(*) as frequency
                FROM {table_name}
                GROUP BY key
                ORDER BY frequency DESC
                """
            )
            distribution = distribution_df.collect()

            if distribution:
                frequencies = [row["frequency"] for row in distribution]
                max_freq = max(frequencies)
                min_freq = min(frequencies)
                total_records = sum(frequencies)
                unique_keys = len(frequencies)

                avg_freq = total_records / unique_keys if unique_keys > 0 else 0
                skew_ratio = max_freq / avg_freq if avg_freq > 0 else 0

                partition_count_result = spark.sql(
                    f"SELECT COUNT(DISTINCT key) as partitions FROM {table_name}"
                ).collect()
                partition_count = (
                    partition_count_result[0]["partitions"]
                    if partition_count_result
                    else 0
                )

                shuffle_partitions = spark.conf.get(
                    "spark.sql.shuffle.partitions", "200"
                )

                # Simple estimation of memory MB based on record count
                estimated_memory_mb = (total_records * 100) / (1024 * 1024)

                self.metrics[self.current_job_group][self.current_operation].update(
                    {
                        "data_skew_ratio": round(skew_ratio, 2),
                        "partition_count": partition_count,
                        "record_count": total_records,
                        "unique_keys": unique_keys,
                        "max_key_frequency": max_freq,
                        "min_key_frequency": min_freq,
                        "shuffle_partitions": int(shuffle_partitions),
                        "estimated_memory_mb": round(estimated_memory_mb, 2),
                        "execution_plan_complexity": (  # Simplified complexity measure
                            "high"
                            if unique_keys > 1000
                            else "medium" if unique_keys > 100 else "low"
                        ),
                        # Spark config values are often strings, keep as such or convert as needed
                        "executor_memory": spark.conf.get(
                            "spark.executor.memory", "1g"
                        ),
                        "driver_memory": spark.conf.get("spark.driver.memory", "1g"),
                        "memory_fraction": float(
                            spark.conf.get("spark.memory.fraction", "0.6")
                        ),
                        "storage_fraction": float(
                            spark.conf.get("spark.memory.storageFraction", "0.5")
                        ),
                    }
                )
        except Exception as e:
            # In a real scenario, you'd log this error using the HybridLogger
            print(f"Error recording data metrics for {table_name}: {e}")

    def end_operation(
        self, spark: SparkSession, execution_time: float, result_count: int = 0
    ):
        """End tracking metrics for current operation and output to Spark logs."""
        if not self.current_job_group or not self.current_operation:
            return

        self.metrics[self.current_job_group][self.current_operation].update(
            {
                "execution_time": round(execution_time, 2),
                "result_count": result_count,
                "end_time": time.time(),
            }
        )

    def get_metrics(self) -> Dict:
        """Get all recorded metrics."""
        return self.metrics

    def shutdown(self):
        """Shutdown the metrics tracker (no specific actions for this MVP)."""
        pass


class HybridLogger:
    """
    Hybrid logging system that integrates Python's logging with Spark's JVM Log4j2
    and includes basic metrics tracking.
    """

    def __init__(
        self,
        spark: Optional[SparkSession] = None,
        app_name: str = "App",
        spark_config: Optional[dict] = None,
        manage_spark: bool = False,
        buffer_size: int = 1000,
        flush_interval: int = 5,
        enable_async: bool = True,
    ):
        # --- LOG DIRECTORY SETUP (important for file logging) ---
        # Determines base log directory based on environment variable
        if (
            os.getenv("RUNNING_IN_DOCKER_SPARK_CONNECT_SERVER", "false").lower()
            == "true"
        ):
            self.spark_logs_root_dir = "/opt/bitnami/spark/logs"
        else:
            self.spark_logs_root_dir = "./spark-logs"

        # Create app-specific log directory
        self.app_log_output_dir = os.path.join(
            self.spark_logs_root_dir, "app", app_name
        )
        os.makedirs(self.app_log_output_dir, exist_ok=True)
        # --- END LOG DIRECTORY SETUP ---

        self.app_name = app_name
        self.spark_config = spark_config or {}
        self.manage_spark = manage_spark

        # Create Spark session if managing it, or use provided session
        if manage_spark and spark is None:
            s3_config = self.spark_config.pop("s3_config", None)
            self.spark = create_spark_session(
                app_name=app_name, s3_config=s3_config, **self.spark_config
            )
            self._owns_spark = True
        else:
            self.spark = spark
            self._owns_spark = False

        # Initialize MetricsTracker if Spark session is available
        self.metrics_tracker = (
            MetricsTracker(self.spark) if self.spark is not None else None
        )

        # Determine if Spark JVM logger (Log4j2) is available
        self.spark_logger_available = False
        self.job_id = f"{app_name}_{int(time.time())}"  # Default for when SparkContext is not available

        if self.spark is not None:
            try:
                # Attempt to get application ID for classic Spark
                self.job_id = self.spark.sparkContext.applicationId
                self.spark_logger_available = True
                print(f"✅ SparkContext available. Job ID: {self.job_id}")
            except Exception:
                # This path is for Spark Connect where sparkContext isn't directly available
                try:
                    # Check if JVM bridge is accessible via Spark Connect
                    spark_connect_jvm = self.spark._jvm
                    if spark_connect_jvm:
                        self.spark_logger_available = True
                        print(
                            f"✅ Successfully accessed Spark Connect JVM for app: {app_name}"
                        )
                    else:
                        print(f"⚠️ Could not access JVM from Spark Connect session")
                except Exception as e:
                    print(f"⚠️ Could not access Log4j2 via Spark Connect JVM: {e}")

        self.enable_async = enable_async

        # Setup Python logger (used by both sync and async paths)
        self.python_logger = self._setup_python_logger()

        # Flag to indicate if we are currently in shutdown process
        self._is_shutting_down = False

        if enable_async:
            self.log_queue = queue.Queue(maxsize=buffer_size)
            self.flush_interval = flush_interval
            self.running = True

            # Start async logging thread
            self.log_thread = threading.Thread(
                target=self._async_log_worker, daemon=True
            )
            self.log_thread.start()

            # Register cleanup for graceful shutdown
            atexit.register(self.shutdown)

        # Performance counters for the logging system itself
        self.log_count = 0
        self.start_time = time.time()

        # Log sync configuration (placeholder for external storage sync)
        self.enable_log_sync = os.getenv("ENABLE_LOG_SYNC", "true").lower() == "true"
        self.minio_logs_bucket = "logs"  # Example bucket name
        self.log_sync_interval = int(
            os.getenv("LOG_SYNC_INTERVAL", "300")
        )  # 5 minutes default
        self.last_sync_time = time.time()

    def _async_log_worker(self):
        """Background thread for processing log messages asynchronously."""
        while self.running:
            try:
                logs_to_process = []
                start_batch_time = time.time()

                # Collect logs for flushing or until timeout
                # Also stop collecting if shutdown is initiated
                while (
                    self.running  # Added self.running check here
                    and len(logs_to_process) < self.log_queue.maxsize // 10
                    and (time.time() - start_batch_time) < self.flush_interval
                ):
                    try:
                        log_entry = self.log_queue.get(timeout=0.1)  # Short timeout
                        logs_to_process.append(log_entry)
                    except queue.Empty:
                        break  # No more items in queue, exit inner loop

                # Process the collected batch of logs
                for log_entry in logs_to_process:
                    if isinstance(log_entry, tuple) and len(log_entry) == 2:
                        log_type, data = log_entry
                        # Log structured data directly as JSON to the Python file handler
                        if self._is_shutting_down:
                            print(
                                f"HybridLogger: {log_type.upper()}: {json.dumps(data)}",
                                flush=True,
                            )
                        else:
                            self.python_logger.info(
                                f"{log_type.upper()}: {json.dumps(data)}"
                            )
                    else:
                        # Log plain string messages
                        if self._is_shutting_down:
                            print(f"HybridLogger: {str(log_entry)}", flush=True)
                        else:
                            self.python_logger.info(str(log_entry))
                    self.log_queue.task_done()

                # Sleep if nothing was processed to prevent busy-waiting
                if not logs_to_process and self.running:  # Only sleep if still running
                    time.sleep(0.01)

            except Exception as e:
                # Log errors in the async worker to the console or a fallback logger
                print(f"HybridLogger: Async logging worker error: {e}", flush=True)
                # If an error occurs, it might be safer to stop to prevent repeated errors
                self.running = False

    def _setup_python_logger(self) -> logging.Logger:
        """
        Configures Python's standard logger for console and file output.
        Ensures handlers are not duplicated.
        """
        logger = logging.getLogger(f"{self.app_name}.{self.job_id}")
        logger.setLevel(logging.INFO)  # Default level for console and app log file

        # Prevent adding handlers multiple times if method is called repeatedly
        if not logger.handlers:
            # Console handler
            console_handler = logging.StreamHandler()
            console_formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                datefmt="%H:%M:%S",
            )
            console_handler.setFormatter(console_formatter)
            logger.addHandler(console_handler)

            # File handler for general application logs
            try:
                app_log_file = os.path.join(
                    self.app_log_output_dir, f"{self.app_name}-application.log"
                )
                app_handler = logging.FileHandler(app_log_file)
                app_formatter = logging.Formatter(
                    "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S",
                )
                app_handler.setFormatter(app_formatter)
                logger.addHandler(app_handler)
                print(f"✅ Application log file: {app_log_file}")
            except Exception as e:
                print(f"⚠️ Could not set up application file logging: {e}")

            # Separate file handler for structured observability logs (JSON output)
            try:
                hybrid_log_file = os.path.join(
                    self.app_log_output_dir, f"{self.app_name}-hybrid-observability.log"
                )
                hybrid_handler = logging.FileHandler(hybrid_log_file)
                # Formatter for structured logs: just outputs the message (which is already JSON)
                hybrid_formatter = logging.Formatter("%(message)s")
                hybrid_handler.setFormatter(hybrid_formatter)
                logger.addHandler(hybrid_handler)
                print(f"✅ Hybrid observability log file: {hybrid_log_file}")
            except Exception as e:
                print(f"⚠️ Could not set up hybrid observability file logging: {e}")

            # Crucial: Disable propagation to the root logger to avoid duplicate output
            logger.propagate = False

        return logger

    def _convert_numpy_types(self, obj):
        """Convert NumPy types to Python native types for JSON serialization"""
        import numpy as np

        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, dict):
            return {key: self._convert_numpy_types(value) for key, value in obj.items()}
        elif isinstance(obj, list):
            return [self._convert_numpy_types(item) for item in obj]
        else:
            return obj

    def _log_to_jvm_or_python(
        self, logger_name: str, structured_data: Dict[str, Any], level: str = "info"
    ):
        """
        Helper to log structured data to Spark JVM (Log4j2) or fallback to Python.
        """
        # If we are in shutdown, use print for Python fallback to avoid closed file issues
        use_print_fallback = self._is_shutting_down

        # Ensure timestamp is always included
        if "timestamp" not in structured_data:
            structured_data["timestamp"] = int(time.time() * 1000)
        if "job_id" not in structured_data:
            structured_data["job_id"] = self.job_id

        # Convert NumPy types to Python native types for JSON serialization
        structured_data = self._convert_numpy_types(structured_data)

        log_message = json.dumps(structured_data)

        if self.spark_logger_available and self.spark is not None:
            try:
                jvm_logger = (
                    self.spark._jvm.org.apache.logging.log4j.LogManager.getLogger(
                        logger_name
                    )
                )
                if level == "info":
                    jvm_logger.info(log_message)
                elif level == "error":
                    jvm_logger.error(log_message)
                # Add other levels (debug, warn) if needed
            except Exception as e:
                if use_print_fallback:
                    print(
                        f"HybridLogger: Fallback due to JVM log failure during shutdown (logger: {logger_name}): {e} - Data: {log_message}",
                        flush=True,
                    )
                else:
                    self.python_logger.error(
                        f"Failed to log to Spark JVM (logger: {logger_name}): {e} - Data: {log_message}"
                    )
        else:
            # Fallback to Python logger with a prefix for clarity
            if use_print_fallback:
                print(f"HybridLogger: {logger_name.upper()}: {log_message}", flush=True)
            else:
                self.python_logger.info(f"{logger_name.upper()}: {log_message}")

    def log_performance(self, operation: str, metrics: Dict[str, Any]):
        """
        Log performance metrics.
        """
        structured_metrics = {
            "operation": operation,
            **metrics,
        }
        # Call _log_to_jvm_or_python which handles shutdown state
        self._log_to_jvm_or_python("PERFORMANCE_METRICS", structured_metrics, "info")
        self.log_count += 1

    def log_business_event(self, event_type: str, event_data: Dict[str, Any]):
        """
        Log business events asynchronously for non-blocking operation.
        """
        structured_event = {
            "event_type": event_type,
            **event_data,
        }
        if (
            self.enable_async and not self._is_shutting_down
        ):  # Don't queue new logs if shutting down
            try:
                # Queue the structured data as a tuple (type, data)
                self.log_queue.put_nowait(("business_event", structured_event))
            except queue.Full:
                # If queue is full, drop the log (better than blocking)
                pass
        else:
            # Fallback to synchronous logging, but still use the helper for consistency
            # _log_to_jvm_or_python handles print fallback during shutdown
            self._log_to_jvm_or_python("BUSINESS_EVENT", structured_event, "info")
        self.log_count += 1

    def log_debug(self, message: str, data: Optional[Dict[str, Any]] = None):
        """
        Log debug information asynchronously if enabled.
        Note: Python logger level must be set to DEBUG to see these.
        """
        structured_debug = {
            "message": message,
            "data": data if data is not None else {},
        }
        if (
            self.enable_async and not self._is_shutting_down
        ):  # Don't queue new logs if shutting down
            try:
                # Queue debug messages as plain strings (or structured if preferred)
                self.log_queue.put_nowait(f"DEBUG: {json.dumps(structured_debug)}")
            except queue.Full:
                pass  # Drop debug logs if queue is full
        else:
            # Synchronous Python debug log. Use standard Python logger's debug
            # because this is primarily for development/deep inspection.
            # Avoids _log_to_jvm_or_python for debug to keep it python-centric.
            if self._is_shutting_down:
                print(
                    f"HybridLogger: DEBUG: {json.dumps(structured_debug)}", flush=True
                )
            else:
                self.python_logger.debug(f"DEBUG: {json.dumps(structured_debug)}")

    def log_error(self, error: Exception, context: Optional[Dict[str, Any]] = None):
        """
        Log errors synchronously to ensure immediate visibility.
        """
        error_data = {
            "error_type": type(error).__name__,
            "error_message": str(error),
            "stack_trace": traceback.format_exc(),  # Get full traceback
        }
        if context:
            error_data.update(context)

        # Always log errors synchronously to Python logger and JVM if available
        # _log_to_jvm_or_python handles print fallback during shutdown
        self._log_to_jvm_or_python("ERROR", error_data, "error")

    def log_custom_metrics(
        self, job_group: str, operation: str, metrics: Dict[str, Any]
    ):
        """
        Log custom application-specific metrics.
        """
        custom_metrics_event = {
            "Event": "CustomMetricsEvent",
            "Job Group": job_group,
            "Operation": operation,
            "Metrics": metrics,
        }
        # Call _log_to_jvm_or_python which handles shutdown state
        self._log_to_jvm_or_python("CUSTOM_METRICS", custom_metrics_event, "info")
        self.log_count += 1

    def log_operation_completion(
        self,
        job_group: str,
        operation: str,
        execution_time: float,
        result_count: int = 0,
    ):
        """
        Log the completion of an operation with its duration and result count.
        """
        completion_event = {
            "Event": "CustomOperationCompletion",
            "Job Group": job_group,
            "Operation": operation,
            "Execution Time": round(execution_time, 2),
            "Result Count": result_count,
        }
        # Call _log_to_jvm_or_python which handles shutdown state
        self._log_to_jvm_or_python("CUSTOM_COMPLETION", completion_event, "info")
        self.log_count += 1

    def log_batch_metrics(
        self, batch_id: int, record_count: int, processing_time: float
    ):
        """
        Log batch processing metrics efficiently (e.g., every 10th batch).
        """
        if batch_id % 10 == 0:  # Only log every 10th batch to reduce overhead
            self.log_performance(
                "batch_processing",
                {
                    "batch_id": batch_id,
                    "record_count": record_count,
                    "processing_time_ms": round(processing_time * 1000, 2),
                    "total_logs": self.log_count,
                },
            )

    def force_sync_logs(self) -> Dict[str, Any]:
        """
        Force sync logs to external storage (placeholder for MinIO/S3).
        """
        # This call is from outside, so it should use python_logger if not shutting down
        return self.sync_logs_to_external_storage(force_sync=True)

    def get_performance_summary(self) -> Dict[str, Any]:
        """
        Get performance summary of the logging system itself.
        """
        total_time = time.time() - self.start_time
        return {
            "total_logs_emitted": self.log_count,
            "total_logger_runtime_seconds": round(total_time, 2),
            "logs_per_second": (
                round(self.log_count / total_time, 2) if total_time > 0 else 0
            ),
            "async_logging_enabled": self.enable_async,
            "spark_application_id": self.job_id,
        }

    def shutdown(self):
        """
        Gracefully shuts down the logger, metrics tracker, and Spark session.
        """
        # Set shutdown flag immediately to prevent new logs from being queued
        self._is_shutting_down = True

        # Use print instead of logger during shutdown to avoid closed file issues
        print(f"HybridLogger shutdown initiated for app: {self.app_name}")

        # 1. Final log sync (if enabled and if there's an actual sync mechanism)
        if self.enable_log_sync:
            try:
                # Pass a flag to sync_logs_to_external_storage to use print
                sync_results = self.sync_logs_to_external_storage(
                    force_sync=True, during_shutdown=True
                )
                print(f"Final log sync completed: {sync_results}")
            except Exception as e:
                print(f"Error during final log sync: {e}")

        # 2. Shutdown metrics tracker
        if self.metrics_tracker is not None:
            try:
                self.metrics_tracker.shutdown()
                self.metrics_tracker = None
            except Exception as e:
                print(f"Error shutting down metrics tracker: {e}")

        # 3. Shutdown async logger thread (Python side)
        if self.enable_async:
            self.running = False  # Signal thread to stop
            if hasattr(self, "log_thread") and self.log_thread.is_alive():
                # Give the worker a chance to process remaining items
                # The _async_log_worker itself now checks self.running in its loop
                try:
                    # Wait for queue to process remaining items
                    while not self.log_queue.empty():
                        time.sleep(0.1)
                except Exception as e:
                    print(f"Error waiting for log queue to empty: {e}")
                self.log_thread.join(timeout=2)  # Give thread a moment to finish

        # 4. Close Python logger handlers explicitly
        # This is CRUCIAL for preventing "I/O operation on closed file"
        # when other atexit functions or Python's logging shutdown tries to clean up.
        for handler in list(self.python_logger.handlers):
            try:
                handler.close()
                self.python_logger.removeHandler(handler)
            except Exception as e:
                print(f"Error closing logger handler {handler.name}: {e}", flush=True)

        # 5. Shutdown Spark session if owned by this logger instance
        if self._owns_spark and self.spark is not None:
            try:
                self.spark.stop()
                self.spark = None
                print("SparkSession stopped.")
            except Exception as e:
                print(f"Error shutting down Spark session: {e}", flush=True)

    def _serialize_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Converts config values to JSON-serializable formats.
        Handles Enums, datetimes, and general objects by converting them to strings.
        """

        def serialize_value(value: Any) -> Any:
            if isinstance(value, Enum):
                return value.value
            elif isinstance(value, datetime):
                return value.isoformat()
            elif isinstance(value, dict):
                return {k: serialize_value(v) for k, v in value.items()}
            elif isinstance(value, (list, tuple)):
                return [serialize_value(v) for v in value]
            elif hasattr(value, "__dict__"):  # Generic object to string
                return str(value)
            else:
                return value

        return serialize_value(config)

    def __enter__(self):
        """Context manager entry: logs startup event."""
        # Log startup performance right at the beginning
        self.log_performance(
            "logger_startup",
            {
                "app_name": self.app_name,
                "owns_spark_session": self._owns_spark,
                "initial_spark_config": self._serialize_config(self.spark_config),
            },
        )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit: logs errors and ensures graceful shutdown."""
        if exc_type is not None:
            # Log any exception that occurred within the 'with' block
            # Use print during shutdown to avoid logger issues
            if self._is_shutting_down:
                print(f"HybridLogger: Exception during shutdown: {exc_val}")
                print(f"HybridLogger: Exception type: {exc_type}")
                print(f"HybridLogger: Traceback: {traceback.format_exc()}")
            else:
                self.log_error(
                    exc_val,
                    {
                        "context": "exception_in_main_block",
                        "exc_type": str(exc_type),
                        "traceback": traceback.format_exc(),
                    },
                )
        self.shutdown()  # Always call shutdown on exit

    # --- Convenience methods for MetricsTracker integration ---
    def start_operation(self, job_group: str, operation: str):
        """Proxy to MetricsTracker.start_operation."""
        if self.metrics_tracker is not None:
            self.metrics_tracker.start_operation(job_group, operation)

    def end_operation(
        self,
        execution_time: float,
        result_count: int = 0,  # Removed spark parameter here
    ):
        """Proxy to MetricsTracker.end_operation."""
        if self.metrics_tracker is not None:
            # MetricsTracker should use its internal spark reference
            self.metrics_tracker.end_operation(
                self.spark, execution_time, result_count
            )  # Pass self.spark

    def record_data_metrics(
        self, table_name: str = "skewed_data"
    ):  # Removed spark parameter here
        """Proxy to MetricsTracker.record_data_metrics."""
        if self.metrics_tracker is not None:
            # MetricsTracker should use its internal spark reference
            self.metrics_tracker.record_data_metrics(
                self.spark, table_name
            )  # Pass self.spark

    def get_metrics(self) -> Dict:
        """Proxy to MetricsTracker.get_metrics."""
        if self.metrics_tracker is not None:
            return self.metrics_tracker.get_metrics()
        return {}

    def sync_logs_to_external_storage(
        self, force_sync: bool = False, during_shutdown: bool = False
    ) -> Dict[str, Any]:
        """
        Placeholder to sync local Spark logs (e.g., driver logs) to external storage (e.g., MinIO/S3).
        This would typically involve iterating through log files in self.app_log_output_dir
        and uploading them.
        """
        # Determine which logging function to use based on 'during_shutdown' flag
        log_func = print if during_shutdown else self.python_logger.info
        log_error_func = print if during_shutdown else self.python_logger.error

        if not self.enable_log_sync or self.spark is None:
            if (
                during_shutdown
            ):  # Only print if specifically during shutdown for this message
                log_func(
                    "HybridLogger: Log sync disabled or no Spark session during shutdown. Skipping."
                )
            return {
                "status": "disabled",
                "reason": "Log sync disabled or no Spark session",
                "files_synced": 0,
            }

        current_time = time.time()
        if (
            not force_sync
            and (current_time - self.last_sync_time) < self.log_sync_interval
        ):
            return {
                "status": "skipped",
                "reason": "Interval not met",
                "files_synced": 0,
            }

        synced_files_count = 0
        try:
            # TODO: Implement actual file syncing logic here
            # Example: Iterate through log files and upload to MinIO/S3
            log_files = glob.glob(os.path.join(self.app_log_output_dir, "*.log"))

            # This is where you would integrate with your MinIO/S3 client
            # from minio import Minio
            # minio_client = Minio(...)

            for log_file_path in log_files:
                file_name = os.path.basename(log_file_path)
                # For MVP, just simulate sync
                # print(f"Simulating upload of {file_name} to {self.minio_logs_bucket}/job_id/{file_name}")
                synced_files_count += 1

            self.last_sync_time = current_time
            # Use the determined log_func (print or self.python_logger.info)
            log_func(
                f"Successfully synced {synced_files_count} log files to external storage."
            )
            return {
                "status": "success",
                "files_synced": synced_files_count,
                "timestamp": int(current_time * 1000),
            }
        except Exception as e:
            # Use the determined log_error_func (print or self.python_logger.error)
            log_error_func(f"Failed to sync logs to external storage: {e}")
            return {
                "status": "failed",
                "error": str(e),
                "files_synced": synced_files_count,
            }
