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
from .session import create_spark_session
from datetime import datetime, timezone
from enum import Enum


class MetricsTracker:
    def __init__(self, spark: SparkSession):
        self.metrics = {}
        self.current_job_group = None
        self.current_operation = None

    def start_operation(self, job_group: str, operation: str):
        """Start tracking metrics for a new operation"""
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
                "memory_usage_mb": 0,
                "execution_plan_complexity": "low",
            }

    def record_data_metrics(self, spark: SparkSession, table_name: str = "skewed_data"):
        """Record data distribution metrics and output to Spark logs"""
        if not self.current_job_group or not self.current_operation:
            return

        try:
            # Get data distribution
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

                # Calculate skew ratio (max frequency / average frequency)
                avg_freq = total_records / unique_keys
                skew_ratio = max_freq / avg_freq if avg_freq > 0 else 0

                # Get partition count
                partition_count = spark.sql(
                    f"SELECT COUNT(DISTINCT key) as partitions FROM {table_name}"
                ).collect()[0]["partitions"]

                # Get shuffle partitions setting
                shuffle_partitions = spark.conf.get(
                    "spark.sql.shuffle.partitions", "200"
                )

                # Get memory configuration
                executor_memory = spark.conf.get("spark.executor.memory", "1g")
                driver_memory = spark.conf.get("spark.driver.memory", "1g")
                memory_fraction = spark.conf.get("spark.memory.fraction", "0.6")
                storage_fraction = spark.conf.get("spark.memory.storageFraction", "0.5")

                # Calculate memory usage (approximate)
                estimated_memory_mb = (total_records * 100) / (
                    1024 * 1024
                )  # Rough estimate: 100 bytes per record

                # Update metrics
                self.metrics[self.current_job_group][self.current_operation].update(
                    {
                        "data_skew_ratio": round(skew_ratio, 2),
                        "partition_count": partition_count,
                        "record_count": total_records,
                        "unique_keys": unique_keys,
                        "max_key_frequency": max_freq,
                        "min_key_frequency": min_freq,
                        "shuffle_partitions": int(shuffle_partitions),
                        "execution_plan_complexity": (
                            "high"
                            if unique_keys > 1000
                            else "medium" if unique_keys > 100 else "low"
                        ),
                        "executor_memory": executor_memory,
                        "driver_memory": driver_memory,
                        "memory_fraction": float(memory_fraction),
                        "storage_fraction": float(storage_fraction),
                        "estimated_memory_mb": round(estimated_memory_mb, 2),
                    }
                )

        except Exception as e:
            pass

    def end_operation(
        self, spark: SparkSession, execution_time: float, result_count: int = 0
    ):
        """End tracking metrics for current operation and output to Spark logs"""
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
        """Get all recorded metrics"""
        return self.metrics

    def save_metrics(self, filename: str = "shuffling_metrics.json"):
        """Save metrics to JSON file (kept for backward compatibility)"""
        try:
            with open(filename, "w") as f:
                json.dump(self.metrics, f, indent=2)
            print(f"Metrics saved to {filename}")
        except Exception as e:
            pass

    def get_logging_performance_summary(self) -> Dict:
        """Get performance summary of the logging system"""
        return {}

    def shutdown(self):
        """Shutdown the metrics tracker"""
        pass


class HybridLogger:
    """
    Hybrid logging system that can optionally manage Spark sessions
    """

    def __init__(
        self,
        spark: Optional[SparkSession] = None,
        app_name: str = "App",
        spark_config: dict = None,
        manage_spark: bool = False,
        buffer_size: int = 1000,
        flush_interval: int = 5,
        enable_async: bool = True,
    ):
        self.app_name = app_name
        self.spark_config = spark_config or {}
        self.manage_spark = manage_spark

        # Create Spark session if managing it
        if manage_spark and spark is None:
            # Extract s3_config from spark_config if present
            s3_config = (
                self.spark_config.pop("s3_config", None) if self.spark_config else None
            )

            self.spark = create_spark_session(
                app_name=app_name, s3_config=s3_config, **self.spark_config
            )
            self._owns_spark = True
        else:
            self.spark = spark
            self._owns_spark = False

        # Always create MetricsTracker if spark is provided
        if self.spark is not None:
            self.metrics_tracker = MetricsTracker(self.spark)
        else:
            self.metrics_tracker = None

        # Handle case where spark is None
        if self.spark is not None:
            # Check if this is a Spark Connect session (no sparkContext)
            try:
                self.job_id = self.spark.sparkContext.applicationId
                # Setup Spark logger for performance metrics (synchronous)
                self.spark_logger = (
                    self.spark.sparkContext._jvm.org.apache.log4j.LogManager.getLogger(
                        app_name
                    )
                )
            except Exception:
                # Spark Connect session - no sparkContext available
                self.job_id = f"{app_name}_{int(time.time())}"
                self.spark_logger = None
        else:
            self.job_id = f"{app_name}_{int(time.time())}"
            self.spark_logger = None

        self.enable_async = enable_async

        # Setup async Python logger for business events
        if enable_async:
            self.log_queue = queue.Queue(maxsize=buffer_size)
            self.flush_interval = flush_interval
            self.running = True

            # Start async logging thread
            self.log_thread = threading.Thread(
                target=self._async_log_worker, daemon=True
            )
            self.log_thread.start()

            # Register cleanup
            atexit.register(self.shutdown)
        else:
            # Fallback to synchronous Python logging
            self.python_logger = self._setup_python_logger()

        # Performance counters
        self.log_count = 0
        self.start_time = time.time()

        # Log sync configuration
        self.enable_log_sync = os.getenv("ENABLE_LOG_SYNC", "true").lower() == "true"
        self.spark_logs_dir = "/opt/bitnami/spark/logs"
        self.minio_logs_bucket = "logs"
        self.log_sync_interval = int(
            os.getenv("LOG_SYNC_INTERVAL", "300")
        )  # 5 minutes default
        self.last_sync_time = time.time()

        # Add job and batch tracking state
        self._job_tracking = {}
        self._batch_tracking = {}
        self._file_tracking = {}

    def _async_log_worker(self):
        """Background thread for processing log messages asynchronously"""
        python_logger = self._setup_python_logger()

        while self.running:
            try:
                # Batch process logs
                logs = []
                start_time = time.time()

                while (
                    len(logs) < 100 and (time.time() - start_time) < self.flush_interval
                ):
                    try:
                        log_entry = self.log_queue.get(timeout=1)
                        logs.append(log_entry)
                    except queue.Empty:
                        break

                # Process batch
                for log_entry in logs:
                    if isinstance(log_entry, tuple):
                        # Structured log entry (log_type, data)
                        log_type, data = log_entry
                        if log_type in [
                            "job_metrics",
                            "batch_metrics",
                            "operation_metrics",
                        ]:
                            # Log structured metrics with appropriate prefix
                            python_logger.info(
                                f"{log_type.upper()}: {json.dumps(data)}"
                            )
                        else:
                            # Fallback for unknown structured logs
                            python_logger.info(f"STRUCTURED_LOG: {json.dumps(data)}")
                    else:
                        # Simple string log entry (backward compatibility)
                        python_logger.info(log_entry)

                # Small delay to prevent CPU spinning
                time.sleep(0.01)

            except Exception as e:
                # Fallback to synchronous logging for errors
                python_logger.error(f"Async logging error: {e}")

    def _setup_python_logger(self) -> logging.Logger:
        """Setup Python logger with performance optimizations"""
        logger = logging.getLogger(f"{self.app_name}.{self.job_id}")
        logger.setLevel(logging.INFO)

        # Avoid duplicate handlers
        if not logger.handlers:
            handler = logging.StreamHandler()

            # Performance-optimized formatter
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                datefmt="%H:%M:%S",  # Shorter timestamp format
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)

            # Disable propagation to avoid duplicate logs
            logger.propagate = False

        return logger

    def log_performance(self, operation: str, metrics: dict):
        """
        Log performance metrics using Spark logger (synchronous for immediate availability)

        Args:
            operation: Name of the operation being measured
            metrics: Dictionary of performance metrics
        """
        structured_metrics = {
            "job_id": self.job_id,
            "operation": operation,
            "timestamp": int(time.time() * 1000),
            **metrics,
        }

        if self.spark_logger is not None:
            self.spark_logger.info(
                f"PERFORMANCE_METRICS: {json.dumps(structured_metrics)}"
            )
        else:
            # Fallback to Python logger when Spark logger is not available
            python_logger = self._setup_python_logger()
            python_logger.info(f"PERFORMANCE_METRICS: {json.dumps(structured_metrics)}")

    def log_business_event(self, event_type: str, event_data: dict):
        """
        Log business events asynchronously (non-blocking)

        Args:
            event_type: Type of business event
            event_data: Dictionary of event data
        """
        structured_event = {
            "job_id": self.job_id,
            "event_type": event_type,
            "timestamp": int(time.time() * 1000),
            **event_data,
        }

        if self.enable_async:
            # Non-blocking async logging
            try:
                self.log_queue.put_nowait(
                    f"BUSINESS_EVENT: {json.dumps(structured_event)}"
                )
                self.log_count += 1
            except queue.Full:
                # If queue is full, drop the log (better than blocking)
                pass
        else:
            # Synchronous fallback
            self.python_logger.info(f"BUSINESS_EVENT: {json.dumps(structured_event)}")
            self.log_count += 1

    def log_debug(self, message: str, data: dict = None):
        """
        Log debug information asynchronously

        Args:
            message: Debug message
            data: Optional data dictionary
        """
        if data:
            log_message = f"DEBUG: {message} - {json.dumps(data)}"
        else:
            log_message = f"DEBUG: {message}"

        if self.enable_async:
            try:
                self.log_queue.put_nowait(log_message)
            except queue.Full:
                pass  # Drop debug logs if queue is full
        else:
            self.python_logger.debug(log_message)

    def log_error(self, error: Exception, context: dict = None):
        """
        Log errors synchronously (errors should be immediate)

        Args:
            error: Exception object
            context: Optional context dictionary
        """
        error_data = {
            "job_id": self.job_id,
            "error_type": type(error).__name__,
            "error_message": str(error),
            "timestamp": int(time.time() * 1000),
        }
        if context:
            error_data.update(context)

        # Log errors synchronously to both systems
        if self.spark_logger is not None:
            self.spark_logger.error(f"ERROR: {json.dumps(error_data)}")

        # Also log to Python logger synchronously for errors
        python_logger = self._setup_python_logger()
        python_logger.error(f"ERROR: {json.dumps(error_data)}")

    def log_custom_metrics(self, job_group: str, operation: str, metrics: dict):
        """
        Log custom metrics using Spark logger (for compatibility with existing code)

        Args:
            job_group: Job group identifier
            operation: Operation name
            metrics: Dictionary of metrics
        """
        custom_metrics_event = {
            "Event": "CustomMetricsEvent",
            "Job Group": job_group,
            "Operation": operation,
            "Timestamp": int(time.time() * 1000),
            "Metrics": metrics,
        }

        if self.spark_logger is not None:
            self.spark_logger.info(
                f"CUSTOM_METRICS: {json.dumps(custom_metrics_event)}"
            )
        else:
            # Fallback to Python logger when Spark logger is not available
            python_logger = self._setup_python_logger()
            python_logger.info(f"CUSTOM_METRICS: {json.dumps(custom_metrics_event)}")

    def log_operation_completion(
        self,
        job_group: str,
        operation: str,
        execution_time: float,
        result_count: int = 0,
    ):
        """
        Log operation completion using Spark logger (for compatibility with existing code)

        Args:
            job_group: Job group identifier
            operation: Operation name
            execution_time: Execution time in seconds
            result_count: Number of results
        """
        completion_event = {
            "Event": "CustomOperationCompletion",
            "Job Group": job_group,
            "Operation": operation,
            "Timestamp": int(time.time() * 1000),
            "Execution Time": round(execution_time, 2),
            "Result Count": result_count,
        }

        if self.spark_logger:
            self.spark_logger.info(f"CUSTOM_COMPLETION: {json.dumps(completion_event)}")

    def log_batch_metrics(
        self, batch_id: int, record_count: int, processing_time: float
    ):
        """
        Log batch processing metrics efficiently (only every 10th batch)

        Args:
            batch_id: Batch identifier
            record_count: Number of records processed
            processing_time: Processing time in seconds
        """
        # Only log every 10th batch to reduce overhead
        if batch_id % 10 == 0:
            self.log_performance(
                "batch_processing",
                {
                    "batch_id": batch_id,
                    "record_count": record_count,
                    "processing_time_ms": round(processing_time * 1000, 2),
                    "total_logs": self.log_count,
                },
            )

    def get_performance_summary(self) -> Dict[str, Any]:
        """
        Get performance summary of the logging system

        Returns:
            Dictionary with performance metrics
        """
        total_time = time.time() - self.start_time
        return {
            "total_logs": self.log_count,
            "total_time_seconds": round(total_time, 2),
            "logs_per_second": (
                round(self.log_count / total_time, 2) if total_time > 0 else 0
            ),
            "async_enabled": self.enable_async,
            "job_id": self.job_id,
        }

    def shutdown(self):
        """Shutdown logger, metrics tracker, and Spark session gracefully"""
        # Log shutdown start
        self.log_performance(
            "logger_shutdown",
            {
                "app_name": self.app_name,
                "owns_spark": self._owns_spark,
                "total_logs": self.log_count,
            },
        )

        # 1. Final log sync before shutdown
        if self.enable_log_sync:
            try:
                sync_results = self.force_sync_logs()
                self.log_performance("final_log_sync", sync_results)
            except Exception as e:
                self.log_error(e, {"operation": "final_log_sync"})

        # 2. Shutdown metrics tracker
        if self.metrics_tracker is not None:
            try:
                self.metrics_tracker.shutdown()
                self.metrics_tracker = None
            except Exception as e:
                self.log_error(e, {"operation": "metrics_tracker_shutdown"})

        # 3. Shutdown async logger
        if self.enable_async:
            self.running = False
            if hasattr(self, "log_thread") and self.log_thread.is_alive():
                self.log_thread.join(timeout=5)

        # 4. Shutdown Spark session if we own it
        if self._owns_spark and self.spark is not None:
            try:
                self.spark.stop()
                self.spark = None
            except Exception as e:
                # Use print since logger might be shut down
                print(f"Error shutting down Spark session: {e}")

    def _serialize_config(self, config: dict) -> dict:
        """Convert config to JSON-serializable format"""

        def serialize_value(value):
            if isinstance(value, Enum):
                return value.value
            elif isinstance(value, datetime):
                return value.isoformat()
            elif hasattr(value, "__dict__"):
                return str(value)
            elif isinstance(value, dict):
                return {k: serialize_value(v) for k, v in value.items()}
            elif isinstance(value, (list, tuple)):
                return [serialize_value(v) for v in value]
            else:
                return value

        return serialize_value(config)

    def __enter__(self):
        """Context manager entry"""
        # Log startup with serializable config
        self.log_performance(
            "logger_startup",
            {
                "app_name": self.app_name,
                "owns_spark": self._owns_spark,
                "spark_config": self._serialize_config(self.spark_config),
            },
        )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - ensures graceful shutdown"""
        self.shutdown()

    # Convenience methods for metrics tracking
    def start_operation(self, job_group: str, operation: str):
        """Start tracking metrics for a new operation"""
        if self.metrics_tracker is not None:
            self.metrics_tracker.start_operation(job_group, operation)

    def end_operation(
        self, spark: SparkSession, execution_time: float, result_count: int = 0
    ):
        """End tracking metrics for current operation"""
        if self.metrics_tracker is not None:
            self.metrics_tracker.end_operation(spark, execution_time, result_count)

    def record_data_metrics(self, spark: SparkSession, table_name: str = "skewed_data"):
        """Record data distribution metrics"""
        if self.metrics_tracker is not None:
            self.metrics_tracker.record_data_metrics(spark, table_name)

    def get_metrics(self) -> Dict:
        """Get all recorded metrics"""
        if self.metrics_tracker is not None:
            return self.metrics_tracker.get_metrics()
        return {}

    def sync_logs_to_minio(self, force_sync: bool = False) -> Dict[str, Any]:
        """
        Sync Spark logs to MinIO logs bucket

        Args:
            force_sync: Force sync even if interval hasn't passed

        Returns:
            Dictionary with sync results
        """
        if not self.enable_log_sync or not self.spark:
            return {
                "status": "disabled",
                "reason": "Log sync disabled or no Spark session",
            }

        current_time = time.time()
        if (
            not force_sync
            and (current_time - self.last_sync_time) < self.log_sync_interval
        ):
            return {"status": "skipped", "reason": "Sync interval not reached"}

        try:
            self.log_performance(
                "log_sync_started",
                {
                    "sync_interval": self.log_sync_interval,
                    "last_sync": self.last_sync_time,
                    "current_time": current_time,
                },
            )

            sync_results = {
                "status": "success",
                "files_synced": 0,
                "errors": [],
                "start_time": current_time,
            }

            # Define log file patterns and their MinIO destinations
            log_patterns = {
                "spark-application.log*": "application/",
                "hybrid-observability.log*": "observability/",
                "spark-*.out": "system/",
                "executor/": "executor/",
            }

            for pattern, minio_folder in log_patterns.items():
                try:
                    files_synced = self._sync_log_pattern(pattern, minio_folder)
                    sync_results["files_synced"] += files_synced

                    if files_synced > 0:
                        self.log_business_event(
                            "logs_synced",
                            {
                                "pattern": pattern,
                                "destination": f"{self.minio_logs_bucket}/{minio_folder}",
                                "files_count": files_synced,
                            },
                        )

                except Exception as e:
                    error_msg = f"Error syncing {pattern}: {str(e)}"
                    sync_results["errors"].append(error_msg)
                    self.log_error(e, {"operation": "log_sync", "pattern": pattern})

            # Update last sync time
            self.last_sync_time = current_time

            # Log sync completion
            sync_results["end_time"] = time.time()
            sync_results["duration_seconds"] = (
                sync_results["end_time"] - sync_results["start_time"]
            )

            self.log_performance("log_sync_completed", sync_results)

            return sync_results

        except Exception as e:
            self.log_error(e, {"operation": "log_sync"})
            return {
                "status": "failed",
                "error": str(e),
                "files_synced": 0,
                "errors": [str(e)],
            }

    def _sync_log_pattern(self, pattern: str, minio_folder: str) -> int:
        """
        Sync a specific log pattern to MinIO

        Args:
            pattern: File pattern to match (e.g., "spark-application.log*")
            minio_folder: MinIO destination folder

        Returns:
            Number of files synced
        """
        files_synced = 0

        try:
            # Find matching files
            if pattern.endswith("/"):
                # Directory pattern
                source_path = os.path.join(self.spark_logs_dir, pattern)
                if os.path.exists(source_path):
                    # Use Spark to copy directory
                    source_df = self.spark.read.text(source_path)
                    if source_df.count() > 0:
                        source_df.write.mode("append").text(
                            f"s3a://{self.minio_logs_bucket}/{minio_folder}"
                        )
                        files_synced = 1
            else:
                # File pattern
                file_pattern = os.path.join(self.spark_logs_dir, pattern)
                matching_files = glob.glob(file_pattern)

                for file_path in matching_files:
                    if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
                        # Read file and write to MinIO using Spark
                        file_df = self.spark.read.text(file_path)
                        if file_df.count() > 0:
                            # Create timestamped folder for organization
                            timestamp = datetime.now().strftime("%Y/%m/%d/%H")
                            minio_path = f"s3a://{self.minio_logs_bucket}/{minio_folder}{timestamp}/"

                            file_df.write.mode("append").text(minio_path)
                            files_synced += 1

                            # Log individual file sync
                            self.log_business_event(
                                "log_file_synced",
                                {
                                    "source_file": file_path,
                                    "destination": minio_path,
                                    "file_size_bytes": os.path.getsize(file_path),
                                },
                            )

        except Exception as e:
            self.log_error(
                e,
                {
                    "operation": "sync_log_pattern",
                    "pattern": pattern,
                    "minio_folder": minio_folder,
                },
            )
            raise

        return files_synced

    def auto_sync_logs(self):
        """
        Automatically sync logs if interval has passed
        This can be called periodically or after important operations
        """
        if self.enable_log_sync:
            self.sync_logs_to_minio()

    def force_sync_logs(self) -> Dict[str, Any]:
        """
        Force immediate log sync regardless of interval

        Returns:
            Dictionary with sync results
        """
        return self.sync_logs_to_minio(force_sync=True)

    def log_job_metrics(self, job_id: str, metrics: Dict[str, Any]):
        """Log job-level metrics to a separate job metrics log"""
        try:
            job_log_entry = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "level": "JOB_METRICS",
                "job_id": job_id,
                "metrics": metrics,
            }

            if self.enable_async:
                self.log_queue.put(("job_metrics", job_log_entry))
            else:
                self._log_sync("job_metrics", job_log_entry)

        except Exception as e:
            print(f"Error logging job metrics: {e}")

    def log_batch_metrics(self, job_id: str, batch_id: str, metrics: Dict[str, Any]):
        """Log batch-level metrics to a separate batch metrics log"""
        try:
            batch_log_entry = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "level": "BATCH_METRICS",
                "job_id": job_id,
                "batch_id": batch_id,
                "metrics": metrics,
            }

            if self.enable_async:
                self.log_queue.put(("batch_metrics", batch_log_entry))
            else:
                self._log_sync("batch_metrics", batch_log_entry)

        except Exception as e:
            print(f"Error logging batch metrics: {e}")

    def log_operation_metrics(
        self, job_id: str, batch_id: str, operation_name: str, metrics: Dict[str, Any]
    ):
        """Log operation-level metrics to a separate operation metrics log"""
        try:
            operation_log_entry = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "level": "OPERATION_METRICS",
                "job_id": job_id,
                "batch_id": batch_id,
                "operation_name": operation_name,
                "metrics": metrics,
            }

            if self.enable_async:
                self.log_queue.put(("operation_metrics", operation_log_entry))
            else:
                self._log_sync("operation_metrics", operation_log_entry)

        except Exception as e:
            print(f"Error logging operation metrics: {e}")

    # Enhanced job and batch tracking for parallel workloads
    def start_job_tracking(self, job_id: str, job_metadata: Dict[str, Any] = None):
        """
        Start tracking a new job with comprehensive metrics

        Args:
            job_id: Unique job identifier
            job_metadata: Additional job metadata
        """
        self._job_tracking[job_id] = {
            "job_id": job_id,
            "start_time": datetime.now(timezone.utc),
            "end_time": None,
            "status": "running",
            "batches": [],
            "total_files_to_be_processed": 0,
            "total_files_processed": 0,
            "total_files_successful": 0,
            "total_files_failed": 0,
            "total_files_missing": 0,
            "total_records_processed": 0,
            "total_records_loaded": 0,
            "total_records_failed": 0,
            "total_file_size_bytes": 0,
            "errors": [],
            "error_codes": {},
            "performance_metrics": {},
            "metadata": job_metadata or {},
        }

        self.log_job_metrics(
            job_id,
            {
                "event": "job_started",
                "start_time": self._job_tracking[job_id]["start_time"].isoformat(),
                "status": "running",
                "metadata": job_metadata or {},
            },
        )

    def start_batch_tracking(
        self, job_id: str, batch_id: str, batch_metadata: Dict[str, Any] = None
    ):
        """
        Start tracking a new batch within a job

        Args:
            job_id: Parent job identifier
            batch_id: Unique batch identifier
            batch_metadata: Additional batch metadata
        """
        batch_key = f"{job_id}_{batch_id}"
        self._batch_tracking[batch_key] = {
            "job_id": job_id,
            "batch_id": batch_id,
            "start_time": datetime.now(timezone.utc),
            "end_time": None,
            "status": "running",
            "files_to_be_processed": 0,
            "files_processed": 0,
            "files_successful": 0,
            "files_failed": 0,
            "files_missing": 0,
            "records_processed": 0,
            "records_loaded": 0,
            "records_failed": 0,
            "file_size_bytes": 0,
            "errors": [],
            "error_codes": {},
            "performance_metrics": {},
            "metadata": batch_metadata or {},
        }

        # Add batch to job tracking
        if job_id in self._job_tracking:
            self._job_tracking[job_id]["batches"].append(batch_id)

        self.log_batch_metrics(
            job_id,
            batch_id,
            {
                "event": "batch_started",
                "start_time": self._batch_tracking[batch_key]["start_time"].isoformat(),
                "status": "running",
                "metadata": batch_metadata or {},
            },
        )

    def set_files_to_be_processed(self, job_id: str, batch_id: str, file_count: int):
        """
        Set the number of files to be processed (discovered files)

        Args:
            job_id: Parent job identifier
            batch_id: Batch identifier
            file_count: Number of files discovered/listed
        """
        batch_key = f"{job_id}_{batch_id}"
        if batch_key in self._batch_tracking:
            self._batch_tracking[batch_key]["files_to_be_processed"] = file_count

        # Update job tracking
        if job_id in self._job_tracking:
            self._job_tracking[job_id]["total_files_to_be_processed"] += file_count

    def log_file_success(
        self,
        batch_id: str,
        file_path: str,
        file_size: int = 0,
        records_loaded: int = 0,
        processing_time_ms: float = 0,
        metadata: Dict[str, Any] = None,
    ):
        """
        Log successful file processing

        Args:
            batch_id: Batch identifier
            file_path: Path to the processed file
            file_size: Size of the file in bytes
            records_loaded: Number of records loaded from the file
            processing_time_ms: Processing time in milliseconds
            metadata: Additional file metadata
        """
        # Track file success
        file_key = f"{batch_id}_{file_path}"
        self._file_tracking[file_key] = {
            "batch_id": batch_id,
            "file_path": file_path,
            "status": "success",
            "file_size": file_size,
            "records_loaded": records_loaded,
            "processing_time_ms": processing_time_ms,
            "timestamp": datetime.now(timezone.utc),
            "metadata": metadata or {},
        }

        # Update batch tracking
        for batch_key, batch_data in self._batch_tracking.items():
            if batch_data["batch_id"] == batch_id:
                batch_data["files_processed"] += 1
                batch_data["files_successful"] += 1
                batch_data["records_loaded"] += records_loaded
                batch_data["file_size_bytes"] += file_size
                break

        # Update job tracking
        job_id = None
        for batch_key, batch_data in self._batch_tracking.items():
            if batch_data["batch_id"] == batch_id:
                job_id = batch_data["job_id"]
                break

        if job_id and job_id in self._job_tracking:
            self._job_tracking[job_id]["total_files_processed"] += 1
            self._job_tracking[job_id]["total_files_successful"] += 1
            self._job_tracking[job_id]["total_records_loaded"] += records_loaded
            self._job_tracking[job_id]["total_file_size_bytes"] += file_size

        # Log business event
        self.log_business_event(
            "file_processed_success",
            {
                "batch_id": batch_id,
                "file_path": file_path,
                "file_size": file_size,
                "records_loaded": records_loaded,
                "processing_time_ms": processing_time_ms,
                "metadata": metadata or {},
            },
        )

    def log_file_failure(
        self,
        batch_id: str,
        file_path: str,
        error_type: str,
        error_message: str,
        error_code: str = "E999",
        file_size: int = 0,
        metadata: Dict[str, Any] = None,
    ):
        """
        Log failed file processing

        Args:
            batch_id: Batch identifier
            file_path: Path to the failed file
            error_type: Type of error
            error_message: Error message
            error_code: Error code
            file_size: Size of the file in bytes
            metadata: Additional file metadata
        """
        # Track file failure
        file_key = f"{batch_id}_{file_path}"
        self._file_tracking[file_key] = {
            "batch_id": batch_id,
            "file_path": file_path,
            "status": "failed",
            "error_type": error_type,
            "error_message": error_message,
            "error_code": error_code,
            "file_size": file_size,
            "timestamp": datetime.now(timezone.utc),
            "metadata": metadata or {},
        }

        # Update batch tracking
        for batch_key, batch_data in self._batch_tracking.items():
            if batch_data["batch_id"] == batch_id:
                batch_data["files_processed"] += 1
                batch_data["files_failed"] += 1
                batch_data["file_size_bytes"] += file_size
                batch_data["errors"].append(
                    {
                        "file_path": file_path,
                        "error_type": error_type,
                        "error_message": error_message,
                        "error_code": error_code,
                    }
                )
                batch_data["error_codes"][error_code] = (
                    batch_data["error_codes"].get(error_code, 0) + 1
                )
                break

        # Update job tracking
        job_id = None
        for batch_key, batch_data in self._batch_tracking.items():
            if batch_data["batch_id"] == batch_id:
                job_id = batch_data["job_id"]
                break

        if job_id and job_id in self._job_tracking:
            self._job_tracking[job_id]["total_files_processed"] += 1
            self._job_tracking[job_id]["total_files_failed"] += 1
            self._job_tracking[job_id]["total_file_size_bytes"] += file_size
            self._job_tracking[job_id]["errors"].append(
                {
                    "batch_id": batch_id,
                    "file_path": file_path,
                    "error_type": error_type,
                    "error_message": error_message,
                    "error_code": error_code,
                }
            )
            self._job_tracking[job_id]["error_codes"][error_code] = (
                self._job_tracking[job_id]["error_codes"].get(error_code, 0) + 1
            )

        # Log business event
        self.log_business_event(
            "file_processed_failure",
            {
                "batch_id": batch_id,
                "file_path": file_path,
                "error_type": error_type,
                "error_message": error_message,
                "error_code": error_code,
                "file_size": file_size,
                "metadata": metadata or {},
            },
        )

    def end_batch_tracking(
        self,
        job_id: str,
        batch_id: str,
        expected_files: int = None,
        performance_metrics: Dict[str, Any] = None,
    ):
        """
        End tracking for a batch and determine its status

        Args:
            job_id: Parent job identifier
            batch_id: Batch identifier
            expected_files: Expected number of files (for missing file calculation)
            performance_metrics: Additional performance metrics
        """
        batch_key = f"{job_id}_{batch_id}"
        if batch_key not in self._batch_tracking:
            return

        batch_data = self._batch_tracking[batch_key]
        batch_data["end_time"] = datetime.now(timezone.utc)
        batch_data["duration_ms"] = (
            batch_data["end_time"] - batch_data["start_time"]
        ).total_seconds() * 1000

        # Calculate missing files
        if expected_files is not None:
            batch_data["files_expected"] = expected_files
            batch_data["files_missing"] = max(
                0, expected_files - batch_data["files_processed"]
            )

        # Determine batch status
        if batch_data["files_failed"] == 0 and batch_data["files_missing"] == 0:
            batch_data["status"] = "success"
        elif batch_data["files_successful"] > 0:
            batch_data["status"] = "partial_success"
        else:
            batch_data["status"] = "failed"

        # Add performance metrics
        if performance_metrics:
            batch_data["performance_metrics"].update(performance_metrics)

        # Calculate processing rates
        if batch_data["duration_ms"] > 0:
            batch_data["processing_rate_files_per_sec"] = batch_data[
                "files_processed"
            ] / (batch_data["duration_ms"] / 1000)
            if batch_data["records_loaded"] > 0:
                batch_data["processing_rate_records_per_sec"] = batch_data[
                    "records_loaded"
                ] / (batch_data["duration_ms"] / 1000)

        self.log_batch_metrics(
            job_id,
            batch_id,
            {
                "event": "batch_completed",
                "end_time": batch_data["end_time"].isoformat(),
                "duration_ms": batch_data["duration_ms"],
                "status": batch_data["status"],
                "files_processed": batch_data["files_processed"],
                "files_successful": batch_data["files_successful"],
                "files_failed": batch_data["files_failed"],
                "files_missing": batch_data["files_missing"],
                "records_loaded": batch_data["records_loaded"],
                "file_size_bytes": batch_data["file_size_bytes"],
                "error_count": len(batch_data["errors"]),
                "error_codes": batch_data["error_codes"],
                "performance_metrics": batch_data["performance_metrics"],
            },
        )

    def end_job_tracking(
        self,
        job_id: str,
        expected_files: int = None,
        performance_metrics: Dict[str, Any] = None,
    ):
        """
        End tracking for a job and determine its overall status

        Args:
            job_id: Job identifier
            expected_files: Expected number of files (for missing file calculation)
            performance_metrics: Additional performance metrics
        """
        if job_id not in self._job_tracking:
            return

        job_data = self._job_tracking[job_id]
        job_data["end_time"] = datetime.now(timezone.utc)
        job_data["duration_ms"] = (
            job_data["end_time"] - job_data["start_time"]
        ).total_seconds() * 1000

        # Calculate missing files
        if expected_files is not None:
            job_data["total_files_expected"] = expected_files
            job_data["total_files_missing"] = max(
                0, expected_files - job_data["total_files_processed"]
            )

        # Determine job status based on all batches
        failed_batches = 0
        partial_batches = 0
        successful_batches = 0

        for batch_id in job_data["batches"]:
            batch_key = f"{job_id}_{batch_id}"
            if batch_key in self._batch_tracking:
                batch_status = self._batch_tracking[batch_key]["status"]
                if batch_status == "failed":
                    failed_batches += 1
                elif batch_status == "partial_success":
                    partial_batches += 1
                elif batch_status == "success":
                    successful_batches += 1

        # Determine overall job status
        if failed_batches == 0 and partial_batches == 0:
            job_data["status"] = "success"
        elif failed_batches == len(job_data["batches"]):
            job_data["status"] = "failed"
        else:
            job_data["status"] = "partial_success"

        # Add performance metrics
        if performance_metrics:
            job_data["performance_metrics"].update(performance_metrics)

        # Calculate processing rates
        if job_data["duration_ms"] > 0:
            job_data["processing_rate_files_per_sec"] = job_data[
                "total_files_processed"
            ] / (job_data["duration_ms"] / 1000)
            if job_data["total_records_loaded"] > 0:
                job_data["processing_rate_records_per_sec"] = job_data[
                    "total_records_loaded"
                ] / (job_data["duration_ms"] / 1000)

        self.log_job_metrics(
            job_id,
            {
                "event": "job_completed",
                "end_time": job_data["end_time"].isoformat(),
                "duration_ms": job_data["duration_ms"],
                "status": job_data["status"],
                "total_files_processed": job_data["total_files_processed"],
                "total_files_successful": job_data["total_files_successful"],
                "total_files_failed": job_data["total_files_failed"],
                "total_files_missing": job_data["total_files_missing"],
                "total_records_loaded": job_data["total_records_loaded"],
                "total_file_size_bytes": job_data["total_file_size_bytes"],
                "batches_total": len(job_data["batches"]),
                "batches_successful": successful_batches,
                "batches_partial": partial_batches,
                "batches_failed": failed_batches,
                "error_count": len(job_data["errors"]),
                "error_codes": job_data["error_codes"],
                "performance_metrics": job_data["performance_metrics"],
            },
        )

    def get_job_summary(self, job_id: str) -> Dict[str, Any]:
        """Get comprehensive summary of a job including all its batches"""
        if job_id not in self._job_tracking:
            return {}

        job_data = self._job_tracking[job_id].copy()

        # Add batch details
        job_data["batch_details"] = {}
        for batch_id in job_data["batches"]:
            batch_key = f"{job_id}_{batch_id}"
            if batch_key in self._batch_tracking:
                job_data["batch_details"][batch_id] = self._batch_tracking[
                    batch_key
                ].copy()

        return job_data

    def get_batch_summary(self, job_id: str, batch_id: str) -> Dict[str, Any]:
        """Get comprehensive summary of a specific batch"""
        batch_key = f"{job_id}_{batch_id}"
        if batch_key not in self._batch_tracking:
            return {}

        return self._batch_tracking[batch_key].copy()

    def _log_sync(self, log_type: str, log_entry: Dict[str, Any]):
        """Synchronous logging fallback"""
        python_logger = self._setup_python_logger()
        python_logger.info(f"{log_type.upper()}: {json.dumps(log_entry)}")
