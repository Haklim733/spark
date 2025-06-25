import atexit
import json
import logging
import queue
import threading
import time
from typing import Dict, Any
from pyspark.sql import SparkSession


class HybridLogger:
    """
    Hybrid logging system combining Spark's built-in logging with asynchronous Python logging
    for optimal performance and debugging capabilities.
    """

    def __init__(
        self,
        spark: SparkSession,
        app_name: str = "HybridApp",
        buffer_size: int = 1000,
        flush_interval: int = 5,
        enable_async: bool = True,
    ):
        self.spark = spark
        self.app_name = app_name
        self.job_id = spark.sparkContext.applicationId
        self.enable_async = enable_async

        # Setup Spark logger for performance metrics (synchronous)
        self.spark_logger = (
            spark.sparkContext._jvm.org.apache.log4j.LogManager.getLogger(app_name)
        )

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
        self.spark_logger.info(f"PERFORMANCE_METRICS: {json.dumps(structured_metrics)}")

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

        self.spark_logger.info(f"CUSTOM_METRICS: {json.dumps(custom_metrics_event)}")

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
        """Shutdown async logger gracefully"""
        if self.enable_async:
            self.running = False
            if self.log_thread.is_alive():
                self.log_thread.join(timeout=5)


# Global metrics tracking with hybrid logging
class MetricsTracker:
    def __init__(self, spark: SparkSession):
        self.metrics = {}
        self.current_job_group = None
        self.current_operation = None
        # Initialize hybrid logger
        self.logger = HybridLogger(spark, "ShufflingMetrics", enable_async=True)

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

        # Log operation start asynchronously
        self.logger.log_business_event(
            "operation_started",
            {
                "job_group": job_group,
                "operation": operation,
                "start_time": int(time.time() * 1000),
            },
        )

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
                # This is a rough estimate based on data size and operations
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

                # Use hybrid logger for custom metrics
                metrics_data = {
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

                self.logger.log_custom_metrics(
                    self.current_job_group, self.current_operation, metrics_data
                )

        except Exception as e:
            self.logger.log_error(
                e, {"operation": "record_data_metrics", "table_name": table_name}
            )

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

        # Use hybrid logger for operation completion
        self.logger.log_operation_completion(
            self.current_job_group, self.current_operation, execution_time, result_count
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
            self.logger.log_error(
                e, {"operation": "save_metrics", "filename": filename}
            )

    def get_logging_performance_summary(self) -> Dict:
        """Get performance summary of the logging system"""
        return self.logger.get_performance_summary()

    def shutdown(self):
        """Shutdown the hybrid logger"""
        self.logger.shutdown()
