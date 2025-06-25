#!/usr/bin/env python3
"""
Performance-Optimized PySpark Logging
Demonstrates how to minimize logging performance impact
"""

import os
import logging
import json
import time
import threading
import queue
from concurrent.futures import ThreadPoolExecutor
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from typing import Dict, Any
import atexit

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "admin")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "password")

class AsyncLogger:
    """Asynchronous logger to minimize performance impact"""
    
    def __init__(self, spark: SparkSession, app_name: str = "AsyncApp", 
                 buffer_size: int = 1000, flush_interval: int = 5):
        self.spark = spark
        self.app_name = app_name
        self.job_id = spark.sparkContext.applicationId
        
        # Setup Spark logger (already optimized)
        self.spark_logger = spark.sparkContext._jvm.org.apache.log4j.LogManager.getLogger(app_name)
        
        # Setup async Python logger
        self.log_queue = queue.Queue(maxsize=buffer_size)
        self.flush_interval = flush_interval
        self.running = True
        
        # Start async logging thread
        self.log_thread = threading.Thread(target=self._async_log_worker, daemon=True)
        self.log_thread.start()
        
        # Register cleanup
        atexit.register(self.shutdown)
    
    def _async_log_worker(self):
        """Background thread for processing log messages"""
        python_logger = self._setup_python_logger()
        
        while self.running:
            try:
                # Batch process logs
                logs = []
                start_time = time.time()
                
                while len(logs) < 100 and (time.time() - start_time) < self.flush_interval:
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
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                datefmt='%H:%M:%S'  # Shorter timestamp format
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            
            # Disable propagation to avoid duplicate logs
            logger.propagate = False
        
        return logger
    
    def log_performance(self, operation: str, metrics: dict):
        """Log performance metrics using Spark logger (synchronous, but optimized)"""
        # Keep performance metrics synchronous for immediate availability
        structured_metrics = {
            "job_id": self.job_id,
            "operation": operation,
            "timestamp": int(time.time() * 1000),
            **metrics
        }
        self.spark_logger.info(f"PERFORMANCE_METRICS: {json.dumps(structured_metrics)}")
    
    def log_business_event(self, event_type: str, event_data: dict):
        """Log business events asynchronously"""
        structured_event = {
            "job_id": self.job_id,
            "event_type": event_type,
            "timestamp": int(time.time() * 1000),
            **event_data
        }
        
        # Non-blocking async logging
        try:
            self.log_queue.put_nowait(f"BUSINESS_EVENT: {json.dumps(structured_event)}")
        except queue.Full:
            # If queue is full, drop the log (better than blocking)
            pass
    
    def log_debug(self, message: str, data: dict = None):
        """Log debug information asynchronously"""
        if data:
            log_message = f"DEBUG: {message} - {json.dumps(data)}"
        else:
            log_message = f"DEBUG: {message}"
        
        try:
            self.log_queue.put_nowait(log_message)
        except queue.Full:
            pass  # Drop debug logs if queue is full
    
    def log_error(self, error: Exception, context: dict = None):
        """Log errors synchronously (errors should be immediate)"""
        error_data = {
            "job_id": self.job_id,
            "error_type": type(error).__name__,
            "error_message": str(error),
            "timestamp": int(time.time() * 1000)
        }
        if context:
            error_data.update(context)
        
        # Log errors synchronously to both systems
        self.spark_logger.error(f"ERROR: {json.dumps(error_data)}")
        
        # Also log to Python logger synchronously for errors
        python_logger = self._setup_python_logger()
        python_logger.error(f"ERROR: {json.dumps(error_data)}")
    
    def shutdown(self):
        """Shutdown async logger gracefully"""
        self.running = False
        if self.log_thread.is_alive():
            self.log_thread.join(timeout=5)

class PerformanceOptimizedLogger:
    """Logger with performance optimizations"""
    
    def __init__(self, spark: SparkSession, app_name: str = "PerfApp"):
        self.spark = spark
        self.app_name = app_name
        self.job_id = spark.sparkContext.applicationId
        
        # Use Spark logger for critical metrics
        self.spark_logger = spark.sparkContext._jvm.org.apache.log4j.LogManager.getLogger(app_name)
        
        # Use async logger for business events
        self.async_logger = AsyncLogger(spark, app_name)
        
        # Performance counters
        self.log_count = 0
        self.start_time = time.time()
    
    def log_performance(self, operation: str, metrics: dict):
        """Log performance metrics (synchronous, optimized)"""
        self.spark_logger.info(f"PERFORMANCE_METRICS: {json.dumps({
            'job_id': self.job_id,
            'operation': operation,
            'timestamp': int(time.time() * 1000),
            **metrics
        })}")
    
    def log_business_event(self, event_type: str, event_data: dict):
        """Log business events (asynchronous)"""
        self.async_logger.log_business_event(event_type, event_data)
        self.log_count += 1
    
    def log_batch_metrics(self, batch_id: int, record_count: int, processing_time: float):
        """Log batch processing metrics efficiently"""
        # Only log every 10th batch to reduce overhead
        if batch_id % 10 == 0:
            self.log_performance("batch_processing", {
                "batch_id": batch_id,
                "record_count": record_count,
                "processing_time_ms": round(processing_time * 1000, 2),
                "total_logs": self.log_count
            })
    
    def shutdown(self):
        """Shutdown all loggers"""
        self.async_logger.shutdown()

def get_spark_session() -> SparkSession:
    """Create Spark session with performance optimizations"""
    spark = (SparkSession.builder.appName("PerformanceLogging")
        .config("spark.sql.streaming.schemaInference", "true")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.defaultCatalog", "iceberg")
        .config("spark.sql.catalog.iceberg.type", "rest")
        .config("spark.sql.catalog.iceberg.uri", "http://spark-rest:8181")
        .config("spark.sql.catalog.iceberg.s3.endpoint", "http://minio:9000")
        .config("spark.sql.catalog.iceberg.warehouse", "s3://data/wh")
        .config("spark.sql.catalog.iceberg.s3.access-key", AWS_ACCESS_KEY_ID)
        .config("spark.sql.catalog.iceberg.s3.secret-key", AWS_SECRET_ACCESS_KEY)
        .config("spark.sql.catalog.iceberg.s3.region", 'us-east-1')
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.region", "us-east-1")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # Performance optimizations
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", "file:///opt/bitnami/spark/logs")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .master("spark://spark-master:7077")
        .getOrCreate())
    
    spark.sparkContext.setLogLevel("WARN")  # Reduce Spark's own logging
    return spark

def process_data_with_optimized_logging(spark: SparkSession, num_batches: int = 10):
    """Process data with performance-optimized logging"""
    logger = PerformanceOptimizedLogger(spark, "DataProcessor")
    
    # Log application start
    logger.log_business_event("application_started", {
        "version": "1.0.0",
        "batches_to_process": num_batches
    })
    
    total_records = 0
    
    for batch_id in range(num_batches):
        batch_start = time.time()
        
        # Create batch data
        data = [(i + batch_id * 1000, f"value_{i}", i * 2) for i in range(1000)]
        df = spark.createDataFrame(data, ["id", "description", "value"])
        
        # Process data
        result = df.filter(col("value") > 100).groupBy("description").count()
        count = result.count()
        
        batch_time = time.time() - batch_start
        total_records += count
        
        # Log batch metrics (efficiently)
        logger.log_batch_metrics(batch_id, count, batch_time)
        
        # Log business event (asynchronously)
        logger.log_business_event("batch_completed", {
            "batch_id": batch_id,
            "records_processed": count,
            "processing_time_ms": round(batch_time * 1000, 2)
        })
    
    # Log final performance metrics
    total_time = time.time() - logger.start_time
    logger.log_performance("data_processing_complete", {
        "total_records": total_records,
        "total_time_ms": round(total_time * 1000, 2),
        "records_per_second": round(total_records / total_time, 2),
        "total_logs_generated": logger.log_count
    })
    
    logger.shutdown()
    return total_records

def benchmark_logging_performance(spark: SparkSession):
    """Benchmark different logging approaches"""
    print("=== LOGGING PERFORMANCE BENCHMARK ===")
    
    # Test 1: No logging
    print("\n1. Testing without logging...")
    start_time = time.time()
    data = [(i, f"value_{i}") for i in range(10000)]
    df = spark.createDataFrame(data, ["id", "value"])
    result = df.filter(col("id") > 5000).count()
    no_logging_time = time.time() - start_time
    print(f"   Time without logging: {no_logging_time:.3f}s")
    
    # Test 2: Synchronous Python logging
    print("\n2. Testing with synchronous Python logging...")
    import logging
    logging.basicConfig(level=logging.INFO)
    test_logger = logging.getLogger("test")
    
    start_time = time.time()
    for i in range(1000):
        test_logger.info(f"Processing record {i}")
    sync_logging_time = time.time() - start_time
    print(f"   Time with sync logging: {sync_logging_time:.3f}s")
    
    # Test 3: Asynchronous logging
    print("\n3. Testing with asynchronous logging...")
    async_logger = AsyncLogger(spark, "BenchmarkApp")
    
    start_time = time.time()
    for i in range(1000):
        async_logger.log_business_event("record_processed", {"record_id": i})
    async_logging_time = time.time() - start_time
    async_logger.shutdown()
    print(f"   Time with async logging: {async_logging_time:.3f}s")
    
    # Results
    print(f"\n=== BENCHMARK RESULTS ===")
    print(f"Baseline (no logging):     {no_logging_time:.3f}s")
    print(f"Synchronous logging:       {sync_logging_time:.3f}s ({(sync_logging_time/no_logging_time-1)*100:.1f}% overhead)")
    print(f"Asynchronous logging:      {async_logging_time:.3f}s ({(async_logging_time/no_logging_time-1)*100:.1f}% overhead)")

def main():
    """Main function demonstrating performance-optimized logging"""
    print("Starting Performance-Optimized PySpark Logging Demo...")
    
    try:
        # Create Spark session
        spark = get_spark_session()
        
        # Run performance benchmark
        benchmark_logging_performance(spark)
        
        # Demonstrate optimized logging
        print("\n=== DEMONSTRATING OPTIMIZED LOGGING ===")
        result_count = process_data_with_optimized_logging(spark, num_batches=5)
        
        print(f"\n=== PERFORMANCE LOGGING COMPLETE ===")
        print(f"Processed {result_count} records with optimized logging")
        print("\nPerformance optimizations used:")
        print("1. Asynchronous logging for business events")
        print("2. Synchronous logging only for critical metrics")
        print("3. Batch processing of log messages")
        print("4. Queue-based buffering to prevent blocking")
        print("5. Selective logging (every 10th batch)")
        
    except Exception as e:
        print(f"Error in performance logging demo: {e}")
        raise

if __name__ == "__main__":
    main() 