#!/usr/bin/env python3
"""
Parse Spark Event Logs to extract shuffling metrics and performance data
"""

import json
import sys
from collections import defaultdict
import argparse


# ANSI color codes for better readability
class Colors:
    HEADER = "\033[95m"
    OKBLUE = "\033[94m"
    OKCYAN = "\033[96m"
    OKGREEN = "\033[92m"
    WARNING = "\033[93m"
    FAIL = "\033[91m"
    ENDC = "\033[0m"
    BOLD = "\033[1m"
    UNDERLINE = "\033[4m"


def print_header(title):
    """Print a formatted header"""
    print(f"\n{Colors.HEADER}{Colors.BOLD}{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}{Colors.ENDC}")


def print_section(title):
    """Print a formatted section header"""
    print(f"\n{Colors.OKBLUE}{Colors.BOLD}📊 {title}{Colors.ENDC}")
    print(f"{Colors.OKBLUE}{'-'*50}{Colors.ENDC}")


def print_metric(label, value, unit="", is_good=True, is_warning=False):
    """Print a formatted metric"""
    color = (
        Colors.OKGREEN if is_good else (Colors.WARNING if is_warning else Colors.FAIL)
    )
    print(f"  {label}: {color}{value}{unit}{Colors.ENDC}")


def format_duration(ms):
    """Format duration in human readable format"""
    if ms < 1000:
        return f"{ms:.0f}ms"
    elif ms < 60000:
        return f"{ms/1000:.1f}s"
    else:
        return f"{ms/60000:.1f}min"


def format_bytes(bytes_val):
    """Format bytes in human readable format"""
    if bytes_val < 1024:
        return f"{bytes_val}B"
    elif bytes_val < 1024 * 1024:
        return f"{bytes_val/1024:.1f}KB"
    elif bytes_val < 1024 * 1024 * 1024:
        return f"{bytes_val/(1024*1024):.1f}MB"
    else:
        return f"{bytes_val/(1024*1024*1024):.1f}GB"


def parse_spark_logs(log_file_path):
    """Parse Spark event logs and extract key metrics"""

    print_header("SPARK PERFORMANCE ANALYSIS")
    print(f"📁 Analyzing log file: {log_file_path}")

    # Metrics storage
    jobs = {}
    stages = {}
    tasks = defaultdict(list)
    shuffle_metrics = defaultdict(dict)
    custom_metrics = {}  # Store custom metrics from our application

    # Summary statistics
    total_jobs = 0
    total_duration = 0
    total_shuffle_write = 0
    total_shuffle_read = 0
    issues_found = []

    try:
        with open(log_file_path, "r") as f:
            for line_num, line in enumerate(f, 1):
                try:
                    # Check for custom metrics lines first
                    if line.strip().startswith("CUSTOM_METRICS:"):
                        custom_metrics_json = line.strip().replace(
                            "CUSTOM_METRICS: ", ""
                        )
                        custom_event = json.loads(custom_metrics_json)

                        job_group = custom_event.get("Job Group")
                        operation = custom_event.get("Operation")
                        metrics = custom_event.get("Metrics", {})

                        if job_group not in custom_metrics:
                            custom_metrics[job_group] = {}
                        custom_metrics[job_group][operation] = metrics
                        continue

                    elif line.strip().startswith("CUSTOM_COMPLETION:"):
                        completion_json = line.strip().replace(
                            "CUSTOM_COMPLETION: ", ""
                        )
                        completion_event = json.loads(completion_json)

                        job_group = completion_event.get("Job Group")
                        operation = completion_event.get("Operation")
                        execution_time = completion_event.get("Execution Time", 0)
                        result_count = completion_event.get("Result Count", 0)

                        if (
                            job_group in custom_metrics
                            and operation in custom_metrics[job_group]
                        ):
                            custom_metrics[job_group][operation].update(
                                {
                                    "execution_time": execution_time,
                                    "result_count": result_count,
                                }
                            )
                        continue

                    # Parse standard Spark events
                    event = json.loads(line.strip())
                    event_type = event.get("Event", "")

                    # Parse different event types
                    if event_type == "SparkListenerJobStart":
                        job_id = event.get("Job ID")
                        jobs[job_id] = {
                            "start_time": event.get("Submission Time"),
                            "stages": [
                                stage["Stage ID"]
                                for stage in event.get("Stage Infos", [])
                            ],
                            "description": event.get("Properties", {}).get(
                                "spark.job.description", "Unknown"
                            ),
                            "group_id": event.get("Properties", {}).get(
                                "spark.jobGroup.id", "Unknown"
                            ),
                        }
                        total_jobs += 1

                    elif event_type == "SparkListenerJobEnd":
                        job_id = event.get("Job ID")
                        if job_id in jobs:
                            jobs[job_id]["end_time"] = event.get("Completion Time")
                            jobs[job_id]["result"] = event.get("Job Result", {}).get(
                                "Result"
                            )

                    elif event_type == "SparkListenerStageCompleted":
                        stage_info = event.get("Stage Info", {})
                        stage_id = stage_info.get("Stage ID")
                        stages[stage_id] = {
                            "name": stage_info.get("Stage Name"),
                            "completion_time": stage_info.get("Completion Time"),
                            "submission_time": stage_info.get("Submission Time"),
                            "num_tasks": stage_info.get("Number of Tasks"),
                            "accumulables": stage_info.get("Accumulables", []),
                        }

                        # Extract shuffle metrics from accumulables
                        for acc in stage_info.get("Accumulables", []):
                            name = acc.get("Name", "")
                            value = acc.get("Value", 0)

                            # Convert value to int if it's a string
                            try:
                                if isinstance(value, str):
                                    value = int(value)
                                else:
                                    value = int(value)
                            except (ValueError, TypeError):
                                value = 0

                            if "shuffle" in name.lower():
                                shuffle_metrics[stage_id][name] = value

                    elif event_type == "SparkListenerTaskEnd":
                        task_info = event.get("Task Info", {})
                        stage_id = event.get("Stage ID")
                        task_id = task_info.get("Task ID")

                        # Extract task metrics
                        task_metrics = {
                            "task_id": task_id,
                            "executor_id": task_info.get("Executor ID"),
                            "duration": task_info.get("Finish Time", 0)
                            - task_info.get("Launch Time", 0),
                            "shuffle_read": 0,
                            "shuffle_write": 0,
                            "memory_spilled": 0,
                            "disk_spilled": 0,
                        }

                        # Extract shuffle and spill metrics from accumulables
                        for acc in task_info.get("Accumulables", []):
                            name = acc.get("Name", "")
                            value = acc.get("Value", 0)

                            # Convert value to int if it's a string
                            try:
                                if isinstance(value, str):
                                    value = int(value)
                                else:
                                    value = int(value)
                            except (ValueError, TypeError):
                                value = 0

                            if "shuffle.read" in name.lower():
                                task_metrics["shuffle_read"] += value
                            elif "shuffle.write" in name.lower():
                                task_metrics["shuffle_write"] += value
                            elif "memory.bytes.spilled" in name.lower():
                                task_metrics["memory_spilled"] = value
                            elif "disk.bytes.spilled" in name.lower():
                                task_metrics["disk_spilled"] = value

                        tasks[stage_id].append(task_metrics)

                except json.JSONDecodeError:
                    continue  # Skip malformed lines
                except Exception as e:
                    print(f"Error parsing line {line_num}: {e}")
                    continue

        # Calculate summary statistics
        for job_id, job_info in jobs.items():
            if "end_time" in job_info and "start_time" in job_info:
                duration = job_info["end_time"] - job_info["start_time"]
                total_duration += duration

        # Analyze the parsed data
        analyze_jobs_human_readable(
            jobs, stages, shuffle_metrics, tasks, issues_found, custom_metrics
        )

        # Print summary
        print_summary(total_jobs, total_duration, issues_found)

        # Display custom metrics from logs
        display_custom_metrics_from_logs(custom_metrics)

    except FileNotFoundError:
        print(f"{Colors.FAIL}❌ Log file not found: {log_file_path}{Colors.ENDC}")
        print("Available log files:")
        print("docker exec spark-master ls -la /opt/bitnami/spark/logs/ | grep app-")
    except Exception as e:
        print(f"{Colors.FAIL}❌ Error reading log file: {e}{Colors.ENDC}")
        import traceback

        traceback.print_exc()


def display_custom_metrics_from_logs(custom_metrics):
    """Display custom metrics found in the Spark logs"""
    if not custom_metrics:
        print(
            f"\n  {Colors.WARNING}📋 No custom metrics found in Spark logs{Colors.ENDC}"
        )
        print(f"    Custom metrics are now integrated into Spark logs during execution")
        return

    print_header("CUSTOM METRICS FROM SPARK LOGS")

    for job_group, operations in custom_metrics.items():
        print(f"\n  {Colors.OKBLUE}{Colors.BOLD}📊 {job_group.upper()}:{Colors.ENDC}")

        for operation, data in operations.items():
            print(f"\n    {Colors.OKCYAN}🔧 {operation}:{Colors.ENDC}")

            # Data skew analysis
            if "data_skew_ratio" in data and data["data_skew_ratio"] > 0:
                skew_ratio = data["data_skew_ratio"]
                if skew_ratio > 10:
                    print(
                        f"      📈 Data Skew: {Colors.FAIL}🚨 {skew_ratio:.1f}x (SEVERE){Colors.ENDC}"
                    )
                elif skew_ratio > 5:
                    print(
                        f"      📈 Data Skew: {Colors.WARNING}⚠️  {skew_ratio:.1f}x (HIGH){Colors.ENDC}"
                    )
                else:
                    print(
                        f"      📈 Data Skew: {Colors.OKGREEN}✅ {skew_ratio:.1f}x (LOW){Colors.ENDC}"
                    )

            # Data volume metrics
            if "record_count" in data:
                print(f"      📊 Records: {data['record_count']:,}")
            if "unique_keys" in data:
                print(f"      🔑 Unique Keys: {data['unique_keys']}")
            if "max_key_frequency" in data:
                print(f"      📈 Max Key Frequency: {data['max_key_frequency']}")

            # Performance metrics
            if "execution_time" in data:
                exec_time = data["execution_time"]
                if exec_time > 10:
                    print(
                        f"      ⏱️  Execution: {Colors.FAIL}🚨 {exec_time}s (SLOW){Colors.ENDC}"
                    )
                elif exec_time > 5:
                    print(
                        f"      ⏱️  Execution: {Colors.WARNING}⚠️  {exec_time}s (MODERATE){Colors.ENDC}"
                    )
                else:
                    print(
                        f"      ⏱️  Execution: {Colors.OKGREEN}✅ {exec_time}s (FAST){Colors.ENDC}"
                    )

            # Configuration metrics
            if "shuffle_partitions" in data:
                partitions = data["shuffle_partitions"]
                if partitions > 1000:
                    print(
                        f"      🔧 Shuffle Partitions: {Colors.WARNING}⚠️  {partitions} (TOO MANY){Colors.ENDC}"
                    )
                elif partitions < 10:
                    print(
                        f"      🔧 Shuffle Partitions: {Colors.WARNING}⚠️  {partitions} (TOO FEW){Colors.ENDC}"
                    )
                else:
                    print(
                        f"      🔧 Shuffle Partitions: {Colors.OKGREEN}✅ {partitions}{Colors.ENDC}"
                    )

            if "execution_plan_complexity" in data:
                complexity = data["execution_plan_complexity"]
                if complexity == "high":
                    print(
                        f"      🧠 Plan Complexity: {Colors.WARNING}⚠️  {complexity.upper()}{Colors.ENDC}"
                    )
                else:
                    print(
                        f"      🧠 Plan Complexity: {Colors.OKGREEN}✅ {complexity.upper()}{Colors.ENDC}"
                    )

            # Memory metrics
            if "executor_memory" in data:
                print(f"      🖥️  Executor Memory: {data['executor_memory']}")
            if "driver_memory" in data:
                print(f"      🖥️  Driver Memory: {data['driver_memory']}")
            if "memory_fraction" in data:
                mem_frac = data["memory_fraction"]
                if mem_frac < 0.5:
                    print(
                        f"      📊 Memory Fraction: {Colors.WARNING}⚠️  {mem_frac} (LOW){Colors.ENDC}"
                    )
                else:
                    print(
                        f"      📊 Memory Fraction: {Colors.OKGREEN}✅ {mem_frac}{Colors.ENDC}"
                    )
            if "storage_fraction" in data:
                storage_frac = data["storage_fraction"]
                if storage_frac < 0.3:
                    print(
                        f"      💾 Storage Fraction: {Colors.WARNING}⚠️  {storage_frac} (LOW){Colors.ENDC}"
                    )
                else:
                    print(
                        f"      💾 Storage Fraction: {Colors.OKGREEN}✅ {storage_frac}{Colors.ENDC}"
                    )
            if "estimated_memory_mb" in data:
                est_mem = data["estimated_memory_mb"]
                if est_mem > 1000:  # > 1GB
                    print(
                        f"      💾 Estimated Memory: {Colors.FAIL}🚨 {est_mem:.1f}MB (HIGH){Colors.ENDC}"
                    )
                elif est_mem > 500:  # > 500MB
                    print(
                        f"      💾 Estimated Memory: {Colors.WARNING}⚠️  {est_mem:.1f}MB (MODERATE){Colors.ENDC}"
                    )
                else:
                    print(
                        f"      💾 Estimated Memory: {Colors.OKGREEN}✅ {est_mem:.1f}MB{Colors.ENDC}"
                    )


def analyze_jobs_human_readable(
    jobs, stages, shuffle_metrics, tasks, issues_found, custom_metrics=None
):
    """Analyze job performance in human-readable format with grouped stages and tasks"""
    if custom_metrics is None:
        custom_metrics = {}

    print_section("JOB PERFORMANCE SUMMARY")

    if not jobs:
        print("  No jobs found in log file")
        return

    # Sort jobs by duration (longest first)
    job_list = []
    function_summary = {}  # Track function execution summary

    for job_id, job_info in jobs.items():
        if "end_time" in job_info and "start_time" in job_info:
            duration = job_info["end_time"] - job_info["start_time"]
            job_list.append((job_id, job_info, duration))

    job_list.sort(key=lambda x: x[2], reverse=True)

    print(f"  📈 Total Jobs: {len(job_list)}")

    for i, (job_id, job_info, duration) in enumerate(job_list, 1):
        print(f"\n  {Colors.HEADER}{Colors.BOLD}{'='*50}{Colors.ENDC}")
        print(f"  {Colors.BOLD}Job #{i} (ID: {job_id}){Colors.ENDC}")
        print(f"  {Colors.HEADER}{'='*50}{Colors.ENDC}")

        # Duration analysis
        duration_sec = duration / 1000
        if duration_sec > 10:
            print_metric("⏱️  Duration", format_duration(duration), "", False)
            issues_found.append(
                f"Job {job_id} took {format_duration(duration)} - very slow!"
            )
        elif duration_sec > 5:
            print_metric("⏱️  Duration", format_duration(duration), "", True, True)
            issues_found.append(
                f"Job {job_id} took {format_duration(duration)} - could be optimized"
            )
        else:
            print_metric("⏱️  Duration", format_duration(duration), "", True)

        # Enhanced job details with function identification
        description = job_info.get("description", "Unknown")
        group_id = job_info.get("group_id", "Unknown")

        # Extract function name and operation details from description
        function_name = "Unknown"
        operation_details = ""

        if description != "Unknown":
            if "() - " in description:
                parts = description.split("() - ", 1)
                function_name = parts[0] + "()"
                operation_details = parts[1] if len(parts) > 1 else ""
            elif "()" in description:
                function_name = description.split("()")[0] + "()"
                operation_details = (
                    description.split("()", 1)[1] if "()" in description else ""
                )

            print(f"  🔧 Operation: {Colors.OKCYAN}{description}{Colors.ENDC}")
            if operation_details:
                print(f"  📋 Details: {Colors.OKCYAN}{operation_details}{Colors.ENDC}")
        else:
            print(f"  🔧 Operation: {Colors.WARNING}{description}{Colors.ENDC}")

        # Display job group information and function mapping
        if group_id != "Unknown":
            print(f"  🏷️  Group: {Colors.OKCYAN}{group_id}{Colors.ENDC}")

            # Map group IDs to function names for better identification
            group_to_function = {
                "data_generation": "generate_skewed_data()",
                "data_skew_demo": "demonstrate_data_skew()",
                "large_partitions_demo": "demonstrate_large_shuffle_partitions()",
                "small_partitions_demo": "demonstrate_small_shuffle_partitions()",
                "cartesian_demo": "demonstrate_cartesian_product()",
                "join_strategies_demo": "demonstrate_join_strategies()",
                "shuffle_join_demo": "demonstrate_join_strategies() - Shuffle Join",
                "broadcast_join_demo": "demonstrate_join_strategies() - Broadcast Join",
                "window_functions_demo": "demonstrate_window_shuffling()",
                "repartitioning_demo": "demonstrate_repartitioning()",
                "coalescing_demo": "demonstrate_coalescing()",
                "bucketing_demo": "demonstrate_bucketing()",
                "adaptive_query_demo": "demonstrate_adaptive_query_execution()",
                "shuffling_issues_demo": "demonstrate_shuffling_issues()",
            }

            if group_id in group_to_function:
                actual_function = group_to_function[group_id]
                # Only show function if it's different from what we extracted from description
                if function_name == "Unknown" or function_name != actual_function:
                    print(
                        f"  📝 Function: {Colors.OKBLUE}{actual_function}{Colors.ENDC}"
                    )
                function_name = (
                    actual_function  # Use the mapped function name for summary
                )

                # Track function summary
                if actual_function not in function_summary:
                    function_summary[actual_function] = {
                        "jobs": 0,
                        "total_duration": 0,
                        "job_ids": [],
                    }
                function_summary[actual_function]["jobs"] += 1
                function_summary[actual_function]["total_duration"] += duration
                function_summary[actual_function]["job_ids"].append(job_id)
        else:
            print(f"  🏷️  Group: {Colors.WARNING}No job group{Colors.ENDC}")
            # If we have a function name from description, show it
            if function_name != "Unknown":
                print(f"  📝 Function: {Colors.OKBLUE}{function_name}{Colors.ENDC}")

        result = job_info.get("result", "Unknown")
        if result == "JobSucceeded":
            print(f"  ✅ Status: {Colors.OKGREEN}{result}{Colors.ENDC}")
        else:
            print(f"  ❌ Status: {Colors.FAIL}{result}{Colors.ENDC}")
            issues_found.append(f"Job {job_id} failed: {result}")

        stages_count = len(job_info.get("stages", []))
        print(f"  📊 Stages: {stages_count}")

        # Show stages for this job
        job_stages = job_info.get("stages", [])
        if job_stages:
            print(f"\n    {Colors.OKBLUE}📋 STAGES FOR JOB {job_id}:{Colors.ENDC}")
            print(f"    Expected stages: {job_stages}")
            print(f"    Available stages in log: {list(stages.keys())}")

            for stage_id in job_stages:
                if stage_id in stages:
                    analyze_stage_for_job(
                        stage_id, stages[stage_id], shuffle_metrics, tasks, issues_found
                    )
                else:
                    print(
                        f"\n      {Colors.WARNING}Stage {stage_id} - Not found in log data{Colors.ENDC}"
                    )
                    print(
                        f"        This stage may have been skipped or failed to complete"
                    )
        else:
            print(
                f"\n    {Colors.WARNING}No stages found for Job {job_id}{Colors.ENDC}"
            )

        print(f"  {Colors.HEADER}{'='*50}{Colors.ENDC}")

    # Print function execution summary
    print_function_summary(function_summary)


def analyze_stage_for_job(stage_id, stage_info, shuffle_metrics, tasks, issues_found):
    """Analyze a specific stage within a job context"""
    print(f"\n      {Colors.BOLD}Stage {stage_id}{Colors.ENDC}")

    # Duration analysis - handle missing timing data gracefully
    if "completion_time" in stage_info and "submission_time" in stage_info:
        duration = stage_info["completion_time"] - stage_info["submission_time"]
        duration_sec = duration / 1000

        if duration_sec > 5:
            print(
                f"        ⏱️  Duration: {Colors.FAIL}{format_duration(duration)}{Colors.ENDC}"
            )
        elif duration_sec > 2:
            print(
                f"        ⏱️  Duration: {Colors.WARNING}{format_duration(duration)}{Colors.ENDC}"
            )
        else:
            print(
                f"        ⏱️  Duration: {Colors.OKGREEN}{format_duration(duration)}{Colors.ENDC}"
            )
    else:
        print(
            f"        ⏱️  Duration: {Colors.WARNING}Incomplete timing data{Colors.ENDC}"
        )

    # Stage details
    name = stage_info.get("name", "Unknown")
    print(f"        🔧 Operation: {Colors.OKCYAN}{name}{Colors.ENDC}")

    num_tasks = stage_info.get("num_tasks", 0)
    print(f"        📊 Tasks: {num_tasks}")

    # Shuffle metrics for this stage
    stage_shuffle = shuffle_metrics.get(stage_id, {})
    if stage_shuffle:
        print(f"        🔄 Shuffle Activity:")

        # Key metrics to focus on for performance analysis
        key_metrics = {
            "shuffle bytes written": "Shuffle Write Size",
            "shuffle records written": "Shuffle Write Records",
            "shuffle write time": "Shuffle Write Time (ns)",
            "internal.metrics.shuffle.read.remoteBytesRead": "Shuffle Read Size",
            "internal.metrics.shuffle.read.recordsRead": "Shuffle Read Records",
            "internal.metrics.shuffle.read.fetchWaitTime": "Shuffle Read Wait Time",
            "internal.metrics.shuffle.read.remoteBlocksFetched": "Remote Blocks Fetched",
            "internal.metrics.shuffle.read.localBlocksFetched": "Local Blocks Fetched",
        }

        shuffle_write_bytes = 0
        shuffle_read_bytes = 0
        shuffle_write_time = 0
        shuffle_read_wait = 0

        for metric, value in stage_shuffle.items():
            if metric in key_metrics:
                display_name = key_metrics[metric]

                if "bytes" in metric.lower():
                    formatted_value = format_bytes(value)
                    if value > 10 * 1024 * 1024:  # > 10MB
                        print(
                            f"          {display_name}: {Colors.FAIL}🚨 {formatted_value} (EXPENSIVE!){Colors.ENDC}"
                        )
                        issues_found.append(
                            f"Stage {stage_id} has very high shuffle: {formatted_value}"
                        )
                    elif value > 1024 * 1024:  # > 1MB
                        print(
                            f"          {display_name}: {Colors.WARNING}⚠️  {formatted_value} (HIGH){Colors.ENDC}"
                        )
                        issues_found.append(
                            f"Stage {stage_id} has high shuffle: {formatted_value}"
                        )
                    else:
                        print(
                            f"          {display_name}: {Colors.OKGREEN}✅ {formatted_value}{Colors.ENDC}"
                        )

                    # Track total bytes for summary
                    if "write" in metric:
                        shuffle_write_bytes = value
                    elif "read" in metric:
                        shuffle_read_bytes = value

                elif "time" in metric.lower() or "wait" in metric.lower():
                    if value > 100000000:  # > 100ms in nanoseconds
                        print(
                            f"          {display_name}: {Colors.FAIL}🚨 {value:,}ns (SLOW!){Colors.ENDC}"
                        )
                        issues_found.append(
                            f"Stage {stage_id} has slow shuffle: {value:,}ns"
                        )
                    elif value > 10000000:  # > 10ms
                        print(
                            f"          {display_name}: {Colors.WARNING}⚠️  {value:,}ns (SLOW){Colors.ENDC}"
                        )
                    else:
                        print(
                            f"          {display_name}: {Colors.OKGREEN}✅ {value:,}ns{Colors.ENDC}"
                        )

                    # Track timing for summary
                    if "write" in metric:
                        shuffle_write_time = value
                    elif "wait" in metric:
                        shuffle_read_wait = value

                elif "blocks" in metric.lower():
                    if value > 10:
                        print(
                            f"          {display_name}: {Colors.WARNING}⚠️  {value} blocks{Colors.ENDC}"
                        )
                    else:
                        print(
                            f"          {display_name}: {Colors.OKGREEN}✅ {value} blocks{Colors.ENDC}"
                        )

                else:
                    print(f"          {display_name}: {value}")

        # Show shuffle performance summary
        if shuffle_write_bytes > 0 or shuffle_read_bytes > 0:
            print(f"\n          📊 Shuffle Performance Summary:")
            if shuffle_write_bytes > 0:
                write_mb = shuffle_write_bytes / (1024 * 1024)
                if write_mb > 10:
                    print(
                        f"            📤 Write: {Colors.FAIL}🚨 {write_mb:.1f}MB (EXPENSIVE!){Colors.ENDC}"
                    )
                elif write_mb > 1:
                    print(
                        f"            📤 Write: {Colors.WARNING}⚠️  {write_mb:.1f}MB (HIGH){Colors.ENDC}"
                    )
                else:
                    print(
                        f"            📤 Write: {Colors.OKGREEN}✅ {write_mb:.1f}MB{Colors.ENDC}"
                    )

            if shuffle_read_bytes > 0:
                read_mb = shuffle_read_bytes / (1024 * 1024)
                if read_mb > 10:
                    print(
                        f"            📥 Read: {Colors.FAIL}🚨 {read_mb:.1f}MB (EXPENSIVE!){Colors.ENDC}"
                    )
                elif read_mb > 1:
                    print(
                        f"            📥 Read: {Colors.WARNING}⚠️  {read_mb:.1f}MB (HIGH){Colors.ENDC}"
                    )
                else:
                    print(
                        f"            📥 Read: {Colors.OKGREEN}✅ {read_mb:.1f}MB{Colors.ENDC}"
                    )

            if shuffle_write_time > 100000000:  # > 100ms
                print(
                    f"            ⏱️  Write Time: {Colors.FAIL}🚨 {shuffle_write_time/1000000:.1f}ms (SLOW!){Colors.ENDC}"
                )
            elif shuffle_write_time > 10000000:  # > 10ms
                print(
                    f"            ⏱️  Write Time: {Colors.WARNING}⚠️  {shuffle_write_time/1000000:.1f}ms{Colors.ENDC}"
                )

            if shuffle_read_wait > 100000000:  # > 100ms
                print(
                    f"            ⏱️  Read Wait: {Colors.FAIL}🚨 {shuffle_read_wait/1000000:.1f}ms (SLOW!){Colors.ENDC}"
                )
            elif shuffle_read_wait > 10000000:  # > 10ms
                print(
                    f"            ⏱️  Read Wait: {Colors.WARNING}⚠️  {shuffle_read_wait/1000000:.1f}ms{Colors.ENDC}"
                )
    else:
        print(f"        🔄 Shuffle Activity: {Colors.OKGREEN}None{Colors.ENDC}")

    # Show tasks for this stage
    if stage_id in tasks and tasks[stage_id]:
        print(f"\n          {Colors.OKCYAN}📋 TASKS FOR STAGE {stage_id}:{Colors.ENDC}")
        analyze_tasks_for_stage(stage_id, tasks[stage_id], issues_found)
    else:
        print(
            f"          {Colors.WARNING}No task data available for Stage {stage_id}{Colors.ENDC}"
        )


def analyze_tasks_for_stage(stage_id, stage_tasks, issues_found):
    """Analyze tasks for a specific stage"""
    if not stage_tasks:
        return

    # Calculate statistics
    durations = [t["duration"] for t in stage_tasks]
    shuffle_reads = [t["shuffle_read"] for t in stage_tasks]
    shuffle_writes = [t["shuffle_write"] for t in stage_tasks]

    # Duration analysis
    avg_duration = sum(durations) / len(durations)
    max_duration = max(durations)
    min_duration = min(durations)

    print(f"            📊 Task Count: {len(stage_tasks)}")
    print(f"            ⏱️  Avg Duration: {format_duration(avg_duration)}")
    print(f"            ⏱️  Max Duration: {format_duration(max_duration)}")
    print(f"            ⏱️  Min Duration: {format_duration(min_duration)}")

    # Check for skew
    if len(durations) > 1:
        duration_skew = max_duration / avg_duration
        if duration_skew > 3:
            print(
                f"            {Colors.FAIL}⚠️  SEVERE DURATION SKEW: {duration_skew:.1f}x{Colors.ENDC}"
            )
            issues_found.append(
                f"Stage {stage_id} has severe task skew: {duration_skew:.1f}x"
            )
        elif duration_skew > 2:
            print(
                f"            {Colors.WARNING}⚠️  MODERATE DURATION SKEW: {duration_skew:.1f}x{Colors.ENDC}"
            )
            issues_found.append(
                f"Stage {stage_id} has moderate task skew: {duration_skew:.1f}x"
            )

    # Shuffle analysis
    total_shuffle_read = sum(shuffle_reads)
    total_shuffle_write = sum(shuffle_writes)

    if total_shuffle_read > 0:
        print(f"            📥 Total Shuffle Read: {format_bytes(total_shuffle_read)}")
    if total_shuffle_write > 0:
        print(
            f"            📤 Total Shuffle Write: {format_bytes(total_shuffle_write)}"
        )

    # Check for spills
    spills = [
        t for t in stage_tasks if t["memory_spilled"] > 0 or t["disk_spilled"] > 0
    ]
    if spills:
        print(
            f"            {Colors.FAIL}⚠️  MEMORY SPILLS: {len(spills)} tasks spilled{Colors.ENDC}"
        )
        issues_found.append(
            f"Stage {stage_id} has {len(spills)} tasks with memory spills"
        )


def print_summary(total_jobs, total_duration, issues_found):
    """Print a summary of the analysis"""
    print_header("ANALYSIS SUMMARY")

    print(f"  📊 Total Jobs Analyzed: {total_jobs}")
    print(f"  ⏱️  Total Execution Time: {format_duration(total_duration)}")

    # Group jobs by function for better overview
    print(f"\n  {Colors.OKBLUE}📋 FUNCTION EXECUTION SUMMARY:{Colors.ENDC}")

    # This will be populated by the job analysis
    function_summary = {}

    if issues_found:
        print(f"\n  {Colors.FAIL}🚨 ISSUES FOUND ({len(issues_found)}):{Colors.ENDC}")
        for i, issue in enumerate(issues_found, 1):
            print(f"    {i}. {issue}")

        print(f"\n  {Colors.WARNING}💡 RECOMMENDATIONS:{Colors.ENDC}")
        print("    • Check Spark Web UI for detailed metrics")
        print("    • Consider repartitioning for skewed data")
        print("    • Optimize shuffle partitions")
        print("    • Use broadcast joins for small tables")
        print("    • Monitor memory usage and spills")
    else:
        print(f"\n  {Colors.OKGREEN}✅ No major issues detected!{Colors.ENDC}")
        print("    Your Spark job appears to be running efficiently.")


def print_function_summary(function_summary):
    """Print function execution summary"""
    if not function_summary:
        return

    print(
        f"\n  {Colors.OKBLUE}{Colors.BOLD}📋 FUNCTION EXECUTION SUMMARY:{Colors.ENDC}"
    )
    print(f"  {Colors.OKBLUE}{'-'*50}{Colors.ENDC}")

    # Sort functions by total duration (longest first)
    sorted_functions = sorted(
        function_summary.items(), key=lambda x: x[1]["total_duration"], reverse=True
    )

    for function, summary in sorted_functions:
        duration_sec = summary["total_duration"] / 1000

        # Color code based on performance
        if duration_sec > 10:
            duration_color = Colors.FAIL
            performance_indicator = "🚨 SLOW"
        elif duration_sec > 5:
            duration_color = Colors.WARNING
            performance_indicator = "⚠️  MODERATE"
        else:
            duration_color = Colors.OKGREEN
            performance_indicator = "✅ FAST"

        print(f"  {Colors.OKBLUE}{Colors.BOLD}{function}:{Colors.ENDC}")
        print(f"    📊 Jobs: {summary['jobs']}")
        print(
            f"    ⏱️  Total Time: {duration_color}{format_duration(summary['total_duration'])} {performance_indicator}{Colors.ENDC}"
        )
        print(f"    📋 Job IDs: {', '.join(map(str, summary['job_ids']))}")
        print()


def main():
    """Main function"""
    parser = argparse.ArgumentParser(
        description="Parse a Spark event log file for shuffling metrics and performance data."
    )
    parser.add_argument(
        "--file-path",
        dest="file_path",
        help="Path to the Spark event log file to parse",
    )
    parser.add_argument(
        "log_file",
        nargs="?",
        help="(Optional) Path to the Spark event log file (positional)",
    )
    args = parser.parse_args()

    # Priority: positional > --file-path > default
    log_file = args.log_file or args.file_path
    if not log_file:
        print(
            "Error: You must specify a log file path with --file-path or as a positional argument."
        )
        parser.print_help()
        sys.exit(1)

    parse_spark_logs(log_file)


if __name__ == "__main__":
    main()
