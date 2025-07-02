#!/usr/bin/env python3
"""
Python script to run tests in Docker containers
Run with: python scripts/run_docker_tests.py [options]
"""

import subprocess
import sys
import argparse
from typing import List, Optional


def run_docker_test(
    test_file: str = None,
    test_marker: str = None,
    container: str = "spark-master",
    verbose: bool = True,
    coverage: bool = False,
) -> bool:
    """
    Run tests in Docker container

    Args:
        test_file: Specific test file to run (optional)
        test_marker: Pytest marker to filter tests (optional)
        container: Docker container name
        verbose: Run with verbose output
        coverage: Run with coverage report

    Returns:
        bool: True if tests passed, False otherwise
    """

    # Build pytest command
    cmd = ["docker", "exec"]

    # Add environment variables
    cmd.extend(["-e", "AWS_ACCESS_KEY_ID=admin"])
    cmd.extend(["-e", "AWS_SECRET_ACCESS_KEY=password"])
    cmd.extend(["-e", "PYTHONPATH=/home/app:/opt/bitnami/spark/python"])

    # Build the pytest command with options
    pytest_cmd = "python -m pytest"

    if verbose:
        pytest_cmd += " -v"

    if test_marker:
        pytest_cmd += f" -m {test_marker}"

    if coverage:
        pytest_cmd += " --cov=/home/app/src --cov-report=html --cov-report=term"

    # Add container and python command with working directory
    if test_file:
        # For specific test file, construct the full command
        cmd.extend(
            [
                container,
                "bash",
                "-c",
                f"cd /home/app && {pytest_cmd} tests/{test_file}",
            ]
        )
    else:
        # For all tests, use the default approach
        cmd.extend([container, "bash", "-c", f"cd /home/app && {pytest_cmd} tests/"])

    print(f"Running: {' '.join(cmd)}")

    try:
        result = subprocess.run(cmd, capture_output=False, text=True)
        return result.returncode == 0
    except subprocess.CalledProcessError as e:
        print(f"Error running tests: {e}")
        return False
    except FileNotFoundError:
        print("Error: Docker not found or not running")
        return False


def check_docker_available() -> bool:
    """Check if Docker is available and running"""
    try:
        result = subprocess.run(["docker", "info"], capture_output=True, text=True)
        return result.returncode == 0
    except FileNotFoundError:
        return False


def check_container_running(container: str) -> bool:
    """Check if the specified container is running"""
    try:
        result = subprocess.run(
            ["docker", "ps", "--filter", f"name={container}"],
            capture_output=True,
            text=True,
        )
        return container in result.stdout
    except FileNotFoundError:
        return False


def main():
    parser = argparse.ArgumentParser(description="Run tests in Docker container")
    parser.add_argument(
        "--test-file", help="Specific test file to run (e.g., test_environment.py)"
    )
    parser.add_argument(
        "--marker", help="Pytest marker to filter tests (e.g., unit, integration)"
    )
    parser.add_argument(
        "--container",
        default="spark-master",
        help="Docker container name (default: spark-master)",
    )
    parser.add_argument(
        "--no-verbose", action="store_true", help="Run without verbose output"
    )
    parser.add_argument(
        "--coverage", action="store_true", help="Run with coverage report"
    )
    parser.add_argument(
        "--check-only",
        action="store_true",
        help="Only check if Docker and container are available",
    )

    args = parser.parse_args()

    # Check Docker availability
    if not check_docker_available():
        print("❌ Docker is not available or not running")
        sys.exit(1)

    # Check container availability
    if not check_container_running(args.container):
        print(f"❌ Container '{args.container}' is not running")
        print("💡 Start containers with: docker-compose up -d")
        sys.exit(1)

    if args.check_only:
        print("✅ Docker and container are available")
        return

    # Run tests
    success = run_docker_test(
        test_file=args.test_file,
        test_marker=args.marker,
        container=args.container,
        verbose=not args.no_verbose,
        coverage=args.coverage,
    )

    if success:
        print("✅ Tests completed successfully")
    else:
        print("❌ Tests failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
