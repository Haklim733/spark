#!/usr/bin/env python3
"""
Simple job submission script - run with: python scripts/submit.py <file_path>
"""

import subprocess
import os
import sys
import argparse


def submit_job(file_path: str):
    """Submit a Python file to Docker Spark cluster"""
    print(f"Submitting {file_path} to Spark cluster...")

    # Get just the filename from the path
    file_name = os.path.basename(file_path)

    result = subprocess.run(
        [
            "docker",
            "exec",
            "spark-master",
            "/opt/bitnami/spark/bin/spark-submit",
            "--master",
            "spark://spark-master:7077",
            "--deploy-mode",
            "client",
            f"/home/app/src/{file_name}",
        ],
        capture_output=True,
        text=True,
    )

    print(f"Return code: {result.returncode}")

    if result.stdout:
        print("STDOUT:")
        print(result.stdout)

    if result.stderr:
        print("STDERR:")
        print(result.stderr)


def main():
    parser = argparse.ArgumentParser(
        description="Submit a Python file to Spark cluster"
    )
    parser.add_argument(
        "--file-path", dest="file_path", help="Path to the Python file to submit"
    )
    args = parser.parse_args()

    # Check if file exists
    if not os.path.exists(args.file_path):
        print(f"Error: File {args.file_path} does not exist")
        sys.exit(1)

    # Check if file is in src directory
    if not args.file_path.startswith("src/"):
        print(f"Warning: File {args.file_path} is not in src/ directory")
        print(
            "The file will be submitted as-is, but make sure it exists in the container at /home/app/src/"
        )

    submit_job(args.file_path)


if __name__ == "__main__":
    main()
