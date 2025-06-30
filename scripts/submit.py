#!/usr/bin/env python3
"""
Simple job submission script - run with: python scripts/submit.py <file_path> --args "arg1 arg2"

This version zips the entire src directory and uses --py-files to distribute all custom code to Spark workers.
"""

import subprocess
import os
import sys
import argparse
import zipfile
import tempfile


def create_src_archive():
    """Create a zip archive of the entire src directory"""
    src_dir = "src"
    if not os.path.exists(src_dir):
        print("❌ Error: src directory not found")
        return None

    # Create temporary zip file
    temp_file = tempfile.NamedTemporaryFile(suffix=".zip", delete=False)
    temp_path = temp_file.name
    temp_file.close()

    print(f"📦 Creating src archive: {temp_path}")
    with zipfile.ZipFile(temp_path, "w", zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(src_dir):
            # Skip cache and git directories
            dirs[:] = [d for d in dirs if not d.startswith("__") and d != ".git"]
            for file in files:
                if file.endswith((".py", ".json", ".yaml", ".yml")):
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, src_dir)
                    zipf.write(file_path, arcname)
                    print(f"   📄 Added: {arcname}")
    return temp_path


def submit_job(file_path: str, additional_args: str = None):
    """Submit a Python file to Docker Spark cluster"""
    print(f"Submitting {file_path} to Spark cluster...")
    if additional_args:
        print(f"Additional arguments: {additional_args}")

    # Get just the filename from the path
    file_name = os.path.basename(file_path)

    # Create src archive
    src_archive = create_src_archive()
    if not src_archive:
        return 1

    # Build the command
    cmd = [
        "docker",
        "exec",
        "spark-master",
        "/opt/bitnami/spark/bin/spark-submit",
        "--master",
        "spark://spark-master:7077",
        "--deploy-mode",
        "client",
        "--py-files",
        "/home/app/src.zip",
        f"/home/app/src/{file_name}",
    ]

    # Add additional arguments if provided
    if additional_args:
        cmd.extend(additional_args.split())

    # Copy archive to container
    print("📤 Copying src archive to container...")
    subprocess.run(
        ["docker", "cp", src_archive, "spark-master:/home/app/src.zip"], check=True
    )

    print(f"🚀 Executing: {' '.join(cmd)}")
    result = subprocess.run(
        cmd,
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
    # Clean up temporary archive
    if os.path.exists(src_archive):
        os.unlink(src_archive)
        print("🧹 Cleaned up temporary archive")
    return result.returncode


def main():
    parser = argparse.ArgumentParser(
        description="Submit a Python file to Spark cluster (zips entire src directory)"
    )
    parser.add_argument(
        "--file-path", dest="file_path", help="Path to the Python file to submit"
    )
    parser.add_argument(
        "--args",
        dest="additional_args",
        help="Additional arguments to pass to the Python script (space-separated)",
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

    return submit_job(args.file_path, args.additional_args)


if __name__ == "__main__":
    sys.exit(main())
