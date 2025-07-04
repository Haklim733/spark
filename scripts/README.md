# Scripts Directory

This directory contains utility scripts for the Spark project.

## Available Scripts

### `submit.py`
Submit Python files to the Spark cluster using `spark-submit`.

**Usage:**
```bash
uv run scripts/submit.py --file-path src/shuffling.py
```

**Purpose:**
- Submit Spark jobs to the Docker cluster
- Use for production workloads and data processing jobs
- Runs files using `spark-submit` inside the container

### `run_docker_tests.py`
Run pytest tests inside the Docker container where PySpark is properly installed.

**Usage:**
```bash
# Run all tests
uv run scripts/run_docker_tests.py

# Run specific test file
uv run scripts/run_docker_tests.py --test-file test_environment.py

# Run with coverage
uv run scripts/run_docker_tests.py --coverage

# Check if Docker is available
uv run scripts/run_docker_tests.py --check-only
```

**Purpose:**
- Run tests in Docker environment where PySpark is fully installed
- Avoid PySpark import issues in local environment
- Test against actual Spark cluster

### `start-spark.sh`
Start the Spark cluster using Docker Compose.

**Usage:**
```bash
./scripts/start-spark.sh
```

### `upload-minio.sh`
Upload data to MinIO storage.

**Usage:**
```bash
./scripts/upload-minio.sh
```

## Quick Commands

### Testing
```bash
# Run environment tests in Docker
uv run -m scripts.run_docker_tests --test-file test_environment.py

# Run session creation tests in Docker
uv run -m scripts.run_docker_tests --test-file test_session_creation.py

# Run all tests with coverage
uv run -m scripts.run_docker_tests --coverage
```

### Job Submission
```bash
# Submit shuffling demo
uv run -m scripts.submit --file-path src/shuffling.py

# Submit any Python file
uv run -m scripts.submit --file-path src/your_file.py
```

### Direct Docker Commands
```bash
# Run tests directly in container
docker exec spark-master python -m pytest /home/app/tests/ -v

# Run specific test file
docker exec spark-master python -m pytest /home/app/tests/test_environment.py -v
```

## Why This Structure?

1. **Separation of Concerns**: Scripts are separate from source code and tests
2. **Standard Convention**: Most projects put utility scripts in `scripts/`
3. **Easy Discovery**: Developers know where to find utility tools
4. **Clear Purpose**: Each script has a specific, well-defined purpose

## Environment Requirements

- Docker and Docker Compose must be running
- Spark cluster must be started (`docker-compose up -d`)
- Python 3.10+ for running the scripts locally 