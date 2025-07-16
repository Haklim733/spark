#!/bin/bash
# Initialize MinIO buckets and folder structure for data lake
# This script runs automatically when docker-compose starts

set -e

# Configuration
MINIO_ENDPOINT="http://minio:9000"
MINIO_ACCESS_KEY="admin"
MINIO_SECRET_KEY="password"
MINIO_REGION="us-east-1"

# Bucket names
RAW_BUCKET="raw"           # Incoming data (landing zone)
LOGS_BUCKET="logs"         # Application logs  
WAREHOUSE_BUCKET="iceberg" # Iceberg tables (changed from "wh")
ARCHIVE_BUCKET="archive"   # Historical data (optional)

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to wait for MinIO to be ready
wait_for_minio() {
    print_status "Waiting for MinIO to be ready..."
    until /usr/bin/mc alias set minio $MINIO_ENDPOINT $MINIO_ACCESS_KEY $MINIO_SECRET_KEY; do
        print_warning "MinIO not ready yet, waiting..."
        sleep 2
    done
    print_success "MinIO is ready!"
}

# Function to create bucket if it doesn't exist
create_bucket_if_not_exists() {
    local bucket_name=$1
    local description=$2
    
    print_status "Creating bucket: $bucket_name ($description)"
    
    # Try to create the bucket
    if /usr/bin/mc mb minio/$bucket_name 2>/dev/null; then
        print_success "Created bucket: $bucket_name"
    else
        # Check if bucket already exists
        if /usr/bin/mc ls minio/$bucket_name >/dev/null 2>&1; then
            print_warning "Bucket $bucket_name already exists"
        else
            print_error "Failed to create bucket: $bucket_name"
            return 1
        fi
    fi
}

# Function to create folder if it doesn't exist
create_folder_if_not_exists() {
    local folder_path=$1
    local description=$2
    
    print_status "Creating folder: $folder_path ($description)"
    
    # Try to create the folder
    if /usr/bin/mc mb minio/$folder_path 2>/dev/null; then
        print_success "Created folder: $folder_path"
    else
        # Check if folder already exists
        if /usr/bin/mc ls minio/$folder_path >/dev/null 2>&1; then
            print_warning "Folder $folder_path already exists"
        else
            print_error "Failed to create folder: $folder_path"
            return 1
        fi
    fi
}

# Function to create separate buckets
create_buckets() {
    print_status "Creating separate buckets..."
    
    create_bucket_if_not_exists $RAW_BUCKET "Incoming data (landing zone)"
    create_bucket_if_not_exists $LOGS_BUCKET "Application and system logs"
    create_bucket_if_not_exists $WAREHOUSE_BUCKET "Iceberg tables and analytics data"
    create_bucket_if_not_exists $ARCHIVE_BUCKET "Historical data"
}

# Function to create folder structure within buckets
create_folder_structure() {
    print_status "Creating folder structure..."
    
    # Create folders within logs bucket
    create_folder_if_not_exists "$LOGS_BUCKET/application" "Spark application logs"
    create_folder_if_not_exists "$LOGS_BUCKET/observability" "HybridLogger structured logs"
    create_folder_if_not_exists "$LOGS_BUCKET/system" "Spark system logs"
    create_folder_if_not_exists "$LOGS_BUCKET/executor" "Spark executor logs"
}

# Function to display bucket structure
display_bucket_structure() {
    echo ""
    print_status "MinIO Data Lake Structure:"
    echo "┌─────────────────────────────────────────────────────────────┐"
    echo "│ minio/                                                      │"
    echo "│ ├── raw/                    # Incoming data (landing zone)  │"
    echo "│ ├── logs/                  # Application and system logs   │"
    echo "│ │   ├── application/       # Spark application logs        │"
    echo "│ │   ├── observability/     # HybridLogger structured logs  │"
    echo "│ │   ├── system/           # Spark system logs              │"
    echo "│ │   └── executor/         # Spark executor logs            │"
    echo "│ ├── iceberg/               # Iceberg tables and analytics  │"
    echo "│ └── archive/              # Historical data                │"
    echo "└─────────────────────────────────────────────────────────────┘"
}

# Function to display access information
display_access_info() {
    echo ""
    print_status "Access Information:"
    echo "┌─────────────────────────────────────────────────────────────┐"
    echo "│ MinIO Console: http://localhost:9001                        │"
    echo "│   Username: admin                                           │"
    echo "│   Password: password                                        │"
    echo "│                                                             │"
    echo "│ MinIO API: http://localhost:9000                            │"
    echo "│   Access Key: admin                                         │"
    echo "│   Secret Key: password                                      │"
    echo "│                                                             │"
    echo "└─────────────────────────────────────────────────────────────┘"
}

# Function to list all buckets
list_buckets() {
    echo ""
    print_status "Listing all buckets:"
    /usr/bin/mc ls minio/ || print_error "Could not list buckets"
}

mount_files() {
    echo ""
    print_status "Mounting files..."
    /usr/bin/mc mirror /data/energy/ minio/raw/
}

# Main execution
main() {
    echo "🚀 Initializing MinIO Data Lake Architecture..."
    echo "=================================================="
    
    # Wait for MinIO to be ready
    wait_for_minio
    
    # Create separate buckets
    create_buckets
    
    # Create folder structure
    create_folder_structure
    
    # Mount files
    mount_files
    
    # Display results
    display_bucket_structure
    display_access_info
    list_buckets
    
    echo ""
    print_success "MinIO Data Lake initialization complete!"
    echo "=================================================="
}

# Run main function
main "$@" 