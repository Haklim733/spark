#!/usr/bin/env python3
"""
ELT Pipeline - Extract and Load operations
Transformations and data quality checks should be handled by dbt/SQLMesh
Note: Iceberg tables don't have constraints, so validation is done at application level
"""

import argparse
import random
from datetime import datetime
import os
from pathlib import Path
from dataclasses import dataclass
import time
from typing import List, Optional, Dict, Any

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType,
    MapType,
    BooleanType,
)
from soli_data_generator.procedural.template import TemplateFormatter
from utils.logger import custom_logger
from utils.session import (
    create_spark_session,
    SparkVersion,
    IcebergConfig,
    S3FileSystemConfig,
)

# Import functions from generate_legal_docs.py
from generate_legal_docs import (
    generate_fallback_content,
    get_document_generator,
)

# Import shared legal document models
from models import (
    LEGAL_DOC_TYPES,
    VALID_DOCUMENT_TYPES,
    get_all_legal_doc_types,
)


def is_minio_path(path_str: str) -> bool:
    """Check if the path is a MinIO/S3 path"""
    return path_str.startswith(("s3a://", "s3://", "minio://"))


def get_minio_path_info(path_str: str) -> Dict[str, str]:
    """Parse MinIO path to extract bucket and key information"""
    if path_str.startswith("s3a://"):
        path_str = path_str[6:]  # Remove s3a://
    elif path_str.startswith("s3://"):
        path_str = path_str[5:]  # Remove s3://
    elif path_str.startswith("minio://"):
        path_str = path_str[8:]  # Remove minio://

    # Split into bucket and key
    parts = path_str.split("/", 1)
    if len(parts) == 2:
        return {"bucket": parts[0], "key": parts[1]}
    else:
        return {"bucket": parts[0], "key": ""}


def list_minio_files(spark: SparkSession, minio_path: str) -> List[str]:
    """List files in MinIO bucket/path using Spark"""
    try:
        # Use Spark to list files in MinIO
        files_df = spark.read.format("binaryFile").load(minio_path)
        file_paths = [row.path for row in files_df.select("path").collect()]
        return file_paths
    except Exception as e:
        print(f"❌ Error listing MinIO files with Spark: {e}")
        return []


def read_minio_file_content(spark: SparkSession, file_path: str) -> str:
    """Read content from a MinIO file using Spark"""
    try:
        # Read as text file using Spark
        df = spark.read.text(file_path)
        content = "\n".join([row.value for row in df.collect()])
        return content
    except Exception as e:
        print(f"❌ Error reading MinIO file {file_path} with Spark: {e}")
        return ""


def get_minio_file_size(spark: SparkSession, file_path: str) -> int:
    """Get file size from MinIO using Spark"""
    try:
        # Use binaryFile format to get file metadata
        df = spark.read.format("binaryFile").load(file_path)
        metadata = df.select("length").collect()
        if metadata:
            return metadata[0]["length"]
        return 0
    except Exception as e:
        print(f"❌ Error getting file size for {file_path} with Spark: {e}")
        return 0


@dataclass
class Document:
    """Dataclass for document data"""

    document_id: str
    document_type: str
    content: str
    filename: str
    document_length: int
    word_count: int


@dataclass
class DocumentData:
    """Dataclass for processed document data ready for insertion"""

    document_id: str
    document_type: str
    raw_text: str
    generation_date: datetime
    file_path: str
    document_length: int
    word_count: int
    language: str
    metadata: Dict[str, str]


@dataclass
class ProcessingError:
    """Dataclass for processing errors"""

    message: str
    type: str = "processing_error"

    def to_dict(self) -> dict:
        """Convert to dictionary format for backward compatibility"""
        return {"message": self.message, "type": self.type}


@dataclass
class ValidationError:
    """Dataclass for validation errors"""

    field: str
    value: str
    message: str
    type: str = "validation_error"

    def to_dict(self) -> dict:
        """Convert to dictionary format for backward compatibility"""
        return {
            "field": self.field,
            "value": self.value,
            "message": self.message,
            "type": self.type,
        }


@dataclass
class ProcessingResult:
    """Dataclass for file processing results"""

    is_valid: bool
    data: Optional[DocumentData]
    document: Document
    errors: Optional[List[ProcessingError]]

    def to_dict(self) -> dict:
        """Convert to dictionary format for backward compatibility"""
        return {
            "is_valid": self.is_valid,
            "data": self.data.__dict__ if self.data else None,
            "document": self.document.__dict__,
            "errors": (
                [error.to_dict() for error in self.errors] if self.errors else None
            ),
        }


def validate_document_quality(document: Document):
    """
    Validate document quality before insertion
    Returns: (is_valid, errors_list)
    """
    errors = []

    # Check for null values in critical fields
    if not document.document_id:
        errors.append(
            ValidationError(
                field="document_id",
                value=document.document_id,
                message="document_id is null or empty",
            )
        )

    if not document.document_type:
        errors.append(
            ValidationError(
                field="document_type",
                value=document.document_type,
                message="document_type is null or empty",
            )
        )

    if not document.content:
        errors.append(
            ValidationError(
                field="content", value="empty", message="content is null or empty"
            )
        )

    # Check for data quality issues
    if document.document_length <= 0:
        errors.append(
            ValidationError(
                field="document_length",
                value=str(document.document_length),
                message="document_length must be positive",
            )
        )

    if document.word_count <= 0:
        errors.append(
            ValidationError(
                field="word_count",
                value=str(document.word_count),
                message="word_count must be positive",
            )
        )

    # Check for reasonable limits
    if document.document_length > 100000:  # 100KB limit
        errors.append(
            ValidationError(
                field="document_length",
                value=str(document.document_length),
                message="document_length exceeds 100KB limit",
            )
        )

    if document.word_count > 50000:  # 50K words limit
        errors.append(
            ValidationError(
                field="word_count",
                value=str(document.word_count),
                message="word_count exceeds 50K words limit",
            )
        )

    # Check document type validity
    if document.document_type and document.document_type not in VALID_DOCUMENT_TYPES:
        errors.append(
            ValidationError(
                field="document_type",
                value=document.document_type,
                message=f"invalid document_type: {document.document_type}",
            )
        )

    return len(errors) == 0, errors


def insert_to_quarantine(
    spark: SparkSession,
    document: Document,
    errors_list: List[ValidationError],
    error_type="validation_error",
):
    """Insert failed record into quarantine table with support for multiple errors"""
    try:
        # Insert one row per error
        for i, error in enumerate(errors_list, 1):
            quarantine_sql = f"""
            INSERT INTO legal.documents_error (
                original_document_id, document_type, raw_text, generation_date,
                file_path, document_length, word_count, language, metadata,
                error_message, error_type, quarantined_at, error_sequence, field_name, field_value
            ) VALUES (
                '{document.document_id}',
                '{document.document_type}',
                '{document.content.replace("'", "''")}',
                CURRENT_TIMESTAMP(),
                '{document.filename}',
                {document.document_length},
                {document.word_count},
                'en',
                map('source_file', '{document.filename}'),
                '{error.message.replace("'", "''")}',
                '{error.type}',
                CURRENT_TIMESTAMP(),
                {i},
                '{error.field}',
                '{str(error.value).replace("'", "''")}'
            )
            """

            spark.sql(quarantine_sql)

        return True
    except Exception as e:
        print(f"❌ Error inserting to quarantine: {e}")
        return False


def insert_single_error_to_quarantine(spark, document, error_message, error_type):
    """Insert single error record into quarantine table"""
    try:
        # Convert document to MAP format for original_data
        original_data_map = {
            "document_id": str(document.get("document_id", "")),
            "document_type": str(document.get("document_type", "")),
            "content": str(document.get("content", ""))[:1000],  # Truncate for storage
            "filename": str(document.get("filename", "")),
            "document_length": str(document.get("document_length", 0)),
            "word_count": str(document.get("word_count", 0)),
        }

        quarantine_sql = f"""
        INSERT INTO legal.documents_error (
            original_document_id, document_type, raw_text, generation_date,
            file_path, document_length, word_count, language, metadata,
            error_message, error_type, quarantined_at, error_sequence, field_name, field_value
        ) VALUES (
            '{document.get('document_id', 'unknown')}',
            '{document.get('document_type', 'unknown')}',
            '{document.get('content', '').replace("'", "''")}',
            CURRENT_TIMESTAMP(),
            '{document.get('filename', 'unknown')}',
            {document.get('document_length', 0)},
            {document.get('word_count', 0)},
            'en',
            map('source_file', '{document.get('filename', 'unknown')}'),
            '{error_message.replace("'", "''")}',
            '{error_type}',
            CURRENT_TIMESTAMP(),
            1,
            'general',
            'unknown'
        )
        """

        spark.sql(quarantine_sql)
        return True
    except Exception as e:
        print(f"❌ Error inserting to quarantine: {e}")
        return False


def basic_load_validation(spark, table_name, expected_count=None):
    """
    Basic ELT validation - only checks essential load operations
    Complex validations should be handled by dbt/SQLMesh
    """
    print(f"\n🔍 Basic load validation: {table_name}")

    try:
        # 1. Check if table exists
        tables = spark.sql(f"SHOW TABLES IN {table_name.split('.')[0]}")
        table_exists = (
            tables.filter(f"tableName = '{table_name.split('.')[1]}'").count() > 0
        )

        if not table_exists:
            print(f"❌ Table {table_name} does not exist!")
            return False

        print(f"✅ Table {table_name} exists")

        # 2. Get row count (essential for ELT)
        count_result = spark.sql(f"SELECT COUNT(*) as row_count FROM {table_name}")
        actual_count = count_result.collect()[0]["row_count"]
        print(f"📊 Row count: {actual_count:,}")

        # 3. Validate expected count if provided
        if expected_count is not None:
            if actual_count == expected_count:
                print(f"✅ Row count matches expected: {expected_count:,}")
            else:
                print(
                    f"❌ Row count mismatch! Expected: {expected_count:,}, Actual: {actual_count:,}"
                )
                return False

        # 4. Quick sample check (for debugging)
        print(f"📋 Sample record:")
        spark.sql(f"SELECT * FROM {table_name} LIMIT 1").show(truncate=False)

        return True

    except Exception as e:
        print(f"❌ Error in basic validation for {table_name}: {e}")
        return False


def insert_legal_documents_from_generation(spark, num_docs=1000):
    """Insert legal documents from generation - ELT operation only"""
    print(f"Generating and inserting {num_docs} legal documents...")

    # Generate documents
    documents = generate_legal_documents_for_validation(spark, num_docs)
    if not documents:
        print("❌ Failed to generate documents")
        return False

    print(f"Generated {len(documents)} documents")

    # Clear existing data
    try:
        spark.sql("TRUNCATE TABLE legal.documents")
        print("✅ Cleared existing legal documents")
    except Exception as e:
        print(f"⚠️  Could not clear existing data: {e}")

    # Insert documents
    successful_inserts = 0
    failed_inserts = 0

    for document in documents:
        try:
            # Validate document quality
            is_valid, errors_list = validate_document_quality(document)

            if is_valid:
                # Insert valid document
                insert_sql = f"""
                INSERT INTO legal.documents (
                    document_id, document_type, raw_text, generation_date, 
                    file_path, document_length, word_count, language, metadata
                ) VALUES (
                    '{document['document_id']}', '{document['document_type']}', 
                    '{document['content'].replace("'", "''")}', 
                    CURRENT_TIMESTAMP(), '{document['filename']}', 
                    {document['document_length']}, {document['word_count']}, 
                    'en', map('source_file', '{document['filename']}', 'generated', 'true')
                )
                """

                spark.sql(insert_sql)
                successful_inserts += 1
            else:
                # Insert invalid record to quarantine
                insert_to_quarantine(spark, document, errors_list, "validation_error")
                failed_inserts += 1

        except Exception as e:
            print(f"❌ Error inserting document {document['document_id']}: {e}")
            insert_to_quarantine(
                spark,
                document,
                [{"message": f"Insert error: {str(e)}", "type": "insert_error"}],
                "insert_error",
            )
            failed_inserts += 1

    print(f"✅ Successfully inserted {successful_inserts} legal documents!")
    if failed_inserts > 0:
        print(f"⚠️  Failed to insert {failed_inserts} documents (sent to quarantine)")

    # Basic ELT validation only
    return basic_load_validation(
        spark, "legal.documents", expected_count=successful_inserts
    )


def generate_legal_documents_for_validation(spark, num_docs):
    """
    Generate legal documents and return them for validation before insertion
    This is a modified version that returns documents instead of directly saving
    Uses models from legal_document_models.py
    """
    # Import the generation logic from generate_legal_docs.py

    try:
        formatter = TemplateFormatter()
        print("SOLI data generator initialized successfully.")
    except Exception as e:
        print(f"Error initializing SOLI: {e}")
        print("Falling back to basic content generation.")
        formatter = None

    # Use shared legal document types from legal_document_models.py
    legal_doc_types = get_all_legal_doc_types()

    print(f"Generating {num_docs} legal documents for validation...")

    documents = []

    for i in range(num_docs):
        # Randomly select document type
        doc_type = random.choice(legal_doc_types)

        try:
            if formatter:
                doc_generator = get_document_generator(doc_type["name"])
                content = doc_generator(formatter, doc_type, i)
            else:
                content = generate_fallback_content(i, doc_type)

            # Create filename
            filename = f"legal_doc_{i+1:04d}_{doc_type['name']}.txt"

            # Create document record
            document = {
                "document_id": f"doc_{i+1:04d}",
                "document_type": doc_type["name"],
                "content": content,
                "keywords": ", ".join(doc_type["keywords"]),
                "filename": filename,
                "document_length": len(content),
                "word_count": len(content.split()),
                "generated_at": datetime.now(),
            }

            documents.append(document)

            if (i + 1) % 100 == 0:
                print(f"Generated {i + 1} documents...")

        except Exception as e:
            print(f"Error generating document {i+1}: {e}")
            fallback_content = generate_fallback_content(i, doc_type)

            filename = f"legal_doc_{i+1:04d}_{doc_type['name']}.txt"

            document = {
                "document_id": f"doc_{i+1:04d}",
                "document_type": doc_type["name"],
                "content": fallback_content,
                "keywords": ", ".join(doc_type["keywords"]),
                "filename": filename,
                "document_length": len(fallback_content),
                "word_count": len(fallback_content.split()),
                "generated_at": datetime.now(),
            }

            documents.append(document)

    return documents


def insert_legal_documents_from_files(spark, docs_dir="data/docs/legal"):
    """Insert legal documents from existing files - ELT operation only"""
    print(f"Inserting legal documents from: {docs_dir}")

    # Check if docs directory exists
    if not os.path.exists(docs_dir):
        print(f"⚠️  Documents directory not found: {docs_dir}")
        print("Please run generate_legal_docs.py first to create documents")
        return False

    # Get list of document files
    doc_files = list(Path(docs_dir).glob("*.txt"))
    if not doc_files:
        print(f"⚠️  No document files found in: {docs_dir}")
        return False

    print(f"Found {len(doc_files)} document files")

    # Clear existing data
    try:
        spark.sql("TRUNCATE TABLE legal.documents")
        print("✅ Cleared existing legal documents")
    except Exception as e:
        print(f"⚠️  Could not clear existing data: {e}")

    # Process each document file
    successful_inserts = 0
    failed_inserts = 0

    for i, doc_file in enumerate(doc_files):
        try:
            # Read document content
            with open(doc_file, "r", encoding="utf-8") as f:
                content = f.read()

            # Extract metadata from filename
            # Expected format: legal_doc_0001_contract.txt
            filename = doc_file.name
            parts = filename.replace(".txt", "").split("_")

            if len(parts) >= 4:
                doc_id = f"{parts[1]}_{parts[2]}"  # legal_doc_0001
                doc_type = parts[3]  # contract
            else:
                doc_id = filename.replace(".txt", "")
                doc_type = "unknown"

            # Calculate document statistics
            doc_length = len(content)
            word_count = len(content.split())

            # Create document object for validation
            document = {
                "document_id": doc_id,
                "document_type": doc_type,
                "content": content,
                "filename": filename,
                "document_length": doc_length,
                "word_count": word_count,
            }

            # Validate document quality
            is_valid, errors_list = validate_document_quality(document)

            if is_valid:
                # Insert valid document
                insert_sql = f"""
                INSERT INTO legal.documents (
                    document_id, document_type, raw_text, generation_date, 
                    file_path, document_length, word_count, language, metadata
                ) VALUES (
                    '{doc_id}', '{doc_type}', '{content.replace("'", "''")}', 
                    CURRENT_TIMESTAMP(), '{str(doc_file)}', {doc_length}, {word_count}, 
                    'en', map('source_file', '{filename}', 'file_size', '{doc_file.stat().st_size}')
                )
                """

                try:
                    spark.sql(insert_sql)
                    successful_inserts += 1
                except Exception as e:
                    error_msg = f"Insert error: {str(e)}"
                    insert_to_quarantine(
                        spark,
                        document,
                        [{"message": error_msg, "type": "insert_error"}],
                        "insert_error",
                    )
                    failed_inserts += 1
            else:
                # Insert invalid record to quarantine
                insert_to_quarantine(spark, document, errors_list, "validation_error")
                failed_inserts += 1

            if (i + 1) % 100 == 0:
                print(f"Processed {i + 1} documents...")

        except Exception as e:
            print(f"❌ Error processing {doc_file}: {e}")
            # Create document object for quarantine
            document = {
                "document_id": doc_file.name.replace(".txt", ""),
                "document_type": "unknown",
                "content": "",
                "filename": doc_file.name,
                "document_length": 0,
                "word_count": 0,
            }
            insert_to_quarantine(
                spark,
                document,
                [
                    {
                        "message": f"Processing error: {str(e)}",
                        "type": "processing_error",
                    }
                ],
                "processing_error",
            )
            failed_inserts += 1

    print(f"✅ Successfully inserted {successful_inserts} legal documents!")
    if failed_inserts > 0:
        print(f"⚠️  Failed to insert {failed_inserts} documents (sent to quarantine)")

    # Basic ELT validation only
    return basic_load_validation(
        spark, "legal.documents", expected_count=successful_inserts
    )


def insert_nyc_taxi_data(spark):
    """Insert NYC taxi data - ELT operation only"""
    print("Inserting data into NYC taxi table...")

    # Clear existing data
    try:
        spark.sql("TRUNCATE TABLE nyc_taxi_data.yellow_tripdata")
        print("✅ Cleared existing NYC taxi data")
    except Exception as e:
        print(f"⚠️  Could not clear existing data: {e}")

    # Insert data using SQL
    insert_sql = """
    INSERT INTO nyc_taxi_data.yellow_tripdata
    SELECT * FROM parquet.`s3a://data/yellow_tripdata_2022-01.parquet`
    """

    try:
        spark.sql(insert_sql)
        print("✅ NYC taxi data inserted successfully!")

        # Basic ELT validation only
        return basic_load_validation(spark, "nyc_taxi_data.yellow_tripdata")

    except Exception as e:
        print(f"❌ Error inserting NYC taxi data: {e}")
        return False


def verify_elt_operations(spark):
    """Verify basic ELT operations completed successfully"""
    print(f"\n{'='*60}")
    print("ELT OPERATIONS VERIFICATION")
    print(f"{'='*60}")
    print("Note: Data quality checks should be handled by dbt/SQLMesh")

    tables_to_verify = [
        ("nyc_taxi_data.yellow_tripdata", "NYC Taxi Data"),
        ("legal.documents", "Legal Documents"),
    ]

    all_valid = True

    for table_name, description in tables_to_verify:
        print(f"\n🔍 Verifying {description}...")
        valid = basic_load_validation(spark, table_name)
        if not valid:
            all_valid = False

    # Check quarantine table
    print(f"\n🔍 Checking quarantine table...")
    try:
        quarantine_count = spark.sql(
            "SELECT COUNT(*) as count FROM legal.documents_error"
        ).collect()[0]["count"]
        if quarantine_count > 0:
            print(f"⚠️  Found {quarantine_count} records in quarantine table")
            print("📋 Sample quarantined records:")
            spark.sql(
                "SELECT document_id, error_type, error_message FROM legal.documents_error LIMIT 5"
            ).show(truncate=False)
        else:
            print("✅ No records in quarantine table")
    except Exception as e:
        print(f"⚠️  Could not check quarantine table: {e}")

    return all_valid


def insert_files(
    spark,
    docs_dir="data/docs",
    table_name="documents",
    use_parallel=True,
    num_partitions=4,
):
    """
    Unified file insertion function - agnostic to file types and formats
    Supports JSON, text, and parquet files with automatic format detection
    Supports both local filesystem and MinIO/S3 storage

    Args:
        spark: SparkSession
        docs_dir: Directory containing files to process (local path or MinIO path like s3a://bucket/path)
        table_name: Target table name (e.g., "legal.documents")
        use_parallel: Whether to use SQL parallel approach (True) or original approach (False)
        num_partitions: Number of partitions for parallel processing
    """
    print(f"Inserting files from: {docs_dir} into {table_name}")
    print(f"Using {'parallel' if use_parallel else 'original'} approach")

    # Check if this is a MinIO path
    is_minio = is_minio_path(docs_dir)

    if is_minio:
        print(f"🔗 Detected MinIO path: {docs_dir}")
        # List files from MinIO using Spark
        if spark is not None:
            print("📋 Listing MinIO files with Spark...")
            all_files = list_minio_files(spark, docs_dir)
        else:
            print("❌ Spark session required for MinIO access")
            return False

        if not all_files:
            print(f"⚠️  No files found in MinIO path: {docs_dir}")
            return False

        # Filter for supported extensions
        supported_extensions = [".txt", ".json", ".parquet"]
        filtered_files = []
        for file_path in all_files:
            if any(file_path.endswith(ext) for ext in supported_extensions):
                filtered_files.append(file_path)

        if not filtered_files:
            print(f"⚠️  No supported files found in MinIO path: {docs_dir}")
            print(f"Supported formats: {', '.join(supported_extensions)}")
            return False

        all_files = filtered_files
        print(f"Found {len(all_files)} files in MinIO")
    else:
        # Local filesystem processing (existing logic)
        if not os.path.exists(docs_dir):
            print(f"⚠️  Directory not found: {docs_dir}")
            return False

        # Get list of all supported files
        supported_extensions = [".txt", ".json", ".parquet"]
        all_files = []

        for ext in supported_extensions:
            all_files.extend(list(Path(docs_dir).glob(f"*{ext}")))
            all_files.extend(
                list(Path(docs_dir).glob(f"**/*{ext}"))
            )  # Include subdirectories

        if not all_files:
            print(f"⚠️  No supported files found in: {docs_dir}")
            print(f"Supported formats: {', '.join(supported_extensions)}")
            return False

        print(f"Found {len(all_files)} files")

    # Group files by format
    file_groups = {
        "text": [
            f
            for f in all_files
            if f.endswith(".txt") or (hasattr(f, "suffix") and f.suffix == ".txt")
        ],
        "json": [
            f
            for f in all_files
            if f.endswith(".json") or (hasattr(f, "suffix") and f.suffix == ".json")
        ],
        "parquet": [
            f
            for f in all_files
            if f.endswith(".parquet")
            or (hasattr(f, "suffix") and f.suffix == ".parquet")
        ],
    }

    print(f"File breakdown:")
    for format_type, files in file_groups.items():
        if files:
            print(f"   - {format_type}: {len(files)} files")

    # Clear existing data
    try:
        spark.sql(f"TRUNCATE TABLE {table_name}")
        print(f"✅ Cleared existing data in {table_name}")
    except Exception as e:
        print(f"⚠️  Could not clear existing data: {e}")

    if use_parallel:
        return _insert_files_parallel(spark, file_groups, table_name, num_partitions)
    else:
        return _insert_files_base(spark, file_groups, table_name)


def _insert_files_base(spark, file_groups, table_name):
    """base approach - sequential processing"""
    print("🔄 Using original (sequential) approach")

    successful_inserts = 0
    failed_inserts = 0

    for format_type, files in file_groups.items():
        if not files:
            continue

        print(f"\n📁 Processing {format_type} files...")

        for i, file_path in enumerate(files):
            try:
                if format_type == "text":
                    result = _process_text_file(file_path, spark)
                elif format_type == "json":
                    result = _process_json_file(file_path, spark)
                elif format_type == "parquet":
                    result = _process_parquet_file(file_path, spark)
                else:
                    continue

                if result["is_valid"]:
                    # Insert using DataFrame for consistency
                    df = spark.createDataFrame([result["data"]])
                    df.writeTo(table_name).append()
                    successful_inserts += 1
                else:
                    # Insert to quarantine
                    insert_to_quarantine(
                        spark, result["document"], result["errors"], "validation_error"
                    )
                    failed_inserts += 1

            except Exception as e:
                print(f"❌ Error processing {file_path}: {e}")
                # Create error document for quarantine
                filename = (
                    file_path.name
                    if hasattr(file_path, "name")
                    else str(file_path).split("/")[-1]
                )
                error_doc = {
                    "document_id": filename,
                    "document_type": "unknown",
                    "content": "",
                    "filename": filename,
                    "document_length": 0,
                    "word_count": 0,
                }
                insert_to_quarantine(
                    spark,
                    error_doc,
                    [
                        {
                            "message": f"Processing error: {str(e)}",
                            "type": "processing_error",
                        }
                    ],
                    "processing_error",
                )
                failed_inserts += 1

            if (i + 1) % 50 == 0:
                print(f"   Processed {i + 1}/{len(files)} {format_type} files...")

    print(f"\n✅ Successfully inserted {successful_inserts} files!")
    if failed_inserts > 0:
        print(f"⚠️  Failed to insert {failed_inserts} files (sent to quarantine)")

    return basic_load_validation(spark, table_name, expected_count=successful_inserts)


def process_file_sql(file_path_str, spark):
    """Process a single file and return document data - used in SQL UDF"""
    try:
        file_path = (
            Path(file_path_str) if not is_minio_path(file_path_str) else file_path_str
        )
        format_type = (
            file_path.suffix[1:]
            if hasattr(file_path, "suffix")
            else file_path_str.split(".")[-1]
        )

        if format_type == "txt":
            result = _process_text_file(file_path, spark)
        elif format_type == "json":
            result = _process_json_file(file_path, spark)
        elif format_type == "parquet":
            result = _process_parquet_file(file_path, spark)
        else:
            filename = (
                file_path.name
                if hasattr(file_path, "name")
                else file_path_str.split("/")[-1]
            )
            return {
                "document_id": filename,
                "document_type": "unknown",
                "raw_text": "",
                "generation_date": datetime.now(),
                "file_path": file_path_str,
                "document_length": 0,
                "word_count": 0,
                "language": "en",
                "metadata": {"source_file": filename, "file_size": 0},
                "is_valid": False,
                "errors": f"Unsupported format: {format_type}",
            }

        if result["is_valid"]:
            return {**result["data"], "is_valid": True, "errors": None}
        else:
            filename = result["document"]["filename"]
            file_size = result["document"].get("file_size", 0)
            return {
                "document_id": result["document"]["document_id"],
                "document_type": result["document"]["document_type"],
                "raw_text": result["document"]["content"][
                    :1000
                ],  # Truncate for storage
                "generation_date": datetime.now(),
                "file_path": file_path_str,
                "document_length": result["document"]["document_length"],
                "word_count": result["document"]["word_count"],
                "language": "en",
                "metadata": {
                    "source_file": filename,
                    "file_size": file_size,
                },
                "is_valid": False,
                "errors": str(result["errors"]),
            }

    except Exception as e:
        filename = (
            Path(file_path_str).name
            if not is_minio_path(file_path_str)
            else file_path_str.split("/")[-1]
        )
        return {
            "document_id": filename,
            "document_type": "unknown",
            "raw_text": "",
            "generation_date": datetime.now(),
            "file_path": file_path_str,
            "document_length": 0,
            "word_count": 0,
            "language": "en",
            "metadata": {"source_file": filename, "file_size": 0},
            "is_valid": False,
            "errors": f"Processing error: {str(e)}",
        }


def _register_process_file_udf(spark, process_file_sql, schema):
    """Register the file processing UDF with the given schema."""
    return spark.udf.register("process_file", process_file_sql, schema)


def _create_file_paths_df(spark, file_groups, num_partitions):
    """Create a DataFrame of file paths and repartition for parallelism."""
    all_file_paths = []
    for files in file_groups.values():
        all_file_paths.extend([str(f) for f in files])
    file_paths_df = spark.createDataFrame(
        [(path,) for path in all_file_paths], ["file_path"]
    )
    return file_paths_df.repartition(num_partitions, "file_path"), all_file_paths


def _insert_valid_documents(spark, valid_docs, table_name):
    """Insert valid documents into the target table."""
    successful_inserts = 0
    failed_inserts = 0
    valid_count = valid_docs.count()
    if valid_count > 0:
        try:
            print("🔄 Inserting valid documents...")
            valid_docs.writeTo(table_name).append()
            successful_inserts = valid_count
            print(f"✅ Successfully inserted {successful_inserts} documents")
        except Exception as e:
            print(f"❌ Error inserting documents: {e}")
            # Fallback to individual inserts
            for row in valid_docs.collect():
                try:
                    insert_sql = f"""
                    INSERT INTO {table_name} (
                        document_id, document_type, raw_text, generation_date, 
                        file_path, document_length, word_count, language, metadata
                    ) VALUES (
                        '{row['document_id']}', '{row['document_type']}', 
                        '{row['raw_text'].replace("'", "''")}', 
                        CURRENT_TIMESTAMP(), '{row['file_path']}', 
                        {row['document_length']}, {row['word_count']}, 
                        '{row['language']}', map('source_file', '{row['metadata']['source_file']}', 
                        'file_size', '{row['metadata']['file_size']}')
                    )
                    """
                    spark.sql(insert_sql)
                    successful_inserts += 1
                except Exception as insert_error:
                    print(
                        f"❌ Error inserting document {row['document_id']}: {insert_error}"
                    )
                    failed_inserts += 1
    return successful_inserts, failed_inserts


def _insert_invalid_documents(spark, invalid_docs):
    """Insert invalid documents into the quarantine table."""
    failed_inserts = 0
    for row in invalid_docs.collect():
        document = {
            "document_id": row["document_id"],
            "document_type": row["document_type"],
            "content": row["raw_text"],
            "filename": row["metadata"]["source_file"],
            "document_length": row["document_length"],
            "word_count": row["word_count"],
        }
        if row["errors"]:
            insert_to_quarantine(
                spark,
                document,
                [{"message": row["errors"], "type": "validation_error"}],
                "validation_error",
            )
        failed_inserts += 1
    return failed_inserts


def _insert_files_parallel(spark, file_groups, table_name, num_partitions):
    """SQL parallel approach - distributed processing"""
    print("🔄 Using SQL parallel approach")
    spark.conf.set("spark.sql.shuffle.partitions", num_partitions)

    schema = StructType(
        [
            StructField("document_id", StringType(), False),
            StructField("document_type", StringType(), False),
            StructField("raw_text", StringType(), False),
            StructField("generation_date", TimestampType(), False),
            StructField("file_path", StringType(), False),
            StructField("document_length", IntegerType(), False),
            StructField("word_count", IntegerType(), False),
            StructField("language", StringType(), False),
            StructField("metadata", MapType(StringType(), StringType()), False),
            StructField("is_valid", BooleanType(), False),
            StructField("errors", StringType(), True),
        ]
    )

    # Register UDF
    process_file_udf = _register_process_file_udf(spark, process_file_sql, schema)

    # Create DataFrame of file paths
    file_paths_df, all_file_paths = _create_file_paths_df(
        spark, file_groups, num_partitions
    )

    print("🔄 Starting SQL parallel file processing...")
    start_time = time.time()

    # Process files using SQL with UDF
    processed_df = file_paths_df.select(
        process_file_udf("file_path").alias("result")
    ).select("result.*")

    # Split into valid and invalid documents
    valid_docs = processed_df.filter("is_valid = true").drop("is_valid", "errors")
    invalid_docs = processed_df.filter("is_valid = false")

    processing_time = time.time() - start_time
    print(f"✅ SQL parallel processing completed in {processing_time:.2f}s")

    # Insert valid and invalid documents
    successful_inserts, failed_valid_inserts = _insert_valid_documents(
        spark, valid_docs, table_name
    )
    failed_invalid_inserts = _insert_invalid_documents(spark, invalid_docs)
    failed_inserts = failed_valid_inserts + failed_invalid_inserts

    total_time = time.time() - start_time
    throughput = len(all_file_paths) / total_time if total_time > 0 else 0

    print(f"\n📈 Performance Summary:")
    print(f"   - Total files processed: {len(all_file_paths)}")
    print(f"   - Total time: {total_time:.2f}s")
    print(f"   - Throughput: {throughput:.2f} files/sec")
    print(f"   - Successful inserts: {successful_inserts}")
    print(f"   - Failed inserts: {failed_inserts}")

    return basic_load_validation(spark, table_name, expected_count=successful_inserts)


def _process_text_file(file_path, spark=None):
    """Process a text file and return document data - supports both local and MinIO files"""
    try:
        file_path_str = str(file_path)

        # Check if this is a MinIO path
        if is_minio_path(file_path_str):
            if spark is None:
                raise ValueError("Spark session required for MinIO file processing")

            # Read content from MinIO using Spark
            print(f"📖 Reading MinIO file with Spark: {file_path_str}")
            content = read_minio_file_content(spark, file_path_str)
            file_size = get_minio_file_size(spark, file_path_str)

            # Extract filename from MinIO path
            path_info = get_minio_path_info(file_path_str)
            filename = (
                path_info["key"].split("/")[-1] if path_info["key"] else "unknown"
            )
        else:
            # Local file processing (existing logic)
            content = ""
            with open(file_path, "r", encoding="utf-8") as f:
                chunk_size = 1024 * 1024  # 1MB chunks
                while True:
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break
                    content += chunk

                    # Check if we're exceeding reasonable limits
                    if len(content) > 100 * 1024 * 1024:  # 100MB limit
                        raise ValueError(f"File {file_path.name} exceeds 100MB limit")

            file_size = file_path.stat().st_size
            filename = file_path.name

        # Extract metadata from filename
        parts = filename.replace(".txt", "").split("_")

        if len(parts) >= 4:
            doc_id = f"{parts[1]}_{parts[2]}"
            doc_type = parts[3]
        else:
            doc_id = filename.replace(".txt", "")
            doc_type = "unknown"

        # Calculate document statistics
        doc_length = len(content)
        word_count = len(content.split())

        # Create document object for validation
        document = {
            "document_id": doc_id,
            "document_type": doc_type,
            "content": content,
            "filename": filename,
            "document_length": doc_length,
            "word_count": word_count,
        }

        # Validate document quality
        is_valid, errors_list = validate_document_quality(document)

        if is_valid:
            return {
                "is_valid": True,
                "data": {
                    "document_id": doc_id,
                    "document_type": doc_type,
                    "raw_text": content,
                    "generation_date": datetime.now(),
                    "file_path": file_path_str,
                    "document_length": doc_length,
                    "word_count": word_count,
                    "language": "en",
                    "metadata": {
                        "source_file": filename,
                        "file_size": file_size,
                        "source": "minio" if is_minio_path(file_path_str) else "local",
                        "method": "spark" if is_minio_path(file_path_str) else "local",
                    },
                },
                "document": document,
                "errors": None,
            }
        else:
            return {
                "is_valid": False,
                "data": None,
                "document": document,
                "errors": errors_list,
            }

    except Exception as e:
        filename = (
            file_path.name
            if hasattr(file_path, "name")
            else str(file_path).split("/")[-1]
        )
        return {
            "is_valid": False,
            "data": None,
            "document": {
                "document_id": filename.replace(".txt", ""),
                "document_type": "unknown",
                "content": "",
                "filename": filename,
                "document_length": 0,
                "word_count": 0,
            },
            "errors": [
                {"message": f"Processing error: {str(e)}", "type": "processing_error"}
            ],
        }


def _process_json_file(file_path, spark=None):
    """Process a JSON file and return document data - supports both local and MinIO files"""
    try:
        import json

        file_path_str = str(file_path)

        # Check if this is a MinIO path
        if is_minio_path(file_path_str):
            if spark is None:
                raise ValueError("Spark session required for MinIO file processing")

            # Read JSON from MinIO using Spark
            print(f"📖 Reading MinIO JSON file with Spark: {file_path_str}")
            df = spark.read.json(file_path_str)
            # Convert to JSON string
            data = df.toPandas().to_dict("records")[0] if df.count() > 0 else {}
            file_size = get_minio_file_size(spark, file_path_str)

            # Extract filename from MinIO path
            path_info = get_minio_path_info(file_path_str)
            filename = (
                path_info["key"].split("/")[-1] if path_info["key"] else "unknown"
            )
        else:
            # Local file processing (existing logic)
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            file_size = file_path.stat().st_size
            filename = file_path.name

        # Extract document information from JSON
        # Assume JSON has fields like document_id, content, document_type, etc.
        doc_id = data.get("document_id", filename.replace(".json", ""))
        doc_type = data.get("document_type", "json_document")
        content = data.get(
            "content", json.dumps(data, indent=2)
        )  # Fallback to full JSON

        # Calculate document statistics
        doc_length = len(content)
        word_count = len(content.split())

        # Create document object for validation
        document = {
            "document_id": doc_id,
            "document_type": doc_type,
            "content": content,
            "filename": filename,
            "document_length": doc_length,
            "word_count": word_count,
        }

        # Validate document quality
        is_valid, errors_list = validate_document_quality(document)

        if is_valid:
            return {
                "is_valid": True,
                "data": {
                    "document_id": doc_id,
                    "document_type": doc_type,
                    "raw_text": content,
                    "generation_date": datetime.now(),
                    "file_path": file_path_str,
                    "document_length": doc_length,
                    "word_count": word_count,
                    "language": "en",
                    "metadata": {
                        "source_file": filename,
                        "file_size": file_size,
                        "format": "json",
                        "source": "minio" if is_minio_path(file_path_str) else "local",
                        "method": "spark" if is_minio_path(file_path_str) else "local",
                    },
                },
                "document": document,
                "errors": None,
            }
        else:
            return {
                "is_valid": False,
                "data": None,
                "document": document,
                "errors": errors_list,
            }

    except Exception as e:
        filename = (
            file_path.name
            if hasattr(file_path, "name")
            else str(file_path).split("/")[-1]
        )
        return {
            "is_valid": False,
            "data": None,
            "document": {
                "document_id": filename.replace(".json", ""),
                "document_type": "unknown",
                "content": "",
                "filename": filename,
                "document_length": 0,
                "word_count": 0,
            },
            "errors": [
                {
                    "message": f"JSON processing error: {str(e)}",
                    "type": "processing_error",
                }
            ],
        }


def _process_parquet_file(file_path, spark):
    """Process a parquet file and return document data - supports both local and MinIO files"""
    try:
        file_path_str = str(file_path)

        # Read parquet file using Spark (works for both local and MinIO)
        df = spark.read.parquet(file_path_str)

        # Convert to document format
        # For parquet files, we'll create a summary document
        row_count = df.count()
        column_count = len(df.columns)

        # Create a summary of the parquet file
        filename = (
            file_path.name
            if hasattr(file_path, "name")
            else file_path_str.split("/")[-1]
        )
        content = f"Parquet file: {filename}\n"
        content += f"Rows: {row_count}\n"
        content += f"Columns: {column_count}\n"
        content += f"Schema: {', '.join(df.columns)}\n"

        # Add sample data
        sample_data = df.limit(5).toPandas().to_string()
        content += f"\nSample data:\n{sample_data}"

        doc_id = filename.replace(".parquet", "")
        doc_type = "parquet_document"

        # Calculate document statistics
        doc_length = len(content)
        word_count = len(content.split())

        # Get file size
        if is_minio_path(file_path_str):
            file_size = get_minio_file_size(spark, file_path_str)
        else:
            file_size = file_path.stat().st_size if hasattr(file_path, "stat") else 0

        # Create document object for validation
        document = Document(
            document_id=doc_id,
            document_type=doc_type,
            content=content,
            filename=filename,
            document_length=doc_length,
            word_count=word_count,
        )

        # Validate document quality
        is_valid, errors_list = validate_document_quality(document)

        if is_valid:
            document_data = DocumentData(
                document_id=doc_id,
                document_type=doc_type,
                raw_text=content,
                generation_date=datetime.now(),
                file_path=file_path_str,
                document_length=doc_length,
                word_count=word_count,
                language="en",
                metadata={
                    "source_file": filename,
                    "file_size": str(file_size),
                    "format": "parquet",
                    "rows": str(row_count),
                    "columns": str(column_count),
                    "source": "minio" if is_minio_path(file_path_str) else "local",
                },
            )

            return ProcessingResult(
                is_valid=True, data=document_data, document=document, errors=None
            )
        else:
            # Convert ValidationError to ProcessingError for consistency
            processing_errors = [
                ProcessingError(message=error.message, type=error.type)
                for error in errors_list
            ]

            return ProcessingResult(
                is_valid=False, data=None, document=document, errors=processing_errors
            )

    except Exception as e:
        filename = (
            file_path.name
            if hasattr(file_path, "name")
            else str(file_path).split("/")[-1]
        )
        error_document = Document(
            document_id=filename.replace(".parquet", ""),
            document_type="unknown",
            content="",
            filename=filename,
            document_length=0,
            word_count=0,
        )

        return ProcessingResult(
            is_valid=False,
            data=None,
            document=error_document,
            errors=[ProcessingError(message=f"Parquet processing error: {str(e)}")],
        )


def analyze_files_for_processing(docs_dir):
    """
    Analyze files in directory to determine optimal processing approach

    Returns:
        dict: Analysis results with recommendations
    """
    if not os.path.exists(docs_dir):
        return {
            "file_count": 0,
            "total_size_mb": 0,
            "avg_size_mb": 0,
            "recommended_parallel": False,
            "recommended_partitions": 1,
            "reason": "Directory not found",
            "file_breakdown": {"text": 0, "json": 0, "parquet": 0},
        }

    # Get all supported files
    supported_extensions = [".txt", ".json", ".parquet"]
    all_files = []

    for ext in supported_extensions:
        all_files.extend(list(Path(docs_dir).glob(f"*{ext}")))
        all_files.extend(list(Path(docs_dir).glob(f"**/*{ext}")))

    if not all_files:
        return {
            "file_count": 0,
            "total_size_mb": 0,
            "avg_size_mb": 0,
            "recommended_parallel": False,
            "recommended_partitions": 1,
            "reason": "No supported files found",
            "file_breakdown": {"text": 0, "json": 0, "parquet": 0},
        }

    # Calculate statistics
    file_count = len(all_files)
    total_size_bytes = sum(f.stat().st_size for f in all_files)
    total_size_mb = total_size_bytes / (1024 * 1024)
    avg_size_mb = total_size_mb / file_count if file_count > 0 else 0

    # Determine optimal approach
    recommended_parallel = False
    recommended_partitions = 1
    reason = ""

    if file_count >= 100:
        recommended_parallel = True
        recommended_partitions = min(16, max(4, file_count // 100))  # 4-16 partitions
        reason = f"Large file count ({file_count}) - parallel processing recommended"
    elif file_count >= 50 and avg_size_mb > 1:
        recommended_parallel = True
        recommended_partitions = min(8, max(2, file_count // 50))
        reason = f"Medium file count ({file_count}) with large average size ({avg_size_mb:.1f}MB) - parallel processing recommended"
    elif total_size_mb > 100:
        recommended_parallel = True
        recommended_partitions = min(8, max(2, file_count // 25))
        reason = f"Large total size ({total_size_mb:.1f}MB) - parallel processing recommended"
    else:
        recommended_parallel = False
        recommended_partitions = 1
        reason = f"Small dataset ({file_count} files, {total_size_mb:.1f}MB) - sequential processing sufficient"

    return {
        "file_count": file_count,
        "total_size_mb": total_size_mb,
        "avg_size_mb": avg_size_mb,
        "recommended_parallel": recommended_parallel,
        "recommended_partitions": recommended_partitions,
        "reason": reason,
        "file_breakdown": {
            "text": len([f for f in all_files if f.suffix == ".txt"]),
            "json": len([f for f in all_files if f.suffix == ".json"]),
            "parquet": len([f for f in all_files if f.suffix == ".parquet"]),
        },
    }


def main():
    """
    Main function to create tables and insert data with automatic processing optimization
    """
    parser = argparse.ArgumentParser(
        description="Insert files into Spark tables with automatic optimization"
    )
    parser.add_argument("file_dir", help="Directory containing files to process")
    parser.add_argument(
        "table_name", help="Target table name (e.g., 'legal.documents')"
    )
    parser.add_argument(
        "--parallel", action="store_true", help="Force parallel processing"
    )
    parser.add_argument(
        "--partitions",
        type=int,
        help="Number of partitions for parallel processing",
        default=None,
    )
    parser.add_argument(
        "--table-op",
        type=str,
        help="table operation",
        required=False,
        default="insert",
        choices=["insert", "append"],
    )

    args = parser.parse_args()

    file_dir = args.file_dir
    table_name = args.table_name

    print("🚀 Starting file analysis and data insertion...")

    # Analyze files to determine optimal processing approach
    print(f"\n📊 Analyzing files in: {file_dir}")
    analysis = analyze_files_for_processing(file_dir)

    print(f"📈 File Analysis Results:")
    print(f"   - Total files: {analysis['file_count']:,}")
    print(f"   - Total size: {analysis['total_size_mb']:.1f} MB")
    print(f"   - Average file size: {analysis['avg_size_mb']:.1f} MB")
    print(f"   - File breakdown:")
    for file_type, count in analysis["file_breakdown"].items():
        if count > 0:
            print(f"     * {file_type}: {count} files")

    # Determine processing approach
    use_parallel = analysis["recommended_parallel"]
    approach_reason = analysis["reason"]

    if args.parallel:
        use_parallel = True

    # Determine partition count
    partitions = analysis["recommended_partitions"]
    if args.partitions:
        partitions = args.partitions
    partition_reason = f"Auto-optimized to {partitions} partitions"

    print(f"\n🎯 Processing Strategy:")
    print(f"   - Approach: {'Parallel' if use_parallel else 'Sequential'}")
    print(f"   - Reason: {approach_reason}")
    print(f"   - Partitions: {partitions}")
    print(f"   - Partition reason: {partition_reason}")
    print(f"   - Table operation: {args.table_op}")

    app_name = Path(__file__).stem
    spark = create_spark_session(
        spark_version=SparkVersion.SPARK_3_5,
        app_name=app_name,
        iceberg_config=IcebergConfig(
            s3_config=S3FileSystemConfig(),
        ),
    )

    if not spark:
        print("❌ Failed to create Spark session. Exiting.")
        return False

    print("✅ Spark session created successfully")

    if args.table_op == "insert":
        spark.sql(f"TRUNCATE TABLE {table_name}")

    # Insert data using the unified function with optimized settings
    print(f"\n📁 Inserting data into {table_name}...")

    @custom_logger(__name__, require_spark=True)
    def run_etl(spark=spark):
        return insert_files(
            spark=spark,
            docs_dir=file_dir,
            table_name=table_name,
            use_parallel=use_parallel,
            num_partitions=partitions,
        )

    run_etl()

    try:
        count_result = spark.sql(f"SELECT COUNT(*) as count FROM {table_name}")
        final_count = count_result.collect()[0]["count"]
        print(f"📊 Final table count: {final_count:,} records")
    except Exception as e:
        print(f"⚠️  Could not get final count: {e}")
    else:
        print(f"❌ Failed to insert files into {table_name}")

    print("\n🎉 Data insertion completed!")
    print(spark.job_id)


if __name__ == "__main__":
    main()
