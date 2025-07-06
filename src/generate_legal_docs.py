#!/usr/bin/env python3
"""
Script to generate legal text documents using template formatting with Faker data.
Uses Python MinIO library to save documents in structured format: /docs/legal/{doc_type}/{date}/{uuid}.txt
Also creates separate JSON metadata files: /docs/legal/{doc_type}/{date}/{uuid}.json
Implements data validation using schemas from schema.py
Parallelization using spark not used because of the subfolder creation
"""

import os
import random
import uuid
import json
import io
import argparse
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Any

from faker import Faker

from minio import Minio
from minio.error import S3Error

from src.schemas.schema import SchemaManager

# Initialize Faker
fake = Faker()

# Initialize SchemaManager
schema_manager = SchemaManager()


def create_minio_client() -> Minio:
    """
    Create and configure MinIO client

    Returns:
        Minio: Configured MinIO client
    """
    # Hardcode localhost:9000 since this script is for document generation, not Spark
    minio_endpoint = "localhost:9000"

    print(f"Connecting to MinIO at: {minio_endpoint}")

    # Use AWS environment variables for consistency with Spark S3A
    access_key = os.getenv("AWS_ACCESS_KEY_ID", "admin")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "password")

    minio_client = Minio(
        minio_endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=False,  # HTTP for local development
    )
    return minio_client


def load_template_content(template_name: str) -> str:
    """
    Load template content from templates directory

    Args:
        template_name: Name of the template file (without .template extension)

    Returns:
        Template content as string
    """
    template_path = Path(__file__).parent / "templates" / f"{template_name}.template"
    with open(template_path, "r", encoding="utf-8") as f:
        return f.read()


def generate_template_data(
    doc_type: Dict[str, Any], document_number: int
) -> Dict[str, Any]:
    """
    Generate template data for document formatting

    Args:
        doc_type: Document type configuration
        document_number: Document number

    Returns:
        Dictionary with template data
    """
    # Generate realistic data using Faker
    fake = Faker()

    # Company and business data
    company_name = fake.company()
    company_email = fake.company_email()
    company_address = fake.address()
    company_phone = fake.phone_number()

    # Business details
    business_type = fake.random_element(
        ["Corporation", "LLC", "Partnership", "Sole Proprietorship"]
    )
    selected_industry = fake.random_element(
        [
            "Technology",
            "Healthcare",
            "Finance",
            "Manufacturing",
            "Retail",
            "Real Estate",
            "Education",
            "Consulting",
        ]
    )

    # Legal personnel
    attorney_name = fake.name()
    department_head = fake.name()

    # Parties for contracts
    party_a_name = fake.company()
    party_b_name = fake.company()

    # Dates
    current_date = fake.date_between(start_date="-1y", end_date="today").strftime(
        "%Y-%m-%d"
    )
    current_year = datetime.now(timezone.utc).year

    template_data = {
        "document_number": f"{document_number:04d}",
        "date": current_date,
        "company": company_name,
        "industry": selected_industry,
        "legal_area": "General",
        "compliance_area": "General",
        "training_area": "General",
        "guidance_area": "General",
        "practice_area": "General",
        "risk_area": "General",
        "concern_area": "General",
        "documentation_area": "General",
        "legal_matter": "General",
        "policy_type": "General",
        "subject": "Legal",
        "to_recipient": f"Legal Department - {department_head}",
        "from_author": attorney_name,
        "attorney_name": attorney_name,
        "department": "Legal Compliance",
        "contact_email": company_email,
        "company_address": company_address,
        "company_phone": company_phone,
        "filing_date": current_date,
        "effective_date": current_date,
        "start_date": current_date,
        "case_number": f"CV-{current_year}-{document_number:04d}",
        "policy_number": f"POL-{current_year}-{document_number:04d}",
        "parties": f"{party_a_name} and {party_b_name}",
        "party_a": party_a_name,
        "party_b": party_b_name,
        "business_type": business_type,
        "service_area": "general legal services",
        "service_type": "legal",
        "hourly_rate": f"${random.randint(100, 300)}",
        "notice_period": f"{random.randint(15, 60)} days",
    }

    return template_data


def generate_document_content(doc_type: Dict[str, Any], document_number: int) -> str:
    """
    Generate document content using template formatting

    Args:
        doc_type: Document type configuration
        document_number: Document number

    Returns:
        Generated document content
    """
    # Load template content
    template_name = doc_type["name"]
    template_content = load_template_content(template_name)

    # Generate template data
    template_data = generate_template_data(doc_type, document_number)

    # Format template with data using Python string formatting
    try:
        content = template_content.format(**template_data)
        return content
    except Exception as e:
        raise Exception(f"Error formatting template for {doc_type['name']}: {e}")


def create_document_metadata(doc: Dict[str, Any], doc_uuid: str) -> Dict[str, Any]:
    """
    Create metadata dictionary with UTC timestamps using Z suffix
    """
    # Get the metadata schema to understand required fields
    metadata_schema = schema_manager.get_schema("legal_doc_metadata")
    if not metadata_schema:
        raise ValueError("Could not load legal_doc_metadata schema")

    # Get required fields from schema
    required_fields = schema_manager.get_required_fields("legal_doc_metadata")

    # Get schema properties to understand field types and defaults
    properties = metadata_schema.get("properties", {})

    # Build metadata dynamically based on schema
    metadata = {}

    # Handle each required field based on schema definition
    for field_name in required_fields:
        if field_name == "document_id":
            metadata[field_name] = doc["document_id"]
        elif field_name == "document_type":
            metadata[field_name] = doc["document_type"]
        elif field_name == "generated_at":
            # Ensure UTC timestamp in ISO-8601 format with Z suffix
            generated_at = doc["generated_at"]
            if isinstance(generated_at, datetime):
                # Convert to UTC if not already
                if generated_at.tzinfo is None:
                    generated_at = generated_at.replace(tzinfo=timezone.utc)
                elif generated_at.tzinfo != timezone.utc:
                    generated_at = generated_at.astimezone(timezone.utc)

                # Format with Z suffix (not +00:00)
                metadata[field_name] = (
                    generated_at.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
                )
            else:
                # If it's already a string, ensure it ends with Z
                if isinstance(generated_at, str):
                    if generated_at.endswith("+00:00"):
                        generated_at = generated_at.replace("+00:00", "Z")
                    elif not generated_at.endswith("Z"):
                        # Try to parse and reformat
                        try:
                            dt = datetime.fromisoformat(
                                generated_at.replace("Z", "+00:00")
                            )
                            if dt.tzinfo is None:
                                dt = dt.replace(tzinfo=timezone.utc)
                            else:
                                dt = dt.astimezone(timezone.utc)
                            generated_at = (
                                dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
                            )
                        except:
                            pass  # Keep original if parsing fails
                metadata[field_name] = generated_at
        elif field_name == "source":
            # Use default from schema if available
            field_props = properties.get(field_name, {})
            default_value = field_props.get("default", "soli_legal_document_generator")
            metadata[field_name] = default_value
        elif field_name == "language":
            # Use default from schema if available
            field_props = properties.get(field_name, {})
            default_value = field_props.get("default", "en")
            metadata[field_name] = default_value
        elif field_name == "file_size":
            # Calculate file size from content
            content_size = len(doc["content"].encode("utf-8"))
            metadata[field_name] = content_size
        elif field_name == "file_path":
            # Set placeholder that will be updated during save
            metadata[field_name] = f"placeholder/{doc['document_id']}.txt"
        elif field_name == "method":
            # Use default method for generation
            metadata[field_name] = "sequential"
        else:
            # For any other fields, use null if not relevant
            metadata[field_name] = None

    return metadata


def save_documents_to_minio(
    documents: List[Dict[str, Any]],
    bucket_name: str = "data",
    key: str = "docs/legal",
) -> None:
    """
    Save documents with separate content and metadata directories in MinIO.
    Structure: {key}/{document_type}/{date}/content/{uuid}.txt + metadata/{uuid}.json

    Args:
        documents: List of document dictionaries
        bucket_name: MinIO bucket name
        key: Key path for documents within the bucket
    """
    print(f"Saving {len(documents)} documents with metadata to MinIO")
    print(f"Bucket: {bucket_name}")
    print(f"Key: {key}")

    # Create MinIO client
    minio_client = create_minio_client()

    # Get current date for directory structure (use UTC to avoid timezone issues)
    current_date = datetime.now(timezone.utc).strftime("%Y%m%d")

    total_saved = 0
    total_metadata_saved = 0

    # Save each document individually with structured path
    for i, doc in enumerate(documents):
        doc_type = doc["document_type"]
        doc_id = doc["document_id"]

        # Create file paths with separate content and metadata directories
        content_filename = f"{doc_id}.txt"
        metadata_filename = f"{doc_id}.json"

        # Build full paths within the bucket with separate directories
        content_path = f"{key}/{doc_type}/{current_date}/content/{content_filename}"
        metadata_path = f"{key}/{doc_type}/{current_date}/metadata/{metadata_filename}"

        # Prepare content
        content = doc["content"]
        content_bytes = content.encode("utf-8")

        # Create metadata
        metadata = create_document_metadata(doc, doc_id)

        # Validate metadata before saving
        if not schema_manager.validate_legal_document_metadata(metadata):
            print(
                f"❌ Metadata validation failed for document {doc['document_id']} - Skipping save."
            )
            continue

        metadata_bytes = json.dumps(metadata, indent=2).encode("utf-8")

        try:
            # Upload document content using BytesIO
            content_stream = io.BytesIO(content_bytes)
            minio_client.put_object(
                bucket_name,
                content_path,
                content_stream,
                length=len(content_bytes),
                content_type="text/plain",
            )

            # Upload metadata using BytesIO
            metadata_stream = io.BytesIO(metadata_bytes)
            minio_client.put_object(
                bucket_name,
                metadata_path,
                metadata_stream,
                length=len(metadata_bytes),
                content_type="application/json",
            )

            # Update document record with file paths (use s3a:// format)
            doc["filename"] = content_filename
            doc["file_path"] = f"s3a://{bucket_name}/{content_path}"
            doc["metadata_path"] = f"s3a://{bucket_name}/{metadata_path}"

            total_saved += 1
            total_metadata_saved += 1

            if (i + 1) % 100 == 0:
                print(f"Saved {i + 1} document pairs (content + metadata)...")

        except S3Error as e:
            print(f"Error saving document {i+1}: {e}")
            continue

    print(f"\nSuccessfully saved {total_saved} text files to MinIO")
    print(f"Successfully saved {total_metadata_saved} metadata files to MinIO")
    print(
        f"Structure: s3a://{bucket_name}/{key}/{{doc_type}}/{current_date}/content/{{uuid}}.txt + metadata/{{uuid}}.json"
    )
    print(f"Date directory: {current_date}")


def generate_legal_documents(num_docs: int = 1000) -> List[Dict[str, Any]]:
    """
    Generate legal documents with UTC timestamps
    """
    # Get legal document types from models
    legal_doc_types = schema_manager.get_all_legal_doc_types()

    print(f"Generating {num_docs} legal documents using template formatting...")

    # Track document generation statistics
    doc_type_counts = {doc_type["name"]: 0 for doc_type in legal_doc_types}

    # List to store all generated documents
    documents = []

    for i in range(num_docs):
        # Randomly select document type
        doc_type = random.choice(legal_doc_types)
        doc_type_counts[doc_type["name"]] += 1

        try:
            # Generate content using template formatting
            content = generate_document_content(doc_type, i + 1)

            # Generate UUID for document ID
            doc_uuid = str(uuid.uuid4())

            # Create document record with UTC timestamp
            document = {
                "document_id": doc_uuid,  # Use UUID instead of counter
                "document_type": doc_type["name"],
                "content": content,
                "filename": "",
                "file_path": "",
                "metadata_path": "",
                "generated_at": datetime.now(timezone.utc),  # UTC timestamp
            }

            documents.append(document)

            if (i + 1) % 100 == 0:
                print(f"Generated {i + 1} documents...")

        except Exception as e:
            print(f"Error generating document {i+1}: {e}")
            # Continue with next document instead of using fallback content
            continue

    # Print generation statistics
    print(f"\n=== Generation Complete ===")
    print(f"Total documents generated: {len(documents)}")
    print(f"Document type distribution:")
    for doc_type, count in doc_type_counts.items():
        percentage = (count / num_docs) * 100
        print(f"  {doc_type}: {count} ({percentage:.1f}%)")

    return documents


def generate_specific_document_type(
    doc_type_name: str, num_docs: int = 100
) -> List[Dict[str, Any]]:
    """
    Generate documents of a specific type only

    Args:
        doc_type_name: Name of the document type to generate
        num_docs: Number of documents to generate

    Returns:
        List of generated document dictionaries
    """
    # Get specific document type
    doc_type = schema_manager.get_legal_doc_type(doc_type_name)
    if not doc_type:
        raise ValueError(f"Invalid document type: {doc_type_name}")

    print(
        f"Generating {num_docs} {doc_type_name} documents using template formatting..."
    )

    documents = []

    for i in range(num_docs):
        try:
            # Generate content using template formatting
            content = generate_document_content(doc_type, i + 1)

            # Generate UUID for document ID
            doc_uuid = str(uuid.uuid4())

            # Create document record
            document = {
                "document_id": doc_uuid,  # Use UUID instead of counter
                "document_type": doc_type["name"],
                "content": content,
                "filename": "",  # Will be set during save
                "file_path": "",  # Will be set during save
                "metadata_path": "",  # Will be set during save
                "generated_at": datetime.now(timezone.utc),
            }

            documents.append(document)

            if (i + 1) % 50 == 0:
                print(f"Generated {i + 1} {doc_type_name} documents...")

        except Exception as e:
            print(f"Error generating document {i+1}: {e}")
            # Continue with next document instead of using fallback content
            continue

    print(f"Successfully generated {len(documents)} {doc_type_name} documents")
    return documents


def main():
    """Main function to generate legal documents and save to specified location"""

    parser = argparse.ArgumentParser(
        description="Generate legal documents using template formatting"
    )

    parser.add_argument(
        "--num-docs",
        type=int,
        default=1000,
        help="Number of documents to generate (default: 1000)",
    )

    parser.add_argument(
        "--bucket",
        type=str,
        default="data",
        help="MinIO bucket name (default: data)",
    )

    parser.add_argument(
        "--key",
        type=str,
        default="docs/legal",
        help="Key path for documents in MinIO (default: docs/legal)",
    )

    parser.add_argument(
        "--doc-type", type=str, help="Generate only documents of this specific type"
    )

    args = parser.parse_args()

    try:
        # Generate documents
        if args.doc_type:
            # Generate specific document type
            documents = generate_specific_document_type(args.doc_type, args.num_docs)
        else:
            # Generate all document types randomly
            documents = generate_legal_documents(num_docs=args.num_docs)

        if not documents:
            print("No documents were generated successfully. Exiting.")
            return

        save_documents_to_minio(documents, args.bucket, args.key)

    except Exception as e:
        print(f"Error in main execution: {e}")


if __name__ == "__main__":
    main()
