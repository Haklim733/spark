#!/usr/bin/env python3
"""
Script to generate legal text documents using template formatting with Faker data.
Uses Python MinIO library to save documents in structured format: /docs/legal/{doc_type}/{date}/{uuid}.txt
Also creates separate JSON metadata files: /docs/legal/{doc_type}/{date}/{uuid}.json
Implements data validation using schemas from models.py
"""

import random
import uuid
import json
import io
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any

from faker import Faker

# Import MinIO client
from minio import Minio
from minio.error import S3Error

# Import shared legal document models and validation
from models import (
    get_all_legal_doc_types,
    get_legal_doc_type,
    validate_legal_document_metadata,
)

# Initialize Faker
fake = Faker()


def create_minio_client() -> Minio:
    """
    Create and configure MinIO client

    Returns:
        Minio: Configured MinIO client
    """
    minio_client = Minio(
        "localhost:9000",
        access_key="admin",
        secret_key="password",
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
    Generate template data for document generation using Faker for realistic data

    Args:
        doc_type: Document type configuration
        document_number: Document number for this generation

    Returns:
        Dictionary with template variables
    """
    current_date = datetime.now().strftime("%Y-%m-%d")
    current_year = datetime.now().year

    # Generate realistic names and data using Faker
    company_name = fake.company()
    attorney_name = fake.name()
    party_a_name = fake.name()
    party_b_name = fake.name()
    department_head = fake.name()
    company_address = fake.address()
    company_phone = fake.phone_number()
    company_email = fake.company_email()

    # Generate industry-specific data
    industries = [
        "Technology",
        "Healthcare",
        "Finance",
        "Manufacturing",
        "Retail",
        "Consulting",
        "Real Estate",
        "Education",
    ]
    selected_industry = random.choice(industries)

    # Generate business types based on industry
    business_types = {
        "Technology": [
            "software development",
            "IT consulting",
            "cybersecurity",
            "data analytics",
        ],
        "Healthcare": [
            "medical services",
            "pharmaceuticals",
            "healthcare consulting",
            "medical devices",
        ],
        "Finance": [
            "investment banking",
            "asset management",
            "insurance",
            "financial consulting",
        ],
        "Manufacturing": ["automotive", "electronics", "chemicals", "textiles"],
        "Retail": ["e-commerce", "brick-and-mortar", "wholesale", "fashion"],
        "Consulting": [
            "management consulting",
            "strategy consulting",
            "operations consulting",
            "technology consulting",
        ],
        "Real Estate": [
            "property development",
            "property management",
            "real estate investment",
            "commercial leasing",
        ],
        "Education": [
            "higher education",
            "K-12 education",
            "online learning",
            "educational technology",
        ],
    }

    business_type = random.choice(
        business_types.get(selected_industry, ["general business"])
    )

    # Base template data
    template_data = {
        "document_number": f"{document_number:04d}",
        "date": current_date,
        "company": company_name,
        "industry": selected_industry,
        "keywords": ", ".join(doc_type["keywords"]),
        "legal_area": doc_type["keywords"][0] if doc_type["keywords"] else "General",
        "compliance_area": (
            doc_type["keywords"][0] if doc_type["keywords"] else "General"
        ),
        "training_area": doc_type["keywords"][0] if doc_type["keywords"] else "General",
        "guidance_area": doc_type["keywords"][0] if doc_type["keywords"] else "General",
        "practice_area": doc_type["keywords"][0] if doc_type["keywords"] else "General",
        "risk_area": doc_type["keywords"][0] if doc_type["keywords"] else "General",
        "concern_area": doc_type["keywords"][0] if doc_type["keywords"] else "General",
        "documentation_area": (
            doc_type["keywords"][0] if doc_type["keywords"] else "General"
        ),
        "legal_matter": doc_type["keywords"][0] if doc_type["keywords"] else "General",
        "policy_type": doc_type["keywords"][0] if doc_type["keywords"] else "General",
        "subject": doc_type["keywords"][0] if doc_type["keywords"] else "Legal",
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
        "service_area": (
            doc_type["keywords"][0]
            if doc_type["keywords"]
            else "general legal services"
        ),
        "service_type": doc_type["keywords"][0] if doc_type["keywords"] else "legal",
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
    Create metadata dictionary for a document

    Args:
        doc: Document dictionary
        doc_uuid: UUID of the document

    Returns:
        Dict containing document metadata
    """
    metadata = {
        "document_id": doc["document_id"],
        "document_type": doc["document_type"],
        "uuid": doc_uuid,
        "content_filename": f"{doc_uuid}.txt",
        "metadata_filename": f"{doc_uuid}.json",
        "keywords": doc["keywords"],
        "generated_at": (
            doc["generated_at"].isoformat()
            if isinstance(doc["generated_at"], datetime)
            else str(doc["generated_at"])
        ),
        "content_length": len(doc["content"]),
        "content_preview": (
            doc["content"][:200] + "..."
            if len(doc["content"]) > 200
            else doc["content"]
        ),
        "processing_timestamp": datetime.now().isoformat(),
        "source": "soli_legal_document_generator",
        "metadata_version": "1.0",
    }

    return metadata


def save_documents_to_minio(
    documents: List[Dict[str, Any]],
    bucket_name: str = "data",
    base_path: str = "docs/legal",
) -> None:
    """
    Save documents as individual text files with separate JSON metadata files in MinIO.
    Structure: /docs/legal/{document_type}/{date}/{uuid}.txt + {uuid}.json

    Args:
        documents: List of document dictionaries
        bucket_name: MinIO bucket name
        base_path: Base path for documents
    """
    print(f"Saving {len(documents)} documents with metadata to MinIO")

    # Create MinIO client
    minio_client = create_minio_client()

    # Get current date for directory structure
    current_date = datetime.now().strftime("%Y%m%d")

    total_saved = 0
    total_metadata_saved = 0

    # Save each document individually with structured path
    for i, doc in enumerate(documents):
        doc_type = doc["document_type"]

        # Generate UUID for filename
        doc_uuid = str(uuid.uuid4())

        # Create file paths
        content_filename = f"{doc_uuid}.txt"
        metadata_filename = f"{doc_uuid}.json"

        content_path = f"{base_path}/{doc_type}/{current_date}/{content_filename}"
        metadata_path = f"{base_path}/{doc_type}/{current_date}/{metadata_filename}"

        # Prepare content
        content = doc["content"]
        content_bytes = content.encode("utf-8")

        # Create metadata
        metadata = create_document_metadata(doc, doc_uuid)
        metadata["content_file_path"] = f"s3://{bucket_name}/{content_path}"
        metadata["metadata_file_path"] = f"s3://{bucket_name}/{metadata_path}"

        # Validate metadata before saving
        if not validate_legal_document_metadata(metadata):
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

            # Update document record with file paths
            doc["filename"] = content_filename
            doc["file_path"] = f"s3://{bucket_name}/{content_path}"
            doc["metadata_path"] = f"s3://{bucket_name}/{metadata_path}"

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
        f"Structure: s3://{bucket_name}/{base_path}/{{doc_type}}/{current_date}/{{uuid}}.txt + .json"
    )
    print(f"Date directory: {current_date}")


def generate_legal_documents(num_docs: int = 1000) -> List[Dict[str, Any]]:
    """
    Generate legal documents using template formatting

    Args:
        num_docs: Number of documents to generate

    Returns:
        List of generated document dictionaries
    """
    # Get legal document types from models
    legal_doc_types = get_all_legal_doc_types()

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

            # Create document record
            document = {
                "document_id": f"doc_{i+1:04d}",
                "document_type": doc_type["name"],
                "content": content,
                "keywords": doc_type["keywords"],
                "filename": "",  # Will be set during save
                "file_path": "",  # Will be set during save
                "metadata_path": "",  # Will be set during save
                "generated_at": datetime.now(),
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
    doc_type = get_legal_doc_type(doc_type_name)
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

            # Create document record
            document = {
                "document_id": f"{doc_type_name}_{i+1:04d}",
                "document_type": doc_type["name"],
                "content": content,
                "keywords": doc_type["keywords"],
                "filename": "",  # Will be set during save
                "file_path": "",  # Will be set during save
                "metadata_path": "",  # Will be set during save
                "generated_at": datetime.now(),
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


def main(num_docs: int = 1000):
    """Main function to generate legal documents and save to MinIO"""

    try:
        # Generate all document types randomly
        documents = generate_legal_documents(num_docs=num_docs)

        if not documents:
            print("No documents were generated successfully. Exiting.")
            return

        # Save documents to MinIO
        save_documents_to_minio(documents, "data", "docs/legal")

        # Option: Generate specific document type only
        # Uncomment the lines below to generate only contracts
        # documents = generate_specific_document_type("contract", num_docs=100)
        # save_documents_to_minio(documents, "data", "docs/legal")

    except Exception as e:
        print(f"Error in main execution: {e}")


if __name__ == "__main__":
    main()
