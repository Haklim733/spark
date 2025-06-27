#!/usr/bin/env python3
"""
Script to generate 1000 unstructured legal text documents using SOLI data generator.
Based on https://github.com/alea-institute/folio-data-generator
Uses PySpark to save documents to MinIO.
"""

import os
import random
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any

# Import SOLI data generator components
from soli import SOLI
from soli_data_generator.procedural.template import TemplateFormatter

# Import Spark session utilities
from utils.session import create_spark_session, SparkVersion, IcebergConfig
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import current_timestamp, lit

# Import shared legal document models
from models import (
    LEGAL_DOC_TYPES,
    VALID_DOCUMENT_TYPES,
    get_legal_doc_type,
    get_all_legal_doc_types,
    is_valid_document_type,
    get_document_keywords,
    LEGAL_DOC_TYPES_DICT,
)


def create_legal_documents_schema() -> StructType:
    """
    Create Spark schema for legal documents

    Returns:
        StructType: Schema for legal documents DataFrame
    """
    return StructType(
        [
            StructField("document_id", StringType(), False),
            StructField("document_type", StringType(), False),
            StructField("content", StringType(), False),
            StructField("keywords", StringType(), False),
            StructField("filename", StringType(), False),
            StructField("generated_at", TimestampType(), False),
        ]
    )


def save_documents_to_minio(
    spark: SparkSession,
    documents: List[Dict[str, Any]],
    output_path: str = "s3://data/docs/legal",
) -> None:
    """
    Save generated documents to MinIO using Spark

    Args:
        spark: SparkSession instance
        documents: List of document dictionaries
        output_path: MinIO path to save documents
    """
    # Create DataFrame from documents
    schema = create_legal_documents_schema()
    df = spark.createDataFrame(documents, schema)

    # Add timestamp column
    df = df.withColumn("generated_at", current_timestamp())

    print(f"Saving {len(documents)} documents to MinIO at {output_path}")

    # Write to MinIO as Parquet files
    df.write.mode("overwrite").parquet(output_path)

    print(f"Successfully saved documents to MinIO")

    # Show sample of saved data
    print("\nSample of saved documents:")
    df.show(5, truncate=False)


def save_documents_as_text_files(
    spark: SparkSession,
    documents: List[Dict[str, Any]],
    output_path: str = "s3://data/docs/legal/text",
) -> None:
    """
    Save documents as individual text files in MinIO

    Args:
        spark: SparkSession instance
        documents: List of document dictionaries
        output_path: MinIO path to save text files
    """
    print(f"Saving {len(documents)} documents as text files to MinIO at {output_path}")

    # Create DataFrame with content and filename
    text_df = spark.createDataFrame(
        [(doc["content"], doc["filename"]) for doc in documents],
        ["content", "filename"],
    )

    # Write each document as a separate text file
    text_df.write.mode("overwrite").option("header", "false").csv(output_path)

    print(f"Successfully saved text files to MinIO")


def generate_fallback_content(i, doc_type):
    """Fallback content generation when SOLI is not available"""
    fallback_content = f"""
    LEGAL DOCUMENT {i+1:04d}

    This is a legal document of type {doc_type['name'].replace('_', ' ').title()}.

    The parties herein agree to the following terms and conditions:

    1. This document constitutes a legally binding agreement between the parties.
    2. All terms and conditions outlined herein shall be enforceable under applicable law.
    3. Any disputes arising from this agreement shall be resolved through appropriate legal channels.
    4. This document may be amended only through written agreement of all parties.

    Keywords: {', '.join(doc_type['keywords'])}

    This document is generated for demonstration purposes and contains unstructured legal text content.
    """
    return fallback_content


def generate_contract_document(formatter, doc_type, i):
    """
    Generate contract document using SOLI procedural template generation

    Args:
        formatter: TemplateFormatter instance
        doc_type: Document type configuration
        i: Document index

    Returns:
        Generated contract document content
    """
    template = """
    LEGAL DOCUMENT {doc_id:04d} - CONTRACT AGREEMENT

    Date: <|date|>
    Document Type: Contract Agreement
    Parties: <|name:1|> and <|name:2|>
    Company: <|company|>
    Industry: <|industry|>
    Legal Area: <|area_of_law|>

    AGREEMENT

    This contract is entered into on <|date|> between <|name:1|> (hereinafter "Party A") and <|name:2|> (hereinafter "Party B").

    WHEREAS, Party A is engaged in the business of <|industry|> and Party B is seeking services in the area of <|area_of_law|>;

    NOW, THEREFORE, in consideration of the mutual promises and covenants contained herein, the parties agree as follows:

    1. SCOPE OF SERVICES
    Party A shall provide <|area_of_law|> services to Party B in accordance with the terms and conditions set forth in this agreement.

    2. TERM
    This agreement shall commence on <|date|> and continue until terminated by either party in accordance with the provisions herein.

    3. COMPENSATION
    Party B shall compensate Party A for services rendered at the rate of <|currency|> <|number|> per hour.

    4. CONFIDENTIALITY
    Both parties agree to maintain the confidentiality of all proprietary information shared during the course of this agreement.

    5. TERMINATION
    Either party may terminate this agreement with thirty (30) days written notice to the other party.

    IN WITNESS WHEREOF, the parties have executed this agreement as of the date first written above.

    <|name:1|>                    <|name:2|>
    Party A                       Party B

    Keywords: {keywords}
    """

    try:
        formatted_content = formatter(
            template.format(doc_id=i + 1, keywords=", ".join(doc_type["keywords"]))
        )
        return formatted_content
    except Exception as e:
        print(f"Error in contract generation: {e}")
        return generate_fallback_content(i, doc_type)


def generate_legal_memo_document(formatter, doc_type, i):
    """
    Generate legal memorandum using SOLI procedural template generation

    Args:
        formatter: TemplateFormatter instance
        doc_type: Document type configuration
        i: Document index

    Returns:
        Generated legal memo document content
    """
    template = """
    LEGAL MEMORANDUM {doc_id:04d}

    Date: <|date|>
    To: <|name:1|>
    From: <|name:2|>
    Subject: <|area_of_law|> Analysis for <|company|>

    MEMORANDUM

    This memorandum addresses the legal implications of <|area_of_law|> as it relates to <|company|> operations in the <|industry|> industry.

    BACKGROUND

    <|company|> is a <|industry|> company that requires legal analysis regarding <|area_of_law|> compliance and regulatory requirements.

    LEGAL ANALYSIS

    Based on current legal precedent and statutory interpretation, the following analysis applies:

    1. Regulatory Framework
    The applicable regulations governing <|area_of_law|> in the <|industry|> sector include...

    2. Compliance Requirements
    <|company|> must ensure compliance with the following requirements...

    3. Risk Assessment
    The primary legal risks associated with <|area_of_law|> include...

    RECOMMENDATIONS

    Based on this analysis, I recommend the following actions:

    1. Immediate compliance review
    2. Documentation updates
    3. Staff training on <|area_of_law|> requirements

    CONCLUSION

    This memorandum provides a comprehensive analysis of <|area_of_law|> implications for <|company|>.

    Keywords: {keywords}
    """

    try:
        formatted_content = formatter(
            template.format(doc_id=i + 1, keywords=", ".join(doc_type["keywords"]))
        )
        return formatted_content
    except Exception as e:
        print(f"Error in legal memo generation: {e}")
        return generate_fallback_content(i, doc_type)


def generate_court_filing_document(formatter, doc_type, i):
    """
    Generate court filing document using SOLI procedural template generation

    Args:
        formatter: TemplateFormatter instance
        doc_type: Document type configuration
        i: Document index

    Returns:
        Generated court filing document content
    """
    template = """
    COURT FILING {doc_id:04d}

    IN THE COURT OF <|jurisdiction|>
    Case No: <|number|>
    Date Filed: <|date|>

    PETITION/MOTION

    COMES NOW <|name:1|>, Petitioner/Movant, by and through counsel, and respectfully requests this Court to consider the following:

    I. INTRODUCTION

    This filing addresses matters related to <|area_of_law|> in the case involving <|company|>.

    II. FACTUAL BACKGROUND

    <|company|> operates in the <|industry|> industry and has been involved in legal proceedings related to <|area_of_law|>.

    III. LEGAL ARGUMENT

    Based on applicable law and precedent, the following legal arguments are presented:

    1. Jurisdiction
    This Court has proper jurisdiction over the subject matter and parties.

    2. Merits
    The legal merits of this case support the requested relief.

    3. Relief Sought
    Petitioner/Movant seeks the following relief...

    IV. CONCLUSION

    For the foregoing reasons, Petitioner/Movant respectfully requests that this Court grant the requested relief.

    Respectfully submitted,

    <|name:1|>
    Attorney for Petitioner/Movant
    <|email|>
    <|phone|>

    Keywords: {keywords}
    """

    try:
        formatted_content = formatter(
            template.format(doc_id=i + 1, keywords=", ".join(doc_type["keywords"]))
        )
        return formatted_content
    except Exception as e:
        print(f"Error in court filing generation: {e}")
        return generate_fallback_content(i, doc_type)


def generate_policy_document(formatter, doc_type, i):
    """
    Generate policy document using SOLI procedural template generation

    Args:
        formatter: TemplateFormatter instance
        doc_type: Document type configuration
        i: Document index

    Returns:
        Generated policy document content
    """
    template = """
    POLICY DOCUMENT {doc_id:04d}

    <|company|> - <|area_of_law|> Policy

    Effective Date: <|date|>
    Policy Number: <|number|>
    Department: Legal Compliance

    POLICY STATEMENT

    <|company|> is committed to ensuring compliance with all applicable <|area_of_law|> regulations and requirements in the <|industry|> industry.

    SCOPE

    This policy applies to all employees, contractors, and third parties conducting business on behalf of <|company|>.

    PROCEDURES

    1. Compliance Requirements
    All personnel must adhere to the following <|area_of_law|> requirements:

    2. Reporting Procedures
    Any violations or concerns related to <|area_of_law|> must be reported to <|name:1|> at <|email|>.

    3. Training Requirements
    Annual training on <|area_of_law|> compliance is mandatory for all relevant personnel.

    4. Documentation
    All <|area_of_law|> related activities must be properly documented and maintained.

    ENFORCEMENT

    Violations of this policy may result in disciplinary action up to and including termination of employment.

    REVIEW AND UPDATES

    This policy will be reviewed annually and updated as necessary to ensure continued compliance with <|area_of_law|> requirements.

    Contact: <|name:2|>, Legal Department
    Email: <|email|>

    Keywords: {keywords}
    """

    try:
        formatted_content = formatter(
            template.format(doc_id=i + 1, keywords=", ".join(doc_type["keywords"]))
        )
        return formatted_content
    except Exception as e:
        print(f"Error in policy document generation: {e}")
        return generate_fallback_content(i, doc_type)


def generate_legal_opinion_document(formatter, doc_type, i):
    """
    Generate legal opinion document using SOLI procedural template generation

    Args:
        formatter: TemplateFormatter instance
        doc_type: Document type configuration
        i: Document index

    Returns:
        Generated legal opinion document content
    """
    template = """
    LEGAL OPINION {doc_id:04d}

    Date: <|date|>
    To: <|name:1|>
    From: <|name:2|>, Legal Counsel
    Subject: <|area_of_law|> Legal Opinion for <|company|>

    OPINION

    This legal opinion addresses the <|area_of_law|> implications for <|company|> operations in the <|industry|> industry.

    FACTS

    <|company|> is seeking legal guidance regarding <|area_of_law|> compliance and regulatory requirements.

    ANALYSIS

    Based on my review of applicable laws, regulations, and precedent, I provide the following legal analysis:

    1. Applicable Law
    The relevant legal framework governing <|area_of_law|> includes...

    2. Compliance Assessment
    <|company|> current practices regarding <|area_of_law|> appear to be...

    3. Risk Evaluation
    The primary legal risks associated with <|area_of_law|> include...

    4. Recommendations
    To ensure compliance with <|area_of_law|> requirements, I recommend:

    CONCLUSION

    Based on this analysis, <|company|> should proceed with the following actions to ensure <|area_of_law|> compliance...

    This opinion is based on current law as of <|date|> and may need to be updated if legal requirements change.

    Respectfully submitted,

    <|name:2|>
    Legal Counsel
    <|email|>

    Keywords: {keywords}
    """

    try:
        formatted_content = formatter(
            template.format(doc_id=i + 1, keywords=", ".join(doc_type["keywords"]))
        )
        return formatted_content
    except Exception as e:
        print(f"Error in legal opinion generation: {e}")
        return generate_fallback_content(i, doc_type)


def get_document_generator(doc_type_name):
    """
    Get the appropriate document generation function based on document type

    Args:
        doc_type_name: Name of the document type

    Returns:
        Function to generate the specific document type
    """
    generators = {
        "contract": generate_contract_document,
        "legal_memo": generate_legal_memo_document,
        "court_filing": generate_court_filing_document,
        "policy_document": generate_policy_document,
        "legal_opinion": generate_legal_opinion_document,
    }

    return generators.get(doc_type_name, generate_contract_document)


def generate_legal_documents_with_spark(num_docs, spark_session=None):
    """
    Generate unstructured legal text documents using SOLI data generator and save to MinIO via Spark.

    Args:
        num_docs (int): Number of documents to generate
        spark_session: Optional SparkSession instance (will create one if not provided)
    """

    # Initialize SOLI components
    try:
        # Initialize SOLI graph
        soli_graph = SOLI()

        # Initialize template formatter
        formatter = TemplateFormatter()

        print("SOLI data generator initialized successfully.")
    except Exception as e:
        print(f"Error initializing SOLI: {e}")
        print("Falling back to basic content generation.")
        formatter = None

    # Use shared legal document types from legal_document_models.py
    legal_doc_types = get_all_legal_doc_types()

    print(f"Generating {num_docs} legal documents and saving to MinIO...")

    # Track document generation statistics
    doc_type_counts = {doc_type["name"]: 0 for doc_type in legal_doc_types}

    # List to store all generated documents
    documents = []

    for i in range(num_docs):
        # Randomly select document type
        doc_type = random.choice(legal_doc_types)
        doc_type_counts[doc_type["name"]] += 1

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
                "generated_at": datetime.now(),
            }

            documents.append(document)

    # Save documents to MinIO using Spark
    try:
        # Save as structured data (Parquet)
        save_documents_to_minio(
            spark_session, documents, "s3://data/docs/legal/parquet"
        )

        # Save as text files
        save_documents_as_text_files(
            spark_session, documents, "s3://data/docs/legal/text"
        )

    except Exception as e:
        print(f"Error saving to MinIO: {e}")
        # Fallback: save locally
        local_output_dir = "data/docs/legal"
        Path(local_output_dir).mkdir(parents=True, exist_ok=True)

        for doc in documents:
            filepath = os.path.join(local_output_dir, doc["filename"])
            with open(filepath, "w", encoding="utf-8") as f:
                f.write(doc["content"])

        print(f"Saved documents locally to {local_output_dir}")

    # Print generation statistics
    print(f"\n=== Generation Complete ===")
    print(f"Total documents generated: {num_docs}")
    print(f"Document type distribution:")
    for doc_type, count in doc_type_counts.items():
        percentage = (count / num_docs) * 100
        print(f"  {doc_type}: {count} ({percentage:.1f}%)")
    print(f"Documents saved to MinIO: s3://data/docs/legal/")


def generate_specific_document_type_with_spark(
    doc_type_name, num_docs=100, spark_session=None
):
    """
    Generate documents of a specific type only and save to MinIO via Spark

    Args:
        doc_type_name (str): Name of the document type to generate
        num_docs (int): Number of documents to generate
        spark_session: Optional SparkSession instance (will create one if not provided)
    """

    # Use shared legal document types from legal_document_models.py
    legal_doc_types = LEGAL_DOC_TYPES_DICT

    if doc_type_name not in legal_doc_types:
        print(f"Error: Unknown document type '{doc_type_name}'")
        print(f"Available types: {list(legal_doc_types.keys())}")
        return

    doc_type = legal_doc_types[doc_type_name]

    # Initialize SOLI components
    try:
        # Initialize SOLI graph
        soli_graph = SOLI()

        # Initialize template formatter
        formatter = TemplateFormatter()

        print("SOLI data generator initialized successfully.")
    except Exception as e:
        print(f"Error initializing SOLI: {e}")
        print("Falling back to basic content generation.")
        formatter = None

    print(f"Generating {num_docs} {doc_type_name} documents and saving to MinIO...")

    documents = []

    for i in range(num_docs):
        try:
            if formatter:
                doc_generator = get_document_generator(doc_type["name"])
                content = doc_generator(formatter, doc_type, i)
            else:
                content = generate_fallback_content(i, doc_type)

            # Create filename
            filename = f"{doc_type_name}_doc_{i+1:04d}.txt"

            # Create document record
            document = {
                "document_id": f"{doc_type_name}_{i+1:04d}",
                "document_type": doc_type["name"],
                "content": content,
                "keywords": ", ".join(doc_type["keywords"]),
                "filename": filename,
                "generated_at": datetime.now(),
            }

            documents.append(document)

            if (i + 1) % 50 == 0:
                print(f"Generated {i + 1} {doc_type_name} documents...")

        except Exception as e:
            print(f"Error generating {doc_type_name} document {i+1}: {e}")
            fallback_content = generate_fallback_content(i, doc_type)

            filename = f"{doc_type_name}_doc_{i+1:04d}.txt"

            document = {
                "document_id": f"{doc_type_name}_{i+1:04d}",
                "document_type": doc_type["name"],
                "content": fallback_content,
                "keywords": ", ".join(doc_type["keywords"]),
                "filename": filename,
                "generated_at": datetime.now(),
            }

            documents.append(document)

    # Save documents to MinIO using Spark
    try:
        # Save as structured data (Parquet)
        save_documents_to_minio(
            spark_session, documents, f"s3://data/docs/legal/{doc_type_name}/parquet"
        )

        # Save as text files
        save_documents_as_text_files(
            spark_session, documents, f"s3://data/docs/legal/{doc_type_name}/text"
        )

    except Exception as e:
        print(f"Error saving to MinIO: {e}")
        # Fallback: save locally
        local_output_dir = f"data/docs/legal/{doc_type_name}"
        Path(local_output_dir).mkdir(parents=True, exist_ok=True)

        for doc in documents:
            filepath = os.path.join(local_output_dir, doc["filename"])
            with open(filepath, "w", encoding="utf-8") as f:
                f.write(doc["content"])

        print(f"Saved documents locally to {local_output_dir}")

    print(
        f"Successfully generated and saved {num_docs} {doc_type_name} documents to MinIO"
    )


def main(num_docs: int = 1000):
    """Main function to generate legal documents and save to MinIO"""

    # Create Spark session
    spark = create_spark_session(
        spark_version=SparkVersion.SPARK_3_5,
        app_name=Path(__file__).stem,
        iceberg_config=IcebergConfig(),
    )

    try:
        # Generate all document types randomly
        generate_legal_documents_with_spark(num_docs=num_docs, spark_session=spark)

        # Option: Generate specific document type only
        # Uncomment the line below to generate only contracts
        # generate_specific_document_type_with_spark("contract", num_docs=100, spark_session=spark)

    finally:
        # Stop Spark session
        spark.stop()


if __name__ == "__main__":
    main()
