#!/usr/bin/env python3
"""
Shared models and types for legal document generation and processing
Uses JSON schemas for data structure definitions
"""

import json
from pathlib import Path
from typing import List, Dict, Any, Optional
import jsonschema
from jsonschema import validate


def load_json_schema(schema_name: str) -> Dict[str, Any]:
    """
    Load JSON schema from schemas directory

    Args:
        schema_name: Name of schema file (without .json extension)

    Returns:
        Schema dictionary
    """
    schema_path = Path(__file__).parent / "schemas" / f"{schema_name}.json"
    with open(schema_path, "r") as f:
        return json.load(f)


def load_json_data(data_name: str, validate_schema: bool = True) -> Any:
    """
    Load JSON data and optionally validate against schema

    Args:
        data_name: Name of data file (without .json extension)
        validate_schema: Whether to validate against schema

    Returns:
        Loaded data
    """
    data_path = Path(__file__).parent / "schemas" / f"{data_name}_data.json"
    with open(data_path, "r") as f:
        data = json.load(f)

    if validate_schema:
        schema = load_json_schema(data_name)
        validate(instance=data, schema=schema)

    return data


def get_legal_doc_type(name: str) -> Optional[Dict[str, Any]]:
    """
    Get legal document type configuration by name

    Args:
        name: Document type name

    Returns:
        Document type configuration or None if not found
    """
    # Load legal document types from JSON
    legal_doc_types = get_all_legal_doc_types()
    # Dictionary version for easier lookup
    legal_doc_types_dict = {doc_type["name"]: doc_type for doc_type in legal_doc_types}

    return legal_doc_types_dict.get(name)


def get_all_legal_doc_types() -> List[Dict[str, Any]]:
    """
    Get all legal document types

    Returns:
        List of all legal document type configurations
    """
    legal_doc_types = load_json_data("legal_doc_types")

    return legal_doc_types


def is_valid_document_type(doc_type: str) -> bool:
    """
    Check if a document type is valid

    Args:
        doc_type: Document type to validate

    Returns:
        True if valid, False otherwise
    """
    legal_doc_types = get_all_legal_doc_types()
    return doc_type in [dt["name"] for dt in legal_doc_types]


def get_document_keywords(doc_type: str) -> List[str]:
    """
    Get keywords for a specific document type

    Args:
        doc_type: Document type name

    Returns:
        List of keywords for the document type
    """
    doc_config = get_legal_doc_type(doc_type)
    return doc_config["keywords"] if doc_config else []


def validate_legal_document_metadata(data: Dict[str, Any]) -> bool:
    """Validate legal document metadata against schema"""
    legal_document_metadata_schema = load_json_schema("legal_doc_metadata")
    try:
        validate(instance=data, schema=legal_document_metadata_schema)
        return True
    except jsonschema.exceptions.ValidationError as e:
        print(f"❌ Validation error: {e}")
        return False


def validate_legal_doc_types() -> bool:
    """
    Validate that all legal document types conform to schema

    Returns:
        True if valid, False otherwise
    """
    try:
        schema = load_json_schema("legal_doc_types")
        legal_doc_types = get_all_legal_doc_types()
        validate(instance=legal_doc_types, schema=schema)
        return True
    except jsonschema.exceptions.ValidationError as e:
        print(f"❌ Legal document types validation failed: {e}")
        return False


# Validate on import
if __name__ == "__main__":
    print("🔍 Validating legal document types...")
    if validate_legal_doc_types():
        print("✅ All legal document types are valid")
    else:
        print("❌ Legal document types validation failed")
