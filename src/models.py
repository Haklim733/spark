#!/usr/bin/env python3
"""
Shared models and types for legal document generation and processing
Used by both generate_legal_docs.py and insert_tables.py
"""

from typing import List, Dict, Any

# Legal document types and their characteristics
LEGAL_DOC_TYPES = [
    {
        "name": "contract",
        "keywords": [
            "agreement",
            "terms",
            "conditions",
            "parties",
            "obligations",
            "liability",
        ],
    },
    {
        "name": "legal_memo",
        "keywords": [
            "analysis",
            "precedent",
            "jurisdiction",
            "statute",
            "interpretation",
        ],
    },
    {
        "name": "court_filing",
        "keywords": ["petition", "motion", "affidavit", "evidence", "testimony"],
    },
    {
        "name": "policy_document",
        "keywords": [
            "policy",
            "procedure",
            "compliance",
            "regulations",
            "guidelines",
        ],
    },
    {
        "name": "legal_opinion",
        "keywords": [
            "opinion",
            "advice",
            "counsel",
            "legal_analysis",
            "recommendation",
        ],
    },
]

# Dictionary version for easier lookup
LEGAL_DOC_TYPES_DICT = {doc_type["name"]: doc_type for doc_type in LEGAL_DOC_TYPES}

# Valid document types for validation
VALID_DOCUMENT_TYPES = [doc_type["name"] for doc_type in LEGAL_DOC_TYPES]


def get_legal_doc_type(name: str) -> Dict[str, Any]:
    """
    Get legal document type configuration by name

    Args:
        name: Document type name

    Returns:
        Document type configuration or None if not found
    """
    return LEGAL_DOC_TYPES_DICT.get(name)


def get_all_legal_doc_types() -> List[Dict[str, Any]]:
    """
    Get all legal document types

    Returns:
        List of all legal document type configurations
    """
    return LEGAL_DOC_TYPES


def is_valid_document_type(doc_type: str) -> bool:
    """
    Check if a document type is valid

    Args:
        doc_type: Document type to validate

    Returns:
        True if valid, False otherwise
    """
    return doc_type in VALID_DOCUMENT_TYPES


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
