#!/usr/bin/env python3
"""
Schema Validator - Handles schema validation operations
"""

from typing import Dict, Any, List, Tuple, Optional
from jsonschema import validate, ValidationError as JSONValidationError
import json
import os


class SchemaValidator:
    """Handles validation operations for JSON schemas"""

    def __init__(self, schema_loader):
        self.schema_loader = schema_loader

    def validate_metadata(
        self, schema_name: str, data: Dict[str, Any]
    ) -> Tuple[bool, List[str]]:
        """
        Validate data against a specific schema

        Args:
            schema_name: Name of the schema to validate against
            data: Data to validate

        Returns:
            tuple: (is_valid, list_of_errors)
        """
        schema = self.schema_loader.get_schema(schema_name)
        if not schema:
            return False, [f"Schema '{schema_name}' not found"]

        try:
            validate(instance=data, schema=schema)
            return True, []
        except JSONValidationError as e:
            return False, [str(e)]

    def validate_legal_document_metadata(self, data: Dict[str, Any]) -> bool:
        """Validate legal document metadata against schema"""
        is_valid, errors = self.validate_metadata("legal_doc_metadata", data)
        if not is_valid:
            print(f"❌ Validation errors: {errors}")
        return is_valid

    def validate_document_type(self, doc_type: str) -> bool:
        """Validate if a document type is valid according to the schema"""
        valid_types = self.get_document_types()
        return doc_type in valid_types

    def get_document_types(self) -> List[str]:
        """Get valid document types from the schema"""
        schema = self.schema_loader.get_schema("legal_doc_metadata")
        if not schema:
            return []

        doc_type_prop = schema.get("properties", {}).get("document_type", {})
        return doc_type_prop.get("enum", [])

    @staticmethod
    def validate_error_code(error_code: str):
        """
        Validate an error code against the batch metrics error code JSON schema.
        This should be used for transform/reporting/data quality, NOT for blocking raw table writes in ELT.
        Raises jsonschema.ValidationError if invalid.
        """
        schema_path = os.path.join(
            os.path.dirname(__file__), "batch_metrics_error_codes.json"
        )
        with open(schema_path, "r") as f:
            error_code_schema = json.load(f)
        validate(instance={"error_code": error_code}, schema=error_code_schema)
