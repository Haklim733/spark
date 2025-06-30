#!/usr/bin/env python3
"""
Unified Schema and Model Management for ELT Pipeline
Refactored to use composition with specialized classes
"""

from typing import List, Dict, Any, Optional, Tuple
from .schema_loader import SchemaLoader
from .schema_validator import SchemaValidator
from .schema_converter import SchemaConverter


class SchemaManager:
    """
    Unified schema manager using composition with specialized classes.

    This class coordinates between:
    - SchemaLoader: Handles schema loading and basic operations
    - SchemaValidator: Handles validation operations
    - SchemaConverter: Handles type conversions for Spark operations
    """

    def __init__(self, schemas_dir: str = "src/schemas"):
        # Initialize specialized components
        self.loader = SchemaLoader(schemas_dir)
        self.validator = SchemaValidator(self.loader)
        self.converter = SchemaConverter(self.loader)

    # Schema loading operations (delegated to SchemaLoader)
    def get_schema(self, schema_name: str) -> Optional[Dict[str, Any]]:
        """Get a specific schema by name"""
        return self.loader.get_schema(schema_name)

    def list_available_schemas(self) -> List[str]:
        """List all available schema names"""
        return self.loader.list_available_schemas()

    def get_schema_info(self, schema_name: str) -> Optional[Dict[str, Any]]:
        """Get schema information including title, description, and field count"""
        return self.loader.get_schema_info(schema_name)

    def get_required_fields(self, schema_name: str) -> List[str]:
        """Get list of required fields for a schema"""
        return self.loader.get_required_fields(schema_name)

    def load_json_data(self, data_name: str, validate_schema: bool = True) -> Any:
        """Load JSON data and optionally validate against schema"""
        return self.loader.load_json_data(data_name, validate_schema)

    # Validation operations (delegated to SchemaValidator)
    def validate_metadata(
        self, schema_name: str, data: Dict[str, Any]
    ) -> Tuple[bool, List[str]]:
        """Validate data against a specific schema"""
        return self.validator.validate_metadata(schema_name, data)

    def validate_legal_document_metadata(self, data: Dict[str, Any]) -> bool:
        """Validate legal document metadata against schema"""
        return self.validator.validate_legal_document_metadata(data)

    def validate_document_type(self, doc_type: str) -> bool:
        """Validate if a document type is valid according to the schema"""
        return self.validator.validate_document_type(doc_type)

    def get_document_types(self) -> List[str]:
        """Get valid document types from the schema"""
        return self.validator.get_document_types()

    # Schema conversion operations (delegated to SchemaConverter)
    def get_spark_schema(self, schema_name: str):
        """Convert JSON schema to Spark StructType for DataFrame operations"""
        return self.converter.get_spark_schema(schema_name)

    # Legacy methods for backward compatibility
    def get_all_legal_doc_types(self) -> List[Dict[str, Any]]:
        """
        Get all legal document types (legacy method)

        Returns:
            List of all legal document type configurations
        """
        try:
            legal_doc_types = self.load_json_data("legal_doc_types")
            return legal_doc_types
        except FileNotFoundError:
            print("⚠️  legal_doc_types_data.json not found, returning empty list")
            return []

    def get_legal_doc_type(self, name: str) -> Optional[Dict[str, Any]]:
        """
        Get legal document type configuration by name (legacy method)

        Args:
            name: Document type name

        Returns:
            Document type configuration or None if not found
        """
        # Load legal document types from JSON
        legal_doc_types = self.get_all_legal_doc_types()
        # Dictionary version for easier lookup
        legal_doc_types_dict = {
            doc_type["name"]: doc_type for doc_type in legal_doc_types
        }

        return legal_doc_types_dict.get(name)
