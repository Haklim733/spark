#!/usr/bin/env python3
"""
Schema Loader - Handles loading and basic schema operations
"""

import json
from pathlib import Path
from typing import Dict, Any, Optional, List


class SchemaLoader:
    """Handles loading and basic operations on JSON schemas"""

    def __init__(self, schemas_dir: str = "src/schemas"):
        self.schemas_dir = Path(schemas_dir)
        self.schemas = {}
        self._load_schemas()

    def _load_schemas(self):
        """Load all JSON schemas from the schemas directory"""
        if not self.schemas_dir.exists():
            print(f"⚠️  Schemas directory not found: {self.schemas_dir}")
            return

        for schema_file in self.schemas_dir.glob("*.json"):
            try:
                with open(schema_file, "r") as f:
                    schema_name = schema_file.stem
                    self.schemas[schema_name] = json.load(f)
                print(f"✅ Loaded schema: {schema_name}")
            except Exception as e:
                print(f"❌ Error loading schema {schema_file}: {e}")

    def get_schema(self, schema_name: str) -> Dict[str, Any]:
        """Get a specific schema by name"""
        if schema_name not in self.schemas:
            raise ValueError(f"Schema {schema_name} not found")
        return self.schemas[schema_name]

    def list_available_schemas(self) -> List[str]:
        """List all available schema names"""
        return list(self.schemas.keys())

    def get_schema_info(self, schema_name: str) -> Optional[Dict[str, Any]]:
        """Get schema information including title, description, and field count"""
        schema = self.get_schema(schema_name)
        if not schema:
            return None

        properties = schema.get("properties", {})
        required_fields = schema.get("required", [])

        return {
            "name": schema_name,
            "version": schema.get("version", "1.0.0"),
            "title": schema.get("title", schema_name),
            "description": schema.get("description", ""),
            "field_count": len(properties),
            "required_field_count": len(required_fields),
            "properties": list(properties.keys()),
            "required_fields": required_fields,
        }

    def get_required_fields(self, schema_name: str) -> List[str]:
        """Get list of required fields for a schema"""
        schema = self.get_schema(schema_name)
        if not schema:
            return []

        return schema.get("required", [])

    def load_json_data(self, data_name: str, validate_schema: bool = True) -> Any:
        """
        Load JSON data and optionally validate against schema

        Args:
            data_name: Name of data file (without .json extension)
            validate_schema: Whether to validate against schema

        Returns:
            Loaded data
        """
        data_path = self.schemas_dir / f"{data_name}_data.json"
        if not data_path.exists():
            raise FileNotFoundError(f"Data file not found: {data_path}")

        with open(data_path, "r") as f:
            data = json.load(f)

        if validate_schema:
            from jsonschema import validate

            schema = self.get_schema(data_name)
            if schema:
                validate(instance=data, schema=schema)

        return data
