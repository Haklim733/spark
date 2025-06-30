#!/usr/bin/env python3
"""
Schema Converter - Handles schema type conversions for Spark operations
"""

from typing import Dict, Any, Optional
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType,
    MapType,
    ArrayType,
    BooleanType,
    DoubleType,
    LongType,
)


class SchemaConverter:
    """Handles schema type conversions for Spark operations"""

    def __init__(self, schema_loader):
        self.schema_loader = schema_loader

    def get_spark_schema(self, schema_name: str) -> Optional[StructType]:
        """
        Convert JSON schema to Spark StructType for DataFrame operations

        Args:
            schema_name: Name of the schema to convert

        Returns:
            Spark StructType or None if schema not found
        """
        schema = self.schema_loader.get_schema(schema_name)
        if not schema:
            return None

        # Enhanced type mapping for more data types
        type_mapping = {
            "string": StringType(),
            "integer": IntegerType(),
            "boolean": BooleanType(),
            "array": ArrayType(StringType()),
            "object": MapType(StringType(), StringType()),
            "number": DoubleType(),
        }

        fields = []
        if "properties" in schema:
            for field_name, field_schema in schema["properties"].items():
                field_type = field_schema.get("type", "string")
                spark_type = type_mapping.get(field_type, StringType())

                # Handle special cases
                if field_name in [
                    "generated_at",
                    "load_timestamp",
                    "pickup_datetime",
                    "dropoff_datetime",
                    "generation_date",
                ]:
                    spark_type = TimestampType()
                elif field_name == "keywords":
                    spark_type = ArrayType(StringType())
                elif field_name in [
                    "content_length",
                    "passenger_count",
                    "rate_code_id",
                    "trip_type",
                    "word_count",
                ]:
                    spark_type = IntegerType()
                elif field_name in ["file_size", "document_length"]:
                    spark_type = LongType()
                elif field_name in [
                    "trip_distance",
                    "fare_amount",
                    "extra",
                    "mta_tax",
                    "tip_amount",
                    "tolls_amount",
                    "improvement_surcharge",
                    "total_amount",
                    "ehail_fee",
                    "congestion_surcharge",
                    "airport_fee",
                    "pickup_longitude",
                    "pickup_latitude",
                    "dropoff_longitude",
                    "dropoff_latitude",
                ]:
                    spark_type = DoubleType()
                elif field_name == "metadata":
                    spark_type = MapType(StringType(), StringType())

                fields.append(StructField(field_name, spark_type, True))

        return StructType(fields)
