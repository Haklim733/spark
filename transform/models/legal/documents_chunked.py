from pyspark.sql import DataFrame
from pyspark.sql.functions import to_json, struct
from sqlmesh import model
from sqlmesh.core.model.kind import ModelKindName
from sqlmesh.core.context import ExecutionContext
import typing as t
from datetime import datetime
import re
from typing import List, Dict, Any


@model(
    name="legal.documents_chunked",
    kind=dict(name=ModelKindName.INCREMENTAL_BY_TIME_RANGE, time_column="generated_at"),
    start="2023-01-01",
    grain=["document_id"],
    cron="@daily",
    columns={
        "document_id": "STRING",
        "chunk_id": "STRING",
        "chunk_text": "STRING",
        "chunk_position": "INT",
        "chunk_length": "INT",
        "chunk_word_count": "INT",
        "chunk_quality": "STRING",
        "embedding_status": "STRING",
        "document_type": "STRING",
        "generated_at": "TIMESTAMP",
        "source": "STRING",
        "language": "STRING",
        "total_chunks": "INT",
        "chunk_metadata": "STRING",  # JSON string for additional metadata
    },
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> DataFrame:
    """
    Create semantic chunks from legal documents for embedding.
    """
    table = context.resolve_table("legal.documents_cleaned")

    docs_df = context.spark.read.table(table)

    docs_df = docs_df.filter(
        (docs_df.generated_at >= start)
        & (docs_df.generated_at < end)
        & (docs_df.data_quality_status == "valid")
    )

    def create_semantic_chunks(
        text: str, doc_id: str, chunk_size: int = 512, overlap: int = 128
    ) -> List[Dict[str, Any]]:
        """Create semantic chunks with overlap"""
        if not text or len(text.strip()) == 0:
            return []

        chunks = []
        text = text.strip()

        # Split into sentences first (better than character-based)
        sentences = re.split(r"[.!?]+", text)
        sentences = [s.strip() for s in sentences if len(s.strip()) > 10]

        current_chunk = ""
        chunk_position = 0

        for sentence in sentences:
            # If adding this sentence would exceed chunk size
            if len(current_chunk) + len(sentence) > chunk_size and current_chunk:
                # Save current chunk
                chunk_id = f"{doc_id}_chunk_{chunk_position:03d}"
                chunks.append(
                    {
                        "chunk_id": chunk_id,
                        "chunk_text": current_chunk.strip(),
                        "chunk_position": chunk_position,
                        "chunk_length": len(current_chunk.strip()),
                        "chunk_word_count": len(current_chunk.strip().split()),
                        "chunk_quality": (
                            "optimal"
                            if 100 <= len(current_chunk.strip()) <= 1000
                            else "suboptimal"
                        ),
                        "embedding_status": "ready_for_embedding",
                        "total_chunks": len(chunks) + 1,  # Will be updated
                    }
                )

                # Start new chunk with overlap
                overlap_text = current_chunk[-overlap:] if overlap > 0 else ""
                current_chunk = overlap_text + " " + sentence
                chunk_position += 1
            else:
                current_chunk += " " + sentence if current_chunk else sentence

        # Add final chunk
        if current_chunk.strip():
            chunk_id = f"{doc_id}_chunk_{chunk_position:03d}"
            chunks.append(
                {
                    "chunk_id": chunk_id,
                    "chunk_text": current_chunk.strip(),
                    "chunk_position": chunk_position,
                    "chunk_length": len(current_chunk.strip()),
                    "chunk_word_count": len(current_chunk.strip().split()),
                    "chunk_quality": (
                        "optimal"
                        if 100 <= len(current_chunk.strip()) <= 1000
                        else "suboptimal"
                    ),
                    "embedding_status": "ready_for_embedding",
                    "total_chunks": len(chunks) + 1,
                }
            )

        # Update total_chunks for all chunks
        total_chunks = len(chunks)
        for chunk in chunks:
            chunk["total_chunks"] = total_chunks

        return chunks

    # Register UDF for chunking
    from pyspark.sql.functions import udf, explode, col, struct, lit
    from pyspark.sql.types import (
        ArrayType,
        StructType,
        StructField,
        StringType,
        IntegerType,
    )

    chunk_schema = StructType(
        [
            StructField("chunk_id", StringType(), True),
            StructField("chunk_text", StringType(), True),
            StructField("chunk_position", IntegerType(), True),
            StructField("chunk_length", IntegerType(), True),
            StructField("chunk_word_count", IntegerType(), True),
            StructField("chunk_quality", StringType(), True),
            StructField("embedding_status", StringType(), True),
            StructField("total_chunks", IntegerType(), True),
        ]
    )

    chunk_udf = udf(create_semantic_chunks, ArrayType(chunk_schema))

    # Apply chunking
    chunked_df = (
        docs_df.withColumn("chunks", chunk_udf(col("raw_text"), col("document_id")))
        .select(
            "document_id",
            "document_type",
            "generated_at",
            "source",
            "language",
            explode("chunks").alias("chunk_data"),
        )
        .select(
            "document_id",
            "document_type",
            "generated_at",
            "source",
            "language",
            col("chunk_data.*"),
        )
    )

    chunked_df = chunked_df.withColumn(
        "chunk_metadata",
        to_json(struct("chunk_quality", "embedding_status", "total_chunks")),
    )

    return chunked_df
