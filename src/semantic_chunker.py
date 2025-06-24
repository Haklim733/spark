#!/usr/bin/env python3
"""
Semantic Chunking using Spark - Rule-based approaches
"""

import re
from typing import List, Dict, Tuple
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, explode, array, lit, row_number, size
from pyspark.sql.types import (
    StringType,
    ArrayType,
    StructType,
    StructField,
    IntegerType,
    MapType,
)
from pyspark.sql.window import Window


class SemanticChunker:
    """Rule-based semantic chunking for Spark"""

    def __init__(self, max_tokens=100000):
        self.max_tokens = max_tokens

        # Semantic boundary patterns
        self.semantic_patterns = {
            # Document structure
            "chapter": r"\n\s*(?:Chapter|CHAPTER)\s+\d+[:\s]",
            "section": r"\n\s*(?:Section|SECTION)\s+\d+[:\s]",
            "subsection": r"\n\s*\d+\.\d+\s+[A-Z]",
            # Legal document patterns
            "legal_section": r"\n\s*(?:Article|ARTICLE|Clause|CLAUSE)\s+\d+[:\s]",
            "legal_subsection": r"\n\s*\(\w+\)\s+[A-Z]",
            "legal_paragraph": r"\n\s*\d+\.\s+[A-Z]",
            # Academic patterns
            "academic_section": r"\n\s*(?:Introduction|Methods|Results|Discussion|Conclusion)",
            "academic_subsection": r"\n\s*\d+\.\d+\s+[A-Z][a-z]+",
            # Business document patterns
            "business_section": r"\n\s*(?:Executive Summary|Background|Analysis|Recommendations)",
            "business_subsection": r"\n\s*[A-Z][A-Z\s]{3,}\n",
            # Markdown patterns
            "markdown_h1": r"\n\s*#\s+[^\n]+",
            "markdown_h2": r"\n\s*##\s+[^\n]+",
            "markdown_h3": r"\n\s*###\s+[^\n]+",
            # Natural language patterns
            "topic_shift": r"\n\s*(?:However|Moreover|Furthermore|Additionally|In contrast|On the other hand)",
            "conclusion_indicators": r"\n\s*(?:In conclusion|To summarize|Therefore|Thus|As a result)",
            "new_topic": r"\n\s*(?:The next|Another|A different|Moving on|Now consider)",
        }

        # Priority order for semantic boundaries
        self.boundary_priority = [
            "chapter",
            "section",
            "legal_section",
            "academic_section",
            "business_section",
            "subsection",
            "legal_subsection",
            "academic_subsection",
            "business_subsection",
            "markdown_h1",
            "markdown_h2",
            "markdown_h3",
            "topic_shift",
            "conclusion_indicators",
            "new_topic",
        ]

    def find_semantic_boundaries(self, text: str) -> List[Tuple[int, str, str]]:
        """
        Find semantic boundaries in text

        Returns:
            List of (position, boundary_type, boundary_text)
        """
        boundaries = []

        for boundary_type in self.boundary_priority:
            pattern = self.semantic_patterns[boundary_type]
            matches = re.finditer(pattern, text, re.IGNORECASE)

            for match in matches:
                boundaries.append((match.start(), boundary_type, match.group().strip()))

        # Sort by position
        boundaries.sort(key=lambda x: x[0])
        return boundaries

    def create_semantic_chunks(self, text: str) -> List[Dict]:
        """
        Create semantic chunks based on detected boundaries
        """
        boundaries = self.find_semantic_boundaries(text)
        chunks = []

        if not boundaries:
            # No semantic boundaries found, use paragraph-based chunking
            return self._fallback_chunking(text)

        # Start with the beginning of the document
        current_start = 0
        current_chunk = ""
        current_boundary_type = "document_start"

        for pos, boundary_type, boundary_text in boundaries:
            # Check if adding this section would exceed max tokens
            section_text = text[current_start:pos].strip()
            estimated_tokens = self._estimate_tokens(section_text)

            if estimated_tokens > self.max_tokens and current_chunk:
                # Current chunk is too large, save it and start new one
                chunks.append(
                    {
                        "text": current_chunk.strip(),
                        "boundary_type": current_boundary_type,
                        "estimated_tokens": self._estimate_tokens(current_chunk),
                        "chunk_strategy": "semantic_boundary",
                    }
                )

                # Start new chunk with overlap
                overlap_text = self._get_overlap_text(
                    current_chunk, 5000
                )  # 5k char overlap
                current_chunk = overlap_text + "\n\n" + boundary_text + "\n"
                current_boundary_type = boundary_type
            else:
                # Add to current chunk
                if current_chunk:
                    current_chunk += "\n\n" + boundary_text + "\n"
                else:
                    current_chunk = boundary_text + "\n"
                current_boundary_type = boundary_type

            current_start = pos

        # Add the final section
        final_section = text[current_start:].strip()
        if final_section:
            current_chunk += "\n\n" + final_section

        # Add final chunk
        if current_chunk.strip():
            chunks.append(
                {
                    "text": current_chunk.strip(),
                    "boundary_type": current_boundary_type,
                    "estimated_tokens": self._estimate_tokens(current_chunk),
                    "chunk_strategy": "semantic_boundary",
                }
            )

        return chunks

    def _fallback_chunking(self, text: str) -> List[Dict]:
        """
        Fallback to paragraph-based chunking when no semantic boundaries found
        """
        paragraphs = re.split(r"\n\s*\n", text)
        chunks = []
        current_chunk = ""

        for paragraph in paragraphs:
            paragraph = paragraph.strip()
            if not paragraph:
                continue

            # Check if adding this paragraph would exceed max tokens
            test_chunk = (
                current_chunk + "\n\n" + paragraph if current_chunk else paragraph
            )
            estimated_tokens = self._estimate_tokens(test_chunk)

            if estimated_tokens > self.max_tokens and current_chunk:
                # Save current chunk
                chunks.append(
                    {
                        "text": current_chunk.strip(),
                        "boundary_type": "paragraph_group",
                        "estimated_tokens": self._estimate_tokens(current_chunk),
                        "chunk_strategy": "paragraph_fallback",
                    }
                )

                # Start new chunk with overlap
                overlap_text = self._get_overlap_text(
                    current_chunk, 2000
                )  # 2k char overlap
                current_chunk = overlap_text + "\n\n" + paragraph
            else:
                # Add to current chunk
                if current_chunk:
                    current_chunk += "\n\n" + paragraph
                else:
                    current_chunk = paragraph

        # Add final chunk
        if current_chunk.strip():
            chunks.append(
                {
                    "text": current_chunk.strip(),
                    "boundary_type": "paragraph_group",
                    "estimated_tokens": self._estimate_tokens(current_chunk),
                    "chunk_strategy": "paragraph_fallback",
                }
            )

        return chunks

    def _estimate_tokens(self, text: str) -> int:
        """Estimate token count"""
        return len(text) // 4

    def _get_overlap_text(self, text: str, overlap_chars: int) -> str:
        """Get overlap text from the end of a chunk"""
        if len(text) <= overlap_chars:
            return text

        # Find the last complete sentence within overlap
        overlap_text = text[-overlap_chars:]
        last_sentence = re.search(r"[^.!?]*[.!?]$", overlap_text)

        if last_sentence:
            return last_sentence.group().strip()
        else:
            # If no sentence boundary, find last paragraph
            last_paragraph = re.search(r"[^\n]*\n?$", overlap_text)
            return last_paragraph.group().strip() if last_paragraph else overlap_text


def create_semantic_chunking_udf(max_tokens=100000):
    """
    Create UDF for semantic chunking
    """
    chunker = SemanticChunker(max_tokens)

    def semantic_chunk_udf(text):
        if not text:
            return []

        chunks = chunker.create_semantic_chunks(text)
        return chunks

    return udf(semantic_chunk_udf, ArrayType(MapType(StringType(), StringType())))


def process_documents_semantic(
    spark: SparkSession,
    input_path: str,
    output_path: str,
    max_tokens: int = 100000,
):
    """
    Process documents using semantic chunking
    """

    # Load documents
    df = load_documents(spark, input_path)

    # Create semantic chunking UDF
    semantic_chunk_udf = create_semantic_chunking_udf(max_tokens)

    # Apply semantic chunking
    df_chunked = df.withColumn("semantic_chunks", semantic_chunk_udf(col("text")))

    # Explode chunks
    df_exploded = df_chunked.select(
        col("doc_id"),
        col("text").alias("original_text"),
        explode(col("semantic_chunks")).alias("chunk_data"),
    )

    # Extract chunk information
    df_final = df_exploded.select(
        col("doc_id"),
        col("original_text"),
        col("chunk_data.text").alias("chunk_text"),
        col("chunk_data.boundary_type").alias("boundary_type"),
        col("chunk_data.estimated_tokens").alias("estimated_tokens"),
        col("chunk_data.chunk_strategy").alias("chunk_strategy"),
    )

    # Add metadata
    df_final = df_final.withColumn("max_tokens", lit(max_tokens)).withColumn(
        "chunking_method", lit("semantic_rule_based")
    )

    return df_final


def analyze_semantic_chunks(
    spark: SparkSession, table_name: str = "iceberg.semantic_chunks"
):
    """
    Analyze semantic chunking results
    """
    df = spark.table(table_name)

    print("=== Semantic Chunking Analysis ===")

    # Boundary type distribution
    print("\nBoundary Type Distribution:")
    df.groupBy("boundary_type").count().orderBy("count", ascending=False).show()

    # Chunk strategy distribution
    print("\nChunk Strategy Distribution:")
    df.groupBy("chunk_strategy").count().orderBy("count", ascending=False).show()

    # Token distribution by boundary type
    print("\nAverage Tokens by Boundary Type:")
    df.groupBy("boundary_type").agg(
        {
            "estimated_tokens": "avg",
            "estimated_tokens": "min",
            "estimated_tokens": "max",
        }
    ).show()

    # Documents with most chunks
    print("\nDocuments with Most Chunks:")
    df.groupBy("doc_id").count().orderBy("count", ascending=False).show(10)


def main():
    """Main function for semantic chunking"""

    input_path = "data/docs/legal"
    output_path = "data/semantic_chunks"
    max_tokens = 100000

    print("Starting semantic chunking pipeline...")
    print(f"Max tokens per chunk: {max_tokens:,}")

    try:
        spark = get_spark_session()

        df_chunked = process_documents_semantic(
            spark=spark,
            input_path=input_path,
            output_path=output_path,
            max_tokens=max_tokens,
        )

        # Save results
        df_chunked.writeTo("iceberg.semantic_chunks").using("iceberg").createOrReplace()
        df_chunked.write.mode("overwrite").parquet(output_path)

        # Analyze results
        analyze_semantic_chunks(spark)

        print("Semantic chunking pipeline completed successfully!")

    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback

        traceback.print_exc()
    finally:
        if "spark" in locals():
            spark.stop()


class DocumentTypeClassifier:
    """Classify document type to apply appropriate semantic rules"""

    def __init__(self):
        self.document_patterns = {
            "legal": {
                "patterns": [
                    r"\b(?:contract|agreement|clause|article|section|party|whereas|therefore)\b",
                    r"\n\s*(?:Article|ARTICLE|Clause|CLAUSE)\s+\d+",
                    r"\n\s*\(\w+\)\s+[A-Z]",
                ],
                "weight": 0.8,
            },
            "academic": {
                "patterns": [
                    r"\b(?:abstract|introduction|methods|results|discussion|conclusion|references)\b",
                    r"\n\s*(?:Abstract|Introduction|Methods|Results|Discussion|Conclusion)",
                    r"\n\s*\d+\.\d+\s+[A-Z][a-z]+",
                ],
                "weight": 0.7,
            },
            "business": {
                "patterns": [
                    r"\b(?:executive summary|background|analysis|recommendations|financial|revenue)\b",
                    r"\n\s*(?:Executive Summary|Background|Analysis|Recommendations)",
                    r"\n\s*[A-Z][A-Z\s]{3,}\n",
                ],
                "weight": 0.6,
            },
            "technical": {
                "patterns": [
                    r"\b(?:api|endpoint|function|class|method|parameter|return)\b",
                    r"\n\s*```\w+",  # Code blocks
                    r"\n\s*#\s+[^\n]+",  # Markdown headers
                ],
                "weight": 0.5,
            },
        }

    def classify_document(self, text: str) -> str:
        """Classify document type based on content patterns"""
        scores = {}

        for doc_type, config in self.document_patterns.items():
            score = 0
            for pattern in config["patterns"]:
                matches = len(re.findall(pattern, text, re.IGNORECASE))
                score += matches * config["weight"]
            scores[doc_type] = score

        # Return document type with highest score
        if scores:
            return max(scores, key=scores.get)
        else:
            return "general"


class EnhancedSemanticChunker(SemanticChunker):
    """Enhanced semantic chunker with document type awareness"""

    def __init__(self, max_tokens=100000):
        super().__init__(max_tokens)
        self.classifier = DocumentTypeClassifier()

    def create_semantic_chunks(self, text: str) -> List[Dict]:
        """
        Create semantic chunks with document type awareness
        """
        # Classify document type
        doc_type = self.classifier.classify_document(text)

        # Adjust patterns based on document type
        self._adjust_patterns_for_document_type(doc_type)

        # Create chunks using parent method
        chunks = super().create_semantic_chunks(text)

        # Add document type information
        for chunk in chunks:
            chunk["document_type"] = doc_type

        return chunks

    def _adjust_patterns_for_document_type(self, doc_type: str):
        """Adjust semantic patterns based on document type"""
        if doc_type == "legal":
            # Prioritize legal patterns
            self.boundary_priority = [
                "legal_section",
                "legal_subsection",
                "legal_paragraph",
                "section",
                "subsection",
                "chapter",
            ] + [
                p
                for p in self.boundary_priority
                if p not in ["legal_section", "legal_subsection", "legal_paragraph"]
            ]

        elif doc_type == "academic":
            # Prioritize academic patterns
            self.boundary_priority = [
                "academic_section",
                "academic_subsection",
                "section",
                "subsection",
                "chapter",
            ] + [
                p
                for p in self.boundary_priority
                if p not in ["academic_section", "academic_subsection"]
            ]

        elif doc_type == "business":
            # Prioritize business patterns
            self.boundary_priority = [
                "business_section",
                "business_subsection",
                "section",
                "subsection",
                "chapter",
            ] + [
                p
                for p in self.boundary_priority
                if p not in ["business_section", "business_subsection"]
            ]


def create_enhanced_semantic_chunking_udf(max_tokens=100000):
    """
    Create UDF for enhanced semantic chunking with document type awareness
    """
    chunker = EnhancedSemanticChunker(max_tokens)

    def enhanced_semantic_chunk_udf(text):
        if not text:
            return []

        chunks = chunker.create_semantic_chunks(text)
        return chunks

    return udf(
        enhanced_semantic_chunk_udf, ArrayType(MapType(StringType(), StringType()))
    )


def process_documents_enhanced_semantic(
    spark: SparkSession,
    input_path: str,
    output_path: str,
    max_tokens: int = 100000,
):
    """
    Process documents using enhanced semantic chunking
    """

    # Load documents
    df = load_documents(spark, input_path)

    # Create enhanced semantic chunking UDF
    enhanced_chunk_udf = create_enhanced_semantic_chunking_udf(max_tokens)

    # Apply enhanced semantic chunking
    df_chunked = df.withColumn("enhanced_chunks", enhanced_chunk_udf(col("text")))

    # Explode chunks
    df_exploded = df_chunked.select(
        col("doc_id"),
        col("text").alias("original_text"),
        explode(col("enhanced_chunks")).alias("chunk_data"),
    )

    # Extract chunk information
    df_final = df_exploded.select(
        col("doc_id"),
        col("original_text"),
        col("chunk_data.text").alias("chunk_text"),
        col("chunk_data.boundary_type").alias("boundary_type"),
        col("chunk_data.document_type").alias("document_type"),
        col("chunk_data.estimated_tokens").alias("estimated_tokens"),
        col("chunk_data.chunk_strategy").alias("chunk_strategy"),
    )

    return df_final


if __name__ == "__main__":
    main()
