#!/usr/bin/env python3
"""
PySpark Text Chunker - Splits text documents into chunks based on token length
Using pyspark-client to connect to Docker Spark cluster
"""

import os
import re
from typing import List, Tuple
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, explode, array, lit, row_number
from pyspark.sql.types import StringType, ArrayType, StructType, StructField
from pyspark.sql.window import Window

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "admin")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "password")


def get_spark_session() -> SparkSession:
    """Create and configure Spark session using pyspark-client"""

    print("Creating Spark session with pyspark-client...")

    spark = (
        SparkSession.builder.appName("TextChunker")
        .config("spark.sql.streaming.schemaInference", "true")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.defaultCatalog", "iceberg")
        .config("spark.sql.catalog.iceberg.type", "rest")
        .config("spark.sql.catalog.iceberg.uri", "http://localhost:8181")
        .config("spark.sql.catalog.iceberg.s3.endpoint", "http://localhost:9000")
        .config("spark.sql.catalog.iceberg.warehouse", "s3://data/wh")
        .config("spark.sql.catalog.iceberg.s3.access-key", AWS_ACCESS_KEY_ID)
        .config("spark.sql.catalog.iceberg.s3.secret-key", AWS_SECRET_ACCESS_KEY)
        .config("spark.sql.catalog.iceberg.s3.region", "us-east-1")
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
        .config("spark.hadoop.fs.s3a.region", "us-east-1")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # pyspark-client specific configurations
        .config("spark.driver.host", "localhost")
        .config("spark.driver.bindAddress", "localhost")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        # Resource configuration for cluster
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "1g")
        .config("spark.executor.cores", "1")
        # Connection settings for pyspark-client
        .config("spark.network.timeout", "800s")
        .config("spark.executor.heartbeatInterval", "60s")
        .config("spark.dynamicAllocation.enabled", "false")
        .master("spark://localhost:7077")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("INFO")
    print(f"Spark session created successfully!")
    print(f"Spark version: {spark.version}")
    print(f"Master URL: {spark.conf.get('spark.master')}")
    return spark


def simple_tokenize(text: str) -> List[str]:
    """
    Simple tokenization function that splits text on whitespace and punctuation

    Args:
        text: Input text string

    Returns:
        List of tokens
    """
    if not text:
        return []

    # Clean and normalize text
    text = re.sub(r"\s+", " ", text.strip())

    # Split on whitespace and punctuation
    tokens = re.findall(r"\b\w+\b", text.lower())

    return tokens


def chunk_text_by_tokens(
    text: str, chunk_size: int = 100, overlap: int = 20
) -> List[str]:
    """
    Split text into chunks based on token count with overlap

    Args:
        text: Input text to chunk
        chunk_size: Maximum number of tokens per chunk
        overlap: Number of tokens to overlap between chunks

    Returns:
        List of text chunks
    """
    if not text or chunk_size <= 0:
        return [text] if text else []

    # Tokenize the text
    tokens = simple_tokenize(text)

    if len(tokens) <= chunk_size:
        return [text]

    chunks = []
    start = 0

    while start < len(tokens):
        # Calculate end position for current chunk
        end = min(start + chunk_size, len(tokens))

        # Extract tokens for this chunk
        chunk_tokens = tokens[start:end]

        # Convert back to text (simple space-joining)
        chunk_text = " ".join(chunk_tokens)
        chunks.append(chunk_text)

        # Move start position, accounting for overlap
        start = end - overlap if end < len(tokens) else end

    return chunks


def chunk_text_by_sentences(
    text: str, chunk_size: int = 100, overlap: int = 20
) -> List[str]:
    """
    Split text into chunks by sentences, respecting token limits

    Args:
        text: Input text to chunk
        chunk_size: Maximum number of tokens per chunk
        overlap: Number of tokens to overlap between chunks

    Returns:
        List of text chunks
    """
    if not text or chunk_size <= 0:
        return [text] if text else []

    # Split into sentences (simple approach)
    sentences = re.split(r"[.!?]+", text)
    sentences = [s.strip() for s in sentences if s.strip()]

    if not sentences:
        return [text]

    chunks = []
    current_chunk = []
    current_token_count = 0

    for sentence in sentences:
        sentence_tokens = simple_tokenize(sentence)
        sentence_token_count = len(sentence_tokens)

        # If adding this sentence would exceed chunk size
        if current_token_count + sentence_token_count > chunk_size and current_chunk:
            # Save current chunk
            chunk_text = " ".join(current_chunk)
            chunks.append(chunk_text)

            # Start new chunk with overlap
            if overlap > 0:
                # Calculate overlap tokens from end of previous chunk
                overlap_tokens = []
                overlap_count = 0
                for sent in reversed(current_chunk):
                    sent_tokens = simple_tokenize(sent)
                    if overlap_count + len(sent_tokens) <= overlap:
                        overlap_tokens.insert(0, sent)
                        overlap_count += len(sent_tokens)
                    else:
                        break

                current_chunk = overlap_tokens
                current_token_count = overlap_count
            else:
                current_chunk = []
                current_token_count = 0

        # Add sentence to current chunk
        current_chunk.append(sentence)
        current_token_count += sentence_token_count

    # Add final chunk if not empty
    if current_chunk:
        chunk_text = " ".join(current_chunk)
        chunks.append(chunk_text)

    return chunks if chunks else [text]


def hierarchical_chunking_strategy(document):
    """
    Multi-level chunking for large documents
    Level 1: Document sections (50k-100k tokens)
    Level 2: Subsections (10k-20k tokens)
    Level 3: Paragraphs (1k-5k tokens)
    """
    return {
        "document": full_document,
        "sections": chunk_by_sections(document),
        "subsections": chunk_by_subsections(document),
        "paragraphs": chunk_by_paragraphs(document),
    }


# Register UDFs
def register_udfs(spark: SparkSession):
    """Register User Defined Functions with Spark"""

    # UDF for simple tokenization
    tokenize_udf = udf(simple_tokenize, ArrayType(StringType()))

    # UDF for chunking by tokens
    chunk_by_tokens_udf = udf(
        lambda text, chunk_size, overlap: chunk_text_by_tokens(
            text, chunk_size, overlap
        ),
        ArrayType(StringType()),
    )

    # UDF for chunking by sentences
    chunk_by_sentences_udf = udf(
        lambda text, chunk_size, overlap: chunk_text_by_sentences(
            text, chunk_size, overlap
        ),
        ArrayType(StringType()),
    )

    return tokenize_udf, chunk_by_tokens_udf, chunk_by_sentences_udf


def process_text_files(
    spark: SparkSession,
    input_path: str,
    output_path: str,
    chunk_size: int = 100,
    overlap: int = 20,
    chunk_method: str = "tokens",
):
    """
    Process text files and create chunks

    Args:
        spark: SparkSession
        input_path: Path to input text files
        output_path: Path to save chunked data
        chunk_size: Maximum tokens per chunk
        overlap: Token overlap between chunks
        chunk_method: "tokens" or "sentences"
    """

    # Register UDFs
    tokenize_udf, chunk_by_tokens_udf, chunk_by_sentences_udf = register_udfs(spark)

    # Read text files
    print(f"Reading text files from: {input_path}")

    # Create sample data if input path doesn't exist
    if not os.path.exists(input_path):
        print("Input path not found, creating sample data...")
        sample_data = [
            (
                "doc1",
                "This is a sample legal document about contract law. It contains multiple sentences and paragraphs that need to be processed. The document discusses various aspects of legal agreements and their implications.",
            ),
            (
                "doc2",
                "Another document about corporate law and business regulations. This text includes technical legal terminology and complex sentence structures. It covers topics like mergers, acquisitions, and compliance requirements.",
            ),
            (
                "doc3",
                "A third document focusing on intellectual property rights and patent law. This document contains detailed explanations of legal procedures and case law references. It discusses copyright, trademarks, and patent applications.",
            ),
        ]

        # Create DataFrame from sample data
        schema = StructType(
            [
                StructField("doc_id", StringType(), False),
                StructField("text", StringType(), False),
            ]
        )

        df = spark.createDataFrame(sample_data, schema)
    else:
        # Read from actual files
        df = spark.read.text(input_path)
        df = df.withColumnRenamed("value", "text")
        df = df.withColumn("doc_id", row_number().over(Window.orderBy("text")))

    print(f"Processing {df.count()} documents...")

    # Apply chunking based on method
    if chunk_method == "tokens":
        df_chunked = df.withColumn(
            "chunks", chunk_by_tokens_udf(col("text"), lit(chunk_size), lit(overlap))
        )
    elif chunk_method == "sentences":
        df_chunked = df.withColumn(
            "chunks", chunk_by_sentences_udf(col("text"), lit(chunk_size), lit(overlap))
        )
    else:
        raise ValueError("chunk_method must be 'tokens' or 'sentences'")

    # Explode chunks into separate rows
    df_exploded = df_chunked.select(
        col("doc_id"),
        col("text").alias("original_text"),
        explode(col("chunks")).alias("chunk_text"),
    )

    # Add chunk metadata
    df_final = (
        df_exploded.withColumn(
            "chunk_id",
            row_number().over(Window.partitionBy("doc_id").orderBy("chunk_text")),
        )
        .withColumn(
            "token_count",
            udf(lambda x: len(simple_tokenize(x)), StringType())(col("chunk_text")),
        )
        .withColumn("chunk_size", lit(chunk_size))
        .withColumn("overlap", lit(overlap))
        .withColumn("chunk_method", lit(chunk_method))
    )

    # Show sample results
    print("\nSample chunked data:")
    df_final.show(10, truncate=False)

    # Save results
    print(f"\nSaving chunked data to: {output_path}")

    # Save as Iceberg table
    df_final.writeTo(f"iceberg.text_chunks").using("iceberg").tableProperty(
        "write.merge.isolation-level", "snapshot"
    ).tableProperty("write.format.default", "avro").createOrReplace()

    # Also save as Parquet for easy access
    df_final.write.mode("overwrite").parquet(output_path)

    # Print statistics
    total_chunks = df_final.count()
    avg_tokens = (
        df_final.select("token_count").agg({"token_count": "avg"}).collect()[0][0]
    )

    print(f"\n=== Processing Complete ===")
    print(f"Total documents processed: {df.count()}")
    print(f"Total chunks created: {total_chunks}")
    print(f"Average tokens per chunk: {avg_tokens:.1f}")
    print(f"Chunk size: {chunk_size}")
    print(f"Overlap: {overlap}")
    print(f"Method: {chunk_method}")

    return df_final


def analyze_chunks(spark: SparkSession, table_name: str = "iceberg.text_chunks"):
    """
    Analyze the chunked data and provide statistics

    Args:
        spark: SparkSession
        table_name: Name of the table containing chunks
    """
    print(f"\n=== Chunk Analysis ===")

    # Read the chunked data
    df = spark.table(table_name)

    # Basic statistics
    print("Basic Statistics:")
    df.select("token_count").summary().show()

    # Distribution of chunk sizes
    print("\nChunk Size Distribution:")
    df.groupBy("token_count").count().orderBy("token_count").show(20)

    # Documents with most chunks
    print("\nDocuments with Most Chunks:")
    df.groupBy("doc_id").count().orderBy("count", ascending=False).show(10)

    # Average chunks per document
    avg_chunks = df.groupBy("doc_id").count().agg({"count": "avg"}).collect()[0][0]
    print(f"\nAverage chunks per document: {avg_chunks:.1f}")


def sliding_window_128k(text, window_size=100000, overlap=20000):
    """
    Large sliding windows with significant overlap
    - Window: 100k tokens (78% of context)
    - Overlap: 20k tokens (15% overlap for context continuity)
    - Leaves 8k tokens for model processing
    """
    return sliding_window_chunks(text, window_size, overlap)


def main():
    """Main function to run the text chunking pipeline"""

    # Configuration
    input_path = "data/docs/legal"  # Path to input text files
    output_path = "data/chunked_text"  # Path to save chunked data
    chunk_size = 100  # Maximum tokens per chunk
    overlap = 20  # Token overlap between chunks
    chunk_method = "tokens"  # "tokens" or "sentences"

    print("Starting text chunking pipeline with pyspark-client...")
    print(f"Input path: {input_path}")
    print(f"Output path: {output_path}")
    print(f"Chunk size: {chunk_size}")
    print(f"Overlap: {overlap}")
    print(f"Method: {chunk_method}")

    try:
        # Create Spark session
        spark = get_spark_session()

        # Process text files
        df_chunked = process_text_files(
            spark=spark,
            input_path=input_path,
            output_path=output_path,
            chunk_size=chunk_size,
            overlap=overlap,
            chunk_method=chunk_method,
        )

        # Analyze results
        analyze_chunks(spark)

        print("\nText chunking pipeline completed successfully!")

    except Exception as e:
        print(f"Error in text chunking pipeline: {str(e)}")
        import traceback

        traceback.print_exc()
        raise
    finally:
        if "spark" in locals():
            print("Stopping Spark session...")
            spark.stop()


if __name__ == "__main__":
    main()
