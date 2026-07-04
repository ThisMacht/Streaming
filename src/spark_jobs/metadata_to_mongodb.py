"""Stream Kafka metadata events to MongoDB with idempotent per-batch upserts."""

from collections.abc import Iterable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pymongo import MongoClient, ReplaceOne
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from src.common.config import load_settings

UPSERT_BATCH_SIZE = 500


def metadata_upsert_filter(document: dict[str, Any]) -> dict[str, Any]:
    """Build the stable MongoDB identity filter for one metadata event."""
    if document.get("metadata_id"):
        return {"metadata_id": document["metadata_id"]}
    if document.get("repo_name") and document.get("file_path"):
        return {"repo_name": document["repo_name"], "file_path": document["file_path"]}
    raise ValueError("Metadata event needs metadata_id or repo_name + file_path")


def _chunks(items: Iterable[ReplaceOne], size: int) -> Iterable[list[ReplaceOne]]:
    chunk: list[ReplaceOne] = []
    for item in items:
        chunk.append(item)
        if len(chunk) == size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def upsert_metadata_batch(batch_df: DataFrame, batch_id: int) -> None:
    """Upsert one Spark micro-batch without collecting the whole batch in driver memory."""
    settings = load_settings()
    updated_at = datetime.now(timezone.utc)

    def operations() -> Iterable[ReplaceOne]:
        for row in batch_df.toLocalIterator():
            document = row.asDict(recursive=True)
            document["ingested_at"] = updated_at
            document["spark_batch_id"] = batch_id
            yield ReplaceOne(metadata_upsert_filter(document), document, upsert=True)

    client = MongoClient(settings.mongodb_uri)
    try:
        collection = client[settings.mongodb_database][settings.mongodb_collection_metadata]
        for chunk in _chunks(operations(), UPSERT_BATCH_SIZE):
            collection.bulk_write(chunk, ordered=False)
    finally:
        client.close()


def build_metadata_schema() -> StructType:
    """Return the schema of metadata events published by the parser service."""
    return StructType(
        [
            StructField("schema_version", StringType(), True),
            StructField("event_time", StringType(), True),
            StructField("repo_name", StringType(), True),
            StructField("file_path", StringType(), True),
            StructField("metadata_id", StringType(), True),
            StructField("file_hash", StringType(), True),
            StructField("line_count", IntegerType(), True),
            StructField("function_count", IntegerType(), True),
            StructField("class_count", IntegerType(), True),
            StructField("import_count", IntegerType(), True),
            StructField("node_count", IntegerType(), True),
            StructField("edge_count", IntegerType(), True),
            StructField("status", StringType(), True),
        ]
    )


def main() -> None:
    """Run the Kafka-to-MongoDB Structured Streaming query."""
    settings = load_settings()
    Path(settings.spark_checkpoint_location).mkdir(parents=True, exist_ok=True)

    spark = SparkSession.builder.appName("CPGMetadataToMongoDB").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    raw_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("subscribe", settings.kafka_topic_metadata)
        .option("startingOffsets", "earliest")
        .load()
    )
    metadata_df = (
        raw_df.select(from_json(col("value").cast("string"), build_metadata_schema()).alias("data"))
        .select("data.*")
        .where(
            col("metadata_id").isNotNull()
            | (col("repo_name").isNotNull() & col("file_path").isNotNull())
        )
    )

    # foreachBatch keeps Kafka consumption and checkpointing in Structured
    # Streaming while allowing stable-key ReplaceOne(upsert=True) semantics.
    # toLocalIterator and bounded bulk chunks avoid collecting a full batch.
    query = (
        metadata_df.writeStream.foreachBatch(upsert_metadata_batch)
        .option("checkpointLocation", settings.spark_checkpoint_location)
        .outputMode("append")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
