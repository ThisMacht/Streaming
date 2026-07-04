"""Stream Kafka metadata events to MongoDB through MongoDB Spark Connector."""

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, concat_ws, current_timestamp, from_json, lit, sha2, when
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from src.common.config import load_settings

MONGODB_FORMAT = "mongodb"


def mongodb_write_options() -> dict[str, str]:
    """Return connector options for stable-key replace-with-upsert writes."""
    settings = load_settings()
    return {
        "connection.uri": settings.mongodb_uri,
        "database": settings.mongodb_database,
        "collection": settings.mongodb_collection_metadata,
        "operationType": "replace",
        "idFieldList": "metadata_id",
        "upsertDocument": "true",
        "ordered": "false",
    }


def upsert_metadata_batch(batch_df: DataFrame, batch_id: int) -> None:
    """Write one micro-batch using the MongoDB Spark Connector.

    Connector batch writes support replace + upsert keyed by ``idFieldList``.
    ``foreachBatch`` keeps Kafka checkpoint ownership in Structured Streaming;
    it does not use PyMongo or collect rows in the Python driver.
    """
    enriched_df = batch_df.withColumn("ingested_at", current_timestamp()).withColumn(
        "spark_batch_id", lit(batch_id)
    )
    enriched_df.write.format(MONGODB_FORMAT).mode("append").options(
        **mongodb_write_options()
    ).save()


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
    parsed_df = (
        raw_df.select(from_json(col("value").cast("string"), build_metadata_schema()).alias("data"))
        .select("data.*")
        .where(col("repo_name").isNotNull() & col("file_path").isNotNull())
    )
    # Current producers always send metadata_id. The deterministic fallback
    # preserves compatibility with older repo_name + file_path events.
    metadata_df = parsed_df.withColumn(
        "metadata_id",
        when(col("metadata_id").isNotNull(), col("metadata_id")).otherwise(
            sha2(concat_ws("\u001f", col("repo_name"), col("file_path")), 256)
        ),
    )

    # foreachBatch keeps Kafka consumption and checkpointing in Structured
    # Streaming while the MongoDB Spark Connector performs replace/upsert.
    query = (
        metadata_df.writeStream.foreachBatch(upsert_metadata_batch)
        .option("checkpointLocation", settings.spark_checkpoint_location)
        .outputMode("append")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
