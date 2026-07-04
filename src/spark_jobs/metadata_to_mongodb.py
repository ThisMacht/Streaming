"""Stream Kafka metadata events to MongoDB through the Spark Connector."""

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from src.common.config import load_settings


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

    spark = (
        SparkSession.builder.appName("CPGMetadataToMongoDB")
        .config("spark.mongodb.write.connection.uri", settings.mongodb_uri)
        .config("spark.mongodb.write.database", settings.mongodb_database)
        .config("spark.mongodb.write.collection", settings.mongodb_collection_metadata)
        .getOrCreate()
    )
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
        .where(col("metadata_id").isNotNull())
        .withColumn("ingested_at", current_timestamp())
    )

    # This job intentionally uses the MongoDB Spark Connector through
    # writeStream.format("mongodb") because the lab requirement explicitly
    # asks for Spark Structured Streaming with the MongoDB Spark Connector.
    # Idempotency is supported at the data model level through stable
    # metadata_id values and MongoDB unique indexes.
    query = (
        metadata_df.writeStream.format("mongodb")
        .option("checkpointLocation", settings.spark_checkpoint_location)
        .option("forceDeleteTempCheckpointLocation", "false")
        .outputMode("append")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
