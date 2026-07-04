"""Environment-backed application settings."""

import os
from dataclasses import dataclass

from dotenv import load_dotenv


@dataclass(frozen=True)
class Settings:
    repo_name: str
    repo_url: str
    repo_local_path: str
    kafka_bootstrap_servers: str
    kafka_topic_nodes: str
    kafka_topic_edges: str
    kafka_topic_metadata: str
    kafka_topic_errors: str
    neo4j_uri: str
    neo4j_username: str
    neo4j_password: str
    mongodb_uri: str
    mongodb_database: str
    mongodb_collection_metadata: str
    spark_checkpoint_location: str


def load_settings() -> Settings:
    """Load settings from .env and the process environment."""
    load_dotenv()
    return Settings(
        repo_name=os.getenv("REPO_NAME", "accelerate"),
        repo_url=os.getenv("REPO_URL", "https://github.com/huggingface/accelerate.git"),
        repo_local_path=os.getenv("REPO_LOCAL_PATH", "data/raw/accelerate"),
        kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        kafka_topic_nodes=os.getenv("KAFKA_TOPIC_NODES", "cpg.nodes.v1"),
        kafka_topic_edges=os.getenv("KAFKA_TOPIC_EDGES", "cpg.edges.v1"),
        kafka_topic_metadata=os.getenv("KAFKA_TOPIC_METADATA", "cpg.metadata.v1"),
        kafka_topic_errors=os.getenv("KAFKA_TOPIC_ERRORS", "cpg.errors.v1"),
        neo4j_uri=os.getenv("NEO4J_URI", "bolt://localhost:7687"),
        neo4j_username=os.getenv("NEO4J_USERNAME", "neo4j"),
        neo4j_password=os.getenv("NEO4J_PASSWORD", "password123"),
        mongodb_uri=os.getenv("MONGODB_URI", "mongodb://localhost:27017"),
        mongodb_database=os.getenv("MONGODB_DATABASE", "cpg_lab"),
        mongodb_collection_metadata=os.getenv("MONGODB_COLLECTION_METADATA", "source_metadata"),
        spark_checkpoint_location=os.getenv(
            "SPARK_CHECKPOINT_LOCATION", "data/checkpoints/mongodb_metadata"
        ),
    )
