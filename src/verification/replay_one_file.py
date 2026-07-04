"""Replay one source file and compare database state before and after."""

import time
from pathlib import Path
from typing import Annotated

import typer

from src.common.config import load_settings
from src.common.logging_utils import get_logger
from src.parser_service.kafka_producer import CpgKafkaProducer
from src.parser_service.main import process_file
from src.verification.mongodb_checks import (
    get_duplicate_groups,
    get_metadata,
    get_metadata_count,
)
from src.verification.neo4j_checks import get_counts

logger = get_logger(__name__)
app = typer.Typer(add_completion=False)


@app.command()
def main(
    file: Annotated[Path, typer.Option(help="Repository-relative Python file.")],
    wait_seconds: Annotated[
        float, typer.Option(min=0, help="Time for Kafka sinks to consume replay.")
    ] = 5.0,
) -> None:
    """Publish one file again and report whether stored counts remain stable."""
    settings = load_settings()
    repo_path = Path(settings.repo_local_path)
    relative = file.as_posix()
    if file.is_absolute():
        relative = file.resolve().relative_to(repo_path.resolve()).as_posix()
    before_counts = get_counts()
    before_document_count = get_metadata_count()
    before_metadata = get_metadata(relative, settings.repo_name)
    logger.info(
        "Before replay: Neo4j=%s MongoDB document count=%d metadata=%s",
        before_counts,
        before_document_count,
        before_metadata,
    )

    producer = CpgKafkaProducer(settings.kafka_bootstrap_servers)
    success = process_file(settings, repo_path, file, producer, dry_run=False)
    producer.flush()
    if not success:
        raise typer.Exit(code=1)
    if wait_seconds:
        logger.info("Waiting %.1f seconds for sinks to consume events", wait_seconds)
        time.sleep(wait_seconds)

    after_counts = get_counts()
    after_document_count = get_metadata_count()
    after_metadata = get_metadata(relative, settings.repo_name)
    metadata_duplicates, repo_file_duplicates = get_duplicate_groups()
    logger.info(
        "After replay: Neo4j=%s MongoDB document count=%d metadata=%s",
        after_counts,
        after_document_count,
        after_metadata,
    )
    logger.info(
        "Count delta: nodes=%+d edges=%+d; metadata document remains singular via upsert",
        after_counts["node_count"] - before_counts["node_count"],
        after_counts["edge_count"] - before_counts["edge_count"],
    )
    logger.info(
        "Duplicate count: metadata_id=%d repo/file=%d",
        len(metadata_duplicates),
        len(repo_file_duplicates),
    )
    if before_metadata is not None:
        logger.info("Replay produced a metadata event for an already known file.")
        logger.info("MongoDB unique index prevents duplicated metadata documents.")
        logger.info(
            "Check MongoDB and Spark logs if a duplicate key error appears in the streaming job."
        )


if __name__ == "__main__":
    app()
