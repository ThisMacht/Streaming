"""Modify and replay exactly one source file, with optional file-level graph replacement."""

import time
from pathlib import Path
from typing import Annotated

import typer

from src.common.config import load_settings
from src.common.hashing import file_sha256
from src.common.logging_utils import get_logger
from src.parser_service.event_builder import build_events_for_file
from src.parser_service.kafka_producer import CpgKafkaProducer
from src.verification.cleanup_neo4j_file import cleanup_file
from src.verification.mongodb_checks import get_duplicate_groups, get_metadata, get_metadata_count
from src.verification.neo4j_checks import get_counts

logger = get_logger(__name__)
app = typer.Typer(add_completion=False)

DEFAULT_PROBE = Path("src/accelerate/_lab_replay_probe.py")
BASELINE_SOURCE = """\
def lab_replay_value(x):
    return x + 1
"""
MODIFIED_SOURCE = """\
def lab_replay_value(x):
    return x + 2


def lab_replay_marker():
    return "modified"
"""


def controlled_source(modified: bool) -> str:
    return MODIFIED_SOURCE if modified else BASELINE_SOURCE


def set_probe_state(path: Path, modified: bool) -> tuple[str | None, str]:
    """Write the requested deterministic probe state and return old/new hashes."""
    before_hash = file_sha256(path) if path.exists() else None
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(controlled_source(modified), encoding="utf-8")
    after_hash = file_sha256(path)
    return before_hash, after_hash


def _relative_path(repo_path: Path, file: Path) -> tuple[Path, str]:
    absolute = file.resolve() if file.is_absolute() else (repo_path / file).resolve()
    return absolute, absolute.relative_to(repo_path.resolve()).as_posix()


@app.command()
def main(
    file: Annotated[Path, typer.Option(help="Repository-relative Python file.")] = DEFAULT_PROBE,
    modify: Annotated[bool, typer.Option(help="Write the controlled modified probe.")] = False,
    restore: Annotated[bool, typer.Option(help="Restore the controlled baseline probe.")] = False,
    dry_run: Annotated[
        bool, typer.Option(help="Build events but do not touch Kafka/Neo4j.")
    ] = False,
    cleanup_neo4j_before_replay: Annotated[
        bool, typer.Option(help="Replace graph topology for only the target file.")
    ] = True,
    wait_seconds: Annotated[
        float, typer.Option(min=0, help="Time for Kafka sinks and Spark to consume replay.")
    ] = 8.0,
) -> None:
    """Prepare/modify a probe, then publish only that file and compare state."""
    if modify and restore:
        raise typer.BadParameter("Use only one of --modify and --restore")

    settings = load_settings()
    repo_path = Path(settings.repo_local_path)
    absolute, relative = _relative_path(repo_path, file)

    before_hash: str | None = file_sha256(absolute) if absolute.exists() else None
    if modify or restore:
        before_hash, after_hash = set_probe_state(absolute, modified=modify)
    elif absolute.exists():
        after_hash = file_sha256(absolute)
    else:
        raise typer.BadParameter(
            "Target does not exist; use --restore to create the baseline probe"
        )

    nodes, edges, metadata = build_events_for_file(settings.repo_name, repo_path, absolute)
    logger.info("Replay target: %s", relative)
    logger.info("Before hash: %s", before_hash or "<missing>")
    logger.info("After hash:  %s", after_hash)
    logger.info("Modified: %s", before_hash != after_hash)
    logger.info(
        "Events: nodes=%d edges=%d metadata_id=%s",
        len(nodes),
        len(edges),
        metadata.metadata_id,
    )
    if dry_run:
        logger.info("Dry-run complete; no Neo4j cleanup or Kafka publish performed.")
        return

    before_counts = get_counts()
    before_document_count = get_metadata_count()
    before_metadata = get_metadata(relative, settings.repo_name)
    logger.info(
        "Before replay: Neo4j=%s MongoDB document count=%d metadata=%s",
        before_counts,
        before_document_count,
        before_metadata,
    )

    if cleanup_neo4j_before_replay:
        deleted = cleanup_file(settings.repo_name, relative)
        logger.info("Neo4j target-file cleanup deleted %d nodes", deleted)

    producer = CpgKafkaProducer(settings.kafka_bootstrap_servers)
    try:
        for event in nodes:
            producer.send_node(settings.kafka_topic_nodes, event)
        for event in edges:
            producer.send_edge(settings.kafka_topic_edges, event)
        producer.send_metadata(settings.kafka_topic_metadata, metadata)
    finally:
        producer.flush()

    if wait_seconds:
        logger.info("Waiting %.1f seconds for Neo4j and the running Spark job", wait_seconds)
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
        "Count delta: nodes=%+d edges=%+d metadata_documents=%+d",
        after_counts["node_count"] - before_counts["node_count"],
        after_counts["edge_count"] - before_counts["edge_count"],
        after_document_count - before_document_count,
    )
    logger.info(
        "Duplicate count: metadata_id=%d repo/file=%d",
        len(metadata_duplicates),
        len(repo_file_duplicates),
    )
    if after_metadata and after_metadata.get("file_hash") == after_hash:
        logger.info("MongoDB metadata reflects the replayed file hash.")
    else:
        logger.error("MongoDB metadata has not yet reflected the replayed file hash.")
        raise typer.Exit(code=2)


if __name__ == "__main__":
    app()
