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
from src.verification.neo4j_checks import (
    get_counts,
    get_duplicate_identity_counts,
    get_file_counts,
)

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
    output: Annotated[
        Path, typer.Option(help="Structured replay evidence summary.")
    ] = Path("evidence/logs/identity_replay_verification.log"),
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
    before_file_counts = get_file_counts(settings.repo_name, relative)
    before_document_count = get_metadata_count()
    before_metadata = get_metadata(relative, settings.repo_name)
    logger.info(
        "Before replay: Neo4j=%s MongoDB document count=%d metadata=%s",
        before_counts,
        before_document_count,
        before_metadata,
    )
    logger.info("Before replay target-file graph: %s", before_file_counts)

    if cleanup_neo4j_before_replay:
        logger.info(
            "Replay protocol: file-scoped Neo4j cleanup before connector-backed replacement; "
            "this cleanup is not the main graph ingestion path."
        )
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
    after_file_counts = get_file_counts(settings.repo_name, relative)
    after_document_count = get_metadata_count()
    after_metadata = get_metadata(relative, settings.repo_name)
    metadata_duplicates, repo_file_duplicates = get_duplicate_groups()
    graph_duplicates = get_duplicate_identity_counts()
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
    logger.info(
        "Duplicate graph IDs: node_id=%d edge_id=%d",
        graph_duplicates["node_id"],
        graph_duplicates["edge_id"],
    )
    logger.info("After replay target-file graph: %s", after_file_counts)

    checkpoint = Path(settings.spark_checkpoint_location)
    summary = [
        "protocol=file-scoped replay verification; graph replacement cleanup is direct Neo4j "
        "maintenance, while replacement graph events use Kafka -> Neo4j Connector",
        f"target_file={relative}",
        f"baseline_file_hash={before_hash or '<missing>'}",
        f"modified_file_hash={after_hash}",
        f"file_hash_changed={before_hash != after_hash}",
        f"mongodb_document_count_before={before_document_count}",
        f"mongodb_document_count_after={after_document_count}",
        f"mongodb_document_count_delta={after_document_count - before_document_count:+d}",
        f"mongodb_file_hash_matches_replay={bool(after_metadata and after_metadata.get('file_hash') == after_hash)}",
        f"neo4j_target_nodes_before={before_file_counts['node_count']}",
        f"neo4j_target_nodes_after={after_file_counts['node_count']}",
        f"neo4j_target_edges_before={before_file_counts['edge_count']}",
        f"neo4j_target_edges_after={after_file_counts['edge_count']}",
        f"duplicate_node_id_groups={graph_duplicates['node_id']}",
        f"duplicate_edge_id_groups={graph_duplicates['edge_id']}",
        f"duplicate_metadata_id_groups={len(metadata_duplicates)}",
        f"duplicate_repo_file_groups={len(repo_file_duplicates)}",
        f"checkpoint_location={checkpoint}",
        f"checkpoint_exists={checkpoint.is_dir()}",
    ]
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("\n".join(summary) + "\n", encoding="utf-8")
    logger.info("Saved structured replay evidence to %s", output)
    if (
        after_document_count != before_document_count
        or metadata_duplicates
        or repo_file_duplicates
        or graph_duplicates["node_id"]
        or graph_duplicates["edge_id"]
        or not after_metadata
        or after_metadata.get("file_hash") != after_hash
    ):
        logger.error("Replay identity/count verification failed; inspect %s", output)
        raise typer.Exit(code=3)
    logger.info("MongoDB metadata reflects the replayed file hash.")


if __name__ == "__main__":
    app()
