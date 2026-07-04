"""Translate per-file CPG output into Kafka event contracts."""

from datetime import datetime, timezone
from pathlib import Path

from src.common.hashing import file_sha256, make_edge_id, make_metadata_id
from src.common.schemas import EdgeEvent, ErrorEvent, MetadataEvent, NodeEvent
from src.parser_service.ast_parser import _relative_file_path, parse_python_file
from src.parser_service.cpg_builder import build_cpg_edges

SCHEMA_VERSION = "1.0"


def _event_time() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def build_error_event(repo_name: str, file_path: str, error: Exception) -> ErrorEvent:
    """Build the same versioned parser-error contract used by the CLI."""
    return ErrorEvent(
        schema_version=SCHEMA_VERSION,
        event_time=_event_time(),
        repo_name=repo_name,
        file_path=file_path,
        error_type=type(error).__name__,
        error_message=str(error),
    )


def build_events_for_file(
    repo_name: str, repo_path: Path, file_path: Path
) -> tuple[list[NodeEvent], list[EdgeEvent], MetadataEvent]:
    """Parse one file and construct deterministic node, edge, and metadata events."""
    absolute, relative = _relative_file_path(repo_path, file_path)
    nodes, ast_edges, metadata = parse_python_file(repo_name, repo_path, absolute)
    cpg_edges = build_cpg_edges(nodes, ast_edges)
    digest = file_sha256(absolute)
    event_time = _event_time()
    base = {
        "schema_version": SCHEMA_VERSION,
        "event_time": event_time,
        "repo_name": repo_name,
        "file_path": relative,
        "file_hash": digest,
    }
    node_events = [
        NodeEvent(
            **base,
            node_id=node.node_id,
            node_type=node.node_type,
            name=node.name,
            structural_path=node.structural_path,
            lineno=node.lineno,
            col_offset=node.col_offset,
            end_lineno=node.end_lineno,
            end_col_offset=node.end_col_offset,
        )
        for node in nodes
    ]
    edge_events = [
        EdgeEvent(
            **base,
            edge_id=make_edge_id(
                repo_name, relative, edge["edge_type"], edge["source_id"], edge["target_id"]
            ),
            **edge,
        )
        for edge in cpg_edges
    ]
    metadata_event = MetadataEvent(
        **base,
        metadata_id=make_metadata_id(repo_name, relative),
        **metadata,
        node_count=len(node_events),
        edge_count=len(edge_events),
    )
    return node_events, edge_events, metadata_event
