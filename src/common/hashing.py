"""Stable identifiers used to make replay idempotent."""

import hashlib
import json
from pathlib import Path
from typing import Any


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def stable_hash(value: Any) -> str:
    """Hash JSON-compatible data with deterministic key and Unicode handling."""
    serialized = json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return sha256_text(serialized)


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def make_node_id(
    repo_name: str,
    file_path: str,
    node_type: str,
    lineno: int,
    col_offset: int,
    name: str | None = None,
    structural_path: str = "",
    end_lineno: int = 0,
    end_col_offset: int = 0,
) -> str:
    """Build a stable per-occurrence AST node ID.

    New optional fields preserve compatibility with callers using the original
    positional signature while structural paths distinguish positionless AST
    contexts and operators.
    """
    return stable_hash(
        {
            "repo_name": repo_name,
            "file_path": file_path,
            "node_type": node_type,
            "structural_path": structural_path,
            "lineno": lineno,
            "col_offset": col_offset,
            "end_lineno": end_lineno,
            "end_col_offset": end_col_offset,
            "name": name or "",
        }
    )


def make_edge_id(
    repo_name: str,
    file_path: str,
    edge_type: str,
    source_id: str,
    target_id: str,
) -> str:
    return sha256_text("\x1f".join((repo_name, file_path, edge_type, source_id, target_id)))


def make_metadata_id(repo_name: str, file_path: str) -> str:
    return sha256_text("\x1f".join((repo_name, file_path)))
