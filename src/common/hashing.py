"""Stable identifiers used to make replay idempotent."""

import hashlib
from pathlib import Path


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


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
) -> str:
    return sha256_text(
        "\x1f".join((repo_name, file_path, node_type, str(lineno), str(col_offset), name or ""))
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
