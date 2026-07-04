"""Python AST extraction with stable node identities."""

import ast
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.common.hashing import make_node_id


@dataclass
class ParsedAstNode:
    node_id: str
    node_type: str
    name: str | None
    lineno: int
    col_offset: int
    ast_obj: ast.AST


def _relative_file_path(repo_path: Path, file_path: Path) -> tuple[Path, str]:
    root = repo_path.expanduser().resolve()
    candidate = file_path.expanduser()
    if not candidate.is_absolute():
        # CLI paths are normally relative to the repository, but accepting a path
        # already prefixed by repo_path makes the public API less surprising.
        prefixed = candidate.resolve()
        candidate = prefixed if prefixed.is_relative_to(root) else root / candidate
    absolute = candidate.resolve()
    try:
        relative = absolute.relative_to(root).as_posix()
    except ValueError as exc:
        raise ValueError(f"File {absolute} is outside repository {root}") from exc
    return absolute, relative


def _call_name(node: ast.Call) -> str | None:
    parts: list[str] = []
    current: ast.AST = node.func
    while isinstance(current, ast.Attribute):
        parts.append(current.attr)
        current = current.value
    if isinstance(current, ast.Name):
        parts.append(current.id)
    return ".".join(reversed(parts)) if parts else None


def _node_name(node: ast.AST) -> str | None:
    if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef):
        return node.name
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Call):
        return _call_name(node)
    if isinstance(node, ast.Import):
        return ",".join(alias.name for alias in node.names)
    if isinstance(node, ast.ImportFrom):
        names = ",".join(alias.name for alias in node.names)
        return f"{node.module or ''}:{names}"
    if isinstance(node, ast.Attribute):
        return node.attr
    return None


def parse_python_file(
    repo_name: str, repo_path: Path, file_path: Path
) -> tuple[list[ParsedAstNode], list[tuple[str, str]], dict[str, Any]]:
    """Parse one Python file and return unique nodes, AST edges, and counters."""
    absolute, relative = _relative_file_path(repo_path, file_path)
    source = absolute.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=relative)
    nodes_by_id: dict[str, ParsedAstNode] = {}
    edges: set[tuple[str, str]] = set()

    def visit(node: ast.AST, parent_id: str | None = None) -> None:
        node_type = type(node).__name__
        name = _node_name(node)
        lineno = int(getattr(node, "lineno", 0) or 0)
        col_offset = int(getattr(node, "col_offset", 0) or 0)
        node_id = make_node_id(repo_name, relative, node_type, lineno, col_offset, name)
        nodes_by_id.setdefault(
            node_id, ParsedAstNode(node_id, node_type, name, lineno, col_offset, node)
        )
        if parent_id is not None and parent_id != node_id:
            edges.add((parent_id, node_id))
        for child in ast.iter_child_nodes(node):
            visit(child, node_id)

    visit(tree)
    metadata = {
        "line_count": len(source.splitlines()),
        "function_count": sum(
            isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef) for node in ast.walk(tree)
        ),
        "class_count": sum(isinstance(node, ast.ClassDef) for node in ast.walk(tree)),
        "import_count": sum(
            isinstance(node, ast.Import | ast.ImportFrom) for node in ast.walk(tree)
        ),
    }
    return list(nodes_by_id.values()), sorted(edges), metadata
