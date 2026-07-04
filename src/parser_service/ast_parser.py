"""Python AST extraction with stable node identities."""

import ast
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.common.hashing import make_node_id


@dataclass
class ParsedAstNode:
    node_id: str
    node_type: str
    name: str | None
    structural_path: str
    lineno: int
    col_offset: int
    end_lineno: int
    end_col_offset: int
    ast_obj: ast.AST


def iter_ast_with_path(root: ast.AST) -> Iterator[tuple[ast.AST, str]]:
    """Yield every AST occurrence with a deterministic field/index path."""

    def walk(node: ast.AST, path: str) -> Iterator[tuple[ast.AST, str]]:
        yield node, path
        for field, value in ast.iter_fields(node):
            if isinstance(value, ast.AST):
                yield from walk(value, f"{path}.{field}")
            elif isinstance(value, list):
                for index, child in enumerate(value):
                    if isinstance(child, ast.AST):
                        yield from walk(child, f"{path}.{field}[{index}]")

    yield from walk(root, "module")


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
    if isinstance(node, ast.arg):
        return node.arg
    return None


def parse_python_file(
    repo_name: str, repo_path: Path, file_path: Path
) -> tuple[list[ParsedAstNode], list[tuple[str, str]], dict[str, Any]]:
    """Parse one Python file and return unique nodes, AST edges, and counters."""
    absolute, relative = _relative_file_path(repo_path, file_path)
    source = absolute.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=relative)
    nodes_by_id: dict[str, ParsedAstNode] = {}
    ids_by_path: dict[str, str] = {}
    edges: set[tuple[str, str]] = set()

    for node, structural_path in iter_ast_with_path(tree):
        node_type = type(node).__name__
        name = _node_name(node)
        lineno = int(getattr(node, "lineno", 0) or 0)
        col_offset = int(getattr(node, "col_offset", 0) or 0)
        end_lineno = int(getattr(node, "end_lineno", 0) or 0)
        end_col_offset = int(getattr(node, "end_col_offset", 0) or 0)
        node_id = make_node_id(
            repo_name,
            relative,
            node_type,
            lineno,
            col_offset,
            name,
            structural_path,
            end_lineno,
            end_col_offset,
        )
        if node_id in nodes_by_id:
            raise ValueError(f"Duplicate node ID at structural path {structural_path}")
        if structural_path in ids_by_path:
            raise ValueError(f"Duplicate AST structural path {structural_path}")
        nodes_by_id[node_id] = ParsedAstNode(
            node_id,
            node_type,
            name,
            structural_path,
            lineno,
            col_offset,
            end_lineno,
            end_col_offset,
            node,
        )
        ids_by_path[structural_path] = node_id

    for structural_path, node_id in ids_by_path.items():
        if structural_path == "module":
            continue
        parent_path = structural_path.rsplit(".", 1)[0]
        edges.add((ids_by_path[parent_path], node_id))
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
