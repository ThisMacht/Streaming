"""Build lightweight intra-file CFG, DFG, and CALL approximations.

This lab intentionally stays bounded to one file. CFG edges represent source
order rather than complete branch semantics. DFG edges connect the latest
simple ``Name`` store to subsequent loads; scope/import/alias analysis remains
outside the lab implementation.
"""

import ast

from src.parser_service.ast_parser import ParsedAstNode


def _ordered(nodes: list[ParsedAstNode]) -> list[ParsedAstNode]:
    return sorted(
        nodes,
        key=lambda node: (node.lineno, node.col_offset, node.structural_path, node.node_id),
    )


def build_cfg_edges(nodes: list[ParsedAstNode]) -> list[tuple[str, str]]:
    ordered = _ordered(nodes)
    return [
        (left.node_id, right.node_id) for left, right in zip(ordered, ordered[1:], strict=False)
    ]


def build_call_edges(nodes: list[ParsedAstNode]) -> list[tuple[str, str]]:
    functions: dict[str, ParsedAstNode] = {}
    for node in _ordered(nodes):
        if node.node_type in {"FunctionDef", "AsyncFunctionDef"} and node.name:
            functions.setdefault(node.name, node)
    result = []
    for node in _ordered(nodes):
        if node.node_type == "Call" and node.name:
            target = functions.get(node.name) or functions.get(node.name.rsplit(".", 1)[-1])
            if target and node.node_id != target.node_id:
                result.append((node.node_id, target.node_id))
    return result


def build_dfg_edges(nodes: list[ParsedAstNode]) -> list[tuple[str, str]]:
    """Connect the latest simple assignment of a name to later reads."""
    last_definition: dict[str, ParsedAstNode] = {}
    result: list[tuple[str, str]] = []
    for node in _ordered(nodes):
        if not isinstance(node.ast_obj, ast.Name) or not node.name:
            continue
        if isinstance(node.ast_obj.ctx, ast.Store):
            last_definition[node.name] = node
        elif isinstance(node.ast_obj.ctx, ast.Load) and node.name in last_definition:
            result.append((last_definition[node.name].node_id, node.node_id))
    return result


def build_cpg_edges(
    nodes: list[ParsedAstNode], ast_edges: list[tuple[str, str]]
) -> list[dict[str, str]]:
    """Build and deduplicate all supported edge types."""
    typed_edges = (
        [("AST", *edge) for edge in ast_edges]
        + [("CFG", *edge) for edge in build_cfg_edges(nodes)]
        + [("DFG", *edge) for edge in build_dfg_edges(nodes)]
        + [("CALL", *edge) for edge in build_call_edges(nodes)]
    )
    unique = sorted(set(typed_edges))
    return [
        {"edge_type": edge_type, "source_id": source, "target_id": target}
        for edge_type, source, target in unique
    ]
