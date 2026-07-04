"""Kafka event contracts."""

from typing import Literal

import orjson
from pydantic import BaseModel


class BaseEvent(BaseModel):
    schema_version: str
    event_time: str
    repo_name: str
    file_path: str


class NodeEvent(BaseEvent):
    file_hash: str
    node_id: str
    node_type: str
    name: str | None = None
    structural_path: str
    lineno: int
    col_offset: int
    end_lineno: int
    end_col_offset: int


class EdgeEvent(BaseEvent):
    file_hash: str
    edge_id: str
    edge_type: Literal["AST", "CFG", "DFG", "CALL"]
    source_id: str
    target_id: str


class MetadataEvent(BaseEvent):
    metadata_id: str
    file_hash: str
    line_count: int
    function_count: int
    class_count: int
    import_count: int
    node_count: int
    edge_count: int
    status: str = "parsed"


class ErrorEvent(BaseEvent):
    error_type: str
    error_message: str
    status: str = "failed"


def to_json_bytes(event: BaseModel) -> bytes:
    """Serialize a Pydantic event as compact UTF-8 JSON."""
    return orjson.dumps(event.model_dump(mode="json"))
