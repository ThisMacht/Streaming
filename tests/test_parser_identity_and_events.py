"""Parser identity, event-contract, and same-content replay tests."""

import ast
from pathlib import Path
from unittest.mock import Mock

import pytest

from src.common.config import Settings
from src.common.schemas import BaseEvent
from src.parser_service.ast_parser import iter_ast_with_path
from src.parser_service.event_builder import build_error_event, build_events_for_file
from src.parser_service.kafka_producer import CpgKafkaProducer
from src.parser_service.main import process_file


def build_source_events(tmp_path: Path, source: str):
    repo = tmp_path / "repo"
    file = repo / "sample.py"
    repo.mkdir(exist_ok=True)
    file.write_text(source, encoding="utf-8")
    return build_events_for_file("example", repo, file)


def test_node_ids_are_unique_for_positionless_ast_nodes(tmp_path: Path) -> None:
    nodes, _, _ = build_source_events(tmp_path, "a = b + c\nd = e + f\n")
    node_ids = [node.node_id for node in nodes]
    assert len(node_ids) == len(set(node_ids))
    assert len([node for node in nodes if node.node_type == "Load"]) == 4
    assert len([node for node in nodes if node.node_type == "Store"]) == 2
    assert len([node for node in nodes if node.node_type == "Add"]) == 2


def test_ast_structural_paths_include_fields_and_indexes() -> None:
    paths = {path for _, path in iter_ast_with_path(ast.parse("result = left + right\n"))}
    assert "module.body[0].targets[0].ctx" in paths
    assert "module.body[0].value.left.ctx" in paths
    assert "module.body[0].value.op" in paths


def test_node_ids_are_stable_for_same_content(tmp_path: Path) -> None:
    first, _, _ = build_source_events(tmp_path, "value = source + 1\nprint(value)\n")
    second, _, _ = build_source_events(tmp_path, "value = source + 1\nprint(value)\n")
    assert [event.node_id for event in first] == [event.node_id for event in second]


def test_edge_ids_are_stable_for_same_content(tmp_path: Path) -> None:
    _, first, _ = build_source_events(tmp_path, "value = source + 1\nprint(value)\n")
    _, second, _ = build_source_events(tmp_path, "value = source + 1\nprint(value)\n")
    assert [event.edge_id for event in first] == [event.edge_id for event in second]


def test_parser_error_event_schema(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    broken = repo / "broken.py"
    broken.write_text("def broken(:\n    pass\n", encoding="utf-8")
    with pytest.raises(SyntaxError) as captured:
        build_events_for_file("example", repo, broken)
    event = build_error_event("example", "broken.py", captured.value)
    assert event.schema_version == "1.0"
    assert event.event_time.endswith("Z")
    assert event.repo_name == "example"
    assert event.file_path == "broken.py"
    assert event.error_type == "SyntaxError"
    assert event.error_message


def test_required_event_schema_fields(tmp_path: Path) -> None:
    nodes, edges, metadata = build_source_events(tmp_path, "value = source + 1\n")
    error = build_error_event("example", "sample.py", ValueError("bad source"))
    common = {"schema_version", "event_time", "repo_name", "file_path"}
    for event in (nodes[0], edges[0], metadata, error):
        assert isinstance(event, BaseEvent)
        assert common <= set(event.model_dump())
    assert {"node_id", "structural_path"} <= set(nodes[0].model_dump())
    assert {"edge_id", "source_id", "target_id"} <= set(edges[0].model_dump())
    assert {"metadata_id", "file_hash"} <= set(metadata.model_dump())
    assert {"error_type", "error_message"} <= set(error.model_dump())


def test_same_content_reprocess_does_not_change_ids(tmp_path: Path) -> None:
    source = "result = input_value + 1\nprint(result)\n"
    first_nodes, first_edges, _ = build_source_events(tmp_path, source)
    second_nodes, second_edges, _ = build_source_events(tmp_path, source)
    assert {event.node_id for event in first_nodes} == {event.node_id for event in second_nodes}
    assert {event.edge_id for event in first_edges} == {event.edge_id for event in second_edges}


def test_process_file_publishes_parser_error(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    broken = repo / "broken.py"
    broken.write_text("def broken(:\n    pass\n", encoding="utf-8")
    settings = Settings(
        repo_name="example",
        repo_url="unused",
        repo_local_path=str(repo),
        kafka_bootstrap_servers="unused:9092",
        kafka_topic_nodes="nodes",
        kafka_topic_edges="edges",
        kafka_topic_metadata="metadata",
        kafka_topic_errors="errors",
        neo4j_uri="bolt://unused",
        neo4j_username="unused",
        neo4j_password="unused",
        mongodb_uri="mongodb://unused",
        mongodb_database="unused",
        mongodb_collection_metadata="unused",
        spark_checkpoint_location="unused",
    )
    producer = Mock(spec=CpgKafkaProducer)

    assert process_file(settings, repo, broken, producer, dry_run=False) is False
    producer.send_error.assert_called_once()
    topic, event = producer.send_error.call_args.args
    assert topic == "errors"
    assert event.file_path == "broken.py"
    assert event.error_type == "SyntaxError"


def test_kafka_entity_keys_follow_event_identity(tmp_path: Path) -> None:
    nodes, edges, metadata = build_source_events(tmp_path, "value = source + 1\n")
    error = build_error_event("example", "sample.py", ValueError("bad source"))
    producer = object.__new__(CpgKafkaProducer)
    producer._send = Mock()  # type: ignore[method-assign]

    producer.send_node("nodes", nodes[0])
    producer.send_edge("edges", edges[0])
    producer.send_metadata("metadata", metadata)
    producer.send_error("errors", error)

    assert [call.args[1] for call in producer._send.call_args_list] == [
        nodes[0].node_id,
        edges[0].edge_id,
        metadata.metadata_id,
        "example:sample.py",
    ]
