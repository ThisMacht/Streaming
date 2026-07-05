# Task 2: Incremental CPG Parser Service

## Goal

The second task implements the Parser Service. Its purpose is to process Python source files
incrementally, one file at a time, and emit structured Code Property Graph (CPG) events to Kafka.

The lab requires the parser to extract the main CPG elements from Python source code:

- AST nodes and AST edges;
- CFG edges;
- DFG edges;
- CALL edges;
- source metadata;
- parser error events.

The parser must also assign stable identifiers to emitted elements. This is important because later
reprocessing should update or merge the same logical node, edge, or metadata document instead of
creating duplicates downstream.

## Implementation approach

The implementation uses Python's standard-library `ast` module. The parser does not load the whole
repository at once. Instead, it reads one file, parses that file, emits events, and then moves to the
next file from the discovery manifest created in Task 1.

A single-file dry run can be executed with:

```bash
python -m src.parser_service.main \
  --mode one \
  --file src/accelerate/_lab_replay_probe.py \
  --dry-run
```

A full baseline run consumes the manifest and repeats the same one-file operation for every
discovered Python file. This keeps memory bounded by the current file and makes modified-file replay
straightforward.

The parser emits three successful output streams:

```text
Python file -> AST parser -> node events
                        -> edge events
                        -> metadata event
```

If a file cannot be parsed, the service emits a structured parser error event instead of silently
dropping the failure.

## Stable identity design

Stable identity is the most important part of this task. The parser uses deterministic identifiers
for every emitted object:

| Object | Stable identifier |
|---|---|
| Graph node | `node_id` |
| Graph edge | `edge_id` |
| Metadata document | `metadata_id` |
| Parser error | `repo_name:file_path` Kafka key |

A node identity is derived from stable repository, file, structural, and source attributes. An edge
identity is derived from the edge type and its stable endpoints. A metadata identity is derived from
the repository and file path.

The parser also stores an AST structural path. This is necessary because not every AST node has a
meaningful source position. For example, Python AST context nodes such as `Load`, `Store`, and
operator nodes may not have line and column information. Without a structural path, multiple
positionless nodes could collapse into the same identity. The structural path distinguishes them by
their location in the AST tree, while keeping the same source file stable across repeated parsing.

An example captured node event contains a stable node ID, node type, structural path, and source
position:

```json
{
  "schema_version": "1.0",
  "repo_name": "accelerate",
  "file_path": "benchmarks/big_model_inference/big_model_inference.py",
  "node_id": "38e41e11cb41c8fe7ec4ceb45a3aaa640e5639df205125aea4282b6d9e0e4a10",
  "node_type": "alias",
  "structural_path": "module.body[0].names[0]",
  "lineno": 15,
  "col_offset": 7
}
```

## Event semantics

The parser emits several categories of events.

| Event category | Meaning |
|---|---|
| Node event | One AST-derived graph node |
| Edge event | One AST, CFG, DFG, or CALL relationship |
| Metadata event | Per-file parse summary and file hash |
| Error event | Structured parser failure information |

A captured edge event includes the schema version, event time, repository name, file path, file
hash, stable edge ID, edge type, source node ID, and target node ID:

```json
{
  "schema_version": "1.0",
  "repo_name": "accelerate",
  "file_path": "benchmarks/big_model_inference/big_model_inference.py",
  "edge_type": "AST",
  "source_id": "008c0dcb1c46c6c0b1b186735756ee692ae164bc14969b7d3b6d4c8fcd3469f3",
  "target_id": "40946bc411075d50a641a36f4c9b42f94bebf70c08bde71ecf39862d873f5c99"
}
```

A captured metadata event records file-level statistics:

```json
{
  "schema_version": "1.0",
  "repo_name": "accelerate",
  "file_path": "benchmarks/big_model_inference/big_model_inference.py",
  "line_count": 143,
  "function_count": 2,
  "class_count": 0,
  "import_count": 7,
  "node_count": 877,
  "edge_count": 1849,
  "status": "parsed"
}
```

The parser also emits structured errors. The recorded error sample shows that syntax failures are
represented as Kafka events with the file path, error type, message, and failed status:

```json
{
  "schema_version": "1.0",
  "repo_name": "accelerate",
  "file_path": "src/accelerate/_lab_parser_error.py",
  "error_type": "SyntaxError",
  "error_message": "'(' was never closed (_lab_parser_error.py, line 1)",
  "status": "failed"
}
```

This error contract makes parser failures observable without stopping the rest of the pipeline.

## Evidence and result

The small replay probe was first parsed in dry-run mode:

```text
Parsed src/accelerate/_lab_replay_probe.py: nodes=10 edges=18 metadata=1 (dry-run)
Finished: successful=1 failed=0
```

The full baseline run then parsed all discovered files from Task 1 and published events to Kafka:

```text
Finished: successful=99 failed=0
```

The log shows real per-file parser output. For example:

```text
Parsed benchmarks/big_model_inference/big_model_inference.py: nodes=877 edges=1849 metadata=1
Parsed src/accelerate/accelerator.py: nodes=15837 edges=32872 metadata=1
Parsed src/accelerate/utils/dataclasses.py: nodes=13574 edges=27527 metadata=1
Parsed src/accelerate/utils/modeling.py: nodes=11772 edges=24763 metadata=1
```

These counts demonstrate that the parser did not only process the small replay probe. It processed a
large real repository and emitted file-level graph and metadata events for each selected Python file.

Kafka evidence was captured for all parser output categories:

```text
cpg.nodes.v1
cpg.edges.v1
cpg.metadata.v1
cpg.errors.v1
```

The automated test suite also passed:

```text
19 passed
```

The tests provide additional confidence that stable IDs, required schema fields, parser error
events, and replay-related contracts behave as expected.

## Limitations

The parser is intentionally educational and lightweight. AST extraction is based on Python's
standard `ast` module, while CFG, DFG, and CALL edges are approximate.

The CFG edges represent structural control-flow ordering, but they do not fully model branches,
loops, exceptions, or basic blocks. The DFG edges use a lightweight name-definition/use
approximation and are not scope-complete. CALL edges are best-effort and mainly capture local call
relationships; they do not fully resolve imports, methods, dynamic dispatch, or cross-file targets.

These limitations are acceptable for the lab because the main focus is incremental parsing,
streaming event design, stable identity, and idempotent ingestion into Neo4j and MongoDB.

## Reflection

The one-file parser design worked well for this lab. It keeps memory usage bounded, makes failures
isolated, and supports replaying exactly one modified file. Stable identifiers allow Kafka, Neo4j,
and MongoDB to converge on the same logical records during repeated processing.

The most important improvement was adding structural identity for positionless AST nodes. Without
that, nodes such as `Load`, `Store`, and operators could collide. With structural paths, repeated
parsing of the same file remains stable while distinct AST occurrences remain distinguishable.

The tradeoff is semantic depth. The parser produces useful CPG-like graph events for the streaming
pipeline, but it should not be interpreted as a production-grade static analysis engine.
