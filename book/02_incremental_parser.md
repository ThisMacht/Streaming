# Task 2: Incremental CPG Parser Service

## Objective

The second task is to build an incremental parser service for Python source files.

The parser service processes one Python file at a time and emits structured events for the streaming pipeline. The goal is not to parse the whole repository in one batch, but to incrementally process each selected `.py` file and publish its Code Property Graph information.

The emitted information includes:

| Event category | Description |
|---|---|
| AST nodes | Syntax elements extracted from Python source code |
| AST edges | Parent-child relationships between AST nodes |
| CFG edges | Approximate control-flow relationships |
| DFG edges | Approximate data-flow relationships |
| Call edges | Function call relationships detected from the parsed file |
| Metadata | Source file statistics and parsing status |
| Parser errors | Structured error messages for files that fail to parse |

## Implementation

The parser service is implemented under:

```text
src/parser_service/
```

The main modules are:

| Module | Purpose |
|---|---|
| `ast_parser.py` | Reads and parses a Python file using Python AST |
| `cpg_builder.py` | Builds additional CPG edges from parsed AST nodes |
| `event_builder.py` | Converts parser output into structured event objects |
| `main.py` | Runs the parser in dry-run or publishing mode |

The service accepts a file path and processes that file independently.

Example command:

```bash
python -m src.parser_service.main --mode one --file src/accelerate/accelerator.py --dry-run
```

Dry-run mode parses the file and prints statistics without publishing Kafka events. This is useful for checking parser correctness before running the streaming pipeline.

## Incremental Processing

The parser works incrementally because each call processes only one file:

```text
one Python file
      |
      v
parse AST
      |
      v
build CPG nodes and edges
      |
      v
build metadata event
      |
      v
publish events to Kafka
```

When running in full mode, the service iterates through the discovered file manifest and applies the same one-file processing function to each file.

This design keeps memory usage bounded by the size of the current file being parsed, instead of requiring the entire repository to be loaded at once.

## Stable Identifiers

Stable identifiers are important for idempotent replay.

The project generates deterministic identifiers for:

| Identifier | Purpose |
|---|---|
| `node_id` | Identifies one CPG node |
| `edge_id` | Identifies one CPG edge |
| `metadata_id` | Identifies one source metadata document |
| `error_id` or error key | Identifies parser error events |

The metadata identity is based on repository name and file path. This allows the same file to update the same MongoDB metadata document during replay.

Node identity additionally contains a deterministic AST `structural_path`, such as
`module.body[0].value.left` or `module.body[1].targets[0].ctx`, plus start/end source positions and
the node name when present. The path distinguishes positionless context and operator occurrences
such as `Load`, `Store`, and `Add`; these nodes can otherwise share the same zero-valued source
position. Tests verify that IDs are unique within one file and stable when identical content is
reprocessed at the same repository path.

## Parser Output

A successful parser run emits three main groups of events:

```text
Node events
Edge events
Metadata event
```

Example dry-run output format:

```text
Parsed src/accelerate/accelerator.py: nodes=... edges=... metadata=1 (dry-run)
Finished: successful=1 failed=0
```

The exact numbers depend on the target file and repository revision.

## Event Publishing

When the parser is run without `--dry-run`, it publishes events to Kafka:

| Output | Kafka topic |
|---|---|
| Node events | `cpg.nodes.v1` |
| Edge events | `cpg.edges.v1` |
| Metadata events | `cpg.metadata.v1` |
| Parser error events | `cpg.errors.v1` |

Example command:

```bash
python -m src.parser_service.main --mode one --file src/accelerate/accelerator.py
```

To process all discovered files:

```bash
python -m src.parser_service.main --mode all
```

## Controlled Parser Error

A malformed Python file was used to verify that the parser service handles errors without stopping the whole pipeline.

The controlled error file was:

```text
src/accelerate/_lab_parser_error.py
```

Its invalid content produced a Python syntax error:

```text
SyntaxError: '(' was never closed
```

The parser caught the exception and published a structured error event.

Example error event:

```json
{
  "schema_version": "1.0",
  "event_time": "2026-07-04T07:24:04.242876Z",
  "repo_name": "accelerate",
  "file_path": "src/accelerate/_lab_parser_error.py",
  "error_type": "SyntaxError",
  "error_message": "'(' was never closed (_lab_parser_error.py, line 1)",
  "status": "failed"
}
```

This verifies that parser failures are isolated and can be inspected through the parser error topic.

## CPG Edge Categories

The parser emits several edge categories.

| Edge category | Meaning |
|---|---|
| AST | Parent-child syntax structure |
| CFG | Approximate execution order between nodes |
| DFG | Approximate data dependency between repeated identifiers |
| CALL | Function call relationships |

The CFG, DFG, and CALL edges are lightweight intra-file approximations:

- CFG connects nodes in deterministic source/structural order; it does not model complete branch,
  loop, exception, or basic-block semantics.
- DFG connects the latest simple `ast.Name` assignment (`Store`) to later reads (`Load`) of the
  same name. It deliberately avoids the earlier Load-to-Load chaining behavior.
- CALL resolves named functions found in the same file and does not fully resolve imports,
  methods, dynamic dispatch, or cross-file targets.

These limits preserve one-file-at-a-time bounded memory for the lab scope and avoid claiming the
precision of a full static-analysis engine.

## Verification

The parser service was verified by:

1. running dry-run mode on one Python file;
2. running full mode on all discovered files;
3. checking that Kafka received node, edge, metadata, and error events;
4. checking that Neo4j received graph topology;
5. checking that MongoDB received source metadata.

## Reflection

The parser service worked well for incremental processing because each file is parsed independently. This made it easier to replay one modified file later.

The main limitation is that the CPG construction is intentionally lightweight. AST extraction is based on Python's standard `ast` module, while CFG, DFG, and CALL edges are approximations. This limitation is documented clearly so that the report does not overclaim semantic precision.

The controlled parser error test was useful because it proved that one malformed Python file does not crash the entire pipeline. Instead, the parser emits an error event that can be inspected later.
