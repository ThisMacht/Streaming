# Task 2: Incremental CPG Parser

## Goal

Parse Python incrementally with bounded memory and emit stable, schema-checked events suitable for
Kafka replay.

## Implementation approach

The parser uses Python's standard-library `ast` and materializes only the current file. A successful
parse emits node events, edge events, and one metadata event; a failure emits a structured error
event. Full mode repeats this same one-file operation over the discovery manifest.

```bash
python -m src.parser_service.main \
  --mode one \
  --file src/accelerate/_lab_replay_probe.py \
  --dry-run
```

Stable identities support replay:

- `node_id` identifies an AST-derived node from stable repository, file, structural, and source
  attributes;
- `edge_id` identifies a typed relation between stable endpoints;
- `metadata_id` identifies a repository/file metadata document;
- the error Kafka key uses the repository/file convention.

The AST structural path distinguishes nodes without meaningful source positions, including
`Load`, `Store`, and operator nodes. This prevents distinct positionless occurrences from
collapsing while keeping identical input at the same path stable.

## Event semantics

| Output | Meaning |
|---|---|
| AST nodes and edges | Syntax extracted from Python `ast` |
| CFG edges | Approximate structural control-flow ordering |
| DFG edges | Lightweight name-definition/use approximation |
| CALL edges | Best-effort function-call relations, mainly intra-file |
| Metadata | File hash, counts, status, and stable identity |
| Error | Structured parser failure contract |

CFG does not model complete branches, loops, exceptions, or basic blocks. DFG is not scope-complete,
and CALL does not fully resolve imports, methods, dynamic dispatch, or cross-file targets.

## Evidence and result

Tests cover stable IDs, required schema fields, positionless AST identity, and the parser error
contract. [`logs/pytest.log`](logs/pytest.log) records:

```text
19 passed in 0.60s
```

Captured node, edge, metadata, and error events are linked in Task 3. The executed notebook also
shows representative intermediate parser output.

## Reflection

One-file parsing provides simple incremental replay and bounds memory by the current source file.
Stable structural identity fixed collisions among positionless AST nodes. The tradeoff is semantic
depth: CFG, DFG, and CALL are deliberately educational approximations, not a production static
analysis claim.
