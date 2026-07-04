# Task 3: Kafka Topic Design

## Objective

This task separates parser output into Kafka topics with distinct contracts and consumers. The
separation prevents graph traffic from being routed through Spark and isolates parser failures
from successful source-file events.

## Topic Layout

| Topic | Producer | Consumer | Purpose |
|---|---|---|---|
| `cpg.nodes.v1` | Parser Service | Neo4j Kafka Sink | CPG node properties |
| `cpg.edges.v1` | Parser Service | Neo4j Kafka Sink | AST, CFG, DFG, and CALL relationships |
| `cpg.metadata.v1` | Parser Service | Spark Structured Streaming | Per-file source metadata |
| `cpg.errors.v1` | Parser Service | Diagnostic consumer/logging | Structured parser failures |

The topic creation script configures three partitions for node, edge, and metadata topics, and one
partition for the lower-volume error topic.

## Event Schema

All event models inherit the common fields defined by `BaseEvent`:

| Field | Meaning |
|---|---|
| `schema_version` | Contract version; the current producer emits `1.0` |
| `event_time` | UTC time at which the event was built |
| `repo_name` | Repository identity, `accelerate` in this demo |
| `file_path` | Repository-relative source path |

Node, edge, and metadata events also carry `file_hash`. The schemas are serialized as compact JSON
without a Kafka Connect schema envelope.

## Kafka Keys and Idempotency

The producer uses entity identities as Kafka keys:

| Event | Kafka key |
|---|---|
| Node | `node_id` |
| Edge | `edge_id` |
| Metadata | `metadata_id` |
| Parser error | `repo_name:file_path` |

`node_id`, `edge_id`, and `metadata_id` are SHA-256 values generated from deterministic source
attributes. These keys support repeatable routing and allow downstream `MERGE` or upsert logic to
address the same logical entity during replay.

## Node Event Topic

`cpg.nodes.v1` contains the stable node ID, AST node type, optional name, line and column position,
file hash, and common event fields. The Neo4j sink merges each event into a `CPGNode` and links it
to its `SourceFile`.

## Edge Event Topic

`cpg.edges.v1` contains `edge_id`, `edge_type`, `source_id`, and `target_id`. The accepted edge
types are `AST`, `CFG`, `DFG`, and `CALL`. The latter three are lightweight approximations rather
than a complete semantic program analysis.

Because node and edge events use separate topics, their cross-topic arrival order is not
guaranteed. The edge sink therefore merges placeholder endpoint nodes when necessary. A later node
event fills the complete properties and sets `placeholder` to `false`.

## Metadata Event Topic

`cpg.metadata.v1` carries one event per parsed file. Besides identity and file hash, it records line,
function, class, import, node, and edge counts plus parse status. Spark is subscribed only to this
topic.

## Parser Error Topic

`cpg.errors.v1` records `error_type`, `error_message`, and failed status. A controlled malformed
file produced the following message, captured by a Kafka console consumer:

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

The parser catches the exception for one file, publishes this event, and does not terminate the
repository-wide loop.

## Verification

The tracked Terminal 2 log shows all four topic names and a `RUNNING` Neo4j connector. It also
records `successful=121 failed=0` for the valid discovery manifest and separately consumes one
message from `cpg.errors.v1` for the controlled syntax error.

Useful inspection commands are:

```bash
docker exec cpg-kafka kafka-topics --bootstrap-server kafka:29092 --list
docker exec cpg-kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 --topic cpg.errors.v1 \
  --from-beginning --max-messages 1
```

## Raw Kafka Samples

The four raw broker samples are captured with:

```bash
./scripts/capture_kafka_samples.sh
```

The helper prints the Kafka key before the JSON value and writes one message per topic:

| Topic | Evidence file | Expected key |
|---|---|---|
| `cpg.nodes.v1` | `evidence/kafka/node-sample.txt` | `node_id` |
| `cpg.edges.v1` | `evidence/kafka/edge-sample.txt` | `edge_id` |
| `cpg.metadata.v1` | `evidence/kafka/metadata-sample.txt` | `metadata_id` |
| `cpg.errors.v1` | `evidence/kafka/error-sample.txt` | `repo_name:file_path` |

Each captured JSON value includes `schema_version` and `event_time`. Node events also expose the
AST `structural_path`; graph and metadata keys use deterministic IDs so identical-content replay
addresses the same downstream entity. If the error sample is empty, first run
`python -m src.verification.emit_parser_error_sample` and capture again. The tracked sample files
are authoritative raw excerpts rather than reformatted examples.

Captured excerpts (values shortened only after the fields relevant to this check):

```text
node key | {"schema_version":"1.0","event_time":"2026-07-04T11:01:47.041898Z",...,"structural_path":"module.body[0].args",...}
edge key | {"schema_version":"1.0","event_time":"2026-07-04T11:01:47.041898Z",...,"edge_type":"AST","source_id":"...","target_id":"..."}
metadata key | {"schema_version":"1.0","event_time":"2026-07-04T11:01:47.041898Z",...,"metadata_id":"59f203...561ea",...}
accelerate:src/accelerate/_lab_parser_error.py | {"schema_version":"1.0","event_time":"2026-07-04T11:01:47.360504Z",...,"error_type":"SyntaxError",...}
```

## Reflection

Separate topics make ownership explicit: Neo4j consumes topology, Spark consumes metadata, and
errors remain available for diagnostics. Stable keys support idempotent consumers, but keys alone
do not delete stale graph entities after a source edit; Task 6 therefore adds file-level graph
replacement for controlled modified-file replay.
