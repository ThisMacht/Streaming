# Task 3: Kafka Topics and Event Contracts

## Goal

The third task designs the Kafka topic layout and event contracts used by the incremental CPG
pipeline. The lab requires separate topics for graph node events, graph edge events, source metadata
events, and parser error events. Each event must include a schema version and an event-time
timestamp so that the contract can evolve and downstream systems can reason about when the event was
created.

Kafka is the central message broker between the Parser Service and the downstream databases. The
Parser Service publishes events to Kafka, the Neo4j Kafka Sink Connector consumes graph topology
events, and Spark Structured Streaming consumes source metadata events.

## Topic layout

The project uses four versioned Kafka topics:

| Topic | Event category | Main consumer | Kafka key |
|---|---|---|---|
| `cpg.nodes.v1` | CPG node events | Neo4j Kafka Sink Connector | `node_id` |
| `cpg.edges.v1` | CPG edge events | Neo4j Kafka Sink Connector | `edge_id` |
| `cpg.metadata.v1` | Source metadata events | Spark Structured Streaming | `metadata_id` |
| `cpg.errors.v1` | Parser error events | Evidence / debugging path | `repo_name:file_path` |

The `.v1` suffix makes the schema version visible at the topic level. This is useful because a future
pipeline version can introduce a new topic contract without breaking existing consumers.

Node and edge topics are consumed by the Neo4j connector. Metadata events are consumed by Spark and
written to MongoDB. Error events are kept separate so parser failures remain observable without
blocking successful files.

## Topic creation evidence

Topics are created by the project script:

```bash
./scripts/create_topics.sh
```

The recorded topic creation log shows that all four required topics were created:

```text
Created topic cpg.nodes.v1.
Created topic cpg.edges.v1.
Created topic cpg.metadata.v1.
Created topic cpg.errors.v1.
```

The final topic list also confirms the expected project topics:

```text
cpg.edges.v1
cpg.errors.v1
cpg.metadata.v1
cpg.nodes.v1
```

The infrastructure check shows the same topics after the environment was running. It also shows the
Kafka Connect internal topics, which are expected for connector configuration, offsets, and status:

```text
connect-configs
connect-offsets
connect-statuses
```

## Common event fields

Every parser event includes common fields that make the event self-describing:

| Field | Purpose |
|---|---|
| `schema_version` | Declares the payload schema version |
| `event_time` | Records when the parser produced the event |
| `repo_name` | Identifies the source repository |
| `file_path` | Identifies the repository-relative source file |

Successful graph and metadata events also include a `file_hash`. This allows the pipeline to detect
whether a replayed file has changed.

The use of stable Kafka keys is deliberate. Reprocessing the same logical node, edge, or metadata
document should result in the same key. Downstream sinks can then use `MERGE`, replace, or upsert
semantics to avoid duplicates.

## Node event contract

Node events describe AST-derived graph nodes. A captured node sample contains a stable `node_id`,
the node type, the AST structural path, and source position fields:

```json
{
  "schema_version": "1.0",
  "event_time": "2026-07-05T05:33:57.750659Z",
  "repo_name": "accelerate",
  "file_path": "benchmarks/big_model_inference/big_model_inference.py",
  "file_hash": "377df29bc399ec413aab5235343a0131d47e1a7cf57a279a341efa53b9018220",
  "node_id": "38e41e11cb41c8fe7ec4ceb45a3aaa640e5639df205125aea4282b6d9e0e4a10",
  "node_type": "alias",
  "name": null,
  "structural_path": "module.body[0].names[0]",
  "lineno": 15,
  "col_offset": 7,
  "end_lineno": 15,
  "end_col_offset": 15
}
```

The structural path is important for stable identity because some Python AST nodes do not have
meaningful source positions. It lets the parser distinguish repeated positionless structures in the
same file.

## Edge event contract

Edge events describe graph relationships. The same edge topic carries AST, CFG, DFG, and CALL edges.
A captured edge sample has a stable `edge_id`, an `edge_type`, and stable source and target node IDs:

```json
{
  "schema_version": "1.0",
  "event_time": "2026-07-05T05:33:57.750659Z",
  "repo_name": "accelerate",
  "file_path": "benchmarks/big_model_inference/big_model_inference.py",
  "file_hash": "377df29bc399ec413aab5235343a0131d47e1a7cf57a279a341efa53b9018220",
  "edge_id": "e5077591dc0c165d2a86d3476884c1127a3aed08154d380e8d0de8342ce4f376",
  "edge_type": "AST",
  "source_id": "008c0dcb1c46c6c0b1b186735756ee692ae164bc14969b7d3b6d4c8fcd3469f3",
  "target_id": "40946bc411075d50a641a36f4c9b42f94bebf70c08bde71ecf39862d873f5c99"
}
```

Using one edge topic with an `edge_type` field keeps the Kafka layout compact while still preserving
the distinction between AST, CFG, DFG, and CALL relationships.

## Metadata event contract

Metadata events summarize each parsed source file. They are consumed by Spark Structured Streaming
and written to MongoDB.

A captured metadata sample contains file statistics and parse status:

```json
{
  "schema_version": "1.0",
  "event_time": "2026-07-05T05:33:57.750659Z",
  "repo_name": "accelerate",
  "file_path": "benchmarks/big_model_inference/big_model_inference.py",
  "metadata_id": "193be55d48d094dd2dabd8f933ffc82c76bef578804d6aa63503a24c22bb11d5",
  "file_hash": "377df29bc399ec413aab5235343a0131d47e1a7cf57a279a341efa53b9018220",
  "line_count": 143,
  "function_count": 2,
  "class_count": 0,
  "import_count": 7,
  "node_count": 877,
  "edge_count": 1849,
  "status": "parsed"
}
```

The `metadata_id` key is later used by MongoDB replace/upsert logic to maintain one document per
source file identity.

## Error event contract

Parser failures are routed to `cpg.errors.v1`. A captured parser error event shows a syntax failure
in a controlled invalid file:

```json
{
  "schema_version": "1.0",
  "event_time": "2026-07-05T05:34:43.384711Z",
  "repo_name": "accelerate",
  "file_path": "src/accelerate/_lab_parser_error.py",
  "error_type": "SyntaxError",
  "error_message": "'(' was never closed (_lab_parser_error.py, line 1)",
  "status": "failed"
}
```

Keeping error events in a separate topic makes parser failures auditable. It also prevents one bad
file from stopping the full repository pipeline.

## Kafka sample capture

The project captures both human-readable Kafka samples and payload-only JSON samples for evidence.
The recorded sample capture log shows all four categories being exported:

```text
Captured cpg.nodes.v1 -> evidence/kafka/nodes_sample.json and evidence/kafka/node-sample.txt
Captured cpg.edges.v1 -> evidence/kafka/edges_sample.json and evidence/kafka/edge-sample.txt
Captured cpg.metadata.v1 -> evidence/kafka/metadata_sample.json and evidence/kafka/metadata-sample.txt
Captured cpg.errors.v1 -> evidence/kafka/errors_sample.json and evidence/kafka/error-sample.txt
Kafka sample capture complete
```

The text captures preserve Kafka key/value output, while the JSON files are easier to validate and
include in the report.

## Connector routing evidence

Kafka Connect lists one connector:

```json
["neo4j-cpg-sink"]
```

The connector status confirms that the Neo4j sink connector and its task were running:

```json
{
  "name": "neo4j-cpg-sink",
  "connector": {
    "state": "RUNNING",
    "worker_id": "kafka-connect:8083"
  },
  "tasks": [
    {
      "id": 0,
      "state": "RUNNING",
      "worker_id": "kafka-connect:8083"
    }
  ],
  "type": "sink"
}
```

This verifies that graph events from `cpg.nodes.v1` and `cpg.edges.v1` can be consumed directly by
the Neo4j Kafka Sink Connector. Metadata events are intentionally not routed through this connector;
they are handled by Spark in Task 5.

## Reflection

The four-topic layout made the pipeline easier to reason about. Graph topology, source metadata, and
parser failures have different consumers and different storage semantics, so separating them at the
Kafka level avoids unnecessary filtering in downstream systems.

Stable keys are also essential for replay. They do not remove stale graph structures by themselves,
but they let downstream systems merge or replace repeated logical identities. This supports
same-content idempotence and provides the foundation for the modified-file replay verification in
Task 6.
