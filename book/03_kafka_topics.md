# Task 3: Kafka Topics and Event Contracts

## Goal

Route graph, metadata, and failures independently while choosing keys that make repeated logical
events converge at downstream sinks.

## Implementation approach

| Topic | Payload | Kafka key |
|---|---|---|
| `cpg.nodes.v1` | Graph node event | `node_id` |
| `cpg.edges.v1` | Graph edge event | `edge_id` |
| `cpg.metadata.v1` | Source metadata | `metadata_id` |
| `cpg.errors.v1` | Parser error | `repo_name:file_path` |

Every event includes `schema_version`, `event_time`, `repo_name`, and `file_path`. Topic versioning
makes contract evolution explicit; separate topics let the Neo4j connector subscribe only to
topology while Spark subscribes only to metadata.

Topics are created and inspected by the project scripts. Their recorded output is in
[`logs/create_topics.log`](logs/create_topics.log) and
[`logs/kafka_sample_capture.log`](logs/kafka_sample_capture.log).

## Evidence and result

Short raw key/value captures are tracked instead of pasting large JSON documents into the report:

- [node sample](kafka/node-sample.txt)
- [edge sample](kafka/edge-sample.txt)
- [metadata sample](kafka/metadata-sample.txt)
- [error sample](kafka/error-sample.txt)

Machine-valid payload-only JSON is also available as [node JSON](kafka/nodes_sample.json),
[edge JSON](kafka/edges_sample.json), [metadata JSON](kafka/metadata_sample.json), and
[error JSON](kafka/errors_sample.json). Kafka keys are intentionally kept only in the companion
text captures.

Connector discovery and status are recorded in
[`logs/kafka_connectors_list.json`](logs/kafka_connectors_list.json) and
[`logs/kafka_connect_status.json`](logs/kafka_connect_status.json). The latter shows the Neo4j
connector and task both in `RUNNING` state.

## Reflection

Separate topics make sink routing and troubleshooting simpler. Stable entity keys support
idempotent upsert/`MERGE` behavior, although keys alone cannot remove graph nodes that disappear
after an edit; that lifecycle concern is handled explicitly in the Task 6 verification protocol.
