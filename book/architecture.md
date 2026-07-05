# Pipeline Architecture

## Goal

The architecture is designed to satisfy the Lab 04 requirement for an incremental Code Property
Graph streaming pipeline. The system parses Python source files one at a time, publishes structured
events to Kafka, and stores two different views of the result:

- graph topology in Neo4j;
- source metadata in MongoDB.

The key design decision is to separate graph ingestion from metadata ingestion. Node and edge events
go directly from Kafka to Neo4j through the Neo4j Kafka Sink Connector. Metadata events go from
Kafka to Spark Structured Streaming and then to MongoDB through the MongoDB Spark Connector.

```{figure} images/architecture.svg
:name: architecture-diagram
:width: 95%

Overall incremental CPG pipeline architecture. The graph path sends node and edge topics directly
through the Neo4j Kafka Sink Connector, the metadata path uses Spark Structured Streaming and the
MongoDB Spark Connector, and the error path preserves parser failures in a dedicated Kafka topic.
```

## High-level pipeline

The full pipeline can be summarized as:

```text
Python repository
    -> file discovery manifest
    -> one-file-at-a-time Parser Service
    -> Kafka topics
    -> Neo4j / MongoDB / error evidence
```

The selected repository is `huggingface/accelerate`. The discovery stage produced 99 Python files.
The parser then processed those files incrementally and completed the baseline run with:

```text
Finished: successful=99 failed=0
```

Each source file is parsed independently. This keeps the parser bounded by the current file and makes
single-file replay possible.

## Runtime services

The local runtime is containerized with Docker. The recorded infrastructure check shows the following
services running:

```text
cpg-kafka-connect
cpg-mongo-express
cpg-kafka
cpg-mongodb
cpg-zookeeper
cpg-neo4j
```

The same check confirms that Kafka exposes the required project topics:

```text
cpg.nodes.v1
cpg.edges.v1
cpg.metadata.v1
cpg.errors.v1
```

Kafka Connect also reports the Neo4j connector as active:

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

This confirms that the graph ingestion service was available while parser events were being
published.

## Parser Service

The Parser Service is the producer side of the architecture. It reads one Python file at a time from
the discovery manifest and uses Python's standard `ast` module to extract CPG-like structures.

For each successfully parsed file, it emits:

- node events;
- edge events;
- one source metadata event.

For a parser failure, it emits a structured error event.

Every event includes common fields such as:

| Field | Purpose |
|---|---|
| `schema_version` | Payload contract version |
| `event_time` | Time when the parser produced the event |
| `repo_name` | Source repository |
| `file_path` | Repository-relative file path |

Stable identities are used throughout the pipeline:

| Entity | Stable identifier |
|---|---|
| Node | `node_id` |
| Edge | `edge_id` |
| Metadata document | `metadata_id` |
| Parser error | `repo_name:file_path` key |

This identity design is what allows later replay checks to detect and prevent duplicate logical
records.

## Kafka topic routing

Kafka acts as the central message bus. The parser writes each event category to a separate topic:

| Topic | Payload | Downstream path |
|---|---|---|
| `cpg.nodes.v1` | CPG node events | Neo4j Kafka Sink Connector |
| `cpg.edges.v1` | CPG edge events | Neo4j Kafka Sink Connector |
| `cpg.metadata.v1` | Source metadata events | Spark Structured Streaming |
| `cpg.errors.v1` | Parser error events | Evidence and debugging |

The topic creation log confirms that all four topics were created. Captured Kafka samples also exist
for node, edge, metadata, and error events.

This separation keeps each downstream consumer focused. Neo4j does not need to consume metadata
events, and Spark does not need to consume graph topology events.

## Graph path: Kafka to Neo4j

The graph path is:

```text
Parser Service
    -> cpg.nodes.v1 / cpg.edges.v1
    -> Neo4j Kafka Sink Connector
    -> Neo4j
```

This path stores the graph topology. It handles repositories, source files, CPG nodes, and CPG
relationships such as AST, CFG, DFG, and CALL edges.

The connector uses stable graph identifiers and `MERGE`-style Cypher logic. This allows repeated
node or edge events to converge on existing graph entities rather than producing duplicates.

The recorded baseline Neo4j verification reported:

```text
Neo4j totals: nodes=263154 edges=626918
Duplicate node IDs: 0
Duplicate edge IDs: 0
Unresolved placeholder nodes: 0
```

The zero duplicate counts show that stable identifiers worked for the baseline ingestion. The zero
unresolved placeholder count shows that endpoint placeholders created for edge-ordering tolerance
were resolved after node events arrived.

Spark is not part of this graph path.

## Metadata path: Kafka to Spark to MongoDB

The metadata path is:

```text
Parser Service
    -> cpg.metadata.v1
    -> Spark Structured Streaming
    -> MongoDB Spark Connector
    -> cpg_lab.source_metadata
```

This path stores one document per source file identity. Spark reads Kafka metadata events, applies an
explicit schema, and writes micro-batches to MongoDB.

The Spark log confirms that the application `CPGMetadataToMongoDB` started and loaded the required
packages:

```text
org.apache.spark#spark-sql-kafka-0-10_2.12;3.5.1
org.mongodb.spark#mongo-spark-connector_2.12;10.3.0
```

The MongoDB collection uses a unique index on `metadata_id`:

```json
{
  "key": {
    "metadata_id": 1
  },
  "name": "metadata_id_1",
  "unique": true
}
```

After baseline ingestion, MongoDB reported:

```text
MongoDB metadata documents: 99
Duplicate metadata_id groups: 0
Duplicate repo/file groups: 0
```

This matches the 99 discovered Python files and verifies that metadata ingestion did not create
duplicate logical documents.

## Error path

Parser errors are routed separately:

```text
Parser failure
    -> cpg.errors.v1
    -> captured error evidence
```

A captured error event records a controlled syntax failure:

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

This error path makes failures visible without stopping the processing of other valid files.

## Replay path

The replay path is scoped to one modified file:

```text
src/accelerate/_lab_replay_probe.py
```

The replay verification modifies this file, republishes only that file, and checks both databases.

The replay log confirms that the file content changed:

```text
file_hash_changed=True
```

MongoDB remained stable at 99 documents:

```text
mongodb_document_count_before=99
mongodb_document_count_after=99
mongodb_document_count_delta=+0
```

Neo4j reflected the updated target-file graph:

```text
neo4j_target_nodes_before=14
neo4j_target_nodes_after=14
neo4j_target_edges_before=27
neo4j_target_edges_after=26
duplicate_node_id_groups=0
duplicate_edge_id_groups=0
```

The decrease from 27 to 26 edges is expected because the controlled source edit changed the
generated CPG structure.

For Neo4j, the replay protocol performs direct file-scoped cleanup before republishing the modified
file's replacement graph. This cleanup is only a verification protocol for changed-file replacement.
The replacement graph events still travel through Kafka and the Neo4j connector.

## Checkpoint behavior

Spark owns metadata offsets through this checkpoint location:

```text
outputs/checkpoints/mongodb_metadata
```

The checkpoint verification reports:

```text
checkpoint_exists=True
checkpoint_artifacts_before=21
checkpoint_artifacts_after=21
metadata_count_before=99
metadata_count_after=99
result=PASSED checkpoint resumed without duplicating unchanged metadata
```

This confirms that the metadata stream can resume from its checkpoint without duplicating unchanged
metadata documents.

## Evidence map

The architecture is supported by the following evidence files:

| Evidence | Purpose |
|---|---|
| `logs/check_infra.log` | Running containers, topics, connector status |
| `logs/create_topics.log` | Topic creation evidence |
| `logs/kafka_connect_status.json` | Neo4j connector and task state |
| `logs/terminal_1_spark_latest.log` | Spark metadata ingestion runtime |
| `logs/terminal_2_pipeline_latest.log` | End-to-end parser and verification run |
| `logs/mongodb_indexes.log` | MongoDB indexes and unique metadata identity |
| `logs/identity_replay_verification.log` | Modified-file replay verification |
| `logs/checkpoint_resume.log` | Spark checkpoint resume verification |
| `kafka/*.json` and `kafka/*.txt` | Captured node, edge, metadata, and error events |
| `images/*.png` and `images/architecture.svg` | UI and architecture evidence |

## Reflection

The split architecture makes the system easier to verify. Neo4j receives only graph topology through
the Kafka Sink Connector, while Spark Structured Streaming is responsible only for checkpointed
metadata ingestion into MongoDB. Kafka topics separate node, edge, metadata, and error events, so each
consumer has a clear responsibility.

The design also supports replay. Stable IDs prevent duplicate logical identities, MongoDB upsert
keeps one metadata document per file, and Spark checkpointing prevents unchanged metadata from being
written again on resume.

The main limitation is graph deletion for modified files. Stable IDs and `MERGE` prevent duplicates,
but they do not automatically remove graph structures that disappear after source edits. The lab
therefore uses file-scoped cleanup during replay verification. A production version could use
generation IDs or tombstone events to make this lifecycle fully event-driven.
