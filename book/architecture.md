# Architecture Diagram

This chapter describes the architecture of the incremental Code Property Graph streaming pipeline.

The selected repository is:

```text
https://github.com/huggingface/accelerate
```

The system parses Python source files one by one, publishes structured events to Apache Kafka, and writes the results into Neo4j and MongoDB.

## Overall Architecture

```{figure} images/architecture.svg
:name: architecture-diagram
:width: 95%

Architecture of the incremental Code Property Graph streaming pipeline.
```

## Main Pipeline

The main pipeline contains four stages:

```text
huggingface/accelerate Repository
        |
        v
Parser Service
        |
        v
Apache Kafka
   |                     |
   v                     v
Neo4j Kafka Sink         Spark Structured Streaming
   |                     |
   v                     v
Neo4j Graph Database     MongoDB source_metadata
```

The Parser Service is responsible for reading Python files incrementally. Instead of parsing the whole repository in one batch, it processes one source file at a time and emits events to Kafka.

## Components

| Component | Purpose |
|---|---|
| Parser Service | Parses Python files one by one and emits CPG events |
| Apache Kafka | Carries node, edge, metadata, and parser error events |
| Neo4j Kafka Sink Connector | Consumes graph events from Kafka and writes them directly to Neo4j |
| Neo4j | Stores CPG graph topology |
| Spark Structured Streaming | Consumes metadata events from Kafka |
| MongoDB | Stores source metadata documents |
| Mongo Express | Provides a database UI for inspecting metadata documents |

## Kafka Topic Layout

The parser emits four categories of events.

| Topic | Event type | Purpose |
|---|---|---|
| `cpg.nodes.v1` | Node events | Carries CPG node information |
| `cpg.edges.v1` | Edge events | Carries AST, CFG, DFG, and call relationships |
| `cpg.metadata.v1` | Metadata events | Carries source file statistics and parsing metadata |
| `cpg.errors.v1` | Error events | Carries parser errors for failed files |

Each event includes a `schema_version` field and an `event_time` timestamp. This makes the event format easier to evolve and helps with debugging the streaming pipeline.

## Neo4j Ingestion Path

Node and edge topics are consumed by the Neo4j Kafka Sink Connector:

```text
cpg.nodes.v1
cpg.edges.v1
      |
      v
Neo4j Kafka Sink Connector
      |
      v
Neo4j
```

This path does not use Spark. Graph topology is written directly from Kafka into Neo4j.

The connector uses merge-based Cypher logic to avoid creating duplicate graph elements when the same file is replayed.

## MongoDB Ingestion Path

Metadata events are consumed by Spark Structured Streaming:

```text
cpg.metadata.v1
      |
      v
Spark Structured Streaming
      |
      v
MongoDB source_metadata
```

Spark keeps a checkpoint directory so that it can resume from the last committed Kafka offsets after restart.

MongoDB metadata documents use a stable `metadata_id`. During replay, the metadata document is replaced through stable-key upsert instead of being inserted as a duplicate document.

## Parser Error Path

Parser errors are sent to a separate Kafka topic:

```text
cpg.errors.v1
      |
      v
Parser Error Logs / Debugging
```

This prevents one malformed Python file from stopping the whole pipeline. The parser catches the exception, creates a structured error event, and continues processing other files.

## Idempotency Strategy

The pipeline avoids duplicates by using deterministic identifiers.

### Node identity

Node identifiers are based on stable source information such as repository name, file path, node type, line number, column offset, and node name.

### Edge identity

Edge identifiers are based on repository name, file path, edge type, source node ID, and target node ID.

### Metadata identity

Metadata documents use:

```text
sha256(repo_name + file_path)
```

This allows MongoDB to update the metadata document for a replayed file instead of inserting another document.

## Replay Strategy

For modified-file replay, the system processes only the target Python file again.

The replay verification checks that:

| Target | Expected behavior |
|---|---|
| Parser Service | Reprocesses one modified file |
| Kafka | Receives replacement node, edge, and metadata events |
| Neo4j | Reflects the updated file graph without duplicate topology |
| MongoDB | Updates the same metadata document using stable `metadata_id` |
| Spark checkpoint | Advances after consuming the replay metadata event |

For Neo4j, the implementation applies file-level graph replacement during modified-file replay to avoid stale nodes and relationships from the previous version of the file.

More precisely, the replay verifier performs a controlled direct Neo4j cleanup scoped to the
target `repo_name` and `file_path` before republishing updated CPG events. This cleanup is part of
the replay verification protocol because source edits can change structural node IDs and otherwise
leave stale topology. Metadata replay remains event-driven through Kafka, Spark Structured
Streaming, and MongoDB, with Kafka offsets owned by the Spark checkpoint.

## Reflection

The architecture separates graph ingestion and metadata ingestion clearly. Neo4j receives graph topology directly from Kafka through the Neo4j Kafka Sink Connector, while MongoDB metadata is handled by Spark Structured Streaming.

This separation matches the lab requirement and also makes verification easier: Neo4j can be checked with Cypher queries, while MongoDB can be checked through Mongo Express and metadata document inspection.
