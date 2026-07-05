# Pipeline Architecture

## Goal

The architecture separates graph topology from source metadata so that each stream has one clear
owner and replay behavior can be verified independently.

```{figure} images/architecture.svg
:name: architecture-diagram
:width: 95%

Incremental CPG pipeline and its independent graph, metadata, and error paths.
```

## Implementation approach

The parser reads one discovered Python file at a time and publishes events with stable identities,
`schema_version`, `event_time`, `repo_name`, and `file_path`.

**Graph path**

```text
Parser -> cpg.nodes.v1 / cpg.edges.v1 -> Neo4j Kafka Sink Connector -> Neo4j
```

The connector applies `MERGE`-based Cypher for nodes, relationships, repositories, and source
files. **Spark is not used for Neo4j graph ingestion.**

**Metadata path**

```text
Parser -> cpg.metadata.v1 -> Spark Structured Streaming -> MongoDB Spark Connector -> MongoDB
```

Spark owns Kafka offsets and the checkpoint at `outputs/checkpoints/mongodb_metadata`. Connector
replace/upsert maintains one `cpg_lab.source_metadata` document per stable `metadata_id`.

**Error path**

```text
Parser failure -> cpg.errors.v1
```

A structured error event isolates a malformed file without terminating processing of other files.

**Replay path**

Only the modified replay probe is parsed and republished. Stable IDs, MongoDB upsert, its unique
index, and Neo4j `MERGE` prevent duplicate identities. Because a source edit can remove or change
structural node IDs, the verification protocol may first delete topology scoped to that one
`repo_name` and `file_path`. This direct Neo4j maintenance step is only a file-scoped replay
verification protocol; it is not part of the normal ingestion architecture.

In the recorded run, the target changed from 14 nodes / 27 edges to 14 nodes / 26 edges. The
controlled edit changed the CPG structure, so the one-edge decrease is expected. Both Neo4j
duplicate-identity checks returned zero, while MongoDB replaced the existing stable metadata
document instead of inserting another document.

## Evidence and result

The connector and its task are `RUNNING` in
[`logs/kafka_connect_status.json`](logs/kafka_connect_status.json). The replay result is 14 nodes
and 26 edges for the modified probe, with zero duplicate node-ID and edge-ID groups. Metadata
remains at 99 documents after an idle checkpoint resume.

## Reflection

The split routing worked well: direct connector ingestion keeps graph traffic out of Spark, while
Structured Streaming supplies checkpointed metadata processing. The error topic makes failures
observable. Modified-file graph replacement still needs explicit lifecycle semantics; the lab uses
a narrowly scoped cleanup protocol rather than claiming that `MERGE` can delete stale topology.
