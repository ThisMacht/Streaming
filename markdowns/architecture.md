# Architecture

This project builds an incremental Code Property Graph streaming pipeline for:

```text
https://github.com/huggingface/accelerate
```

## Main pipeline

```text
huggingface/accelerate
        |
        v
Parser Service
        |
        v
Apache Kafka
  |             |
  v             v
Neo4j Sink      Spark Structured Streaming
  |             |
  v             v
Neo4j           MongoDB
```

## Components

| Component | Purpose |
|---|---|
| Parser Service | Parses Python files one by one and emits CPG events |
| Kafka | Message broker for nodes, edges, metadata, and errors |
| Neo4j Kafka Sink | Writes graph topology directly from Kafka to Neo4j |
| Neo4j | Stores CPG nodes and edges |
| Spark Structured Streaming | Reads metadata from Kafka and writes it through the MongoDB Spark Connector |
| MongoDB | Stores source code metadata |

## Kafka topics

| Topic | Purpose |
|---|---|
| `cpg.nodes.v1` | CPG node events |
| `cpg.edges.v1` | CPG edge events |
| `cpg.metadata.v1` | Source metadata events |
| `cpg.errors.v1` | Parser error events |

## Idempotency strategy

The pipeline avoids duplicates by using stable identifiers.

### Node ID

```text
sha256(repo_name + file_path + node_type + lineno + col_offset + name)
```

### Edge ID

```text
sha256(repo_name + file_path + edge_type + source_id + target_id)
```

### Metadata ID

```text
sha256(repo_name + file_path)
```

Neo4j uses `MERGE` instead of `CREATE`.

MongoDB uses a unique index on the stable identifier:

```text
metadata_id
```

The metadata ingestion job writes its streaming DataFrame with
`writeStream.format("mongodb")`. Spark checkpointing preserves committed Kafka offsets across
job restarts. Because the connector uses append mode, a newly emitted event for an existing
stable `metadata_id` can be rejected by MongoDB's unique index; verification reports both
`metadata_id` and repository/file duplicate groups.
