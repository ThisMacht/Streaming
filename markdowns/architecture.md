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
| Spark Structured Streaming | Reads metadata from Kafka and upserts each micro-batch with PyMongo |
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

The metadata ingestion job uses Structured Streaming `foreachBatch`, then bounded PyMongo
`ReplaceOne(..., upsert=True)` operations keyed by `metadata_id` (falling back to repository and
file path). Spark checkpointing preserves committed Kafka offsets across job restarts.

Modified-file replay performs a file-scoped Neo4j cleanup before publishing replacement topology.
The edge sink uses placeholder endpoint nodes, so cross-topic arrival order does not silently lose
relationships; later node events fill their full properties.
