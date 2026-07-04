# Task 5: Source Metadata Ingestion into MongoDB

## Objective

This task uses Spark Structured Streaming to consume source metadata from Kafka and maintain one
MongoDB document per repository file.

## Implementation

The path is separate from Neo4j topology ingestion:

```text
cpg.metadata.v1 -> Spark Structured Streaming -> MongoDB Spark Connector -> MongoDB
```

The destination is the `cpg_lab.source_metadata` collection. Its indexes include a unique
`metadata_id` index plus indexes for repository/path, file hash, and descending event time.

## Spark Structured Streaming Job

`src/spark_jobs/metadata_to_mongodb.py` uses `readStream.format("kafka")`, subscribes to
`cpg.metadata.v1`, casts the Kafka value to text, and parses it with an explicit Spark schema.
Malformed or unidentified rows are filtered before the write stage.

The metadata document contains:

| Identity and time | Source statistics | Ingestion state |
|---|---|---|
| `metadata_id`, `repo_name`, `file_path`, `file_hash`, `event_time` | `line_count`, `function_count`, `class_count`, `import_count`, `node_count`, `edge_count` | `status`, `ingested_at`, `spark_batch_id` |

## MongoDB Upsert Strategy

The streaming query uses `foreachBatch(upsert_metadata_batch)`, and each micro-batch is written by
`DataFrameWriter.format("mongodb")`. Connector options use `operationType=replace`,
`upsertDocument=true`, and `idFieldList=metadata_id`. If an older event lacks `metadata_id`, Spark
derives the same deterministic SHA-256 identity from repository name and file path before writing.

No rows are collected into the Python driver. `ingested_at` and `spark_batch_id` are added as Spark
columns, which makes replay processing visible in the stored document.

Spark remains the Kafka streaming consumer and checkpoint owner; MongoDB Spark Connector is the
only ingestion write layer. PyMongo is retained only in read-only verification commands.

## Checkpointing

The streaming query configures:

```text
outputs/checkpoints/mongodb_metadata
```

through `checkpointLocation`. The checkpoint stores committed Structured Streaming progress so a
restarted job can resume its Kafka offsets instead of treating every retained message as new. The
demo document moved from `spark_batch_id: 1` to `spark_batch_id: 7` during replay, confirming that
the running query processed a later micro-batch.

## Verification

The database can be checked with:

```javascript
db.source_metadata.countDocuments({})
db.source_metadata.findOne({file_path: "src/accelerate/accelerator.py"})
db.source_metadata.getIndexes()
```

The project verifier additionally groups by `metadata_id` and by `(repo_name, file_path)`. The
tracked full-run output reports 121 documents and zero duplicate groups for both identities.

## Evidence

```{figure} images/mongo-source-metadata-list.png
:name: mongo-source-metadata-list
:width: 90%

Mongo Express view of the `source_metadata` collection from an earlier 120-file ingestion run.
```

The collection screenshot predates the added replay probe. The later tracked terminal evidence
records 121 documents after the probe was included.

```{figure} images/mongo-metadata-document.png
:name: mongo-metadata-document
:width: 90%

A MongoDB metadata document showing source identity, statistics, parse status, and ingestion time.
```

## Reflection

A unique index alone would only reject a repeated insert; it would not update metadata. Connector
replace/upsert keyed by `metadata_id` makes the intended behavior testable: identity remains stable,
document count remains unchanged, and content-dependent fields can change. Spark must remain
running long enough to consume the replay event before the after-state is queried.
