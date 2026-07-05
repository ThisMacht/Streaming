# Task 5: MongoDB Metadata Ingestion

## Goal

Consume source metadata with Spark Structured Streaming and maintain exactly one MongoDB document
per stable `metadata_id`.

## Implementation approach

```text
cpg.metadata.v1 -> Spark Structured Streaming -> MongoDB Spark Connector
                -> cpg_lab.source_metadata
```

`src/spark_jobs/metadata_to_mongodb.py` reads Kafka with an explicit schema and uses `foreachBatch`
with the MongoDB Spark Connector. Connector options select `replace` with upsert enabled and
`idFieldList=metadata_id`. The destination has a unique `metadata_id_1` index.

The streaming checkpoint is:

```text
outputs/checkpoints/mongodb_metadata
```

Spark owns Kafka offsets and checkpoint progress. PyMongo is used only by verification scripts to
read results; it is not the ingestion writer.

## Evidence and result

[`logs/mongodb_indexes.log`](logs/mongodb_indexes.log) records `metadata_id_1` with
`"unique": true`. The replayed document has `status=parsed`, `node_count=14`, and
`edge_count=26`. Checkpoint resume leaves the collection at 99 documents before and after.

```{figure} images/mongo-source-metadata-list.png
:name: mongo-source-metadata-list
:width: 90%

MongoDB `source_metadata` collection.
```

```{figure} images/mongo-metadata-document.png
:name: mongo-metadata-document
:width: 90%

A source metadata document with identity, file statistics, and parse state.
```

```{figure} images/spark-structured-streaming-ui.png
:name: spark-structured-streaming-ui
:width: 90%

Spark Structured Streaming query used for the metadata path.
```

## Reflection

`foreachBatch` plus connector replace/upsert worked because a repeated stable identity updates the
document rather than merely rejecting an insert. The unique index protects the invariant, and the
checkpoint protects committed Kafka progress across restart. Keeping verification reads separate
from ingestion makes it clear that the required write path remains Spark-to-MongoDB Connector.
