# Task 5: MongoDB Source Metadata Ingestion

## Goal

The fifth task ingests source-code metadata into MongoDB using Apache Spark Structured Streaming.
Unlike graph topology, which is written directly from Kafka to Neo4j, metadata must follow the
required Spark path:

```text
Kafka -> Spark Structured Streaming -> MongoDB Spark Connector -> MongoDB
```

The goal is to maintain one metadata document per source file identity. When the same file is
processed again, the existing metadata document should be updated rather than duplicated.

## Ingestion architecture

The metadata ingestion path is:

```text
Parser Service
    -> cpg.metadata.v1
    -> Spark Structured Streaming
    -> MongoDB Spark Connector
    -> cpg_lab.source_metadata
```

The Parser Service publishes one metadata event for each successfully parsed Python file. Spark
Structured Streaming reads the `cpg.metadata.v1` Kafka topic and writes each micro-batch to MongoDB
through the MongoDB Spark Connector.

Neo4j graph events are not handled by this Spark job. Spark is used only for source metadata.

## Metadata event contract

A metadata event summarizes one parsed Python source file. The captured sample below shows the main
fields:

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

The most important field for idempotence is `metadata_id`. It is stable for the same repository and
file identity, so MongoDB can replace the existing document during replay.

## Spark Structured Streaming job

The Spark job is started in Terminal 1 before Terminal 2 publishes parser events:

```bash
./scripts/demo_terminal_1_spark.sh
```

The recorded Spark log shows that the metadata ingestion job started and instructed the user to keep
it running while Terminal 2 publishes events:

```text
Starting Spark Structured Streaming metadata ingestion.
Keep this terminal running while Terminal 2 publishes parser events.
Keep Spark running through modified-file replay so MongoDB upsert can be verified.
```

The Spark application name is:

```text
CPGMetadataToMongoDB
```

The log also confirms that the required Spark packages were loaded:

```text
org.apache.spark#spark-sql-kafka-0-10_2.12;3.5.1
org.mongodb.spark#mongo-spark-connector_2.12;10.3.0
```

This confirms that the streaming job has the Kafka source dependency and the MongoDB Spark Connector
dependency needed for the required ingestion path.

## MongoDB write strategy

The MongoDB collection is:

```text
cpg_lab.source_metadata
```

The write strategy uses replace/upsert behavior keyed by `metadata_id`. This ensures that repeated
events for the same source file update the existing document instead of inserting another document.

The MongoDB index evidence shows a unique index on `metadata_id`:

```json
{
  "key": {
    "metadata_id": 1
  },
  "name": "metadata_id_1",
  "unique": true
}
```

The collection also has indexes on `file_hash`, `event_time`, and the repository/file pair:

```json
{
  "key": {
    "file_hash": 1
  },
  "name": "file_hash_1"
}
```

```json
{
  "key": {
    "event_time": -1
  },
  "name": "event_time_-1"
}
```

```json
{
  "key": {
    "repo_name": 1,
    "file_path": 1
  },
  "name": "repo_name_1_file_path_1"
}
```

The unique `metadata_id_1` index protects the main invariant: one logical metadata identity should
not appear multiple times.

## Baseline ingestion result

After the Parser Service published the baseline events, the verification step reported:

```text
MongoDB metadata documents: 99
Duplicate metadata_id groups: 0
Duplicate repo/file groups: 0
```

This result matches the 99 Python files discovered in Task 1 and parsed in Task 2. It also confirms
that the metadata ingestion path did not create duplicate metadata identities or duplicate
repository/file records.

The baseline target metadata for the replay probe was recorded as:

```text
file_path: src/accelerate/_lab_replay_probe.py
node_count: 10
edge_count: 18
```

This was the metadata state before the modified-file replay verification.

## Replay result

Task 6 modifies and republishes only this file:

```text
src/accelerate/_lab_replay_probe.py
```

The replay verification log confirms that the file hash changed:

```text
file_hash_changed=True
mongodb_file_hash_matches_replay=True
```

MongoDB document count remained stable:

```text
mongodb_document_count_before=99
mongodb_document_count_after=99
mongodb_document_count_delta=+0
```

The stable document count is important. It shows that replay updated the existing MongoDB metadata
document instead of inserting a duplicate.

The final replayed document records the updated target-file state:

```text
node_count=14
edge_count=26
status=parsed
```

## Checkpoint verification

The Spark checkpoint location is:

```text
outputs/checkpoints/mongodb_metadata
```

Checkpointing allows Spark Structured Streaming to resume from committed Kafka offsets after a
restart. The checkpoint verification log reports:

```text
checkpoint_location=outputs/checkpoints/mongodb_metadata
checkpoint_exists=True
checkpoint_artifacts_before=21
checkpoint_artifacts_after=21
metadata_count_before=99
metadata_count_after=99
result=PASSED checkpoint resumed without duplicating unchanged metadata
```

This demonstrates that restarting or resuming the metadata stream with the same checkpoint does not
reprocess unchanged offsets into duplicate MongoDB documents.

## Screenshots and figures

The following figures connect the recorded database state to the running Structured Streaming job.

```{figure} images/mongo-source-metadata-list.png
:name: task5-mongodb-metadata-collection
:width: 90%

MongoDB `source_metadata` collection populated from Kafka metadata events through Spark Structured
Streaming and the MongoDB Spark Connector.
```

```{figure} images/mongo-metadata-document.png
:name: task5-mongodb-metadata-document
:width: 90%

One source metadata document showing the stable identity, file hash, source counters, schema
version, and event time persisted for a Python file.
```

```{figure} images/spark-structured-streaming-ui.png
:name: task5-spark-streaming-query
:width: 90%

Spark Structured Streaming query consuming `cpg.metadata.v1`; its checkpoint preserves committed
Kafka progress across restarts.
```

```{figure} images/spark-jobs-ui.png
:name: task5-spark-jobs-ui
:width: 90%

Spark Jobs UI for the metadata ingestion application, providing runtime evidence of executed
micro-batch work.
```

## Reflection

The metadata path worked well because Spark and MongoDB enforce complementary guarantees. Spark owns
Kafka offset progress through the checkpoint directory, while MongoDB uses stable `metadata_id`
values and a unique index to keep one document per source file identity.

The replay result is the clearest evidence of idempotent metadata ingestion: the replayed file hash
changed, but the collection stayed at 99 documents. This means the modified file updated its
existing document rather than creating a second record.

A useful production improvement would be to add more operational monitoring around failed Spark
batches and connector write errors. For the lab, the recorded Spark logs, MongoDB index evidence,
baseline counts, replay result, and checkpoint verification are sufficient to demonstrate the
required metadata ingestion behavior.
