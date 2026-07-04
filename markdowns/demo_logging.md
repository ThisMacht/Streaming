# Demo Logging

These scripts rerun the Incremental CPG Streaming Pipeline and preserve terminal output as text
logs. Text logs are easier to search, compare, and include as reproducible evidence than a set of
screenshots.

## Why two terminals?

- Terminal 1 runs the Spark Structured Streaming metadata job continuously.
- Terminal 2 runs repository discovery, parser publishing, database verification, and replay.

Generated logs are saved under:

```text
outputs/demo_logs/
```

## Recommended command order

Start from a clean metadata ingestion state. This clears only MongoDB metadata, the Spark
metadata checkpoint, and the Kafka metadata topic. It does not delete Neo4j graph data or the
Kafka node and edge topics. Run it while the Spark demo job is stopped and after infrastructure
has been initialized.

```bash
./scripts/reset_demo_state.sh
```

In Terminal 1, start Spark and keep it running while parser events are produced:

```bash
./scripts/demo_terminal_1_spark.sh
```

In Terminal 2, run the pipeline demo:

```bash
./scripts/demo_terminal_2_run_pipeline.sh
```

Keep Spark running throughout baseline ingestion, controlled modification, replay, and post-replay
verification. Stop Spark in Terminal 1 only after Terminal 2 completes.

Stopping `spark-submit` with `Ctrl+C` can append Py4J shutdown traceback lines to the Terminal 1
log even after successful batches. Judge ingestion from the MongoDB verification, batch/checkpoint
progress, and Terminal 2 results rather than treating those shutdown-only lines as a failed batch.

## Replay and upsert behavior

Terminal 2 prepares a small `_lab_replay_probe.py`, ingests its baseline, changes its content, and
reprocesses only that file. Spark must remain active so `foreachBatch` can replace the existing
MongoDB document using stable `metadata_id`. The document count stays constant while `file_hash`,
`event_time`, `ingested_at`, and `spark_batch_id` reflect the replay.

Before publishing the modified graph, the replay deletes CPG nodes belonging to only the target
repository/file identity. This avoids stale topology caused by line-based node IDs.

## Expected final results

```text
Do not hard-code counts: the Accelerate revision and controlled probe affect totals. A successful
run must show zero duplicate identity groups, zero unresolved Neo4j placeholders, a constant
MongoDB document count across replay, and a target MongoDB `file_hash` equal to the modified
probe hash.
```

Raw logs remain in `outputs/demo_logs/`. The demo scripts copy selected latest logs into tracked
`evidence/logs/`; inspect them for secrets or local paths before committing.

The error topic is not reset by `reset_demo_state.sh`. Step 10 consumes from the beginning with
`--max-messages 1`, so an older error event may be displayed when the topic already contains data.
For evidence tied uniquely to the newly generated controlled error, clear/recreate the error topic
before the demo or consume using a fresh consumer group/known offset.
