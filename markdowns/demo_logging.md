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

After full ingestion and the first verification, the Terminal 2 script pauses before replay. For
the cleanest logs, stop Spark in Terminal 1 with `Ctrl+C`, then continue the replay in Terminal 2.

## Replay and duplicate-key behavior

Replay emits a metadata event for a file that already exists in MongoDB. If Spark is still
running, the MongoDB Spark Connector may attempt to insert the duplicate event. MongoDB rejects
it because `metadata_id` has a unique index. This is expected and confirms duplicate metadata
documents are prevented. For a clean demo log, stop Spark before running replay.

## Expected final results

```text
Parser:
successful=120 failed=0

MongoDB:
metadata documents=120
No duplicate metadata documents found.

Neo4j:
CPG nodes=114772
CPG edges=319284

Replay:
nodes delta=+0
edges delta=+0
metadata duplicates=0
```

Screenshots are optional. The text files in `outputs/demo_logs/` are the primary demo evidence
for this project.
