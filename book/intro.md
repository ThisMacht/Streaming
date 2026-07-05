# Lab 04: Incremental CPG Streaming

This Jupyter Book documents an incremental Code Property Graph (CPG) pipeline for the
[`huggingface/accelerate`](https://github.com/huggingface/accelerate) Python repository. The lab
goal is to discover source files, parse one file at a time, publish stable events, and maintain
queryable graph and metadata views while supporting controlled replacement of one modified file.

## Pipeline at a glance

1. Shallow-clone the repository and deterministically discover Python source files.
2. Parse each file independently with Python's standard-library `ast` module.
3. Publish keyed node, edge, metadata, and error events to Kafka.
4. Send node and edge topics directly through the Neo4j Kafka Sink Connector to Neo4j.
5. Consume the metadata topic with Spark Structured Streaming and write it through the MongoDB
   Spark Connector to `cpg_lab.source_metadata`.
6. Modify and replay only `src/accelerate/_lab_replay_probe.py`, then verify stable identity,
   replacement, duplicate checks, and checkpoint resume.

Spark is used only on the metadata path; it is not part of Neo4j graph ingestion.

## Result summary

| Check | Result |
|---|---|
| Kafka | Four versioned topics created |
| Neo4j connector | Connector and task `RUNNING` |
| MongoDB identity | Unique `metadata_id_1` index exists |
| Modified replay probe | Neo4j changed from 14 nodes / 27 edges to 14 nodes / 26 edges |
| Neo4j duplicate groups | 0 node IDs, 0 edge IDs |
| Checkpoint resume | Passed; metadata remained 99 before and after |
| Automated tests | 19 passed |

## Evidence and reading order

Each task chapter presents its goal, implementation approach, concrete evidence, and a short
reflection. Raw artifacts are kept beside the narrative:

- `book/logs/` contains command, connector, database, replay, checkpoint, and test output;
- `book/kafka/` contains captured Kafka key/value samples;
- `book/images/` contains Neo4j, MongoDB, Spark, and architecture figures.

The architecture chapter explains the routing first. Tasks 1–6 then follow the lab workflow, the
executed notebook collects intermediate outputs, and the conclusion summarizes results and limits.
