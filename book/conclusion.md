# Conclusion

This project implemented an incremental streaming pipeline over the Hugging Face Accelerate
repository. It cloned and discovered Python sources, parsed one file at a time, emitted four Kafka
event categories, ingested graph topology directly into Neo4j, and used Spark Structured Streaming
to upsert source metadata into MongoDB.

The current path-aware discovery manifest includes 99 Python files, including the controlled
replay probe. Some tracked runtime logs and screenshots predate this exclusion update and must be
regenerated before final submission.

## What Worked

- deterministic discovery and per-file parsing;
- versioned node, edge, metadata, and parser-error event contracts;
- direct Kafka-to-Neo4j node and edge ingestion without Spark;
- stable-ID Cypher `MERGE`, uniqueness checks, and ordering-safe endpoint placeholders;
- Spark consumption of `cpg.metadata.v1` with checkpointing;
- stable-key MongoDB replacement through `foreachBatch` and MongoDB Spark Connector;
- controlled modified-file replay while Spark remained active;
- structured `SyntaxError` publication without terminating the valid-file pipeline.

The replay evidence was particularly useful: MongoDB document count remained constant while the target
hash, statistics, ingestion time, and batch marker changed. With structural-path identity, Neo4j
replaced the probe's 10-node/18-edge baseline with 14 nodes/26 edges, with no duplicate IDs or
unresolved placeholders.

## Issues Encountered

An append-only MongoDB write could prevent duplicates with a unique index but could not update the
existing document. The write path was changed to explicit stable-key upsert. The original replay
also stopped Spark too early and republished unchanged content; the revised procedure keeps Spark
running and performs a deterministic source edit.

Neo4j `MERGE` cannot remove nodes that disappear or receive new positional IDs after modification.
This was addressed with cleanup scoped to the target file. Cross-topic node/edge ordering was
addressed with placeholder endpoints and a verifier that detects unresolved placeholders.

## Limitations

- Parsing relies on Python's standard `ast` module and covers Python source only.
- CFG, DFG, and CALL relationships are lightweight approximations.
- CALL resolution is primarily intra-file and does not fully resolve imports, dynamic dispatch, or
  cross-file targets.
- Node identity includes a stable structural path and source span. Identical content is stable and
  positionless nodes remain unique, although structural edits may still change affected paths.
- File-level cleanup is a simple modified-file replay strategy and does not retain graph history.
- MongoDB writes use connector batch replace/upsert inside `foreachBatch`; verification scripts
  still use PyMongo for read-only checks.

## Future Improvements

Future work could add scope-aware data flow, branch-aware control flow, cross-module symbol and call
resolution, and parser support for more languages. A generation-based graph model could atomically
activate a new file version while retaining history. Additional monitoring could expose Kafka lag,
Spark query progress, dead-letter volume, and unresolved placeholder counts as operational metrics.

## Evidence Index

| Requirement | Evidence file/log/screenshot |
|---|---|
| Four Kafka event contracts and keys | `evidence/kafka/*_sample.json` and `evidence/kafka/*-sample.txt` |
| Topic and connector status | `evidence/logs/create_topics.log`, `kafka_connectors_list.json`, `kafka_connect_status.json` |
| MongoDB indexes | `evidence/logs/mongodb_indexes.log` |
| Parser identity and schema tests | `evidence/logs/pytest.log` |
| Spark checkpoint idle resume | `evidence/logs/checkpoint_resume.log` |
| MongoDB metadata and replay | `book/images/mongo-metadata-document.png`, `mongo-replay-after.png`, `evidence/logs/terminal_2_pipeline_latest.log` |
| Neo4j counts, graph, and duplicate checks | `book/images/neo4j-counts.png`, `neo4j-graph-view.png`, `evidence/logs/terminal_2_pipeline_latest.log` |
| Controlled modified-file replay | `evidence/logs/identity_replay_verification.log`, `terminal_2_pipeline_latest.log`, and Task 6 before/after table |
| Architecture routing | `book/images/architecture.svg` |
