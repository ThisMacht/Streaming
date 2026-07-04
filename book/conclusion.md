# Conclusion

This project implemented an incremental streaming pipeline over the Hugging Face Accelerate
repository. It cloned and discovered Python sources, parsed one file at a time, emitted four Kafka
event categories, ingested graph topology directly into Neo4j, and used Spark Structured Streaming
to upsert source metadata into MongoDB.

The final demo included 121 valid Python files after adding the controlled replay probe. Runtime
logs, Kafka output, Neo4j Browser views, Mongo Express documents, and replay screenshots provide
evidence for the major pipeline paths.

## What Worked

- deterministic discovery and per-file parsing;
- versioned node, edge, metadata, and parser-error event contracts;
- direct Kafka-to-Neo4j node and edge ingestion without Spark;
- stable-ID Cypher `MERGE`, uniqueness checks, and ordering-safe endpoint placeholders;
- Spark consumption of `cpg.metadata.v1` with checkpointing;
- stable-key MongoDB replacement through `foreachBatch` and PyMongo;
- controlled modified-file replay while Spark remained active;
- structured `SyntaxError` publication without terminating the valid-file pipeline.

The replay evidence was particularly useful: MongoDB remained at 121 documents while the target
hash, statistics, ingestion time, and batch marker changed. Neo4j replaced the probe's 10-node,
18-edge baseline with 13 nodes and 25 edges, with no duplicate IDs or unresolved placeholders.

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
- Node identity includes line and column position, so source edits may change many IDs.
- File-level cleanup is a simple modified-file replay strategy and does not retain graph history.
- The PyMongo write layer runs inside Spark `foreachBatch`; it is not the MongoDB connector's native
  streaming sink.

## Future Improvements

Future work could add scope-aware data flow, branch-aware control flow, cross-module symbol and call
resolution, and parser support for more languages. A generation-based graph model could atomically
activate a new file version while retaining history. Additional monitoring could expose Kafka lag,
Spark query progress, dead-letter volume, and unresolved placeholder counts as operational metrics.
