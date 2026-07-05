# Conclusion

This project implements the main requirements of Lab 04: Spark Streaming. It builds an incremental
Code Property Graph (CPG) pipeline for the `huggingface/accelerate` Python repository, publishes
parser events through Apache Kafka, writes graph topology into Neo4j, writes source metadata into
MongoDB through Spark Structured Streaming, and verifies replay behavior for one modified source
file.

The final Jupyter Book is organized as a task-by-task narrative. Each chapter explains the chosen
approach, records concrete evidence from the pipeline, and reflects on what worked, what failed, and
what could be improved.

## Final result

The implemented pipeline follows this architecture:

```text
Python files
    -> one-file-at-a-time Parser Service
    -> Kafka topics
    -> Neo4j for graph topology
    -> Spark Structured Streaming -> MongoDB for source metadata
```

The graph and metadata paths are intentionally separated:

```text
cpg.nodes.v1 / cpg.edges.v1
    -> Neo4j Kafka Sink Connector
    -> Neo4j
```

```text
cpg.metadata.v1
    -> Spark Structured Streaming
    -> MongoDB Spark Connector
    -> MongoDB
```

Parser failures are routed to a separate error topic:

```text
cpg.errors.v1
```

This design satisfies the lab requirement that graph topology is ingested into Neo4j directly from
Kafka without an intermediate Spark layer, while source metadata is ingested through Spark Structured
Streaming.

## Evidence summary

The recorded run provides evidence for each major requirement.

| Requirement | Evidence |
|---|---|
| Repository discovery | 99 Python files discovered and saved to the manifest |
| Incremental parser | Parser processed one file at a time and completed with `successful=99 failed=0` |
| Kafka topic layout | Four topics created: `cpg.nodes.v1`, `cpg.edges.v1`, `cpg.metadata.v1`, `cpg.errors.v1` |
| Kafka samples | Node, edge, metadata, and error samples captured as JSON/text evidence |
| Neo4j ingestion | `neo4j-cpg-sink` connector and task reported `RUNNING` |
| MongoDB ingestion | Spark job `CPGMetadataToMongoDB` loaded Kafka and MongoDB Spark connector packages |
| MongoDB identity | Unique `metadata_id_1` index exists |
| Baseline MongoDB result | 99 metadata documents, zero duplicate metadata IDs, zero duplicate repo/file groups |
| Baseline Neo4j result | 263,154 nodes and 626,918 relationships, zero duplicate node IDs and edge IDs |
| Replay target | `src/accelerate/_lab_replay_probe.py` |
| Modified replay | File hash changed and replay was scoped to one file |
| MongoDB replay result | Document count remained 99 before and after replay |
| Neo4j replay result | Target graph changed from 14 nodes / 27 edges to 14 nodes / 26 edges |
| Checkpoint resume | Passed; metadata count remained 99 before and after resume |
| Automated tests | 19 tests passed |

The supporting evidence is stored under:

```text
book/logs/
book/kafka/
book/images/
book/notebooks/
```

## Task-by-task outcome

### Task 1: Repository Cloning and File Discovery

The repository was prepared using a shallow clone and deterministic file discovery. The discovery
policy selected 99 Python files and wrote them to `data/processed/discovered_files.json`. This
manifest became the stable input list for the parser.

### Task 2: Incremental CPG Parser Service

The Parser Service uses Python's standard `ast` module and processes one file at a time. It emits
node events, edge events, metadata events, and parser error events. Stable identifiers are assigned
to graph nodes, graph edges, and metadata documents.

The parser completed the baseline run with:

```text
Finished: successful=99 failed=0
```

The automated test suite passed with:

```text
19 passed
```

### Task 3: Kafka Topics and Event Contracts

The Kafka design uses four separate topics:

```text
cpg.nodes.v1
cpg.edges.v1
cpg.metadata.v1
cpg.errors.v1
```

Each event includes `schema_version`, `event_time`, `repo_name`, and `file_path`. Successful node,
edge, and metadata events also carry stable identity fields. Captured samples show real node, edge,
metadata, and parser error payloads.

### Task 4: Neo4j Graph Ingestion

Graph node and edge events are consumed by the Neo4j Kafka Sink Connector. The connector was
recorded in `RUNNING` state, and Neo4j verification showed a populated graph with no duplicate
logical node or edge identities.

The baseline Neo4j result was:

```text
Neo4j totals: nodes=263154 edges=626918
Duplicate node IDs: 0
Duplicate edge IDs: 0
Unresolved placeholder nodes: 0
```

### Task 5: MongoDB Metadata Ingestion

Source metadata events are consumed by Spark Structured Streaming and written to MongoDB through the
MongoDB Spark Connector. MongoDB uses a unique `metadata_id_1` index to protect one document per
stable metadata identity.

The baseline MongoDB result was:

```text
MongoDB metadata documents: 99
Duplicate metadata_id groups: 0
Duplicate repo/file groups: 0
```

### Task 6: Modified-File Replay Verification

The replay test modified only:

```text
src/accelerate/_lab_replay_probe.py
```

The replay log confirms that the file hash changed. MongoDB document count remained stable at 99,
which means the existing metadata document was updated instead of duplicated.

Neo4j changed the target file graph from:

```text
14 nodes / 27 edges
```

to:

```text
14 nodes / 26 edges
```

The one-edge decrease is expected because the controlled source edit changed the generated CPG
structure. Duplicate checks remained zero for both nodes and edges.

The checkpoint resume check also passed:

```text
result=PASSED checkpoint resumed without duplicating unchanged metadata
```

## What worked well

The split architecture worked well. Neo4j and MongoDB had clear responsibilities, and Kafka topics
kept graph, metadata, and error events separate. The stable identity design made replay verification
possible across all downstream systems.

Spark Structured Streaming handled metadata checkpointing correctly. Resuming from the same
checkpoint did not duplicate unchanged metadata, and MongoDB stayed at 99 documents after the replay
and checkpoint checks.

The parser also handled the selected repository at a practical scale. It processed 99 Python files,
including large files such as `src/accelerate/accelerator.py`,
`src/accelerate/utils/dataclasses.py`, and `src/accelerate/utils/modeling.py`.

## Limitations

The CPG extraction is educational rather than production-grade. The parser uses Python's standard
`ast` module. AST extraction is reliable for syntax structure, but CFG, DFG, and CALL edges are
approximations.

The CFG implementation does not fully model all branches, loops, exceptions, or basic blocks. The
DFG implementation is not scope-complete. CALL edges are best-effort and do not fully resolve
imports, methods, dynamic dispatch, or cross-file calls.

Modified-file graph replacement also has a lifecycle limitation. Stable IDs and `MERGE` prevent
duplicates, but they cannot automatically delete graph structures that disappear after a source
edit. For the lab, replay uses a file-scoped Neo4j cleanup step before republishing the modified
file. This is sufficient for verification, but a production pipeline should implement a fully
event-driven deletion strategy.

## Possible improvements

Future improvements could include:

- richer CFG construction with basic blocks and branch semantics;
- more precise DFG construction with scope-aware name resolution;
- better CALL resolution across imports and modules;
- event-driven tombstone or generation-based graph replacement instead of direct file-scoped cleanup;
- stronger integration tests around Kafka Connect and Spark failure recovery;
- dashboard-style monitoring for failed batches, connector lag, and parser errors.

## Final assessment

The project demonstrates the required end-to-end streaming workflow:

1. discover a real Python repository;
2. parse source files incrementally;
3. publish structured Kafka events;
4. ingest graph topology directly into Neo4j;
5. ingest metadata into MongoDB through Spark Structured Streaming;
6. verify modified-file replay and checkpoint resume behavior.

The recorded evidence shows successful baseline ingestion, duplicate-safe replay, MongoDB upsert
behavior, Neo4j graph replacement for the target file, and Spark checkpoint recovery without
duplicating unchanged metadata.
