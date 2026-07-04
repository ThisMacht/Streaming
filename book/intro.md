# Big Data Lab 04 - Spark Streaming

This Jupyter Book presents our implementation for **Lab 04: Spark Streaming** in the Introduction to Big Data course.

The lab focuses on building an incremental **Code Property Graph (CPG)** streaming pipeline for a real Python repository. Our selected repository is:

```text
https://github.com/huggingface/accelerate
```

The system parses Python source files one by one, emits structured events to Apache Kafka, and persists the results into two database systems:

- **Neo4j** stores the graph topology, including CPG nodes and edges.
- **MongoDB** stores source code metadata, such as file hash, line count, function count, class count, import count, node count, edge count, and parse status.

## Pipeline Overview

The main pipeline is:

```text
Python repository
      |
      v
Parser Service
      |
      v
Apache Kafka
   |         |
   v         v
Neo4j       Spark Structured Streaming
             |
             v
           MongoDB
```

The Parser Service publishes four categories of Kafka events:

| Topic | Purpose |
|---|---|
| `cpg.nodes.v1` | CPG node events |
| `cpg.edges.v1` | CPG edge events |
| `cpg.metadata.v1` | Source metadata events |
| `cpg.errors.v1` | Parser error events |

Node and edge events are consumed directly by the **Neo4j Kafka Sink Connector**. Metadata events are consumed by **Spark Structured Streaming** and written to MongoDB.

## Report Structure

This book is organized according to the lab tasks:

1. **Repository Cloning and File Discovery**  
   Clone the assigned repository and discover Python source files.

2. **Incremental CPG Parser Service**  
   Parse Python files incrementally and emit CPG events.

3. **Kafka Topic Design**  
   Design Kafka topics for nodes, edges, metadata, and parser errors.

4. **Graph Topology Ingestion into Neo4j**  
   Ingest CPG nodes and edges into Neo4j through the Neo4j Kafka Sink Connector.

5. **Source Metadata Ingestion into MongoDB**  
   Use Spark Structured Streaming to consume metadata events and write them to MongoDB.

6. **Idempotent Replay Verification**  
   Modify one Python source file, reprocess only that file, and verify that Neo4j and MongoDB reflect the updated state without duplication.

## Evidence Included

The report includes:

- commands used to run each task;
- parser and streaming logs;
- Kafka event samples;
- Neo4j query results;
- MongoDB document views;
- screenshots from Neo4j Browser and Mongo Express;
- replay verification evidence;
- reflections on issues encountered and how they were resolved.

## Implementation Notes

The implementation uses stable identifiers to support idempotent processing:

- CPG nodes use deterministic node IDs.
- CPG edges use deterministic edge IDs.
- MongoDB metadata documents use stable `metadata_id`.

Neo4j ingestion uses merge-based logic to avoid duplicate graph elements. MongoDB metadata ingestion uses stable-key upsert so that replaying a file updates the existing metadata document instead of inserting a duplicate.
