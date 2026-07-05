# Lab 04: Incremental CPG Streaming Pipeline

This Jupyter Book documents an incremental Code Property Graph (CPG) streaming pipeline for the
`huggingface/accelerate` Python repository. The goal of the lab is to parse Python source files one
at a time, publish structured graph and metadata events to Apache Kafka, and persist the results into
two different database systems: Neo4j for graph topology and MongoDB for source metadata.

The implementation follows the Lab 04 requirement that graph ingestion and metadata ingestion use
separate paths. Node and edge events are written from Kafka directly to Neo4j through the Neo4j Kafka
Sink Connector, while metadata events are consumed by Apache Spark Structured Streaming and written
to MongoDB through the MongoDB Spark Connector. Spark is therefore used only for the metadata path,
not for Neo4j graph ingestion.

## Pipeline overview

The pipeline has six main stages:

1. Shallow-clone the selected repository and discover Python source files deterministically.
2. Parse each Python file independently with a bounded-memory parser based on Python's `ast` module.
3. Emit four categories of Kafka events: graph nodes, graph edges, source metadata, and parser errors.
4. Ingest graph nodes and edges directly into Neo4j using the Neo4j Kafka Sink Connector.
5. Ingest source metadata into MongoDB using Spark Structured Streaming with checkpointing.
6. Modify and replay one Python file, then verify that Neo4j and MongoDB update without duplicate
   logical identities.

The selected repository is `huggingface/accelerate`. After applying the project's discovery policy,
the pipeline discovered and processed 99 Python files. The full baseline run completed with
`successful=99` and `failed=0`.

## Runtime environment

The local environment is containerized with Docker. The recorded infrastructure check shows the
required services running: Kafka, Zookeeper, Kafka Connect, Neo4j, MongoDB, and Mongo Express.
The Kafka cluster contains the four project topics:

- `cpg.nodes.v1`
- `cpg.edges.v1`
- `cpg.metadata.v1`
- `cpg.errors.v1`

Kafka Connect also reports the `neo4j-cpg-sink` connector in `RUNNING` state, with its sink task
also `RUNNING`.

## Event design

Each parser output event includes a schema version, event time, repository name, and file path.
Graph node events use stable `node_id` values, graph edge events use stable `edge_id` values, and
metadata events use stable `metadata_id` values. These stable identifiers allow repeated processing
of the same logical element to converge downstream instead of producing duplicate records.

The captured Kafka samples include:

- a node event with `node_id`, `node_type`, structural path, and source position;
- an edge event with `edge_id`, `edge_type`, source node, and target node;
- a metadata event with file hash, line count, function count, class count, import count, node count,
  edge count, and parse status;
- a parser error event showing a structured `SyntaxError` payload.

## Database ingestion

Neo4j stores the graph topology. The recorded baseline verification reports global Neo4j totals of
263,154 nodes and 626,918 relationships, with zero duplicate node IDs and zero duplicate edge IDs.
For the replay target file, Neo4j reported 14 CPG nodes and 27 CPG edges before the modified replay.

MongoDB stores source metadata in the `cpg_lab.source_metadata` collection. The collection has a
unique `metadata_id_1` index, ensuring that each source file metadata identity is represented once.
After the baseline run, MongoDB contained 99 metadata documents, with zero duplicate `metadata_id`
groups and zero duplicate repository/file groups.

## Replay verification

The replay experiment modifies only one file:

```text
src/accelerate/_lab_replay_probe.py
```

The replay log confirms that the file hash changed. MongoDB kept the document count stable at 99,
meaning the existing metadata document was updated instead of duplicated. Neo4j changed the target
file graph from 14 nodes / 27 edges to 14 nodes / 26 edges. The one-edge decrease is expected because
the controlled source edit changed the generated CPG structure.

After replay, the duplicate checks still report:

| Check | Result |
|---|---:|
| Duplicate Neo4j node-ID groups | 0 |
| Duplicate Neo4j edge-ID groups | 0 |
| Duplicate MongoDB metadata-ID groups | 0 |
| Duplicate MongoDB repo/file groups | 0 |

The checkpoint verification also passed. The Spark checkpoint directory existed at
`outputs/checkpoints/mongodb_metadata`, checkpoint artifacts remained stable at 21 before and after
the resume check, and MongoDB metadata count remained 99 before and after. This verifies that
restarting the metadata stream with the same checkpoint does not duplicate unchanged metadata.

## Evidence organization

This book is organized as a structured narrative that follows the lab tasks in order. Each chapter
explains the implementation approach, shows real evidence from the pipeline, and ends with a short
reflection.

The supporting artifacts are stored beside the report:

- `book/logs/` contains infrastructure checks, topic creation, Spark logs, pipeline logs, replay logs,
  checkpoint verification, MongoDB index output, Kafka Connect status, and test output;
- `book/kafka/` contains captured Kafka node, edge, metadata, and error samples;
- `book/images/` contains the architecture diagram and screenshots from Neo4j, MongoDB, and Spark;
- `book/notebooks/pipeline_demo.ipynb` is an executed, read-only evidence summary covering the
  discovery manifest, parser counts, Kafka samples, infrastructure status, MongoDB indexes,
  recorded database verification output, modified-file replay, checkpoint resume, and artifact
  availability.

The automated test suite also passed with 19 tests, providing additional evidence for parser
identity stability, schema contracts, error events, and replay-related behavior.

## Reading order

Start with the architecture chapter to understand the split graph and metadata paths. Then read
Tasks 1 through 6 in sequence:

1. Repository cloning and file discovery
2. Incremental CPG parser service
3. Kafka topic design and event contracts
4. Neo4j graph topology ingestion
5. MongoDB source metadata ingestion
6. Modified-file replay verification

The conclusion summarizes the final result, limitations, and possible production improvements.
