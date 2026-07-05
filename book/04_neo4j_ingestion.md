# Task 4: Neo4j Graph Ingestion

## Goal

Ingest CPG nodes and edges from Kafka directly into Neo4j, without routing graph data through
Spark, and verify identity uniqueness.

## Implementation approach

```text
cpg.nodes.v1 / cpg.edges.v1 -> Neo4j Kafka Sink Connector -> Neo4j
```

`config/kafka/connect-neo4j-sink.json` subscribes to the node and edge topics. Its Cypher uses
`MERGE` for `Repository`, `SourceFile`, `CPGNode`, and stable-ID `CPG_EDGE` relationships. Database
constraints enforce unique CPG node IDs, source-file IDs, and repository names.

Because node and edge topics may be observed in different orders, edge ingestion creates a
placeholder endpoint when necessary. A later node event fills its properties and clears the
placeholder flag. This avoids losing an edge merely because it arrived first.

## Evidence and result

[`logs/kafka_connect_status.json`](logs/kafka_connect_status.json) records both connector and task
as `RUNNING`.

```{figure} images/neo4j-counts.png
:name: neo4j-counts
:width: 90%

Neo4j global node and relationship count query.
```

```{figure} images/neo4j-graph-view.png
:name: neo4j-graph-view
:width: 90%

Neo4j Browser visualization of CPG topology and properties.
```

The modified replay probe changes from 14 nodes / 27 CPG edges before replacement to 14 nodes /
26 CPG edges afterward. The controlled edit changes the CPG structure, so the one-edge decrease
is expected. Identity checks return zero duplicate groups:

```{figure} images/neo4j-duplicate_node.png
:name: neo4j-duplicate-node
:width: 90%

Neo4j duplicate node-ID check: zero groups.
```

```{figure} images/neo4j-duplicate_edge.png
:name: neo4j-duplicate-edge
:width: 90%

Neo4j duplicate edge-ID check: zero groups.
```

## Reflection

The direct connector path meets the graph-ingestion requirement and keeps Spark focused on
metadata. Stable-ID `MERGE`, constraints, and placeholders address replay and cross-topic ordering.
The optional file-scoped cleanup used in Task 6 replaces stale topology for one modified file; it
is a verification protocol, not a stage in normal graph ingestion.
