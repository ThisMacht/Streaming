# Task 4: Neo4j Graph Topology Ingestion

## Goal

The fourth task ingests the CPG graph topology into Neo4j. The lab requires node and edge events to
flow from Kafka directly into Neo4j through the Neo4j Kafka Connector Sink, without using Spark as an
intermediate layer for graph ingestion.

In this project, Neo4j stores the graph part of the Code Property Graph:

- repositories;
- source files;
- CPG nodes;
- CPG relationships such as AST, CFG, DFG, and CALL edges.

The metadata path is intentionally separated and handled later by Spark and MongoDB in Task 5.

## Ingestion architecture

The graph ingestion path is:

```text
Parser Service
    -> cpg.nodes.v1 / cpg.edges.v1
    -> Neo4j Kafka Sink Connector
    -> Neo4j
```

The Parser Service publishes graph node events to `cpg.nodes.v1` and graph edge events to
`cpg.edges.v1`. The Neo4j Kafka Sink Connector subscribes to these topics and writes graph data into
Neo4j.

Spark is not used in this graph path. This satisfies the lab requirement that graph topology is
written to Neo4j directly from Kafka.

## Connector evidence

Kafka Connect lists the Neo4j connector:

```json
["neo4j-cpg-sink"]
```

The recorded connector status shows that both the connector and its task were running:

```json
{
  "name": "neo4j-cpg-sink",
  "connector": {
    "state": "RUNNING",
    "worker_id": "kafka-connect:8083"
  },
  "tasks": [
    {
      "id": 0,
      "state": "RUNNING",
      "worker_id": "kafka-connect:8083"
    }
  ],
  "type": "sink"
}
```

The infrastructure check also shows the full local environment running, including Kafka Connect,
Kafka, Neo4j, MongoDB, Zookeeper, and Mongo Express. This confirms that the connector was available
while parser events were being published.

## Neo4j write strategy

The Neo4j connector uses Cypher with stable identifiers. The intended behavior is idempotent
upsert-like ingestion:

- `Repository` is matched by repository name;
- `SourceFile` is matched by repository and file path identity;
- `CPGNode` is matched by stable `node_id`;
- `CPG_EDGE` relationships are matched by stable `edge_id`.

Using stable identifiers with `MERGE` prevents repeated publication of the same logical node or edge
from creating duplicate graph identities.

A node event provides fields such as:

```json
{
  "repo_name": "accelerate",
  "file_path": "benchmarks/big_model_inference/big_model_inference.py",
  "node_id": "38e41e11cb41c8fe7ec4ceb45a3aaa640e5639df205125aea4282b6d9e0e4a10",
  "node_type": "alias",
  "structural_path": "module.body[0].names[0]"
}
```

An edge event provides a stable edge identity and stable endpoints:

```json
{
  "repo_name": "accelerate",
  "file_path": "benchmarks/big_model_inference/big_model_inference.py",
  "edge_id": "e5077591dc0c165d2a86d3476884c1127a3aed08154d380e8d0de8342ce4f376",
  "edge_type": "AST",
  "source_id": "008c0dcb1c46c6c0b1b186735756ee692ae164bc14969b7d3b6d4c8fcd3469f3",
  "target_id": "40946bc411075d50a641a36f4c9b42f94bebf70c08bde71ecf39862d873f5c99"
}
```

These fields are enough for the connector to create or update graph nodes and relationships in
Neo4j.

## Handling cross-topic ordering

Node and edge events are published to separate topics. In a streaming system, an edge event can be
observed before one of its endpoint node events. To avoid losing such edges, the ingestion logic can
create placeholder endpoint nodes when needed. When the real node event arrives later, the node
properties are filled in and the placeholder state is cleared.

This makes the graph path more robust to Kafka topic ordering and connector scheduling.

## Baseline ingestion result

The recorded end-to-end pipeline parsed and published all discovered files:

```text
Finished: successful=99 failed=0
```

After waiting for Kafka sinks, the baseline verification reported the global Neo4j graph size:

```text
Neo4j totals: nodes=263154 edges=626918
Duplicate node IDs: 0
Duplicate edge IDs: 0
Unresolved placeholder nodes: 0
```

These results show that graph events were successfully ingested into Neo4j and that the final graph
did not contain duplicate logical node or edge identifiers. The unresolved placeholder count being
zero also indicates that endpoint placeholders were resolved after the corresponding node events
arrived.

For the controlled replay target file, the baseline Neo4j state was:

```text
Target file src/accelerate/_lab_replay_probe.py: nodes=14 edges=27
```

The verification log also recorded sample relationships from Neo4j, including source node type,
target node type, edge ID, edge type, source ID, and target ID. This confirms that relationship
properties and stable endpoint references were present in the database.

## Replay-related result

Task 6 modifies and reprocesses only one file:

```text
src/accelerate/_lab_replay_probe.py
```

For Neo4j, the replay protocol performs file-scoped cleanup before republishing replacement graph
events for the modified file. This cleanup is direct Neo4j maintenance used only for modified-file
replacement verification. The replacement graph itself still travels through the required path:

```text
Kafka -> Neo4j Kafka Sink Connector -> Neo4j
```

The replay verification reported:

```text
neo4j_target_nodes_before=14
neo4j_target_nodes_after=14
neo4j_target_edges_before=27
neo4j_target_edges_after=26
duplicate_node_id_groups=0
duplicate_edge_id_groups=0
```

The target file changed from 14 nodes / 27 edges to 14 nodes / 26 edges. The one-edge decrease is
expected because the controlled source edit changed the CPG structure. The duplicate checks staying
at zero shows that replay did not create duplicate graph identities.

## Screenshots and figures

The following screenshots are rendered directly from the recorded Neo4j run.

```{figure} images/neo4j-counts.png
:name: task4-neo4j-count-query
:width: 90%

Neo4j count query for the ingested CPG, providing visual evidence of the stored node and
relationship totals after baseline ingestion.
```

```{figure} images/neo4j-graph-view.png
:name: task4-neo4j-graph-view
:width: 90%

Neo4j Browser graph view showing CPG nodes and relationships created by the direct Kafka-to-Neo4j
connector path.
```

```{figure} images/neo4j-duplicate_node.png
:name: task4-neo4j-duplicate-node-check
:width: 90%

Neo4j duplicate-node query result. No duplicate `CPGNode.id` groups were found (`0`).
```

```{figure} images/neo4j-duplicate_edge.png
:name: task4-neo4j-duplicate-edge-check
:width: 90%

Neo4j duplicate-edge query result. No duplicate `CPG_EDGE.id` groups were found (`0`).
```

## Reflection

The direct Kafka-to-Neo4j connector path worked well for this lab. It keeps graph ingestion separate
from Spark and makes the architecture easier to explain: Spark owns metadata ingestion, while Neo4j
Connect owns graph ingestion.

Stable `node_id` and `edge_id` values are the key to duplicate-safe ingestion. Reprocessing the same
logical graph elements can converge through `MERGE` instead of creating repeated nodes or
relationships. The placeholder strategy also helps tolerate node and edge events arriving in
different orders.

The main limitation is changed-file deletion semantics. `MERGE` can prevent duplicates, but it
cannot automatically remove graph nodes or edges that disappear after a source edit. For this lab,
Task 6 uses a file-scoped cleanup protocol before republishing the modified file. A production design
could replace this with generation numbers, tombstone events, or a fully event-driven deletion
protocol.
