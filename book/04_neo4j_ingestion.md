# Task 4: Graph Topology Ingestion into Neo4j

## Objective

The graph path must ingest parser topology directly from Kafka into Neo4j. Spark is intentionally
absent from this path:

```text
cpg.nodes.v1 + cpg.edges.v1 -> Neo4j Kafka Sink Connector -> Neo4j
```

## Implementation

The infrastructure initializes Neo4j uniqueness constraints for repository names, source-file
IDs, and CPG node IDs. Parser events contain stable node and relationship IDs, repository identity,
file path, file hash, schema version, and event time.

## Neo4j Kafka Sink Connector

`config/kafka/connect-neo4j-sink.json` subscribes only to `cpg.nodes.v1` and `cpg.edges.v1`. Node
Cypher creates or updates `Repository`, `SourceFile`, and `CPGNode` entities. Edge Cypher creates a
`CPG_EDGE` relationship carrying the logical edge type in its `type` property.

The demo log records both the connector and its task in `RUNNING` state. This is runtime evidence
that topology followed the direct Kafka sink path.

## Idempotent Cypher Logic

The connector uses `MERGE` with stable IDs rather than unconditional `CREATE`:

```cypher
MERGE (n:CPGNode {id: event.node_id})
SET n.repo_name = event.repo_name,
    n.file_path = event.file_path,
    n.file_hash = event.file_hash,
    n.type = event.node_type,
    n.placeholder = false
```

For edges, missing endpoints are first represented by placeholders. This avoids silently losing a
relationship if its edge event arrives before a node event on the other topic. The post-demo
verifier reported zero unresolved placeholder nodes.

`MERGE` is sufficient for unchanged replay, but a modified file can produce different line-based
node IDs. The controlled replay therefore deletes CPG nodes for exactly the target repository and
file before publishing replacement events. It does not clear the full graph.

## Verification Queries

The combined count shown in Neo4j Browser was obtained with:

```cypher
MATCH (n:CPGNode)
WITH count(n) AS node_count
MATCH ()-[r]->()
RETURN node_count, count(r) AS edge_count;
```

Here `edge_count` includes all relationship types, including repository/file containment
relationships. To count only parser topology, the verifier uses `MATCH ()-[r:CPG_EDGE]->()`.

The graph sample query is:

```cypher
MATCH p=(a:CPGNode)-[r]->(b:CPGNode)
RETURN p
LIMIT 25;
```

## Evidence

```{figure} images/neo4j-counts.png
:name: neo4j-counts
:width: 90%

Neo4j Browser records 114,785 CPG nodes and 434,215 total relationships after controlled replay.
```

The tracked verifier log reports 319,309 `CPG_EDGE` relationships at the same stage. The higher
Browser total is expected because its query counts every relationship type.

```{figure} images/neo4j-graph-view.png
:name: neo4j-graph-view
:width: 90%

Neo4j Browser result for 25 CPG paths, including node and relationship properties.
```

After replay, the verifier recorded:

```text
Neo4j totals: nodes=114785 edges=319309
Duplicate node IDs: 0
Duplicate edge IDs: 0
Unresolved placeholder nodes: 0
Target file: nodes=13 edges=25
```

## Reflection

Direct connector ingestion satisfies the required graph route and keeps Spark focused on
metadata. Stable-ID `MERGE`, uniqueness constraints, and endpoint placeholders address duplicate
and ordering risks. Modified-file idempotency needs the additional file-scoped replacement step;
this simple strategy is effective for the lab but is less sophisticated than retaining versioned
graph history.
