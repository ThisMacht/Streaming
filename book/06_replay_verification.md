# Task 6: Modified-File Replay Verification

## Goal

Modify and reprocess only one source file, then prove that MongoDB and Neo4j expose the new state
without duplicate logical identities and that an unchanged checkpoint resume adds no metadata.

The controlled target is:

```text
src/accelerate/_lab_replay_probe.py
```

## Implementation approach

1. Parse and publish the full baseline manifest.
2. Verify baseline MongoDB and Neo4j state.
3. Modify only the replay probe.
4. As a file-scoped verification protocol, remove old Neo4j topology for that exact
   repository/file before publishing its replacement.
5. Parse and publish only the modified probe.
6. Wait for the Neo4j connector and running Spark query, then verify both stores.
7. Check duplicate node IDs and edge IDs.
8. Restart/resume Spark with the same checkpoint and no new publication; compare metadata counts.

```bash
python -m src.verification.replay_one_file \
  --file src/accelerate/_lab_replay_probe.py \
  --modify \
  --cleanup-neo4j-before-replay \
  --wait-seconds 10
```

Neo4j cleanup is direct maintenance scoped to this one file. Replacement graph events still travel
Kafka → Neo4j Connector → Neo4j. The cleanup does not change the main ingestion architecture.
This modified-file replay is therefore a file-scoped replacement, not a claim of pure idempotent
replay without cleanup. Reprocessing unchanged content is idempotent through stable IDs together
with Neo4j `MERGE` and MongoDB upsert.

## Evidence and result

| Verification | Current result |
|---|---|
| MongoDB replay document | `node_count=14`, `edge_count=26`, `status=parsed` |
| Neo4j replay probe | 14 nodes, 26 edges |
| Duplicate node-ID groups | 0 |
| Duplicate edge-ID groups | 0 |
| Checkpoint artifacts | 25 before, 25 after |
| Metadata documents | 99 before, 99 after |
| Checkpoint result | `PASSED checkpoint resumed without duplicating unchanged metadata` |

## Neo4j replay verification

After the baseline ingestion was allowed to finish, we replayed exactly one modified file:

```text
src/accelerate/_lab_replay_probe.py
```

The target-file graph in Neo4j changed as follows:

| Metric | Before replay | After replay |
|---|---:|---:|
| CPG nodes | 14 | 14 |
| CPG edges | 27 | 26 |

The controlled source modification changed the generated CPG structure, so the decrease from 27
to 26 edges is expected. Neo4j reflects the replacement graph state for that file, and the
identity checks show that republishing did not create duplicate graph identifiers.

| Check | Result |
|---|---:|
| Duplicate `CPGNode.id` groups | 0 |
| Duplicate `CPG_EDGE.id` groups | 0 |

The structured replay log records:

```text
neo4j_target_nodes_before=14
neo4j_target_nodes_after=14
neo4j_target_edges_before=27
neo4j_target_edges_after=26
duplicate_node_id_groups=0
duplicate_edge_id_groups=0
```

```{figure} images/mongo-replay-after.png
:name: mongo-replay-after
:width: 90%

MongoDB metadata after modified-file replay: 14 nodes and 26 edges.
```

```{figure} images/node_edge_count_lab_replay_probe.png
:name: neo4j-replay-probe-count
:width: 90%

Neo4j count for the modified replay probe: 14 nodes and 26 edges.
```

```{figure} images/neo4j-duplicate_node.png
:name: replay-duplicate-node
:width: 90%

Duplicate node-ID query: zero groups.
```

```{figure} images/neo4j-duplicate_edge.png
:name: replay-duplicate-edge
:width: 90%

Duplicate edge-ID query: zero groups.
```

Raw evidence is preserved in
[`logs/identity_replay_verification.log`](logs/identity_replay_verification.log),
[`logs/checkpoint_resume.log`](logs/checkpoint_resume.log), and
[`logs/terminal_2_pipeline_latest.log`](logs/terminal_2_pipeline_latest.log). The structured
checkpoint log is the authority for the completed idle-resume check; terminal output also records
the end-to-end procedure and transient sink observations while events were converging.

## Reflection

MongoDB idempotence is strong because stable `metadata_id`, a unique index, and connector upsert
reinforce the same invariant. Neo4j stable IDs and `MERGE` prevent duplicate identities, while
file-scoped replacement prevents stale topology after structural edits. The checkpoint result
confirms that resuming without new events does not duplicate unchanged metadata. A production
design could replace direct cleanup with generations or tombstone events.
