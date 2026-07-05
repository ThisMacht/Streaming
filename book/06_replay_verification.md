# Task 6: Modified-File Replay Verification

## Goal

The sixth task verifies idempotent replay behavior. The lab requires modifying one Python source file,
reprocessing only that file, and checking that both storage systems reflect the updated state without
creating duplicate logical records.

This task verifies three main points:

1. Neo4j reflects the updated graph state for the modified file.
2. MongoDB updates the existing metadata document instead of inserting a duplicate.
3. Spark Structured Streaming can resume from its checkpoint without duplicating unchanged metadata.

The controlled replay target is:

```text
src/accelerate/_lab_replay_probe.py
```

## Replay protocol

The replay workflow is intentionally scoped to one file. The baseline repository is first parsed and
published, then the replay probe is modified and republished.

The replay command is:

```bash
python -m src.verification.replay_one_file \
  --file src/accelerate/_lab_replay_probe.py \
  --modify \
  --cleanup-neo4j-before-replay \
  --wait-seconds 10
```

The replay protocol follows these steps:

1. Run the full baseline ingestion.
2. Verify the baseline MongoDB and Neo4j state.
3. Modify only `src/accelerate/_lab_replay_probe.py`.
4. Remove old Neo4j topology scoped to that repository/file before publishing the replacement graph.
5. Parse and publish only the modified file.
6. Wait for the Neo4j connector and the running Spark metadata stream.
7. Verify MongoDB document count, file hash, and duplicate metadata identities.
8. Verify Neo4j node and edge counts and duplicate graph identities.
9. Resume Spark with the same checkpoint and confirm unchanged metadata is not duplicated.

The direct Neo4j cleanup step is used only for file-scoped modified-file replacement verification.
The replacement graph events still follow the required ingestion path:

```text
Kafka -> Neo4j Kafka Sink Connector -> Neo4j
```

This distinction is important. Stable IDs and `MERGE` are enough to prevent duplicate identities
when the same content is replayed. However, if a source edit removes a node or edge, `MERGE` alone
cannot delete stale topology. The lab therefore uses a scoped cleanup protocol for the modified file
before republishing its replacement graph.

## Baseline state before replay

The baseline run processed the discovered repository files successfully:

```text
Finished: successful=99 failed=0
```

After the baseline ingestion, MongoDB reported:

```text
MongoDB metadata documents: 99
Duplicate metadata_id groups: 0
Duplicate repo/file groups: 0
```

Neo4j reported the global graph state:

```text
Neo4j totals: nodes=263154 edges=626918
Duplicate node IDs: 0
Duplicate edge IDs: 0
Unresolved placeholder nodes: 0
```

For the replay target file, Neo4j reported:

```text
Target file src/accelerate/_lab_replay_probe.py: nodes=14 edges=27
```

This is the graph state before the modified replay.

## File modification evidence

The replay verification log records both the baseline and modified file hashes:

```text
baseline_file_hash=1ebfb627a9d4021a10ddd74fb9f317d9e98c620ff9cdc7a9123fda2b06dba523
modified_file_hash=80731426ce3a3bec87cf61c7c01778c5034da283613482e14c8af88d5740e0fd
file_hash_changed=True
```

This confirms that the replay was not only a duplicate re-run of the same input. The target file
content changed before it was reprocessed.

## MongoDB replay result

MongoDB is expected to keep exactly one metadata document per stable file identity. The replay log
shows that the collection count stayed stable:

```text
mongodb_document_count_before=99
mongodb_document_count_after=99
mongodb_document_count_delta=+0
mongodb_file_hash_matches_replay=True
```

This means the replay updated the existing document for the target file instead of creating a second
metadata document.

The final replayed metadata state for the target file records:

```text
node_count=14
edge_count=26
status=parsed
```

The duplicate checks also remained clean:

```text
duplicate_metadata_id_groups=0
duplicate_repo_file_groups=0
```

These results verify the MongoDB idempotence path: stable `metadata_id`, replace/upsert behavior,
and the unique MongoDB index work together to prevent duplicate metadata records.

## Neo4j replay result

Neo4j is expected to reflect the replacement graph state for the modified file while preserving
unique graph identities.

The replay log records:

```text
neo4j_target_nodes_before=14
neo4j_target_nodes_after=14
neo4j_target_edges_before=27
neo4j_target_edges_after=26
duplicate_node_id_groups=0
duplicate_edge_id_groups=0
```

The node count remained 14, while the edge count changed from 27 to 26. The one-edge decrease is
expected because the controlled edit changed the generated CPG structure.

The duplicate checks show that the modified replay did not create duplicate graph identities:

| Check | Result |
|---|---:|
| Duplicate `CPGNode.id` groups | 0 |
| Duplicate `CPG_EDGE.id` groups | 0 |

The important point is that Neo4j exposes the updated graph state for the modified file and still has
zero duplicate node-ID and edge-ID groups after replay.

## Checkpoint resume verification

The Spark metadata stream uses this checkpoint location:

```text
outputs/checkpoints/mongodb_metadata
```

The checkpoint verification log reports:

```text
checkpoint_location=outputs/checkpoints/mongodb_metadata
checkpoint_exists=True
checkpoint_artifacts_before=21
checkpoint_artifacts_after=21
metadata_count_before=99
metadata_count_after=99
result=PASSED checkpoint resumed without duplicating unchanged metadata
```

This proves that the Spark Structured Streaming job can resume with the same checkpoint without
duplicating unchanged metadata. The checkpoint artifacts remained stable at 21, and the MongoDB
metadata document count remained 99 before and after the resume check.

## Summary of verification results

| Verification | Result |
|---|---:|
| Replay target file | `src/accelerate/_lab_replay_probe.py` |
| File hash changed | `True` |
| MongoDB documents before replay | 99 |
| MongoDB documents after replay | 99 |
| MongoDB document delta | +0 |
| MongoDB replay hash matches | `True` |
| Neo4j target nodes before replay | 14 |
| Neo4j target nodes after replay | 14 |
| Neo4j target edges before replay | 27 |
| Neo4j target edges after replay | 26 |
| Duplicate Neo4j node-ID groups | 0 |
| Duplicate Neo4j edge-ID groups | 0 |
| Duplicate MongoDB metadata-ID groups | 0 |
| Duplicate MongoDB repo/file groups | 0 |
| Checkpoint exists | `True` |
| Checkpoint artifacts before resume | 21 |
| Checkpoint artifacts after resume | 21 |
| Metadata count before checkpoint resume | 99 |
| Metadata count after checkpoint resume | 99 |
| Checkpoint resume result | Passed |

## Screenshots and raw evidence

The replay evidence is rendered below so that the modified-file result and duplicate checks can be
inspected directly in this chapter.

```{figure} images/mongo-replay-after.png
:name: task6-mongodb-replay-after
:width: 90%

MongoDB metadata after the controlled file modification. The existing document is updated with the
modified file hash rather than duplicated.
```

```{figure} images/node_edge_count_lab_replay_probe.png
:name: task6-neo4j-replay-probe-count
:width: 90%

Neo4j count query for the replay probe after replacement: `14` nodes and `26` edges for the
modified file.
```

```{figure} images/neo4j-duplicate_node.png
:name: task6-neo4j-replay-duplicate-node
:width: 90%

Post-replay Neo4j duplicate-node check, showing `0` duplicate node-ID groups.
```

```{figure} images/neo4j-duplicate_edge.png
:name: task6-neo4j-replay-duplicate-edge
:width: 90%

Post-replay Neo4j duplicate-edge check, showing `0` duplicate edge-ID groups.
```

The raw replay and checkpoint logs are stored under `book/logs/`:

```text
logs/identity_replay_verification.log
logs/checkpoint_resume.log
logs/terminal_2_pipeline_latest.log
```

These logs are the authoritative evidence for the replay counts, duplicate checks, file hash change,
and checkpoint resume result.

## Reflection

The replay verification shows that the pipeline handles repeated and modified input safely. MongoDB
idempotence is strong because the metadata path combines stable `metadata_id`, connector
replace/upsert behavior, and a unique index. Replaying the modified file changed its hash and final
counts, but the collection remained at 99 documents.

Neo4j also remained duplicate-safe: stable node and edge identifiers prevented repeated logical
graph identities. The modified file changed from 14 nodes / 27 edges to 14 nodes / 26 edges, and both
duplicate checks stayed at zero.

The main limitation is graph deletion semantics. A changed source file can remove old structural
nodes or edges, and `MERGE` cannot automatically infer deletion. For this lab, the solution uses a
file-scoped Neo4j cleanup step before republishing the replacement graph. This is a verification
protocol, not a fully event-driven deletion lifecycle. A production system could implement this
more cleanly with generation numbers, tombstone events, or a fully event-driven replacement
protocol.
