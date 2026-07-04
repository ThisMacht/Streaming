# Task 6: Idempotent Replay Verification

## Objective

The replay test modifies one small Python file, reprocesses only that file, and verifies replacement
behavior in MongoDB and Neo4j without resetting the complete pipeline.

## Controlled Replay File

The target is:

```text
src/accelerate/_lab_replay_probe.py
```

Its baseline contains one function returning `x + 1`. Controlled modification changes the return
value and adds a marker function. This produces a real file-hash and topology change without
editing a large upstream source file.

## Replay Procedure

The two-terminal procedure keeps Spark active:

1. restore/create the baseline probe and include it in discovery;
2. run full baseline ingestion;
3. capture MongoDB and Neo4j before-state;
4. write the deterministic modified probe and record both hashes;
5. delete Neo4j CPG nodes for only this repository/file identity;
6. publish replacement node, edge, and metadata events for this file only;
7. wait for the Neo4j sink and running Spark query;
8. capture after-state and duplicate checks.

The command used by Terminal 2 is:

```bash
python -m src.verification.replay_one_file \
  --file src/accelerate/_lab_replay_probe.py \
  --modify --cleanup-neo4j-before-replay --wait-seconds 10
```

## Before/After Checks

| Check | Expected result | Evidence |
|---|---|---|
| Same target file reprocessed | Only the probe is passed to the replay publisher | Replay target logged explicitly |
| `metadata_id` stable | Same logical document identity | `59f203...561ea` before and after |
| `file_hash` changed | Controlled source edit is real | `1ebfb6...ba523` → `807314...0e0fd` |
| MongoDB document count | No increase | 121 → 121; delta `+0` |
| Ingestion progress | Later write is visible | `spark_batch_id` 1 → 7 and new `ingested_at` |
| Neo4j replacement | Old file topology removed, replacement added | Cleanup deleted 10 nodes; file became 13 nodes/25 edges |
| Duplicate identities | No duplicate groups | MongoDB, node ID, and edge ID checks all report 0 |

The modified source has two functions instead of one. Consequently, a global node/edge delta of
zero is not expected. The recorded Neo4j delta was `+3` nodes and `+7` CPG edges, matching the
target-file change from 10/18 to 13/25.

## MongoDB Evidence

```{figure} images/mongo-replay-after.png
:name: mongo-replay-after
:width: 90%

MongoDB metadata after controlled modified-file replay, showing the new hash, counts,
`ingested_at`, and `spark_batch_id: 7`.
```

The screenshot and log agree on `event_time` `2026-07-04T07:23:51.440842Z`, six source lines, two
functions, 13 nodes, 25 edges, and the modified file hash. The log also records
`metadata_documents=+0` and zero duplicate metadata identities.

## Neo4j Evidence

Before replay, the probe had 10 nodes and 18 CPG edges. The cleanup deleted those 10 nodes and
their attached relationships before publishing the replacement. After replay, it had 13 nodes and
25 CPG edges. Global verification reported zero duplicate node IDs, zero duplicate edge IDs, and
zero unresolved placeholders.

## Spark Checkpoint Evidence

The job sets `checkpointLocation` to `outputs/checkpoints/mongodb_metadata`. Spark remained active
through replay, and the MongoDB document's batch marker advanced from 1 to 7 with a later
`ingested_at`. This demonstrates that a later streaming micro-batch performed the upsert. The book
does not infer exact Kafka offsets from the batch number; offset recovery is provided by Spark's
checkpoint files.

## Reflection

Replay is only immediately observable in MongoDB when Spark is running. If Spark is stopped before
the replay event, Kafka retains the event but the MongoDB document will not update until the query
resumes.

File-level Neo4j replacement is necessary because node IDs include line and column positions. A
source edit can invalidate old IDs, and `MERGE` alone cannot identify nodes that disappeared. The
scoped cleanup avoids stale topology without deleting unrelated repository data. It is a pragmatic
lab strategy; a production system might use generations, tombstone events, or transactional
version activation.
