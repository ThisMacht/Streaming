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

The command writes `evidence/logs/identity_replay_verification.log` with both hashes, MongoDB
document counts and delta, target-file Neo4j node/edge counts before and after, duplicate identity
group counts, and checkpoint existence. The exact global document total is revision-dependent;
the required replay invariant is a delta of zero.

## Before/After Checks

| Check | Expected result | Evidence |
|---|---|---|
| Same target file reprocessed | Only the probe is passed to the replay publisher | Replay target logged explicitly |
| `metadata_id` stable | Same logical document identity | `59f203...561ea` before and after |
| `file_hash` changed | Controlled source edit is real | `1ebfb6...ba523` → `807314...0e0fd` |
| MongoDB document count | No increase | Before equals after; delta `+0` |
| Ingestion progress | Later write is visible | `spark_batch_id` 1 → 7 and new `ingested_at` |
| Neo4j replacement | Old file topology removed, replacement added | Current identity-hardened verifier reports 14 nodes/26 edges |
| Duplicate identities | No duplicate groups | MongoDB, node ID, and edge ID checks all report 0 |

The modified source has two functions instead of one. Consequently, a global node/edge delta of
zero is not expected. With structural-path identity, the target changes from 10/18 to 14/26; the
additional node/edge preserves a positionless AST occurrence that the earlier identity collapsed.

## MongoDB Evidence

```{figure} images/mongo-replay-after.png
:name: mongo-replay-after
:width: 90%

MongoDB metadata from the original controlled replay, before structural-path identity hardening.
```

The original screenshot records 13 nodes/25 edges. After preserving each positionless AST
occurrence, `evidence/logs/identity_replay_verification.log` records the same stable metadata ID
and modified hash with 14 nodes/26 edges, an unchanged historical document total, and zero
duplicate identities. Regenerate the evidence after applying the current discovery policy.

## Neo4j Evidence

Before replay, the probe had 10 nodes and 18 CPG edges. File-scoped cleanup removes those nodes and
their relationships before replacement. The current verifier reports 14 nodes and 26 edges after
replay, zero duplicate node IDs, zero duplicate edge IDs, and zero unresolved placeholders.

This is a file-scoped replacement protocol, not a Spark graph path. Before replaying a modified
file, the verifier removes only previous topology for the target `repo_name` and `file_path` in
Neo4j. Updated node and edge events then travel through Kafka and the Neo4j Kafka Connector, whose
Cypher uses `MERGE`. Duplicate `node_id` and `edge_id` checks remain zero; Spark consumes only the
metadata topic.

## Spark Checkpoint Evidence

The job sets `checkpointLocation` to `outputs/checkpoints/mongodb_metadata`. Spark remained active
through replay, and the MongoDB document's batch marker advanced from 1 to 7 with a later
`ingested_at`. This demonstrates that a later streaming micro-batch performed the upsert. The book
does not infer exact Kafka offsets from the batch number; offset recovery is provided by Spark's
checkpoint files.

## Spark checkpoint resume verification

Restart the Spark metadata job with the unchanged `checkpointLocation`, publish no new events,
and run in another terminal:

```bash
python -m src.verification.verify_checkpoint_resume \
  --sleep-seconds 10 \
  --output evidence/logs/checkpoint_resume.log
```

The verifier records checkpoint commit/offset/source artifacts and compares the MongoDB document
count before and after an idle window. A passing result states `PASSED checkpoint resumed without
duplicating unchanged metadata`: the count remains constant because the resumed query skips
offsets already committed in the same checkpoint. This check must run without a concurrent
publisher and complements the modified-file replay test.

The tracked run in `evidence/logs/checkpoint_resume.log` recorded:

```text
checkpoint_exists=True
checkpoint_artifacts_before=5
checkpoint_artifacts_after=5
metadata_count_before=121
metadata_count_after=121
result=PASSED checkpoint resumed without duplicating unchanged metadata
```

## Reflection

Replay is only immediately observable in MongoDB when Spark is running. If Spark is stopped before
the replay event, Kafka retains the event but the MongoDB document will not update until the query
resumes.

File-level Neo4j replacement is necessary because node IDs include line and column positions. A
source edit can invalidate old IDs, and `MERGE` alone cannot identify nodes that disappeared. The
scoped cleanup avoids stale topology without deleting unrelated repository data. It is a pragmatic
lab strategy; a production system might use generations, tombstone events, or transactional
version activation.
