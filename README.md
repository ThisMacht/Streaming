# Incremental CPG Streaming Pipeline

This lab incrementally parses Python files from
[`huggingface/accelerate`](https://github.com/huggingface/accelerate) with the standard-library
AST parser. It publishes stable node, edge, metadata, and error events to Kafka. The Neo4j Kafka
Sink consumes graph events, while Spark Structured Streaming upserts metadata through the
MongoDB Spark Connector.

## Quick start

Run all commands from the project root.

1. Create the environment and install dependencies.

   For bash or zsh:

   ```bash
   uv venv
   source .venv/bin/activate
   uv pip install -r requirements.txt
   cp .env.example .env
   ```

   For fish shell:

   ```fish
   uv venv
   source .venv/bin/activate.fish
   uv pip install -r requirements.txt
   cp .env.example .env
   ```

   **Expected output:** the `.venv` environment is available, dependencies install successfully,
   and `.env` contains the local service configuration copied from `.env.example`.

2. Start and initialize Kafka, Neo4j, MongoDB, and Kafka Connect.

   For the first setup, run:

   ```bash
   chmod +x scripts/*.sh
   ./scripts/init_infra.sh
   ```

   This command starts Docker containers, creates Kafka topics, initializes Neo4j constraints,
   initializes MongoDB indexes, and registers the Neo4j Kafka Sink Connector.

   **Expected output:**

   - `cpg-kafka`, `cpg-kafka-connect`, `cpg-neo4j`, `cpg-mongodb`, `cpg-zookeeper`, and
     `cpg-mongo-express` are running;
   - Kafka topics are created or already exist;
   - the Neo4j connector is registered after Kafka Connect becomes ready.

   For later runs after restarting the machine, usually only start the containers again:

   ```bash
   docker compose up -d
   ```

   Then check the infrastructure:

   ```bash
   ./scripts/check_infra.sh
   ```

   **Expected output:**

   - `cpg-kafka-connect` is healthy;
   - Kafka includes `cpg.nodes.v1`, `cpg.edges.v1`, `cpg.metadata.v1`, and `cpg.errors.v1`;
   - Kafka Connect lists `neo4j-cpg-sink`;
   - the connector and its task both report `RUNNING`.

   Run `./scripts/init_infra.sh` again only if you reset Docker data, remove volumes, change Kafka
   topics, change Neo4j connector configuration, or set up the project from scratch.

3. Check the Neo4j Kafka Sink Connector.

   ```bash
   curl http://localhost:8083/connectors/neo4j-cpg-sink/status
   ```

   **Expected output:** the connector and task both show `RUNNING`.

   If the connector configuration is changed, rerun `./scripts/init_infra.sh`, or recreate only
   the connector:

   ```bash
   curl -X DELETE http://localhost:8083/connectors/neo4j-cpg-sink

   curl -X POST http://localhost:8083/connectors \
     -H "Content-Type: application/json" \
     --data @config/kafka/connect-neo4j-sink.json
   ```

4. Clone Accelerate, discover files, and dry-run one file.

   ```bash
   python -m src.repo_tools.clone_repo
   python -m src.repo_tools.discover_files
   ```

   **Expected output:**

   - the shallow clone exists under the configured repository path;
   - `data/processed/discovered_files.json` is created;
   - the recorded lab run discovered 99 Python files for the selected repository revision and
     exclusion policy.

   Prepare or use the replay probe, then run the parser without publishing to Kafka:

   ```bash
   python -m src.parser_service.main \
     --mode one \
     --file src/accelerate/_lab_replay_probe.py \
     --dry-run
   ```

   **Expected output:** the probe is parsed without publishing events. The recorded baseline dry
   run reported `nodes=10 edges=18 metadata=1`.

5. In a separate terminal, start Spark before publishing real events. Keep this process running.

   For bash or zsh:

   ```bash
   source .venv/bin/activate

   PYTHONPATH=. spark-submit \
     --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 \
     src/spark_jobs/metadata_to_mongodb.py
   ```

   For fish shell:

   ```fish
   source .venv/bin/activate.fish

   env PYTHONPATH=. spark-submit \
     --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 \
     src/spark_jobs/metadata_to_mongodb.py
   ```

   **Expected output:** Spark application `CPGMetadataToMongoDB` starts and the Spark UI is
   available at <http://localhost:4040> while the query is running. Keep this terminal running
   while the other terminal publishes baseline and replay events.

6. In another terminal, publish events and run verification.

   For bash or zsh:

   ```bash
   source .venv/bin/activate
   ```

   For fish shell:

   ```fish
   source .venv/bin/activate.fish
   ```

   Then run:

   ```bash
   python -m src.parser_service.main --mode one --file src/accelerate/accelerator.py
   python -m src.verification.neo4j_checks
   python -m src.verification.mongodb_checks
   ```

   **Expected output:** the selected file is published, Neo4j reports graph counts without
   duplicate identities, and MongoDB reports metadata without duplicate `metadata_id` or repo/file
   groups.

7. After one file works, run all discovered files.

   ```bash
   python -m src.parser_service.main --mode all
   python -m src.verification.neo4j_checks
   python -m src.verification.mongodb_checks
   ```

   **Expected output:** files are processed one at a time from the discovery manifest. The recorded
   run finished with `successful=99 failed=0`; after consumers caught up, MongoDB contained 99
   metadata documents and the Neo4j duplicate checks returned 0.

8. Keep Spark running, then perform a controlled modified-file replay. The replay deletes graph
   topology for only the probe file before publishing its replacement and upserts MongoDB by
   stable `metadata_id`.

   ```bash
   python -m src.verification.replay_one_file \
     --file src/accelerate/_lab_replay_probe.py \
     --modify \
     --cleanup-neo4j-before-replay \
     --wait-seconds 10
   python -m src.verification.verify_mongodb_metadata \
     --file src/accelerate/_lab_replay_probe.py
   python -m src.verification.verify_neo4j_counts \
     --file src/accelerate/_lab_replay_probe.py
   ```

   **Expected output for the recorded replay:**

   - `file_hash_changed=True`;
   - MongoDB document count remains `99 -> 99`;
   - the target Neo4j graph changes from 14 nodes / 27 edges to 14 nodes / 26 edges;
   - duplicate node, edge, metadata, and repo/file groups remain 0.

   This Neo4j replacement result uses file-scoped cleanup as a verification protocol before the
   replacement graph is republished through Kafka. It is not a fully event-driven deletion
   lifecycle.

## Useful links

```text
Neo4j Browser:      http://localhost:7474
Mongo Express:      http://localhost:8081
Kafka Connect REST: http://localhost:8083
Spark UI:           http://localhost:4040
```

Neo4j login:

```text
username: neo4j
password: password123
```

## Notes

* Use `source .venv/bin/activate.fish` when using fish shell.
* Use `PYTHONPATH=.` or `env PYTHONPATH=.` when running Spark.
* AST extraction uses Python's standard parser. CFG, DFG, and CALL edges are lightweight educational
  approximations, not production-grade static analysis.
* Spark uses `foreachBatch` with MongoDB Spark Connector `replace` + `upsertDocument=true`, keyed
  by `metadata_id`, so replay updates the stable document. PyMongo is used only by verification
  commands, never by the ingestion write path.
* The checkpoint at `outputs/checkpoints/mongodb_metadata` preserves committed Kafka offsets.
* If Neo4j shows `0` nodes, check Kafka Connect logs:

  ```bash
  docker logs cpg-kafka-connect --tail=100
  ```

See [setup](markdowns/setup.md), [source usage](markdowns/src_usage.md), and
[architecture](markdowns/architecture.md) for details.

The [setup guide](markdowns/setup.md#6-script-reference-and-side-effects) includes a script-by-script
side-effect table. In particular, run `reset_demo_state.sh` only while Spark is stopped: it removes
MongoDB metadata, the Spark metadata checkpoint, and the Kafka metadata topic, while leaving Neo4j
and the other Kafka topics unchanged.

## Demo logs

To generate timestamped text logs for a full demo run, first reset only the metadata demo state.
**Run this reset only while Spark is stopped.** It removes MongoDB metadata, the metadata checkpoint,
and the Kafka metadata topic used by the demo.

```bash
./scripts/reset_demo_state.sh
```

**Expected output:** the metadata-only demo state is cleared while Neo4j and the graph topics remain
available for the controlled workflow.

Then start Spark in Terminal 1 and keep it running:

```bash
./scripts/demo_terminal_1_spark.sh
```

**Expected output:** application `CPGMetadataToMongoDB` starts, the Spark UI is available on port
4040, and the process waits for Kafka metadata. Leave this terminal running.

Run the parser, verification, and replay workflow in Terminal 2:

```bash
./scripts/demo_terminal_2_run_pipeline.sh
```

**Expected output:**

- the infrastructure check passes;
- 99 Python files are discovered;
- the baseline parser finishes with `successful=99 failed=0`;
- MongoDB reaches 99 metadata documents;
- Neo4j duplicate node and edge checks return 0;
- the modified replay updates the existing MongoDB document without increasing collection count.

Keep Terminal 1 running until Terminal 2 finishes the post-replay MongoDB and Neo4j checks. Stop
Spark with `Ctrl+C` only after Terminal 2 reports completion.

Raw logs are saved under `outputs/demo_logs/`; selected latest logs are copied to tracked
`evidence/logs/`. See the complete
[demo logging guide](markdowns/demo_logging.md) for the two-terminal workflow, replay behavior,
and expected results.

After infrastructure is ready, capture the non-visual configuration evidence:

```bash
./scripts/capture_connect_status.sh
./scripts/capture_mongodb_indexes.sh
./scripts/capture_kafka_samples.sh
```

**Expected output:** connector status, MongoDB index details, and one keyed sample for every CPG
Kafka topic are refreshed under `evidence/` and copied into the book evidence where the scripts
specify it.

Kafka capture preserves raw JSON and companion `key=...` / `value=...` text files. Stop Terminal 1
with `Ctrl+C` once, only after Terminal 2 finishes. The wrapper records an explicit user-stop
message; a Py4J traceback caused only by forced/repeated shutdown is a shutdown artifact.

## Verification Commands

```bash
pytest -q
jupyter-book build book
./scripts/capture_kafka_samples.sh
python -m src.verification.verify_checkpoint_resume \
  --output evidence/logs/checkpoint_resume.log
python -m src.verification.verify_mongodb_metadata
python -m src.verification.verify_neo4j_counts
python -m src.verification.replay_one_file \
  --file src/accelerate/_lab_replay_probe.py \
  --modify --cleanup-neo4j-before-replay --wait-seconds 10
```

Expected verification results from the recorded run:

- `pytest -q`: `19 passed`;
- `jupyter-book build book`: HTML is generated under `book/_build/html` without requiring `_build`
  to be committed;
- replay: the file hash changes, MongoDB stays at 99 documents, and all duplicate groups remain 0;
- checkpoint resume: `outputs/checkpoints/mongodb_metadata` exists and metadata count remains stable
  during the idle observation window.

Run checkpoint verification after restarting Spark with the same checkpoint and while no new
events are being published. Generate a controlled error first with
`python -m src.verification.emit_parser_error_sample` if the error topic is empty.

The checkpoint check demonstrates that the resumed/idle check did not duplicate MongoDB metadata.
It does not, by itself, prove exact Kafka offset values.

Published Jupyter Book: <https://thismacht.github.io/Streaming/>

When GitHub Actions deploys the site, `book/_build` is a local build artifact and should not be
committed.

## Before submission

```bash
source .venv/bin/activate
pytest -q
./scripts/init_infra.sh
./scripts/check_infra.sh
./scripts/capture_connect_status.sh
./scripts/capture_mongodb_indexes.sh
# Terminal 1: ./scripts/demo_terminal_1_spark.sh
# Terminal 2: ./scripts/demo_terminal_2_run_pipeline.sh
./scripts/capture_kafka_samples.sh
python -m src.verification.verify_checkpoint_resume \
  --output evidence/logs/checkpoint_resume.log
rm -rf book/_build
jupyter-book build book
git status --short
```

Before the final build, manually capture the required UI screenshots and execute the evidence
notebook. Do not commit `book/_build`.

## Evidence checklist before submission

Before submitting the GitHub Pages URL, verify the tracked book evidence:

```bash
find book/logs book/kafka book/images -type f | sort
```

Important evidence includes:

```text
book/logs/check_infra.log
book/logs/create_topics.log
book/logs/terminal_1_spark_latest.log
book/logs/terminal_2_pipeline_latest.log
book/logs/identity_replay_verification.log
book/logs/checkpoint_resume.log
book/logs/pytest.log
book/kafka/nodes_sample.json
book/kafka/edges_sample.json
book/kafka/metadata_sample.json
book/kafka/errors_sample.json
book/images/architecture.svg
```

Also confirm that `book/images/` contains the Neo4j, MongoDB, Spark, and replay screenshots and that
the corresponding chapters render them as figures. Open `book/_build/html/index.html` after a local
build, or inspect the published site at <https://thismacht.github.io/Streaming/>.

## Troubleshooting

### Kafka Connect is not healthy yet

Kafka Connect can take longer than Kafka or Neo4j to become ready. Wait a few seconds, then run:

```bash
docker compose ps
./scripts/check_infra.sh
```

Expected: `cpg-kafka-connect` becomes healthy and its REST endpoint responds.

### Neo4j connector is missing

If Kafka Connect does not list `neo4j-cpg-sink`, register it again:

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  --data @config/kafka/connect-neo4j-sink.json

curl http://localhost:8083/connectors/neo4j-cpg-sink/status
```

Expected: the connector and task both report `RUNNING`. If the connector already exists but has an
outdated configuration, delete it first as shown in Quick Start step 3, then register it again.

### Kafka topics are missing

Create the topics and rerun the infrastructure check:

```bash
./scripts/create_topics.sh
./scripts/check_infra.sh
```

Expected topics are `cpg.nodes.v1`, `cpg.edges.v1`, `cpg.metadata.v1`, and `cpg.errors.v1`.

### Spark starts but MongoDB does not update

Keep Terminal 1 running while Terminal 2 publishes metadata events. Check the checkpoint and the
MongoDB verification output:

```bash
ls outputs/checkpoints/mongodb_metadata
python -m src.verification.verify_mongodb_metadata
```

Mongo Express is available at <http://localhost:8081>. Also confirm that Spark was submitted with
both the Kafka SQL and MongoDB Spark Connector packages shown in Quick Start step 5.

### Replay seems stale in Neo4j

Modified-file replay uses file-scoped Neo4j cleanup before publishing the replacement graph. Use
the provided verification protocol instead of editing Neo4j manually:

```bash
python -m src.verification.replay_one_file \
  --file src/accelerate/_lab_replay_probe.py \
  --modify \
  --cleanup-neo4j-before-replay \
  --wait-seconds 10
```

This is intentionally a lab verification workflow, not a production-grade event-driven deletion
lifecycle. A production design could use tombstone events or generation-based replacement.

### Do not reset or stop Spark during replay verification

Do not run `reset_demo_state.sh` while Terminal 1 is consuming metadata. Keep Spark running until
Terminal 2 completes its final replay and database checks. Stop Spark once with `Ctrl+C` only after
Terminal 2 reports completion; repeated forced shutdown can produce a misleading Py4J traceback.

### Jupyter Book does not show screenshots

Confirm that every image exists and is referenced with a MyST figure block, for example:

````md
```{figure} images/neo4j-counts.png
:name: readme-example-neo4j-counts
:width: 90%

Neo4j count query after baseline ingestion.
```
````

Then rebuild:

```bash
jupyter-book clean book
jupyter-book build book
```

### GitHub Pages does not update

After pushing, inspect the GitHub Actions workflow and Pages deployment status. The workflow builds
the book and deploys `book/_build/html` as a Pages artifact. Do not commit `book/_build` unless the
course explicitly requires generated HTML.
