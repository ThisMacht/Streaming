# Source Code Usage

Run all commands from the project root.

Before running the source code, make sure that:

1. `.env.example` has been copied to `.env`.
2. Python virtual environment has been created and activated.
3. Dependencies in `requirements.txt` have been installed.
4. Docker infrastructure has been started and initialized.

For fish shell, activate the virtual environment with:

```fish
source .venv/bin/activate.fish
```

For bash or zsh, activate it with:

```bash
source .venv/bin/activate
```

---

## Recommended order

1. Create and activate the virtual environment, then install `requirements.txt`.
2. Copy `.env.example` to `.env`.
3. Start and initialize infrastructure with `./scripts/init_infra.sh`.
4. Check infrastructure with `./scripts/check_infra.sh`.
5. Clone the Hugging Face Accelerate repository.
6. Discover Python files in the repository.
7. Dry-run the parser to check that parsing works.
8. Check or recreate the Neo4j Kafka Sink Connector.
9. Start the Spark metadata job in a separate terminal.
10. Run the parser without `--dry-run` from another terminal.
11. Verify Neo4j and MongoDB.
12. Modify or select one file and replay it.

---

## Start infrastructure

For the first setup, run:

```bash
chmod +x scripts/*.sh
./scripts/init_infra.sh
```

This command starts Docker containers, creates Kafka topics, initializes Neo4j constraints, initializes MongoDB indexes, and registers the Neo4j Kafka Sink Connector.

For later runs after restarting the machine, usually only start the containers again:

```bash
docker compose up -d
```

Then check the infrastructure:

```bash
./scripts/check_infra.sh
```

Expected services:

```text
cpg-kafka
cpg-kafka-connect
cpg-mongodb
cpg-mongo-express
cpg-neo4j
cpg-zookeeper
```

Expected Kafka topics:

```text
cpg.nodes.v1
cpg.edges.v1
cpg.metadata.v1
cpg.errors.v1
```

---

## Neo4j Kafka Sink Connector

Check connector status:

```bash
curl http://localhost:8083/connectors/neo4j-cpg-sink/status
```

The connector and task should both show `RUNNING`.

If the connector configuration is changed, recreate the connector:

```bash
curl -X DELETE http://localhost:8083/connectors/neo4j-cpg-sink
```

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  --data @config/kafka/connect-neo4j-sink.json
```

Check again:

```bash
curl http://localhost:8083/connectors/neo4j-cpg-sink/status
```

Expected result:

```text
"state":"RUNNING"
```

---

## Task 1: Clone repository and discover files

Clone the Hugging Face Accelerate repository:

```bash
python -m src.repo_tools.clone_repo
```

Discover Python files:

```bash
python -m src.repo_tools.discover_files
```

The discovered file manifest is saved to:

```text
data/processed/discovered_files.json
```

You can check it with:

```bash
ls data/processed/
cat data/processed/discovered_files.json | head
```

---

## Task 2: Dry-run parser

Dry-run one file first:

```bash
python -m src.parser_service.main --mode one --file src/accelerate/accelerator.py --dry-run
```

Expected output should contain something similar to:

```text
Parsed src/accelerate/accelerator.py: nodes=... edges=... metadata=1 (dry-run)
Finished: successful=1 failed=0
```

Dry-run all discovered files:

```bash
python -m src.parser_service.main --mode all --dry-run
```

Dry-run mode only parses files and prints statistics. It does not produce Kafka events.

---

## Spark metadata job

Start the Spark metadata job in a separate terminal before running the parser without `--dry-run`.

Keep this terminal running while parser events are being produced.

For bash or zsh:

```bash
PYTHONPATH=. spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 \
  src/spark_jobs/metadata_to_mongodb.py
```

For fish shell:

```fish
env PYTHONPATH=. spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 \
  src/spark_jobs/metadata_to_mongodb.py
```

The metadata ingestion job uses Apache Spark Structured Streaming to consume metadata events from Kafka topic `cpg.metadata.v1`.

Each parsed micro-batch is upserted through MongoDB Spark Connector with:

```python
.writeStream.foreachBatch(upsert_metadata_batch)
batch_df.write.format("mongodb").option("operationType", "replace")
  .option("idFieldList", "metadata_id").option("upsertDocument", "true").save()
```

A checkpoint directory is configured so Spark can resume from the last committed Kafka offsets after restart.

If the same file is replayed, the existing stable metadata document is replaced and receives a
new `ingested_at` and `spark_batch_id`; its document count does not increase.

For a clean demo run, reset MongoDB metadata and Spark checkpoint before starting Spark:

```bash
./scripts/reset_demo_state.sh
```

Run the reset only while Spark is stopped. It also deletes and recreates `cpg.metadata.v1`, but
does not clear Neo4j or the Kafka node, edge, and error topics. Then start the Spark metadata job
again.

---

## Task 3: Run parser and produce Kafka events

Run one file first:

```bash
python -m src.parser_service.main --mode one --file src/accelerate/accelerator.py
```

This sends events to Kafka:

```text
CPG node events      -> cpg.nodes.v1      -> Neo4j Kafka Sink -> Neo4j
CPG edge events      -> cpg.edges.v1      -> Neo4j Kafka Sink -> Neo4j
Metadata events      -> cpg.metadata.v1   -> Spark Streaming  -> MongoDB
Error events         -> cpg.errors.v1
```

If one file works, run all discovered files:

```bash
python -m src.parser_service.main --mode all
```

---

## Verification

Check Neo4j:

```bash
python -m src.verification.neo4j_checks
```

Expected result after running one file:

```text
Neo4j CPG nodes: > 0
Neo4j CPG edges: > 0
Top files by node count:
src/accelerate/accelerator.py: ...
```

Check MongoDB:

```bash
python -m src.verification.mongodb_checks
```

Expected result:

```text
MongoDB metadata documents: > 0
No duplicate metadata documents found.
```

Run controlled modified-file replay verification while Spark remains running:

```bash
python -m src.verification.replay_one_file --file src/accelerate/_lab_replay_probe.py --modify
```

---

## Replay one file

Reprocessing unchanged content can be done directly with the parser:

```bash
python -m src.parser_service.main --mode one --file src/accelerate/accelerator.py
```

Neo4j uses `MERGE`, so existing nodes and edges are updated instead of duplicated.

MongoDB uses stable-key upsert plus a unique `metadata_id` index, so the existing document is
updated without creating a duplicate.

For a **modified** file, use `replay_one_file` instead of only invoking the parser. Node IDs contain
source positions, so a modification can make old topology stale even though `MERGE` prevents ID
duplicates. The replay command deletes Neo4j topology scoped to the target repository/file, then
publishes replacement node, edge, and metadata events:

```bash
python -m src.verification.replay_one_file \
  --file src/accelerate/_lab_replay_probe.py \
  --modify \
  --cleanup-neo4j-before-replay \
  --wait-seconds 10
```

The cleanup is enabled by default, but the explicit option makes the evidence command unambiguous.

---

## Useful UI links

Neo4j Browser:

```text
http://localhost:7474
```

Neo4j login:

```text
username: neo4j
password: password123
```

Mongo Express:

```text
http://localhost:8081
```

Kafka Connect REST API:

```text
http://localhost:8083
```

Spark UI while Spark job is running:

```text
http://localhost:4040
```

---

## Common errors

### `ModuleNotFoundError: No module named 'src'`

Run Spark with `PYTHONPATH=.`.

For bash or zsh:

```bash
PYTHONPATH=. spark-submit ...
```

For fish:

```fish
env PYTHONPATH=. spark-submit ...
```

---

### Fish shell activation error

If this command fails in fish:

```bash
source .venv/bin/activate
```

Use this instead:

```fish
source .venv/bin/activate.fish
```

---

### MongoDB duplicate key error

Error example:

```text
E11000 duplicate key error collection: cpg_lab.source_metadata index: metadata_id_1
```

The current job uses connector replace/upsert keyed by `metadata_id`, so this error usually means an old
append-mode Spark process is still running or the deployed job has not been restarted after the
code change. Stop only the old Spark process, then start the current Terminal 1 script. Do not
reset data merely to hide the error.

If a deliberately clean demonstration is required, use the reset script only while Spark is
stopped. It deletes MongoDB metadata and the metadata checkpoint/topic, so review it first:

```bash
./scripts/reset_demo_state.sh
```

---

### Neo4j connector is not running

Check status:

```bash
curl http://localhost:8083/connectors/neo4j-cpg-sink/status
```

If needed, recreate it:

```bash
curl -X DELETE http://localhost:8083/connectors/neo4j-cpg-sink

curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  --data @config/kafka/connect-neo4j-sink.json
```

---

### Neo4j has 0 nodes and 0 edges

Check Kafka Connect logs:

```bash
docker logs cpg-kafka-connect --tail=100
```

Common causes:

```text
- Wrong connector class
- Wrong Neo4j connector config keys
- Wrong Cypher syntax
- Connector config changed but connector was not recreated
```

After fixing connector config, recreate the connector and run the parser again:

```bash
python -m src.parser_service.main --mode one --file src/accelerate/accelerator.py
```
