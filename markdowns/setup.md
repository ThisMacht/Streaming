# Setup Guide

## 1. Create virtual environment

Using `uv`:

```bash
uv venv
source .venv/bin/activate
uv pip install -r requirements.txt
```

Or using `venv`:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 2. Create environment file

```bash
cp .env.example .env
```

## 3. Start and initialize infrastructure

For the first setup, run:

```bash
chmod +x scripts/*.sh
./scripts/init_infra.sh
```

This command performs the full infrastructure initialization:

1. Starts Docker containers
2. Creates Kafka topics
3. Initializes Neo4j constraints
4. Initializes MongoDB indexes
5. Registers the Neo4j Kafka Sink Connector

Use this command when setting up the project for the first time, or when local Docker data has been reset.

```bash
docker compose up -d
```

Then check the infrastructure:

```bash
./scripts/check_infra.sh
./scripts/capture_connect_status.sh
./scripts/capture_mongodb_indexes.sh
```

Run `./scripts/init_infra.sh` again only when one of the following happens:

- You removed Docker volumes with `docker compose down -v`
- You deleted `docker-data/`
- You changed Kafka topic configuration
- You changed the Neo4j connector configuration
- You are setting up the project from scratch again

Services:

| Service | URL |
|---|---|
| Neo4j Browser | http://localhost:7474 |
| Mongo Express | http://localhost:8081 |
| Kafka Connect REST | http://localhost:8083 |

Neo4j login:

```text
username: neo4j
password: password123
```

## 4. Manual infrastructure operations (optional)

The commands in the subsections below are already executed by `./scripts/init_infra.sh`. Run them
individually only when repairing or reconfiguring one component; they are not additional required
steps after a successful initialization.

### Initialize Neo4j constraints

```bash
cat config/neo4j/constraints.cypher | docker exec -i cpg-neo4j cypher-shell -u neo4j -p password123
```

### Initialize MongoDB indexes

```bash
docker exec -i cpg-mongodb mongosh < config/mongodb/indexes.js
```

### Register Neo4j Kafka Sink Connector

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  --data @config/kafka/connect-neo4j-sink.json
```

### Check connector status

```bash
curl http://localhost:8083/connectors
curl http://localhost:8083/connectors/neo4j-cpg-sink/status
```

## 5. Run the pipeline

Run these steps in order after infrastructure initialization:

```bash
python -m src.repo_tools.clone_repo
python -m src.repo_tools.discover_files
python -m src.parser_service.main --mode one --file src/accelerate/accelerator.py --dry-run
```

Start the Spark job in a separate terminal and keep it running:

```bash
PYTHONPATH=. spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 \
  src/spark_jobs/metadata_to_mongodb.py
```

Then publish parser events from another terminal and verify both databases:

```bash
python -m src.parser_service.main --mode one --file src/accelerate/accelerator.py
python -m src.verification.neo4j_checks
python -m src.verification.mongodb_checks
python -m src.verification.replay_one_file \
  --file src/accelerate/_lab_replay_probe.py --restore --dry-run
python -m src.repo_tools.discover_files
python -m src.parser_service.main --mode all
python -m src.verification.replay_one_file \
  --file src/accelerate/_lab_replay_probe.py --modify
```

Keep Spark running until the modified replay and both database verification commands finish.

## 6. Script reference and side effects

| Script | What it runs | Persistent side effects |
|---|---|---|
| `init_infra.sh` | Starts Compose, waits 20 seconds, creates topics, applies Neo4j constraints and MongoDB indexes, waits for Kafka Connect, then replaces/registers the Neo4j sink | Replaces the existing `neo4j-cpg-sink` connector configuration |
| `create_topics.sh` | Creates the four CPG topics with `--if-not-exists` and lists topics | Creates topics only; it does not delete messages |
| `check_infra.sh` | Prints containers, topics, connector list, and Neo4j connector status | Read-only, but topic/REST checks use `|| true`; inspect the output because a zero exit code alone does not prove readiness |
| `capture_connect_status.sh` | Captures connector list and Neo4j sink status JSON | Read-only; fails when Kafka Connect or the named connector is unavailable |
| `capture_mongodb_indexes.sh` | Captures `source_metadata.getIndexes()` | Read-only; fails when MongoDB is unavailable |
| `run_metadata_stream.sh` | Activates `.venv` and runs the Spark metadata stream | Writes MongoDB metadata and advances the Spark checkpoint |
| `demo_terminal_1_spark.sh` | Runs the same Spark job with timestamped terminal logging | Writes `outputs/demo_logs/` and copies the latest log to `evidence/logs/` |
| `demo_terminal_2_run_pipeline.sh` | Runs the 12-step parser, verification, replay, and error-event demo | Publishes Kafka events, updates both databases, changes then restores the replay probe, and writes evidence logs |
| `reset_demo_state.sh` | Clears MongoDB metadata, removes the metadata checkpoint, deletes and recreates only the metadata topic | Destructive for metadata demo state; does not clear Neo4j or node/edge/error topics |
| `stop_infra.sh` | Runs `docker compose down` | Stops/removes containers and network; bind-mounted `docker-data/` remains |

`run_metadata_stream.sh` and `demo_terminal_1_spark.sh` intentionally start the same Spark job.
Use the former for normal operation and the latter when a tracked demo log is required.

## 7. Stop services

```bash
./scripts/stop_infra.sh
```

## 8. Stop and remove local data

Use carefully:

```bash
docker compose down -v
rm -rf docker-data/
```
