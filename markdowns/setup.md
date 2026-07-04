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

```bassh
./scripts/check_infra.sh
```

Run ./scripts/init_infra.sh again only when one of the following happens:

- You removed Docker volumes with docker compose down -v
- You deleted docker-data/
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

## 4. Initialize Neo4j constraints

```bash
cat config/neo4j/constraints.cypher | docker exec -i cpg-neo4j cypher-shell -u neo4j -p password123
```

## 5. Initialize MongoDB indexes

```bash
docker exec -i cpg-mongodb mongosh < config/mongodb/indexes.js
```

## 6. Register Neo4j Kafka Sink Connector

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  --data @config/kafka/connect-neo4j-sink.json
```

## 7. Check connector status

```bash
curl http://localhost:8083/connectors
curl http://localhost:8083/connectors/neo4j-cpg-sink/status
```

## 8. Run the pipeline

Run these steps in order after infrastructure initialization:

```bash
python -m src.repo_tools.clone_repo
python -m src.repo_tools.discover_files
python -m src.parser_service.main --mode one --file src/accelerate/accelerator.py --dry-run
```

Start the Spark job in a separate terminal and keep it running:

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
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

## 9. Stop services

```bash
./scripts/stop_infra.sh
```

## 10. Stop and remove local data

Use carefully:

```bash
docker compose down -v
rm -rf docker-data/
```
