# Incremental CPG Streaming Pipeline

This lab incrementally parses Python files from
[`huggingface/accelerate`](https://github.com/huggingface/accelerate) with the standard-library
AST parser. It publishes stable node, edge, metadata, and error events to Kafka. The Neo4j Kafka
Sink consumes graph events, while Spark Structured Streaming writes metadata through the MongoDB
Spark Connector.

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

2. Start and initialize Kafka, Neo4j, MongoDB, and Kafka Connect.

   For the first setup, run:

   ```bash
   chmod +x scripts/*.sh
   ./scripts/init_infra.sh
   ```

   This command starts Docker containers, creates Kafka topics, initializes Neo4j constraints,
   initializes MongoDB indexes, and registers the Neo4j Kafka Sink Connector.

   For later runs after restarting the machine, usually only start the containers again:

   ```bash
   docker compose up -d
   ```

   Then check the infrastructure:

   ```bash
   ./scripts/check_infra.sh
   ```

   Run `./scripts/init_infra.sh` again only if you reset Docker data, remove volumes, change Kafka
   topics, change Neo4j connector configuration, or set up the project from scratch.

3. Check the Neo4j Kafka Sink Connector.

   ```bash
   curl http://localhost:8083/connectors/neo4j-cpg-sink/status
   ```

   The connector and task should both show `RUNNING`.

   If the connector configuration is changed, recreate it:

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
   python -m src.parser_service.main --mode one --file src/accelerate/accelerator.py --dry-run
   ```

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

7. After one file works, run all discovered files.

   ```bash
   python -m src.parser_service.main --mode all
   python -m src.verification.neo4j_checks
   python -m src.verification.mongodb_checks
   ```

8. Modify or select a source file, then replay it to verify stable identifiers and unique indexes.

   ```bash
   python -m src.verification.replay_one_file --file src/accelerate/accelerator.py
   ```

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
* MongoDB may reject duplicate metadata during replay because `metadata_id` has a unique index.
* If Neo4j shows `0` nodes, check Kafka Connect logs:

  ```bash
  docker logs cpg-kafka-connect --tail=100
  ```

See [setup](markdowns/setup.md), [source usage](markdowns/src_usage.md), and
[architecture](markdowns/architecture.md) for details.
