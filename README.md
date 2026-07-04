# Incremental CPG Streaming Pipeline

This lab incrementally parses Python files from
[`huggingface/accelerate`](https://github.com/huggingface/accelerate) with the standard-library
AST parser. It publishes stable node, edge, metadata, and error events to Kafka. The Neo4j Kafka
Sink consumes graph events, while Spark Structured Streaming writes metadata through the MongoDB
Spark Connector.

## Quick start

Run all commands from the project root.

1. Create the environment and install dependencies.

   ```bash
   uv venv
   source .venv/bin/activate
   uv pip install -r requirements.txt
   cp .env.example .env
   ```

2. Start and initialize Kafka, Neo4j, MongoDB, and Kafka Connect.

   For the first setup, run:

   ```bash
   chmod +x scripts/*.sh
   ./scripts/init_infra.sh
  ```
  
  This command starts Docker containers, creates Kafka topics, initializes Neo4j constraints, initializes MongoDB indexes, and registers the Neo4j Kafka Sink Connector.

  For later runs after restarting the machine, usually only start the containers again:

  ```
  docker compose up -d
  ```

  Then check the infrastructure:

  ```
  ./scripts/check_infra.sh
  ```

  Run `./scripts/init_infra.sh`again only if you reset Docker data, remove volumes, change Kafka topics, change Neo4j connector configuration, or set up the project from scratch.

3. Clone Accelerate, discover files, and dry-run one file.

   ```bash
   python -m src.repo_tools.clone_repo
   python -m src.repo_tools.discover_files
   python -m src.parser_service.main --mode one --file src/accelerate/accelerator.py --dry-run
   ```

4. In a separate terminal, start Spark before publishing real events. Keep this process running.

   ```bash
   source .venv/bin/activate
   spark-submit \
     --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 \
     src/spark_jobs/metadata_to_mongodb.py
   ```

5. In another terminal, publish events and run verification.

   ```bash
   source .venv/bin/activate
   python -m src.parser_service.main --mode one --file src/accelerate/accelerator.py
   python -m src.verification.neo4j_checks
   python -m src.verification.mongodb_checks
   ```

6. Modify or select a source file, then replay it to verify stable identifiers and unique indexes.

   ```bash
   python -m src.verification.replay_one_file --file src/accelerate/accelerator.py
   ```

See [setup](markdowns/setup.md), [source usage](markdowns/src_usage.md), and
[architecture](markdowns/architecture.md) for details.
