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

8. Keep Spark running, then perform a controlled modified-file replay. The replay deletes graph
   topology for only the probe file before publishing its replacement and upserts MongoDB by
   stable `metadata_id`.

   ```bash
   python -m src.verification.replay_one_file \
     --file src/accelerate/_lab_replay_probe.py --modify
   python -m src.verification.verify_mongodb_metadata \
     --file src/accelerate/_lab_replay_probe.py
   python -m src.verification.verify_neo4j_counts \
     --file src/accelerate/_lab_replay_probe.py
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

To generate timestamped text logs for a full demo run, first reset only the metadata demo state:

```bash
./scripts/reset_demo_state.sh
```

Then start Spark in Terminal 1 and keep it running:

```bash
./scripts/demo_terminal_1_spark.sh
```

Run the parser, verification, and replay workflow in Terminal 2:

```bash
./scripts/demo_terminal_2_run_pipeline.sh
```

Keep Terminal 1 running until Terminal 2 finishes the post-replay MongoDB and Neo4j checks. Stop
Spark with `Ctrl+C` only after Terminal 2 reports completion.

Raw logs are saved under `outputs/demo_logs/`; selected latest logs are copied to tracked
`evidence/logs/`. See the complete
[demo logging guide](markdowns/demo_logging.md) for the two-terminal workflow, replay behavior,
and expected results.

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
  --file src/accelerate/_lab_replay_probe.py --modify
```

Run checkpoint verification after restarting Spark with the same checkpoint and while no new
events are being published. Generate a controlled error first with
`python -m src.verification.emit_parser_error_sample` if the error topic is empty.

Published Jupyter Book: <https://thismacht.github.io/Streaming/>

When GitHub Actions deploys the site, `book/_build` is a local build artifact and should not be
committed.
