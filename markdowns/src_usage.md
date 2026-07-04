# Source Code Usage

Run commands from the project root after copying `.env.example` to `.env` and starting the
infrastructure described in `markdowns/setup.md`.

## Recommended order

1. Create and activate the virtual environment, then install `requirements.txt`.
2. Copy `.env.example` to `.env`.
3. Start and initialize infrastructure with `./scripts/init_infra.sh`.
4. Clone Accelerate and discover its Python files.
5. Dry-run the parser.
6. Start the Spark metadata job in a separate terminal.
7. Run the parser without `--dry-run` from another terminal.
8. Verify Neo4j and MongoDB.
9. Modify or select one file and replay it.

## Task 1

```bash
python -m src.repo_tools.clone_repo
python -m src.repo_tools.discover_files
```

## Task 2 and 3

```bash
python -m src.parser_service.main --mode all --dry-run
python -m src.parser_service.main --mode all
```

## Replay one file

```bash
python -m src.parser_service.main --mode one --file src/accelerate/accelerator.py
```

## Spark metadata job

Start the Spark metadata job in a separate terminal before running the parser without
`--dry-run`. Keep it running while parser events are being produced.

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 \
  src/spark_jobs/metadata_to_mongodb.py
```

The metadata ingestion job uses Apache Spark Structured Streaming to consume metadata events
from Kafka topic `cpg.metadata.v1`. The parsed streaming DataFrame is written to MongoDB using
the MongoDB Spark Connector with `.writeStream.format("mongodb")`. A checkpoint directory is
configured so Spark can resume from the last committed Kafka offsets after restart.

## Verification

```bash
python -m src.verification.neo4j_checks
python -m src.verification.mongodb_checks
python -m src.verification.replay_one_file --file src/accelerate/accelerator.py
```
