# Demo Evidence

`logs/` contains selected terminal evidence copied by the two demo scripts. Raw timestamped logs
remain under the ignored `outputs/demo_logs/` directory. Review curated logs for secrets or local
paths before committing them.

`screenshots/` is reserved for selected Neo4j Browser, MongoDB, and Spark UI images used by the
final Jupyter Book.

Infrastructure capture scripts produce these non-visual artifacts:

- `logs/create_topics.log`
- `logs/kafka_connectors_list.json`
- `logs/kafka_connect_status.json`
- `logs/mongodb_indexes.log`
- `logs/kafka_sample_capture.log`

Kafka samples retain raw JSON in `kafka/*_sample.json` and explicit `key=...` / `value=...`
companions in `kafka/*-sample.txt`. Generate these files from live services; do not fabricate
runtime evidence when infrastructure is unavailable.
