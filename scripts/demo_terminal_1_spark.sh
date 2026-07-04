#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

mkdir -p outputs/demo_logs
timestamp="$(date +%Y%m%d_%H%M%S)"
log_file="outputs/demo_logs/terminal_1_spark_${timestamp}.log"

source .venv/bin/activate
exec > >(tee -a "$log_file") 2>&1

echo "Starting Spark Structured Streaming metadata ingestion."
echo "Keep this terminal running while Terminal 2 publishes parser events."
echo "Press Ctrl+C to stop Spark."
echo "Log file: $log_file"
echo ""
echo "For a clean replay log, stop Spark before replay. The append-mode MongoDB writer"
echo "may otherwise attempt a duplicate insert that the metadata_id unique index rejects."

PYTHONPATH=. spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 \
  src/spark_jobs/metadata_to_mongodb.py
