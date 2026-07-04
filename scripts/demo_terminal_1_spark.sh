#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

mkdir -p outputs/demo_logs
mkdir -p evidence/logs
timestamp="$(date +%Y%m%d_%H%M%S)"
log_file="outputs/demo_logs/terminal_1_spark_${timestamp}.log"
evidence_file="evidence/logs/terminal_1_spark_latest.log"

copy_evidence_log() {
  cp "$log_file" "$evidence_file" 2>/dev/null || true
}
trap copy_evidence_log EXIT

source .venv/bin/activate
exec > >(tee -a "$log_file") 2>&1

echo "Starting Spark Structured Streaming metadata ingestion."
echo "Keep this terminal running while Terminal 2 publishes parser events."
echo "Press Ctrl+C to stop Spark."
echo "Log file: $log_file"
echo ""
echo "Keep Spark running through modified-file replay so MongoDB upsert can be verified."
echo "Stop it with Ctrl+C only after Terminal 2 has captured its final checks."

PYTHONPATH=. spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 \
  src/spark_jobs/metadata_to_mongodb.py
