#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

mkdir -p outputs/demo_logs
timestamp="$(date +%Y%m%d_%H%M%S)"
log_file="outputs/demo_logs/reset_demo_state_${timestamp}.log"

exec > >(tee -a "$log_file") 2>&1

echo "Resetting demo metadata state..."
echo "Log file: $log_file"

echo "Clearing MongoDB metadata documents..."
docker exec -i cpg-mongodb mongosh --eval \
  'db = db.getSiblingDB("cpg_lab"); db.source_metadata.deleteMany({});'

echo "Removing the Spark metadata checkpoint..."
rm -rf outputs/checkpoints/mongodb_metadata

echo "Deleting Kafka metadata topic (it is safe if it does not exist)..."
docker exec cpg-kafka kafka-topics \
  --bootstrap-server kafka:29092 \
  --delete \
  --topic cpg.metadata.v1 || true

echo "Waiting for Kafka topic deletion..."
sleep 5

echo "Recreating Kafka metadata topic..."
docker exec cpg-kafka kafka-topics \
  --bootstrap-server kafka:29092 \
  --create --if-not-exists \
  --topic cpg.metadata.v1 \
  --partitions 3 \
  --replication-factor 1

echo "Kafka topics after reset:"
docker exec cpg-kafka kafka-topics \
  --bootstrap-server kafka:29092 \
  --list

echo "Demo metadata state reset complete. Neo4j data was not changed."
