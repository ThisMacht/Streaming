#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"
mkdir -p evidence/logs
LOG_FILE="evidence/logs/create_topics.log"
: > "$LOG_FILE"
exec > >(tee -a "$LOG_FILE") 2>&1

echo "Creating Kafka topics..."

docker exec cpg-kafka kafka-topics \
  --bootstrap-server kafka:29092 \
  --create --if-not-exists \
  --topic cpg.nodes.v1 \
  --partitions 3 \
  --replication-factor 1

docker exec cpg-kafka kafka-topics \
  --bootstrap-server kafka:29092 \
  --create --if-not-exists \
  --topic cpg.edges.v1 \
  --partitions 3 \
  --replication-factor 1

docker exec cpg-kafka kafka-topics \
  --bootstrap-server kafka:29092 \
  --create --if-not-exists \
  --topic cpg.metadata.v1 \
  --partitions 3 \
  --replication-factor 1

docker exec cpg-kafka kafka-topics \
  --bootstrap-server kafka:29092 \
  --create --if-not-exists \
  --topic cpg.errors.v1 \
  --partitions 1 \
  --replication-factor 1

echo "Kafka topics:"
docker exec cpg-kafka kafka-topics \
  --bootstrap-server kafka:29092 \
  --list
