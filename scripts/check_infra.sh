#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"
mkdir -p evidence/logs
LOG_FILE="evidence/logs/check_infra.log"
: > "$LOG_FILE"
exec > >(tee -a "$LOG_FILE") 2>&1

echo "Docker containers:"
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

echo ""
echo "Kafka topics:"
docker exec cpg-kafka kafka-topics --bootstrap-server kafka:29092 --list || true

echo ""
echo "Kafka Connect connectors:"
curl -s http://localhost:8083/connectors || true
echo ""

echo ""
echo "Neo4j connector status:"
curl -s http://localhost:8083/connectors/neo4j-cpg-sink/status || true
echo ""
