#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

mkdir -p evidence/logs
STATUS_FILE="evidence/logs/kafka_connect_status.json"
LIST_FILE="evidence/logs/kafka_connectors_list.json"
CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
CONNECTOR_NAME="${CONNECTOR_NAME:-neo4j-cpg-sink}"

echo "Capturing Kafka Connect connector list from $CONNECT_URL"
curl -fsS "$CONNECT_URL/connectors" | tee "$LIST_FILE"
printf '\n'

echo "Capturing status for connector $CONNECTOR_NAME"
curl -fsS "$CONNECT_URL/connectors/$CONNECTOR_NAME/status" | tee "$STATUS_FILE"
printf '\n'

echo "Saved connector list to $LIST_FILE"
echo "Saved connector status to $STATUS_FILE"
