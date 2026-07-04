#!/usr/bin/env bash

set -e

echo "Starting Docker services..."
docker compose up -d

echo "Waiting for services to start..."
sleep 20

./scripts/create_topics.sh

echo "Initializing Neo4j constraints..."
cat config/neo4j/constraints.cypher | docker exec -i cpg-neo4j cypher-shell -u neo4j -p password123

echo "Initializing MongoDB indexes..."
docker exec -i cpg-mongodb mongosh < config/mongodb/indexes.js

echo "Waiting for Kafka Connect REST API..."
connect_ready=false
for _ in $(seq 1 36); do
  if curl -fsS http://localhost:8083/connector-plugins > /dev/null; then
    connect_ready=true
    break
  fi
  sleep 5
done

if [[ "$connect_ready" != "true" ]]; then
  echo "Kafka Connect did not become ready within 180 seconds." >&2
  exit 1
fi

echo "Registering Neo4j Kafka Sink Connector..."
if curl -fsS http://localhost:8083/connectors/neo4j-cpg-sink > /dev/null; then
  echo "Replacing existing connector so the checked-in Cypher configuration is active."
  curl -fsS -X DELETE http://localhost:8083/connectors/neo4j-cpg-sink
fi
curl -fsS -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  --data @config/kafka/connect-neo4j-sink.json
echo ""

echo ""
echo "Infrastructure initialized."
echo "Neo4j: http://localhost:7474"
echo "Mongo Express: http://localhost:8081"
echo "Kafka Connect: http://localhost:8083"
