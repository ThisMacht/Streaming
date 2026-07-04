#!/usr/bin/env bash

set -e

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
