#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

mkdir -p evidence/logs
OUTPUT_FILE="evidence/logs/mongodb_indexes.log"
MONGODB_CONTAINER="${MONGODB_CONTAINER:-cpg-mongodb}"
MONGODB_DATABASE="${MONGODB_DATABASE:-cpg_lab}"
MONGODB_COLLECTION_METADATA="${MONGODB_COLLECTION_METADATA:-source_metadata}"

echo "Capturing indexes for ${MONGODB_DATABASE}.${MONGODB_COLLECTION_METADATA}"
docker exec "$MONGODB_CONTAINER" mongosh --quiet --eval \
  "db = db.getSiblingDB('${MONGODB_DATABASE}'); print(EJSON.stringify(db.getCollection('${MONGODB_COLLECTION_METADATA}').getIndexes(), null, 2));" \
  | tee "$OUTPUT_FILE"

echo "Saved MongoDB indexes to $OUTPUT_FILE"
