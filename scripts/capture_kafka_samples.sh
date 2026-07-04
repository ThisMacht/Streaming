#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

KAFKA_CONTAINER="${KAFKA_CONTAINER:-cpg-kafka}"
BOOTSTRAP_SERVER="${BOOTSTRAP_SERVER:-kafka:29092}"
OUTPUT_DIR="evidence/kafka"
mkdir -p "$OUTPUT_DIR"

capture_sample() {
  local topic="$1"
  local output_file="$2"
  local empty_message="$3"
  local temporary_file
  temporary_file="$(mktemp)"

  docker exec -i "$KAFKA_CONTAINER" kafka-console-consumer \
    --bootstrap-server "$BOOTSTRAP_SERVER" \
    --topic "$topic" \
    --from-beginning \
    --max-messages 1 \
    --timeout-ms 10000 \
    --property print.key=true \
    --property 'key.separator= | ' > "$temporary_file" 2>/dev/null || true

  if [[ -s "$temporary_file" ]]; then
    cp "$temporary_file" "$output_file"
    echo "Captured $topic -> $output_file"
  else
    printf '%s\n' "$empty_message" > "$output_file"
    echo "No message available for $topic -> $output_file"
  fi
  rm -f "$temporary_file"
}

capture_sample "cpg.nodes.v1" "$OUTPUT_DIR/node-sample.txt" \
  "No message captured. Run the parser first."
capture_sample "cpg.edges.v1" "$OUTPUT_DIR/edge-sample.txt" \
  "No message captured. Run the parser first."
capture_sample "cpg.metadata.v1" "$OUTPUT_DIR/metadata-sample.txt" \
  "No message captured. Run the parser while Spark is stopped or use a separate evidence topic."
capture_sample "cpg.errors.v1" "$OUTPUT_DIR/error-sample.txt" \
  "No message captured. Run parser error sample first: python -m src.verification.emit_parser_error_sample"

echo "Kafka sample capture complete:"
for sample in "$OUTPUT_DIR"/*-sample.txt; do
  echo "  $sample"
done

