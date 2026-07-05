#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

KAFKA_CONTAINER="${KAFKA_CONTAINER:-cpg-kafka}"
BOOTSTRAP_SERVER="${BOOTSTRAP_SERVER:-kafka:29092}"
OUTPUT_DIR="evidence/kafka"
LOG_DIR="evidence/logs"
LOG_FILE="$LOG_DIR/kafka_sample_capture.log"
mkdir -p "$OUTPUT_DIR" "$LOG_DIR"
: > "$LOG_FILE"
exec > >(tee -a "$LOG_FILE") 2>&1

capture_sample() {
  local topic="$1"
  local json_file="$2"
  local key_value_file="$3"
  local empty_message="$4"
  local temporary_file
  local key
  local value
  temporary_file="$(mktemp)"

  docker exec -i "$KAFKA_CONTAINER" kafka-console-consumer \
    --bootstrap-server "$BOOTSTRAP_SERVER" \
    --topic "$topic" \
    --from-beginning \
    --max-messages 1 \
    --timeout-ms 10000 \
    --property print.key=true \
    --property 'key.separator=|' > "$temporary_file" 2>/dev/null || true

  if [[ -s "$temporary_file" ]]; then
    # Kafka keys in this project are hashes or repo:path identities and never
    # contain "|". Split only the first separator so the JSON payload remains
    # byte-for-byte intact even if a string value contains another pipe.
    IFS='|' read -r key value < "$temporary_file"
    printf '%s\n' "$value" > "$json_file"
    printf 'key=%s\nvalue=%s\n' "$key" "$value" > "$key_value_file"
    if ! .venv/bin/python -m json.tool "$json_file" > /dev/null; then
      echo "Captured payload is not valid JSON: $json_file" >&2
      rm -f "$temporary_file"
      return 1
    fi
    echo "Captured $topic -> $json_file and $key_value_file"
  else
    printf '%s\n' "$empty_message" > "$json_file"
    printf 'key=<not captured>\nvalue=%s\n' "$empty_message" > "$key_value_file"
    echo "No message available for $topic"
  fi
  rm -f "$temporary_file"
}

capture_sample "cpg.nodes.v1" "$OUTPUT_DIR/nodes_sample.json" "$OUTPUT_DIR/node-sample.txt" \
  '{"error":"No message captured. Run the parser first."}'
capture_sample "cpg.edges.v1" "$OUTPUT_DIR/edges_sample.json" "$OUTPUT_DIR/edge-sample.txt" \
  '{"error":"No message captured. Run the parser first."}'
capture_sample "cpg.metadata.v1" "$OUTPUT_DIR/metadata_sample.json" "$OUTPUT_DIR/metadata-sample.txt" \
  '{"error":"No message captured. Run the parser while Spark is stopped."}'
capture_sample "cpg.errors.v1" "$OUTPUT_DIR/errors_sample.json" "$OUTPUT_DIR/error-sample.txt" \
  '{"error":"No message captured. Run python -m src.verification.emit_parser_error_sample first."}'

echo "Kafka sample capture complete:"
for sample in "$OUTPUT_DIR"/*_sample.json; do
  .venv/bin/python -m json.tool "$sample" > /dev/null
  echo "  $sample"
done
