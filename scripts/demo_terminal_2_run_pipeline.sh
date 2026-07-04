#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

mkdir -p outputs/demo_logs evidence/logs
timestamp="$(date +%Y%m%d_%H%M%S)"
log_file="outputs/demo_logs/terminal_2_pipeline_${timestamp}.log"
evidence_file="evidence/logs/terminal_2_pipeline_latest.log"

copy_evidence_log() {
  cp "$log_file" "$evidence_file" 2>/dev/null || true
}
trap copy_evidence_log EXIT

source .venv/bin/activate
exec > >(tee -a "$log_file") 2>&1

probe="src/accelerate/_lab_replay_probe.py"

echo "Terminal 2 pipeline demo"
echo "Log file: $log_file"
echo "Keep Terminal 1 Spark streaming active through Step 10."

echo ""
echo "Step 1/12: Check infrastructure, topics, and Neo4j connector"
./scripts/check_infra.sh

echo ""
echo "Step 2/12: Clone repository and prepare the baseline replay probe"
python -m src.repo_tools.clone_repo
python -m src.verification.replay_one_file --file "$probe" --restore --dry-run

echo ""
echo "Step 3/12: Discover Python files including the baseline probe"
python -m src.repo_tools.discover_files

echo ""
echo "Step 4/12: Dry-run the small replay probe"
python -m src.parser_service.main --mode one --file "$probe" --dry-run

echo ""
echo "Step 5/12: Parse all files and publish baseline events"
python -m src.parser_service.main --mode all

echo ""
echo "Step 6/12: Wait for Spark and Kafka sinks"
sleep 15

echo ""
echo "Step 7/12: Verify baseline MongoDB and Neo4j state"
python -m src.verification.verify_mongodb_metadata --file "$probe"
python -m src.verification.verify_neo4j_counts --file "$probe"

echo ""
echo "Step 8/12: Modify, clean, and replay only the controlled probe"
python -m src.verification.replay_one_file \
  --file "$probe" \
  --modify \
  --cleanup-neo4j-before-replay \
  --wait-seconds 10

echo ""
echo "Step 9/12: Verify post-replay upsert and graph replacement"
python -m src.verification.verify_mongodb_metadata --file "$probe"
python -m src.verification.verify_neo4j_counts --file "$probe"

echo ""
echo "Step 10/12: Publish and inspect one controlled parser error"
python -m src.verification.emit_parser_error_sample
docker exec cpg-kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic cpg.errors.v1 \
  --from-beginning \
  --max-messages 1 \
  --timeout-ms 10000 || true

echo ""
echo "Step 11/12: Restore the probe source for the next demo"
python -m src.verification.replay_one_file --file "$probe" --restore --dry-run

echo ""
echo "Step 12/12: Preserve selected evidence"
copy_evidence_log
echo "Tracked evidence copy: $evidence_file"
echo "Pipeline demo complete. You may now stop Spark in Terminal 1 with Ctrl+C."
