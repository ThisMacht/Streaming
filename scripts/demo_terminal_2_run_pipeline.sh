#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

mkdir -p outputs/demo_logs
timestamp="$(date +%Y%m%d_%H%M%S)"
log_file="outputs/demo_logs/terminal_2_pipeline_${timestamp}.log"

source .venv/bin/activate
exec > >(tee -a "$log_file") 2>&1

echo "Terminal 2 pipeline demo"
echo "Log file: $log_file"

echo ""
echo "Step 1/10: Check infrastructure"
./scripts/check_infra.sh

echo ""
echo "Step 2/10: Check Kafka topics"
docker exec cpg-kafka kafka-topics --bootstrap-server kafka:29092 --list

echo ""
echo "Step 3/10: Check Neo4j Kafka Sink Connector"
curl -fsS http://localhost:8083/connectors/neo4j-cpg-sink/status
echo ""

echo ""
echo "Step 4/10: Clone repository and discover files"
python -m src.repo_tools.clone_repo
python -m src.repo_tools.discover_files

echo ""
echo "Step 5/10: Dry-run one parser file"
python -m src.parser_service.main \
  --mode one \
  --file src/accelerate/accelerator.py \
  --dry-run

echo ""
echo "Step 6/10: Parse all files and publish events"
python -m src.parser_service.main --mode all

echo ""
echo "Step 7/10: Wait for Spark and sinks"
sleep 15

echo ""
echo "Step 8/10: Verify MongoDB and Neo4j"
python -m src.verification.mongodb_checks
python -m src.verification.neo4j_checks

echo ""
echo "Step 9/10: Replay verification"
echo "If Spark is still running in Terminal 1, replay may produce a duplicate metadata event."
echo "MongoDB unique index will reject it, which proves duplicates are prevented, but Spark may show Writing job failed."
echo "For a clean log, stop Spark before replay."
if [[ -t 0 ]]; then
  read -r -p "Stop Spark if desired, then press Enter to continue with replay... "
fi
python -m src.verification.replay_one_file --file src/accelerate/accelerator.py

echo ""
echo "Step 10/10: Final verification after replay"
python -m src.verification.mongodb_checks
python -m src.verification.neo4j_checks

echo ""
echo "Expected success evidence:"
echo "- Parser all: successful=120 failed=0"
echo "- MongoDB: metadata documents=120"
echo "- MongoDB: No duplicate metadata documents found"
echo "- Neo4j: CPG nodes=114772"
echo "- Neo4j: CPG edges=319284"
echo "- Replay: Count delta nodes=+0 edges=+0"
echo "- Replay: Duplicate count metadata_id=0 repo/file=0"
echo ""
echo "Pipeline demo complete. Log saved to $log_file"
