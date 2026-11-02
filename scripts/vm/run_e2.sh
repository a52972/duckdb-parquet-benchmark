#!/usr/bin/env bash
# One core campaign on the VM (SF-1 and SF-10, 20 cold + 3 warm).
# Usage: scripts/vm/run_e2.sh [run_id] [seed]
set -euo pipefail
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$BASE_DIR"
RES="${RESULTS_ROOT:-results/local}"; export RESULTS_ROOT="$RES"
RUN_ID="${1:-core_v2}"
SEED="${2:-20261026}"
mkdir -p "$RES/vm/$RUN_ID"
scripts/setup/capture_env.sh "$RES/vm/$RUN_ID/env.txt"
python3 02_run_benchmark.py --platform vm-vmware --run-id "$RUN_ID" --seed "$SEED" --sf 1 --sf 10 \
    --cold 20 --warm 3 --threads 4 --memory-limit 10GB
echo "E2 $RUN_ID finished: $(date)"
