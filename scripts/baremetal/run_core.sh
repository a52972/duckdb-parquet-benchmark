#!/usr/bin/env bash
# One core campaign on bare metal (SF-1 and SF-10, 20 cold + 3 warm, interleaved).
# Usage: scripts/baremetal/run_core.sh <run_id> <seed>
set -euo pipefail
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$BASE_DIR"
RES="${RESULTS_ROOT:-results/local}"; export RESULTS_ROOT="$RES"
RUN_ID="${1:?run_id}"
SEED="${2:?seed}"
PLATFORM="${BENCH_PLATFORM:-baremetal}"
mkdir -p "$RES/baremetal/$RUN_ID"
echo "uptime since $(uptime -s); start $(date)"
scripts/setup/capture_env.sh "$RES/baremetal/$RUN_ID/env.txt"
python3 02_run_benchmark.py --run-id "$RUN_ID" --seed "$SEED" --sf 1 --sf 10 \
    --cold 20 --warm 3 --threads 4 --memory-limit 10GB \
    --order interleaved --platform "$PLATFORM"
echo "BM core $RUN_ID finished: $(date)"
