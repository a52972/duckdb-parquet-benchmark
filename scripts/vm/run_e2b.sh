#!/usr/bin/env bash
# E2b: re-measures the original (v1.0) layouts.
# Usage: V1_EXPERIMENTS=<dir> scripts/vm/run_e2b.sh
set -euo pipefail
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$BASE_DIR"
RES="${RESULTS_ROOT:-results/local}"; export RESULTS_ROOT="$RES"

: "${V1_EXPERIMENTS:?set V1_EXPERIMENTS to the directory with the layouts written by release v1.0.0}"
V1="$V1_EXPERIMENTS"
COMMON=(--experiments-dir "$V1" --layout-label v1-default-writer
        --threads 4 --memory-limit 10GB --cold 5 --warm 0)

scripts/setup/capture_env.sh $RES/vm/e2b/env.txt

python3 02_run_benchmark.py --platform vm-vmware --run-id e2b "${COMMON[@]}" --strace-runs 1 \
    --sf 10 --models A5,A6 --queries Q1,Q4
python3 02_run_benchmark.py --platform vm-vmware --run-id e2b "${COMMON[@]}" --strace-runs 1 \
    --sf 1 --models A7 --queries Q1,Q4
python3 02_run_benchmark.py --platform vm-vmware --run-id e2b "${COMMON[@]}" --strace-runs 1 \
    --sf 10 --models B4 --queries Q1_hive,Q4_hive,Q3_B4_month

if [[ "${1:-}" == "--with-a7-sf10" ]]; then
    python3 02_run_benchmark.py --platform vm-vmware --run-id e2b_a7sf10 "${COMMON[@]}" --cold 3 \
        --sf 10 --models A7 --queries Q4 --no-baseline
fi
echo "E2b finished: $(date)"
