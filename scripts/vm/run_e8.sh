#!/usr/bin/env bash
# E8: system calls per file and Polars as a second engine.
# Usage: scripts/vm/run_e8.sh
set -euo pipefail
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$BASE_DIR"
RES="${RESULTS_ROOT:-results/local}"; export RESULTS_ROOT="$RES"

python3 02_run_benchmark.py --platform vm-vmware --run-id e8_mechanisms --sf 10 \
    --models A1,A4,A6,A7,B3,B4 \
    --queries A1:Q1,A1:Q4,A4:Q1,A4:Q4,A6:Q1,A6:Q4,A7:Q1,A7:Q4,B3:Q1_hive,B3:Q4_hive,B4:Q1_hive,B4:Q4_hive \
    --cold 5 --warm 0 --strace-runs 1

.venv/bin/python scripts/tools/polars_runner.py --sf 10 --models A1,A4,A6,A7,B3,B4 --cold 10 \
    --out $RES/vm/e8_mechanisms/polars_results.csv --platform vm-vmware
echo "E8 finished: $(date)"
