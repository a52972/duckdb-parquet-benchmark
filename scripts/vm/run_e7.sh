#!/usr/bin/env bash
# E7: rows sorted by l_shipdate vs unsorted.
# Usage: scripts/vm/run_e7.sh
set -euo pipefail
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$BASE_DIR"
RES="${RESULTS_ROOT:-results/local}"; export RESULTS_ROOT="$RES"

./01_generate_layouts.sh --sf 10 --inventory $RES/inventories/inventory_v2.csv --inventory-files $RES/inventories/inventory_v2_files --gen-log $RES/inventories/generation_log.csv \
    --custom A1s:sorted:1 --custom A4s:sorted:75 --custom A6s:sorted:750

python3 02_run_benchmark.py --platform vm-vmware --run-id e7_sorted --sf 10 \
    --models A1,A4,A6,A1s,A4s,A6s --queries Q1,Q2a,Q2b,Q2c,Q5 \
    --cold 20 --warm 3
echo "E7 finished: $(date)"
