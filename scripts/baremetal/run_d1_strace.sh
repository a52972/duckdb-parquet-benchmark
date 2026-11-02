#!/usr/bin/env bash
# D1 system-call counts.
# Usage: scripts/baremetal/run_d1_strace.sh
set -euo pipefail
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$BASE_DIR"
RES="${RESULTS_ROOT:-results/local}"; export RESULTS_ROOT="$RES"
PLATFORM="${BENCH_PLATFORM:-baremetal}"
RID=bm_d1_strace
./01_generate_layouts.sh --sf 10 --force \
    --custom E5b_SY:hive:supp+ship_year --custom E5b_YS:hive:ship_year+supp \
    --inventory $RES/inventories/bm_d1_strace_inventory.csv \
    --inventory-files $RES/inventories/bm_d1_strace_inventory_files \
    --gen-log $RES/inventories/bm_generation_log.csv
find layouts/sf10/E5b_SY -type d | wc -l | sed 's/^/E5b_SY directories: /'
find layouts/sf10/E5b_YS -type d | wc -l | sed 's/^/E5b_YS directories: /'
python3 02_run_benchmark.py --run-id "$RID" --sf 10 --models E5b_SY,E5b_YS \
    --queries E5b_SY:Q3_Year,E5b_SY:Q3_Supp,E5b_SY:Q3_SuppYear,E5b_YS:Q3_Year,E5b_YS:Q3_Supp,E5b_YS:Q3_SuppYear \
    --cold 0 --warm 0 --strace-runs 3 --order interleaved --platform "$PLATFORM"
rm -rf layouts/sf10/E5b_SY layouts/sf10/E5b_YS
df -h /
echo "D1 strace finished: $(date)"
