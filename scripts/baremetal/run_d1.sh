#!/usr/bin/env bash
# D1: high-cardinality top-level partition key (/supp/year/ vs /year/supp/).
# Usage: scripts/baremetal/run_d1.sh
set -euo pipefail
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$BASE_DIR"
RES="${RESULTS_ROOT:-results/local}"; export RESULTS_ROOT="$RES"
PLATFORM="${BENCH_PLATFORM:-baremetal}"
RID=bm_d1_keycard
./01_generate_layouts.sh --sf 10 \
    --custom E5b_SY:hive:supp+ship_year --custom E5b_YS:hive:ship_year+supp \
    --inventory $RES/inventories/bm_inventory_v2.csv \
    --inventory-files $RES/inventories/bm_inventory_v2_files \
    --gen-log $RES/inventories/bm_generation_log.csv
mkdir -p "$RES/baremetal/$RID"
scripts/setup/capture_env.sh "$RES/baremetal/$RID/env.txt"
python3 02_run_benchmark.py --run-id "$RID" --sf 10 --models E5b_SY,E5b_YS \
    --queries E5b_SY:Q3_Year,E5b_SY:Q3_Supp,E5b_SY:Q3_SuppYear,E5b_YS:Q3_Year,E5b_YS:Q3_Supp,E5b_YS:Q3_SuppYear \
    --cold 20 --warm 3 --strace-runs 1 --order interleaved --platform "$PLATFORM"
du -sb layouts/sf10/E5b_SY layouts/sf10/E5b_YS
find layouts/sf10/E5b_SY layouts/sf10/E5b_YS -type f | wc -l
rm -rf layouts/sf10/E5b_SY layouts/sf10/E5b_YS
df -h /
echo "D1 finished: $(date)"
