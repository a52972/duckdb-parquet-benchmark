#!/usr/bin/env bash
# E4: hierarchy depth vs number of files vs number of folders.
# Usage: scripts/vm/run_e4.sh
set -euo pipefail
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$BASE_DIR"
RES="${RESULTS_ROOT:-results/local}"; export RESULTS_ROOT="$RES"

./01_generate_layouts.sh --sf 10 --inventory $RES/inventories/inventory_v2.csv --inventory-files $RES/inventories/inventory_v2_files --gen-log $RES/inventories/generation_log.csv \
    --custom E4_F2526:flat:2526 \
    --custom E4_D1:hive:ship_ym \
    --custom E4_D3:hive:ship_year+ship_quarter+ship_month \
    --custom E4_D2x10:hive:ship_year+ship_month:10 \
    --custom E4_D2x30:hive:ship_year+ship_month:30

python3 02_run_benchmark.py --platform vm-vmware --run-id e4_depth_count --sf 10 \
    --models E4_D1,B3,E4_D3,E4_D2x10,E4_D2x30,E4_F2526,B4 \
    --queries E4_D1:Q3_D1,B3:Q3_B3,E4_D3:Q3_D3,E4_D2x10:Q3_B3,E4_D2x30:Q3_B3,E4_F2526:Q3_B1,B4:Q3_B4_month,E4_D1:Q4_hive,B3:Q4_hive,E4_D3:Q4_hive,E4_D2x10:Q4_hive,E4_D2x30:Q4_hive,E4_F2526:Q4,B4:Q4_hive \
    --cold 20 --warm 3
echo "E4 finished: $(date)"
