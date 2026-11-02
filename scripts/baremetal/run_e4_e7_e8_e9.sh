#!/usr/bin/env bash
# E4, E7, E8 and E9 on bare metal.
# Usage: setsid nohup scripts/baremetal/run_e4_e7_e8_e9.sh > e4_e9.log 2>&1 < /dev/null &
set -uo pipefail
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$BASE_DIR"
RES="${RESULTS_ROOT:-results/local}"; export RESULTS_ROOT="$RES"
export BENCH_PLATFORM="${BENCH_PLATFORM:-baremetal}"
INV=(--inventory $RES/inventories/bm_inventory_v2.csv
     --inventory-files $RES/inventories/bm_inventory_v2_files
     --gen-log $RES/inventories/bm_generation_log.csv)
RUN=(python3 02_run_benchmark.py --sf 10 --order interleaved --platform "$BENCH_PLATFORM")
EXP=layouts/sf10

say() { echo; echo "=== [$(date +%T)] $* ==="; df -h / | tail -1; }
delete() {  # layouts...
    for m in "$@"; do
        [[ -d "$EXP/$m" ]] || continue
        echo "delete $m: $(du -sb "$EXP/$m" | cut -f1) B, $(find "$EXP/$m" -type f | wc -l) files"
        rm -rf "${EXP:?}/$m"
    done
}
step() {  # name function
    say "$1 start"
    ( set -e; "$2" ); local rc=$?
    if [[ $rc -eq 0 ]]; then say "$1 OK"; else say "$1 FAILED (rc=$rc)"; fi
}

e4() {
    ./01_generate_layouts.sh --sf 10 "${INV[@]}" \
        --custom E4_F2526:flat:2526 --custom E4_D1:hive:ship_ym \
        --custom E4_D3:hive:ship_year+ship_quarter+ship_month \
        --custom E4_D2x10:hive:ship_year+ship_month:10 \
        --custom E4_D2x30:hive:ship_year+ship_month:30
    mkdir -p $RES/baremetal/bm_e4; scripts/setup/capture_env.sh $RES/baremetal/bm_e4/env.txt
    "${RUN[@]}" --run-id bm_e4 \
        --models E4_D1,B3,E4_D3,E4_D2x10,E4_D2x30,E4_F2526,B4 \
        --queries E4_D1:Q3_D1,B3:Q3_B3,E4_D3:Q3_D3,E4_D2x10:Q3_B3,E4_D2x30:Q3_B3,E4_F2526:Q3_B1,B4:Q3_B4_month,E4_D1:Q4_hive,B3:Q4_hive,E4_D3:Q4_hive,E4_D2x10:Q4_hive,E4_D2x30:Q4_hive,E4_F2526:Q4,B4:Q4_hive \
        --cold 20 --warm 3
    delete E4_F2526 E4_D1 E4_D3 E4_D2x10 E4_D2x30
}

e7() {
    ./01_generate_layouts.sh --sf 10 "${INV[@]}" \
        --custom A1s:sorted:1 --custom A4s:sorted:75 --custom A6s:sorted:750
    mkdir -p $RES/baremetal/bm_e7; scripts/setup/capture_env.sh $RES/baremetal/bm_e7/env.txt
    "${RUN[@]}" --run-id bm_e7 --models A1,A4,A6,A1s,A4s,A6s \
        --queries Q1,Q2a,Q2b,Q2c,Q5 --cold 20 --warm 3
    delete A1s A4s A6s
}

e8() {
    mkdir -p $RES/baremetal/bm_e8; scripts/setup/capture_env.sh $RES/baremetal/bm_e8/env.txt
    "${RUN[@]}" --run-id bm_e8 --models A1,A4,A6,A7,B3,B4 \
        --queries A1:Q1,A1:Q4,A4:Q1,A4:Q4,A6:Q1,A6:Q4,A7:Q1,A7:Q4,B3:Q1_hive,B3:Q4_hive,B4:Q1_hive,B4:Q4_hive \
        --cold 20 --warm 0 --strace-runs 1
    .venv/bin/python scripts/tools/polars_runner.py --sf 10 --models A1,A4,A6,A7,B3,B4 --cold 20 \
        --out $RES/baremetal/bm_e8/polars_results.csv --platform "$BENCH_PLATFORM"
}

e9() {
    local q="A1:Q1,A1:Q4,A4:Q1,A4:Q4,A6:Q1,A6:Q4,A7:Q1,A7:Q4,B3:Q1_hive,B3:Q4_hive,B4:Q1_hive,B4:Q4_hive"
    for cfg in "1 10" "2 10" "4 10" "4 8" "4 4"; do
        read -r t m <<< "$cfg"
        "${RUN[@]}" --run-id "bm_e9_t${t}_m${m}" --models A1,A4,A6,A7,B3,B4 --queries "$q" \
            --no-baseline --threads "$t" --memory-limit "${m}GB" --cold 20 --warm 0
    done
}

say "E4/E7/E8/E9 start (uptime since $(uptime -s); docker containers running: $(docker ps -q 2>/dev/null | wc -l))"
scripts/tools/cold_cache_test.sh data/sf10/lineitem_staging.parquet pre_bm4 \
    $RES/baremetal/bm_cold_cache/cold_cache.csv
step E4 e4
step E7 e7
step E8 e8
step E9 e9
say "E4/E7/E8/E9 finished"
