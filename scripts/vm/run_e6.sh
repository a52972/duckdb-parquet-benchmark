#!/usr/bin/env bash
# E6: file-size sweep and writer defaults.
# Usage: scripts/vm/run_e6.sh
set -euo pipefail
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$BASE_DIR"
RES="${RESULTS_ROOT:-results/local}"; export RESULTS_ROOT="$RES"

SF=10
EXP="$BASE_DIR/layouts/sf$SF"
STAGING="$BASE_DIR/data/sf$SF/lineitem_staging.parquet"
INV="$BASE_DIR/$RES/inventories/inventory_v2.csv"
INV_FILES="$BASE_DIR/$RES/inventories/inventory_v2_files"
RUN_ID=e6_size
QUERIES=Q1,Q2a,Q4,Q5,Q6,Q7
DONE_LIST="$BASE_DIR/$RES/vm/$RUN_ID/_layouts_done.txt"
mkdir -p "$(dirname "$DONE_LIST")"; touch "$DONE_LIST"
STAGING_BYTES=$(stat -c %s "$(realpath "$STAGING")")

measure() {  # model
    python3 02_run_benchmark.py --platform vm-vmware --run-id "$RUN_ID" --sf $SF --models "$1" \
        --queries "$QUERIES" --cold 20 --warm 3
}
done_before() { grep -qx "$1" "$DONE_LIST"; }
finish() {  # model [delete]
    echo "$1" >> "$DONE_LIST"
    if [[ "${2:-}" == delete ]]; then rm -rf "${EXP:?}/$1"; fi
    echo "[$(date +%T)] $1 done; $(df -h / | tail -1)"
}
inventory() {  # model
    awk -F, -v s="$SF" -v m="$1" 'NR==1 || !($1==s && $2==m)' "$INV" > "$INV.tmp_" && mv "$INV.tmp_" "$INV"
    scripts/tools/inventory.sh "$EXP/$1" "$SF" "$1" "$INV" "$INV_FILES"
}

done_before A1 || { measure A1; finish A1; }

for mb in 512 256 128 64 32 16 8 4 1; do
    m="E6_${mb}MB"
    done_before "$m" && continue
    n=$(( (STAGING_BYTES + mb * 500000) / (mb * 1000000) ))
    ./01_generate_layouts.sh --sf $SF --inventory $RES/inventories/inventory_v2.csv --inventory-files $RES/inventories/inventory_v2_files --gen-log $RES/inventories/generation_log.csv --force --custom "$m:flat:$n"
    measure "$m"; finish "$m" delete
done

m=E6_duckdb_fsb128
if ! done_before $m; then
    rm -rf "${EXP:?}/$m"
    /usr/bin/time -f "%e s, peak RSS %M KB" ./duckdb -bail -c \
        "SET threads=4; SET memory_limit='8GB'; COPY (SELECT * FROM read_parquet('$STAGING')) TO '$EXP/$m' (FORMAT PARQUET, FILE_SIZE_BYTES '128MB');"
    inventory $m
    measure $m; finish $m delete
fi

m=E6_polars_default
if ! done_before $m; then
    rm -rf "${EXP:?}/$m"; mkdir -p "$EXP/$m"
    /usr/bin/time -f "%e s, peak RSS %M KB" .venv/bin/python -c \
        "import polars as pl; pl.scan_parquet('$STAGING').sink_parquet('$EXP/$m/data.parquet'); print('polars', pl.__version__)"
    inventory $m
    measure $m; finish $m delete
fi
echo "E6 finished: $(date)"
