#!/usr/bin/env bash
# D2: codec, row-group size and file size of real writers.
# Usage: scripts/baremetal/run_d2.sh
set -euo pipefail
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$BASE_DIR"
RES="${RESULTS_ROOT:-results/local}"; export RESULTS_ROOT="$RES"
PLATFORM="${BENCH_PLATFORM:-baremetal}"
SF=10
EXP="$BASE_DIR/layouts/sf$SF"
STAGING="$(realpath "$BASE_DIR/data/sf$SF/lineitem_staging.parquet")"
INV="$BASE_DIR/$RES/inventories/bm_inventory_v2.csv"
INV_FILES="$BASE_DIR/$RES/inventories/bm_inventory_v2_files"
RID=bm_d2_writers
OUT="$BASE_DIR/$RES/baremetal/$RID"
mkdir -p "$OUT"
WLOG="$OUT/writers_log.txt"

./01_generate_layouts.sh --sf $SF --custom D2_gen128:flat:12 \
    --inventory "$INV" --inventory-files "$INV_FILES" \
    --gen-log $RES/inventories/bm_generation_log.csv

duck_copy() {  # model extra-options
    rm -rf "${EXP:?}/$1"
    echo "$1: COPY ... (FORMAT PARQUET, FILE_SIZE_BYTES '128MB'$2)" | tee -a "$WLOG"
    /usr/bin/time -f "$1: %e s, peak RSS %M KB" -a -o "$WLOG" ./duckdb -bail -c \
        "SET threads=4; SET memory_limit='8GB'; COPY (SELECT * FROM read_parquet('$STAGING')) TO '$EXP/$1' (FORMAT PARQUET, FILE_SIZE_BYTES '128MB'$2);"
    scripts/tools/inventory.sh "$EXP/$1" "$SF" "$1" "$INV" "$INV_FILES"
}
duck_copy D2_duck_default ""
duck_copy D2_duck_zstd ", COMPRESSION ZSTD"
duck_copy D2_duck_zstd_rg1m ", COMPRESSION ZSTD, ROW_GROUP_SIZE 1048576"

m=D2_polars
rm -rf "${EXP:?}/$m"; mkdir -p "$EXP/$m"
/usr/bin/time -f "$m: %e s, peak RSS %M KB" -a -o "$WLOG" .venv/bin/python -c \
    "import polars as pl; pl.scan_parquet('$STAGING').sink_parquet('$EXP/$m/data.parquet'); print('polars', pl.__version__)" | tee -a "$WLOG"
scripts/tools/inventory.sh "$EXP/$m" "$SF" "$m" "$INV" "$INV_FILES"

./duckdb -csv -c "
SELECT regexp_extract(file_name, 'sf10/([^/]+)/', 1) AS model,
       count(DISTINCT file_name) AS files,
       string_agg(DISTINCT compression, '+') AS codecs,
       count(*) FILTER (WHERE column_id = 0) AS row_groups,
       round(avg(row_group_num_rows) FILTER (WHERE column_id = 0)) AS rg_rows_mean,
       min(row_group_num_rows) FILTER (WHERE column_id = 0) AS rg_rows_min,
       max(row_group_num_rows) FILTER (WHERE column_id = 0) AS rg_rows_max,
       sum(total_compressed_size) AS compressed_bytes,
       sum(total_uncompressed_size) AS uncompressed_bytes
FROM parquet_metadata(['$EXP/A1/*.parquet', '$EXP/D2_gen128/*.parquet',
                       '$EXP/D2_duck_default/*.parquet', '$EXP/D2_duck_zstd/*.parquet',
                       '$EXP/D2_duck_zstd_rg1m/*.parquet', '$EXP/D2_polars/*.parquet'])
GROUP BY 1 ORDER BY 1;" | tee "$OUT/writers_metadata.csv"
./duckdb -csv -noheader -c "SELECT DISTINCT created_by FROM parquet_file_metadata('$EXP/D2_polars/*.parquet')" | sed 's/^/D2_polars created_by: /' | tee -a "$WLOG"

scripts/setup/capture_env.sh "$OUT/env.txt"
python3 02_run_benchmark.py --run-id "$RID" --sf $SF \
    --models A1,D2_gen128,D2_duck_default,D2_duck_zstd,D2_duck_zstd_rg1m,D2_polars \
    --queries Q1,Q4,Q7 --cold 20 --warm 3 --order interleaved --platform "$PLATFORM"

for m in D2_gen128 D2_duck_default D2_duck_zstd D2_duck_zstd_rg1m D2_polars; do
    echo "delete $m: $(du -sb "$EXP/$m" | cut -f1) B, $(find "$EXP/$m" -type f | wc -l) files"
    rm -rf "${EXP:?}/$m"
done
df -h /
echo "D2 finished: $(date)"
