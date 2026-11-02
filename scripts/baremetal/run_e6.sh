#!/usr/bin/env bash
# E6: file-size sweep (512 MB to 1 MB) with the single file in one interleaved campaign.
# Usage: scripts/baremetal/run_e6.sh
set -euo pipefail
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$BASE_DIR"
RES="${RESULTS_ROOT:-results/local}"; export RESULTS_ROOT="$RES"
PLATFORM="${BENCH_PLATFORM:-baremetal}"
SF=10
EXP="$BASE_DIR/layouts/sf$SF"
STAGING="$(realpath "$BASE_DIR/data/sf$SF/lineitem_staging.parquet")"
INV="$RES/inventories/bm_inventory_v2.csv"
INV_FILES="$RES/inventories/bm_inventory_v2_files"
GEN_LOG="$RES/inventories/bm_generation_log.csv"
RID=bm_e6_size
OUT="$RES/baremetal/$RID"
mkdir -p "$OUT"

avail_kb=$(df -k --output=avail "$BASE_DIR" | tail -1)
(( avail_kb > 18 * 1024 * 1024 )) || { echo "need ~18 GB free, have $((avail_kb / 1024 / 1024)) GB" >&2; exit 1; }

STAGING_BYTES=$(stat -c %s "$STAGING")
MODELS=A1
CUSTOM_ARGS=()
declare -A EXPECTED=([A1]=1)
nfiles() { find "$1" -name '*.parquet' 2>/dev/null | wc -l; }
for mb in 512 256 128 64 32 16 8 4 1; do
    n=$(( (STAGING_BYTES + mb * 500000) / (mb * 1000000) ))
    m="E6_${mb}MB"
    MODELS+=",$m"; EXPECTED[$m]=$n
    [[ "$(nfiles "$EXP/$m")" -eq "$n" ]] || CUSTOM_ARGS+=(--custom "$m:flat:$n")
done

[[ "$(nfiles "$EXP/A1")" -eq 1 ]] || ./01_generate_layouts.sh --sf $SF --models A1 \
    --inventory "$INV" --inventory-files "$INV_FILES" --gen-log "$GEN_LOG"
if (( ${#CUSTOM_ARGS[@]} > 0 )); then
    ./01_generate_layouts.sh --sf $SF --force "${CUSTOM_ARGS[@]}" \
        --inventory "$INV" --inventory-files "$INV_FILES" --gen-log "$GEN_LOG"
fi

for m in "${!EXPECTED[@]}"; do
    got=$(nfiles "$EXP/$m")
    [[ "$got" -eq "${EXPECTED[$m]}" ]] || { echo "ERROR: $m has $got files, expected ${EXPECTED[$m]}" >&2; exit 1; }
done
scripts/setup/capture_env.sh "$OUT/env.txt"
echo "uptime since $(uptime -s); start $(date)"
python3 02_run_benchmark.py --run-id "$RID" --seed 606 --sf $SF \
    --models "$MODELS" --queries Q1,Q2a,Q4,Q5,Q6,Q7 \
    --cold 20 --warm 3 --threads 4 --memory-limit 10GB \
    --order interleaved --platform "$PLATFORM"

for mb in 512 256 128 64 32 16 8 4 1; do
    m="E6_${mb}MB"
    echo "delete $m: $(du -sb "$EXP/$m" | cut -f1) B, $(find "$EXP/$m" -type f | wc -l) files"
    rm -rf "${EXP:?}/$m"
done
df -h "$BASE_DIR"
echo "BM E6 finished: $(date)"
