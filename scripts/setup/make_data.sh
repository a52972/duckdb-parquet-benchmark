#!/usr/bin/env bash
# TPC-H input data (lineitem and orders) with DuckDB's tpch extension.
# Usage: scripts/setup/make_data.sh [1] [10]
set -euo pipefail
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$BASE_DIR"
DUCKDB="$BASE_DIR/duckdb"
[[ -x "$DUCKDB" ]] || { echo "./duckdb not found (see README)" >&2; exit 1; }
SFS=("$@"); [[ ${#SFS[@]} -gt 0 ]] || SFS=(1 10)

for SF in "${SFS[@]}"; do
    DIR="data/sf$SF"; mkdir -p "$DIR" "$DIR/_tmp"
    STAGING="$DIR/lineitem_staging.parquet"; ORDERS="$DIR/orders.parquet"
    if [[ -f "$STAGING" && -f "$ORDERS" ]]; then
        echo "SF-$SF: data exists, skipping"; continue
    fi
    "$DUCKDB" -c "INSTALL tpch;" >/dev/null
    echo "SF-$SF: orders"
    "$DUCKDB" -bail -c "SET memory_limit='12GB'; SET temp_directory='$DIR/_tmp';
        LOAD tpch; CALL dbgen(sf=$SF);
        COPY orders TO '$ORDERS' (FORMAT PARQUET, COMPRESSION ZSTD);"
    echo "SF-$SF: lineitem"
    "$DUCKDB" -bail -c "SET memory_limit='12GB'; SET temp_directory='$DIR/_tmp';
        LOAD tpch; CALL dbgen(sf=$SF);
        COPY lineitem TO '$STAGING' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 1048576);"
    rm -rf "$DIR/_tmp"
    "$DUCKDB" -noheader -list -c "SELECT 'SF-$SF lineitem rows: ' || count(*) FROM read_parquet('$STAGING')"
done

if [[ -f data/FINGERPRINTS.csv ]]; then
    echo "Comparing the rows with the published input data (data/FINGERPRINTS.csv)"
    scripts/setup/fingerprint_data.sh --check
fi
if [[ -f data/SHA256SUMS ]]; then
    echo "Comparing the bytes with the published files (data/SHA256SUMS; a regenerated file can differ in bytes with the same rows)"
    sha256sum -c --ignore-missing data/SHA256SUMS || true
fi
