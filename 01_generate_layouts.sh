#!/usr/bin/env bash
# Usage: ./01_generate_layouts.sh [SF]   (SF = 1 or 10, default 10)
set -euo pipefail

SF="${1:-10}"
[[ "$SF" == "1" || "$SF" == "10" ]] || { echo "SF must be 1 or 10"; exit 1; }

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$BASE_DIR"

DUCKDB="$BASE_DIR/duckdb"
DATA_DIR="$BASE_DIR/data/sf${SF}"
EXP_DIR="$BASE_DIR/experiments/sf${SF}"
STAGING="$DATA_DIR/lineitem_staging.parquet"
ORDERS="$DATA_DIR/orders.parquet"
SPILL="$DATA_DIR/duckdb_spill_gen"

mkdir -p "$DATA_DIR" "$EXP_DIR" "$SPILL"
trap 'rm -rf "$SPILL"' EXIT

log()  { echo "[$(date +'%H:%M:%S')] $*"; }
die()  { echo "ERROR: $*" >&2; exit 1; }

# Run DuckDB with -c flag (NOT stdin redirect).
# CRITICAL: DuckDB CLI in stdin mode (duckdb < file) does NOT stop on SQL
# errors — it continues past failed statements and exits 0, silently producing
# no output files.  The -c flag executes the SQL as a batch, stops immediately
# on any error, and returns a non-zero exit code.  All DuckDB calls in this
# script use this wrapper.
run_sql() {
    local sql="$1"
    "$DUCKDB" -c "$sql" || return 1
}

rowcount() {
    "$DUCKDB" -noheader -csv \
        -c "SELECT COUNT(*) FROM read_parquet('$1')"
}

# ── Generate flat layout using hash-based partitioning
generate_flat() {
    local src="$1" dst="$2" n="$3"
    mkdir -p "$dst"

    log "  → flat (hash-partitioned): $n files"

    run_sql "
SET memory_limit='12GB';
SET temp_directory='${SPILL}';
COPY (
    SELECT *,
        (abs(hash(l_orderkey * 10000 + l_linenumber)) % ${n})::INTEGER AS _fid
    FROM read_parquet('${src}')
) TO '${dst}'
(FORMAT PARQUET,
 PARTITION_BY (_fid),
 WRITE_PARTITION_COLUMNS false,
 COMPRESSION ZSTD,
 ROW_GROUP_SIZE 1048576);
" || die "flat layout generation failed for $dst"

    # Flatten: move files out of _fid=N/ subdirs into dst directly
    local counter=0
    while IFS= read -r f; do
        mv "$f" "$dst/part_$(printf '%06d' $counter).parquet"
        counter=$(( counter + 1 ))
    done < <(find "$dst" -mindepth 2 -name "*.parquet" | sort)
    find "$dst" -mindepth 1 -type d -empty -delete 2>/dev/null || true

    local actual; actual=$(find "$dst" -maxdepth 1 -name "*.parquet" | wc -l)
    log "  → done: $actual files in $(du -sh "$dst" | cut -f1)"
}

# ── Generate Hive-partitioned layout
generate_hive() {
    local src="$1" dst="$2" partition_cols="$3"
    mkdir -p "$dst"

    log "  → hive PARTITION BY ($partition_cols)"

    run_sql "
SET memory_limit='12GB';
SET temp_directory='${SPILL}';
COPY (
    SELECT *,
        date_part('year',  l_shipdate)::INTEGER AS ship_year,
        date_part('month', l_shipdate)::INTEGER AS ship_month,
        date_part('day',   l_shipdate)::INTEGER AS ship_day
    FROM read_parquet('${src}')
) TO '${dst}'
(FORMAT PARQUET,
 PARTITION_BY (${partition_cols}),
 COMPRESSION ZSTD,
 ROW_GROUP_SIZE 1048576);
" || die "hive layout generation failed for $dst"

    local actual; actual=$(find "$dst" -name "*.parquet" | wc -l)
    log "  → done: $actual files in $(du -sh "$dst" | cut -f1)"
}

# ── Skip helper
already_done() {
    local dir="$1"
    [[ -d "$dir" ]] && [[ -n "$(find "$dir" -name '*.parquet' 2>/dev/null | head -1)" ]]
}

# ═══════════════════════════════════════════════════════════════════════════════
# PHASE 0 — TPC-H data generation
# ═══════════════════════════════════════════════════════════════════════════════
log "=== PHASE 0: TPC-H SF-$SF ==="

if [[ -f "$STAGING" ]] && [[ -f "$ORDERS" ]]; then
    log "Staging files exist — skipping TPC-H generation."
else
    # WHY LOAD NOT INSTALL:
    # 00_check_env.sh already ran INSTALL tpch for this DuckDB version.
    # Using INSTALL again causes DuckDB to attempt a network check for
    # extension updates. If the VM has no internet access this fails
    # silently. LOAD tpch uses the already-installed local copy directly.
    #
    # WHY ORDERS BEFORE LINEITEM:
    # Orders is small (~25 MB at SF-1, ~200 MB at SF-10). Exporting it
    # first frees the orders table from the buffer pool before the large
    # lineitem export begins.

    log "Exporting orders table…"
    run_sql "
SET memory_limit='12GB';
SET temp_directory='${SPILL}';
LOAD tpch;
CALL dbgen(sf=${SF});
COPY orders TO '${ORDERS}' (FORMAT PARQUET, COMPRESSION ZSTD);
" || die "orders export failed"
    [[ -f "$ORDERS" ]] || die "orders.parquet was not created — check disk space and DuckDB output above"
    log "  → orders: $(du -sh "$ORDERS" | cut -f1)"

    # Lineitem is exported in a separate run_sql call.
    # Both calls share NO session state — dbgen must be called again.
    # This is intentional: each run_sql is a fresh in-memory DuckDB process.
    # The cost is running dbgen twice, which takes ~5-10 seconds at SF-1
    # and ~60-90 seconds at SF-10 but is completely reliable.
    log "Exporting lineitem table…"
    run_sql "
SET memory_limit='12GB';
SET temp_directory='${SPILL}';
LOAD tpch;
CALL dbgen(sf=${SF});
COPY lineitem TO '${STAGING}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 1048576);
" || die "lineitem export failed"
    [[ -f "$STAGING" ]] || die "lineitem_staging.parquet was not created — check disk space and DuckDB output above"
    log "  → lineitem: $(du -sh "$STAGING" | cut -f1)"

    log "TPC-H data ready."
fi

TOTAL=$(rowcount "$STAGING")
log "Total lineitem rows: $TOTAL"

# ═══════════════════════════════════════════════════════════════════════════════
# PHASE 1 — Flat models
# ═══════════════════════════════════════════════════════════════════════════════
log ""
log "=== PHASE 1: Flat models ==="

# A1 — monolithic (staging file is already a single Parquet file)
if already_done "$EXP_DIR/A1"; then
    log "A1 exists — skipping."
else
    log "A1 (1 monolithic file)…"
    mkdir -p "$EXP_DIR/A1"
    cp "$STAGING" "$EXP_DIR/A1/lineitem.parquet"
    log "  → done: $(du -sh "$EXP_DIR/A1" | cut -f1)"
fi

# A2 — ~950 MB (SF-10 only; at SF-1 essentially the same as A1)
if [[ "$SF" == "10" ]]; then
    already_done "$EXP_DIR/A2" && log "A2 exists — skipping." \
        || { log "A2 (~8 files @ ~950 MB)…"
             generate_flat "$STAGING" "$EXP_DIR/A2" 8; }
fi

# A3 — ~200 MB  (SF-10: ~38 files | SF-1: ~4 files)
N_A3=38; [[ "$SF" == "1" ]] && N_A3=4
already_done "$EXP_DIR/A3" && log "A3 exists — skipping." \
    || { log "A3 (~200 MB files, ~$N_A3 files)…"
         generate_flat "$STAGING" "$EXP_DIR/A3" $N_A3; }

# A4 — ~100 MB  (SF-10: ~75 files | SF-1: ~8 files)  ← also used as B1 control
N_A4=75; [[ "$SF" == "1" ]] && N_A4=8
already_done "$EXP_DIR/A4" && log "A4 exists — skipping." \
    || { log "A4 (~100 MB files, ~$N_A4 files)…"
         generate_flat "$STAGING" "$EXP_DIR/A4" $N_A4; }

# A5 — ~50 MB   (SF-10: ~150 files | SF-1: ~15 files)
N_A5=150; [[ "$SF" == "1" ]] && N_A5=15
already_done "$EXP_DIR/A5" && log "A5 exists — skipping." \
    || { log "A5 (~50 MB files, ~$N_A5 files)…"
         generate_flat "$STAGING" "$EXP_DIR/A5" $N_A5; }

# A6 — ~10 MB   (SF-10: ~750 files | SF-1: ~75 files)
N_A6=750; [[ "$SF" == "1" ]] && N_A6=75
already_done "$EXP_DIR/A6" && log "A6 exists — skipping." \
    || { log "A6 (~10 MB files, ~$N_A6 files)…"
         generate_flat "$STAGING" "$EXP_DIR/A6" $N_A6; }

# A7 — ~1 MB    (SF-10: ~7500 files | SF-1: ~750 files)
N_A7=7500; [[ "$SF" == "1" ]] && N_A7=750
already_done "$EXP_DIR/A7" && log "A7 exists — skipping." \
    || { log "A7 (~1 MB files, ~$N_A7 files)…"
         generate_flat "$STAGING" "$EXP_DIR/A7" $N_A7; }

# ═══════════════════════════════════════════════════════════════════════════════
# PHASE 2 — Hierarchical models
# ═══════════════════════════════════════════════════════════════════════════════
log ""
log "=== PHASE 2: Hierarchical models ==="

already_done "$EXP_DIR/B2" && log "B2 exists — skipping." \
    || { log "B2 (/year/)…"
         generate_hive "$STAGING" "$EXP_DIR/B2" "ship_year"; }

already_done "$EXP_DIR/B3" && log "B3 exists — skipping." \
    || { log "B3 (/year/month/)…"
         generate_hive "$STAGING" "$EXP_DIR/B3" "ship_year, ship_month"; }

already_done "$EXP_DIR/B4" && log "B4 exists — skipping." \
    || { log "B4 (/year/month/day/)…"
         generate_hive "$STAGING" "$EXP_DIR/B4" "ship_year, ship_month, ship_day"; }

# ═══════════════════════════════════════════════════════════════════════════════
# PHASE 3 — Inverted hierarchy
# ═══════════════════════════════════════════════════════════════════════════════
log ""
log "=== PHASE 3: Inverted hierarchy ==="

already_done "$EXP_DIR/C1" && log "C1 exists — skipping." \
    || { log "C1 (/month/year/)…"
         generate_hive "$STAGING" "$EXP_DIR/C1" "ship_month, ship_year"; }

# ═══════════════════════════════════════════════════════════════════════════════
# Summary
# ═══════════════════════════════════════════════════════════════════════════════
log ""
log "=== SF-$SF layout summary ==="
for m in A1 A2 A3 A4 A5 A6 A7 B2 B3 B4 C1; do
    [[ -d "$EXP_DIR/$m" ]] || continue
    cnt=$(find "$EXP_DIR/$m" -name "*.parquet" | wc -l)
    sz=$(du -sh "$EXP_DIR/$m" 2>/dev/null | cut -f1)
    printf "  %-4s  %5s files   %s\n" "$m" "$cnt" "$sz"
done
log "Generation complete for SF-$SF."
