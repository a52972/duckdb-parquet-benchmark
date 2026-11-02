#!/usr/bin/env bash
# Writes the Parquet layouts (one COPY per file; exact file and row counts are checked).
# Usage: ./01_generate_layouts.sh --sf 1|10 [--models A1,B3] [--custom NAME:flat:N|NAME:zipf:N:S|NAME:sorted:N|NAME:hive:col1+col2[:K]]
#        [--staging FILE] [--force]
set -euo pipefail

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$BASE_DIR"
DUCKDB="$BASE_DIR/duckdb"

SF=""; MODELS=""; CUSTOM=(); FORCE=0; KEEP=0; STAGING=""
EXP_ROOT="$BASE_DIR/layouts"
INVENTORY="$BASE_DIR/results/local/inventories/inventory_v2.csv"
INV_FILES="$BASE_DIR/results/local/inventories/inventory_v2_files"
GEN_LOG="$BASE_DIR/results/local/inventories/generation_log.csv"
THREADS=4; MEMORY_LIMIT="${GEN_MEMORY_LIMIT:-8GB}"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --sf) SF="$2"; shift 2 ;;
        --models) MODELS="$2"; shift 2 ;;
        --custom) CUSTOM+=("$2"); shift 2 ;;
        --force) FORCE=1; shift ;;
        --keep-intermediates) KEEP=1; shift ;;
        --experiments-dir) EXP_ROOT="$(realpath -m "$2")"; shift 2 ;;
        --inventory) INVENTORY="$2"; shift 2 ;;
        --inventory-files) INV_FILES="$2"; shift 2 ;;
        --gen-log) GEN_LOG="$2"; shift 2 ;;
        --staging) STAGING="$(realpath -m "$2")"; shift 2 ;;
        *) echo "unknown argument: $1" >&2; exit 1 ;;
    esac
done
[[ "$SF" == "1" || "$SF" == "10" ]] || { echo "--sf must be 1 or 10" >&2; exit 1; }

STAGING="${STAGING:-$BASE_DIR/data/sf${SF}/lineitem_staging.parquet}"
EXP_DIR="$EXP_ROOT/sf${SF}"
WORK="$EXP_ROOT/_gen_tmp/sf${SF}"           # intermediates + SQL scripts
SPILL="$EXP_ROOT/_spill_tmp"
INTER_HASH="$WORK/by_hash.parquet"
INTER_DATE="$WORK/by_date.parquet"
[[ -f "$STAGING" ]] || { echo "staging not found: $STAGING" >&2; exit 1; }
mkdir -p "$EXP_DIR" "$WORK" "$SPILL" "$(dirname "$INVENTORY")" "$(dirname "$GEN_LOG")" "$INV_FILES"

cleanup() {
    rm -rf "$SPILL"
    if [[ $KEEP -eq 0 ]]; then
        rm -rf "$WORK"
        rmdir "$EXP_ROOT/_gen_tmp" 2>/dev/null || true
    fi
}
trap cleanup EXIT

log() { echo "[$(date +'%H:%M:%S')] $*"; }
die() { echo "ERROR: $*" >&2; exit 1; }

AVAIL_GB=$(df -BG --output=avail "$EXP_ROOT" | tail -1 | tr -dc 0-9)
SPILL_CAP_GB=$(( AVAIL_GB - 6 ))
(( SPILL_CAP_GB >= 1 )) || die "only ${AVAIL_GB} GB free; need more than 6 GB"
SETTINGS="SET threads=${THREADS}; SET memory_limit='${MEMORY_LIMIT}'; SET temp_directory='${SPILL}'; SET max_temp_directory_size='${SPILL_CAP_GB}GB'; SET parquet_metadata_cache=true;"

run_sql_file() {
    local f="$1" t="$WORK/_time.txt"
    if ! /usr/bin/time -f "%e %M" -o "$t" "$DUCKDB" -bail -f "$f" >/dev/null; then
        cat "$t" >&2    # e.g. "Command terminated by signal 9" (OOM killer)
        die "DuckDB failed on $f"
    fi
    read -r LAST_SECONDS LAST_RSS_KB < <(tail -1 "$t")
}

query() { "$DUCKDB" -noheader -list -c "$1"; }

gen_log() {  # sf, model, step, seconds, peak_rss_kb, files
    [[ -s "$GEN_LOG" ]] || echo "timestamp,sf,model,step,seconds,peak_rss_kb,files,threads,memory_limit" > "$GEN_LOG"
    echo "$(date -u +%Y-%m-%dT%H:%M:%SZ),$1,$2,$3,$4,$5,$6,$THREADS,$MEMORY_LIMIT" >> "$GEN_LOG"
}

STAGING_ROWS=$(query "SELECT count(*) FROM read_parquet('$STAGING')")

HASH_EXPR="hash(l_orderkey * 10000 + l_linenumber)"

ensure_inter_hash() {
    [[ -f "$INTER_HASH" ]] && return
    log "  intermediate sorted by _h → $INTER_HASH"
    cat > "$WORK/inter_hash.sql" <<EOF
$SETTINGS
COPY (SELECT *, ${HASH_EXPR} AS _h FROM read_parquet('${STAGING}') ORDER BY _h)
TO '${INTER_HASH}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 122880);
EOF
    run_sql_file "$WORK/inter_hash.sql"
    gen_log "$SF" "_intermediate" "by_hash" "$LAST_SECONDS" "$LAST_RSS_KB" 1
}

ensure_inter_date() {
    [[ -f "$INTER_DATE" ]] && return
    log "  intermediate sorted by (l_shipdate, _h) → $INTER_DATE"
    cat > "$WORK/inter_date.sql" <<EOF
$SETTINGS
COPY (
    SELECT *,
        date_part('year',    l_shipdate)::INTEGER AS ship_year,
        date_part('quarter', l_shipdate)::INTEGER AS ship_quarter,
        date_part('month',   l_shipdate)::INTEGER AS ship_month,
        date_part('day',     l_shipdate)::INTEGER AS ship_day,
        (date_part('year', l_shipdate) * 100 + date_part('month', l_shipdate))::INTEGER AS ship_ym,
        (l_suppkey % 200)::INTEGER AS supp,
        ${HASH_EXPR} AS _h
    FROM read_parquet('${STAGING}')
    ORDER BY l_shipdate, _h
) TO '${INTER_DATE}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 122880);
EOF
    run_sql_file "$WORK/inter_date.sql"
    gen_log "$SF" "_intermediate" "by_date" "$LAST_SECONDS" "$LAST_RSS_KB" 1
}

OUT_OPTS="FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 1048576"

write_flat() {  # dst n
    local dst="$1" n="$2"
    ensure_inter_hash
    query "
SELECT format('COPY (SELECT * EXCLUDE (_h) FROM read_parquet(''{}'') WHERE _h >= {}{} ORDER BY l_orderkey, l_linenumber) TO ''{}/part_{:06d}.parquet'' ($OUT_OPTS);',
              '${INTER_HASH}',
              (i::HUGEINT * 18446744073709551616 // ${n})::UBIGINT,
              CASE WHEN i < ${n} - 1
                   THEN format(' AND _h < {}', ((i + 1)::HUGEINT * 18446744073709551616 // ${n})::UBIGINT)
                   ELSE '' END,
              '${dst}', i)
FROM range(${n}) t(i) ORDER BY i" > "$WORK/body.sql"
}

write_zipf() {  # dst n s: file i (0-based) gets a share of the rows proportional to 1/(i+1)^s
    local dst="$1" n="$2" s="$3"
    ensure_inter_hash
    query "
WITH w AS (SELECT i, pow(i + 1, -${s}) AS wt FROM range(${n}) t(i)),
     c AS (SELECT i, (sum(wt) OVER (ORDER BY i) - wt) / sum(wt) OVER () AS lo,
                  sum(wt) OVER (ORDER BY i) / sum(wt) OVER () AS hi FROM w)
SELECT format('COPY (SELECT * EXCLUDE (_h) FROM read_parquet(''{}'') WHERE _h >= {}{} ORDER BY l_orderkey, l_linenumber) TO ''{}/part_{:06d}.parquet'' ($OUT_OPTS);',
              '${INTER_HASH}',
              (lo * 18446744073709551616)::HUGEINT::UBIGINT,
              CASE WHEN i < ${n} - 1
                   THEN format(' AND _h < {}', (hi * 18446744073709551616)::HUGEINT::UBIGINT)
                   ELSE '' END,
              '${dst}', i)
FROM c ORDER BY i" > "$WORK/body.sql"
}

write_sorted() {  # dst n — n equal row ranges of the date-sorted order
    local dst="$1" n="$2"
    ensure_inter_date
    query "
SELECT format('COPY (SELECT * EXCLUDE (_h, file_row_number, ship_year, ship_quarter, ship_month, ship_day, ship_ym, supp) FROM read_parquet(''{}'', file_row_number = true) WHERE file_row_number >= {} AND file_row_number < {}) TO ''{}/part_{:06d}.parquet'' ($OUT_OPTS);',
              '${INTER_DATE}',
              (i * ${STAGING_ROWS}) // ${n}, ((i + 1) * ${STAGING_ROWS}) // ${n},
              '${dst}', i)
FROM range(${n}) t(i) ORDER BY i" > "$WORK/body.sql"
}

write_hive() {  # dst "col1, col2" k
    local dst="$1" cols="$2" k="$3"
    ensure_inter_date
    local drop="_h, ship_quarter, ship_ym, supp" c
    for c in ${cols//,/ }; do
        [[ "$c" == "ship_quarter" || "$c" == "ship_ym" || "$c" == "supp" ]] || drop+=", $c"
    done
    local path_expr="" where_expr="" sep=""
    for c in ${cols//,/ }; do
        path_expr+="${sep:+ || '/' || }'${c}=' || ${c}"
        where_expr+="${sep:+ || ' AND ' || }'${c} = ' || ${c}"
        sep=1
    done
    query "SELECT DISTINCT ${path_expr} FROM read_parquet('${INTER_DATE}') ORDER BY 1" \
        | while IFS= read -r leaf; do mkdir -p "$dst/$leaf"; done
    query "
WITH leaves AS (
    SELECT DISTINCT ${path_expr} AS leaf, ${where_expr} AS cond
    FROM read_parquet('${INTER_DATE}'))
SELECT format('COPY (SELECT * EXCLUDE (${drop}) FROM read_parquet(''{}'') WHERE {}{} ORDER BY l_orderkey, l_linenumber) TO ''{}/{}/data_{}.parquet'' ($OUT_OPTS);',
              '${INTER_DATE}', cond,
              CASE WHEN ${k} = 1 THEN ''
                   ELSE format(' AND _h >= {}{}',
                        (j::HUGEINT * 18446744073709551616 // ${k})::UBIGINT,
                        CASE WHEN j < ${k} - 1
                             THEN format(' AND _h < {}', ((j + 1)::HUGEINT * 18446744073709551616 // ${k})::UBIGINT)
                             ELSE '' END) END,
              '${dst}', leaf, j)
FROM leaves, range(${k}) t(j) ORDER BY leaf, j" > "$WORK/body.sql"
}

build_model() {  # name kind arg [k]
    local name="$1" kind="$2" arg="$3" k="${4:-1}" dst="$EXP_DIR/$1"
    if [[ -d "$dst" && $FORCE -eq 0 ]]; then
        log "$name exists — skipping (use --force to regenerate)."
        return
    fi
    rm -rf "$dst"; mkdir -p "$dst"
    log "$name ($kind $arg${4:+, $k})"
    case "$kind" in
        flat)   write_flat   "$dst" "$arg" ;;
        zipf)   write_zipf   "$dst" "$arg" "$k" ;;
        sorted) write_sorted "$dst" "$arg" ;;
        hive)   write_hive   "$dst" "${arg//+/, }" "$k" ;;
        *) die "unknown kind $kind" ;;
    esac
    local expected; expected=$(wc -l < "$WORK/body.sql")
    { echo "$SETTINGS"; cat "$WORK/body.sql"; } > "$WORK/model.sql"
    run_sql_file "$WORK/model.sql"
    local secs="$LAST_SECONDS" rss="$LAST_RSS_KB"

    local files rows
    files=$(find "$dst" -name '*.parquet' | wc -l)
    rows=$(query "SELECT sum(num_rows) FROM parquet_file_metadata('${dst}/**/*.parquet')")
    [[ "$files" -eq "$expected" ]] || die "$name: $files files, expected $expected"
    [[ "$rows" -eq "$STAGING_ROWS" ]] || die "$name: $rows rows, staging has $STAGING_ROWS"
    gen_log "$SF" "$name" "$kind:$arg:$k" "$secs" "$rss" "$files"
    log "  → $files files, $rows rows, $(du -sh "$dst" | cut -f1), ${secs}s, peak RSS ${rss} KB"

    if [[ -f "$INVENTORY" ]]; then
        awk -F, -v s="$SF" -v m="$name" 'NR==1 || !($1==s && $2==m)' "$INVENTORY" > "$INVENTORY.tmp_"
        mv "$INVENTORY.tmp_" "$INVENTORY"
    fi
    scripts/tools/inventory.sh "$dst" "$SF" "$name" "$INVENTORY" "$INV_FILES" >/dev/null
}

declare -A SPEC
if [[ "$SF" == "10" ]]; then
    SPEC=([A1]="flat 1" [A2]="flat 8" [A3]="flat 38" [A4]="flat 75"
          [A5]="flat 150" [A6]="flat 750" [A7]="flat 7500")
else
    SPEC=([A1]="flat 1" [A3]="flat 4" [A4]="flat 8"
          [A5]="flat 15" [A6]="flat 75" [A7]="flat 750")
fi
SPEC+=([B2]="hive ship_year" [B3]="hive ship_year+ship_month"
       [B4]="hive ship_year+ship_month+ship_day" [C1]="hive ship_month+ship_year")
ORDER=(A1 A2 A3 A4 A5 A6 A7 B2 B3 B4 C1)

if [[ -n "$MODELS" ]]; then
    IFS=, read -ra SELECTED <<< "$MODELS"
elif [[ ${#CUSTOM[@]} -eq 0 ]]; then
    SELECTED=()
    for m in "${ORDER[@]}"; do [[ -n "${SPEC[$m]:-}" ]] && SELECTED+=("$m"); done
else
    SELECTED=()
fi
for m in "${SELECTED[@]}"; do
    [[ -n "${SPEC[$m]:-}" ]] || die "model $m is not defined at SF-$SF"
done

log "=== SF-$SF: staging $STAGING_ROWS rows → $EXP_DIR ==="
for m in "${ORDER[@]}"; do
    [[ " ${SELECTED[*]} " == *" $m "* && "${SPEC[$m]}" == flat* ]] || continue
    build_model "$m" ${SPEC[$m]}
done
for c in "${CUSTOM[@]}"; do
    IFS=: read -r name kind arg k <<< "$c"
    [[ "$kind" == flat ]] && build_model "$name" "$kind" "$arg"
    [[ "$kind" == zipf ]] && build_model "$name" "$kind" "$arg" "$k"
done
[[ $KEEP -eq 1 ]] || rm -f "$INTER_HASH"
for m in "${ORDER[@]}"; do
    [[ " ${SELECTED[*]} " == *" $m "* && "${SPEC[$m]}" == hive* ]] || continue
    build_model "$m" ${SPEC[$m]}
done
for c in "${CUSTOM[@]}"; do
    IFS=: read -r name kind arg k <<< "$c"
    [[ "$kind" != flat && "$kind" != zipf ]] && build_model "$name" "$kind" "$arg" "${k:-1}"
done

log "=== SF-$SF done ==="
