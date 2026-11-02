#!/usr/bin/env bash
# E10: skewed data and skewed file sizes, SF-10, one interleaved campaign with the uniform references.
# Usage: setsid nohup scripts/baremetal/run_e10.sh > e10.log 2>&1 < /dev/null &
set -euo pipefail
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$BASE_DIR"
RES="${RESULTS_ROOT:-results/local}"; export RESULTS_ROOT="$RES"
PLATFORM="${BENCH_PLATFORM:-baremetal}"
SF="${SF:-10}"; COLD="${COLD:-20}"; WARM="${WARM:-3}"; SEED="${SEED:-1010}"
LAYOUTS="$(realpath -m "${LAYOUTS:-layouts}")"
RID="${RID:-bm_e10_skew}"
OUT="$RES/baremetal/$RID"
INV=(--inventory "$RES/inventories/bm_inventory_v2.csv" --inventory-files "$RES/inventories/bm_inventory_v2_files"
     --gen-log "$RES/inventories/bm_generation_log.csv" --experiments-dir "$LAYOUTS")
STAGING="data/sf$SF/lineitem_staging.parquet"
SKEW="data/sf$SF/lineitem_skew.parquet"
mkdir -p "$OUT"
nfiles() { find "$1" -name '*.parquet' 2>/dev/null | wc -l; }
say() { echo "[$(date +'%H:%M:%S')] $*"; }

avail_kb=$(df -k --output=avail . | tail -1)
(( avail_kb > 14 * 1024 * 1024 )) || [[ "$SF" == 1 ]] || { echo "need ~14 GB free" >&2; exit 1; }

if [[ ! -f "$SKEW" ]]; then
    say "skewed input: shipping months redrawn from a Zipf(1) distribution centred on 1996-01"
    ./duckdb -bail -c "
SET threads=4; SET memory_limit='8GB';
COPY (
  WITH m AS (SELECT i, 1992 + i // 12 AS y, i % 12 + 1 AS mo FROM range(84) t(i)),
       r AS (SELECT *, row_number() OVER (ORDER BY abs(i - 48), i) AS rk FROM m),
       c AS (SELECT y, mo, (sum(pow(rk, -1.0)) OVER (ORDER BY rk) - pow(rk, -1.0)) / sum(pow(rk, -1.0)) OVER () AS lo,
                    sum(pow(rk, -1.0)) OVER (ORDER BY rk) / sum(pow(rk, -1.0)) OVER () AS hi FROM r),
       l AS (SELECT *, hash(l_orderkey * 10000 + l_linenumber + 1234567)::DOUBLE / 18446744073709551616.0 AS u
             FROM read_parquet('$STAGING'))
  SELECT l.* EXCLUDE (u) REPLACE (make_date(c.y, c.mo, least(day(l.l_shipdate), 28)) AS l_shipdate)
  FROM l JOIN c ON l.u >= c.lo AND l.u < c.hi
) TO '$SKEW' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 1048576);"
fi
./duckdb -csv -c "SELECT year(l_shipdate) AS y, month(l_shipdate) AS m, count(*) AS n_rows
                  FROM read_parquet('$SKEW') GROUP BY ALL ORDER BY ALL" > "$OUT/skew_months.csv"
sha256sum "$SKEW" | sed "s|  .*|  $SKEW|" > "$OUT/skew_staging.sha256"

declare -A WANT=([A4]=75 [A6]=750 [B3]=84 [Z75]=75 [Z750]=750 [S_B1]=75 [S_S75]=75 [S_B3]=84)
[[ "$SF" == 1 ]] && WANT[A4]=8 && WANT[A6]=75
missing() { local out=(); for m in "$@"; do [[ "$(nfiles "$LAYOUTS/sf$SF/$m")" -eq "${WANT[$m]}" ]] || out+=("$m"); done; echo "${out[*]}"; }

std=$(missing A4 A6 B3); [[ -z "$std" ]] || ./01_generate_layouts.sh --sf "$SF" --models "${std// /,}" --force "${INV[@]}"
zz=(); for m in $(missing Z75 Z750); do zz+=(--custom "$m:zipf:${WANT[$m]}:1"); done
(( ${#zz[@]} )) && ./01_generate_layouts.sh --sf "$SF" --force "${zz[@]}" "${INV[@]}"
sk=(); for m in $(missing S_B1 S_S75 S_B3); do
    case $m in S_B1) sk+=(--custom S_B1:flat:75) ;; S_S75) sk+=(--custom S_S75:sorted:75) ;; S_B3) sk+=(--custom S_B3:hive:ship_year+ship_month) ;; esac
done
(( ${#sk[@]} )) && ./01_generate_layouts.sh --sf "$SF" --force --staging "$SKEW" "${sk[@]}" "${INV[@]}"
left=$(missing A4 A6 B3 Z75 Z750 S_B1 S_S75 S_B3); [[ -z "$left" ]] || { echo "incomplete layouts: $left" >&2; exit 1; }

scripts/tools/cold_cache_test.sh "$STAGING" "pre_$RID" "$RES/baremetal/bm_cold_cache/cold_cache.csv"
scripts/setup/capture_env.sh "$OUT/env.txt"
Q=""
for m in A4 A6 Z75 Z750 S_B1 S_S75; do for q in Q1 Q4 Q3_B1 Q3_B1_cold; do Q+="$m:$q,"; done; done
for m in B3 S_B3; do for q in Q1_hive Q4_hive Q3_B3 Q3_B3_cold; do Q+="$m:$q,"; done; done
say "campaign $RID: $COLD cold + $WARM warm, interleaved"
python3 02_run_benchmark.py --run-id "$RID" --seed "$SEED" --sf "$SF" --experiments-dir "$LAYOUTS" \
    --models A4,A6,Z75,Z750,S_B1,S_S75,B3,S_B3 --queries "${Q%,}" \
    --cold "$COLD" --warm "$WARM" --threads 4 --memory-limit 10GB --order interleaved --platform "$PLATFORM"

if [[ "${KEEP_LAYOUTS:-0}" != 1 ]]; then
    for m in Z75 Z750 S_B1 S_S75 S_B3; do rm -rf "${LAYOUTS:?}/sf$SF/$m"; done
fi
say "E10 finished"
