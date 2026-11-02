#!/usr/bin/env bash
# The five bare-metal core campaigns, with a cold-cache check before each.
# Usage: scripts/baremetal/run_core_all.sh
set -euo pipefail
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$BASE_DIR"
RES="${RESULTS_ROOT:-results/local}"; export RESULTS_ROOT="$RES"
for pair in a:101 b:202 c:303 d:404 e:505; do
    c="${pair%%:*}"; seed="${pair##*:}"
    echo "=== bm_core_$c (seed $seed) — $(date) ==="
    scripts/tools/cold_cache_test.sh data/sf10/lineitem_staging.parquet "pre_bm_core_$c" \
        $RES/baremetal/bm_cold_cache/cold_cache.csv
    scripts/baremetal/run_core.sh "bm_core_$c" "$seed" 2>&1 | tee -a "$RES/baremetal/bm_core_$c.log"
done
echo "All BM core campaigns finished: $(date)"
