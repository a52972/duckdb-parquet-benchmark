#!/usr/bin/env bash
# Content fingerprints of the input data (row count and an order-independent hash of all rows).
# Usage: scripts/setup/fingerprint_data.sh [--check]    (prints CSV; --check compares with data/FINGERPRINTS.csv)
set -euo pipefail
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$BASE_DIR"
fp() {
    echo "sf,file,rows,content_hash"
    for sf in 1 10; do
        for f in lineitem_staging orders; do
            p="data/sf$sf/$f.parquet"
            [[ -f "$p" ]] || continue
            ./duckdb -csv -noheader -c "SELECT $sf, '$f', count(*), sum(hash(t)::HUGEINT) FROM read_parquet('$p') t"
        done
    done
}
if [[ "${1:-}" != "--check" ]]; then fp; exit 0; fi
[[ -f data/FINGERPRINTS.csv ]] || { echo "data/FINGERPRINTS.csv not found" >&2; exit 1; }
mine=$(mktemp); fp > "$mine"
bad=0
while IFS=, read -r sf f rows h; do
    [[ "$sf" == sf ]] && continue
    ref=$(awk -F, -v s="$sf" -v n="$f" '$1==s && $2==n {print $3","$4}' data/FINGERPRINTS.csv)
    if [[ -z "$ref" ]]; then echo "SF-$sf $f: no reference"; continue; fi
    if [[ "$rows,$h" == "$ref" ]]; then echo "SF-$sf $f: same rows as the published data ($rows)"
    else echo "SF-$sf $f: DIFFERENT from the published data (here $rows,$h; published $ref)"; bad=1; fi
done < "$mine"
rm -f "$mine"
exit $bad
