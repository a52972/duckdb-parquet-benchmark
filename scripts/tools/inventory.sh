#!/usr/bin/env bash
# Inventory of one layout (files, bytes, rows, row groups).
# Usage: scripts/tools/inventory.sh <layout_dir> <sf> <model> <summary_csv> [<filelist_dir>]
set -euo pipefail

DIR="$(realpath "$1")"; SF="$2"; MODEL="$3"; OUT="$4"; LIST_DIR="${5:-}"
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DUCKDB="${DUCKDB:-$BASE_DIR/duckdb}"

[[ -d "$DIR" ]] || { echo "inventory: not a directory: $DIR" >&2; exit 1; }

read -r -d '' SQL <<EOF || true
SET threads=4;
CREATE TEMP TABLE f AS
SELECT fm.file_name, fm.file_size_bytes AS bytes, fm.num_rows,
       fm.num_row_groups, fm.created_by
FROM parquet_file_metadata('${DIR}/**/*.parquet') fm;
CREATE TEMP TABLE rg AS
SELECT file_name, row_group_num_rows AS n
FROM parquet_metadata('${DIR}/**/*.parquet')
WHERE column_id = 0;
COPY (
  SELECT ${SF} AS sf, '${MODEL}' AS model,
         (SELECT count(*) FROM f) AS files,
         (SELECT count(DISTINCT regexp_replace(file_name, '/[^/]*$', '')) FROM f) AS leaf_dirs,
         (SELECT sum(bytes) FROM f)::BIGINT AS bytes,
         (SELECT round(avg(bytes)) FROM f)::BIGINT AS size_mean_bytes,
         (SELECT min(bytes) FROM f) AS size_min_bytes,
         (SELECT max(bytes) FROM f) AS size_max_bytes,
         (SELECT count(*) FROM rg) AS row_groups,
         (SELECT round(avg(n), 1) FROM rg) AS rg_rows_mean,
         (SELECT min(n) FROM rg) AS rg_rows_min,
         (SELECT max(n) FROM rg) AS rg_rows_max,
         (SELECT sum(num_rows) FROM f)::BIGINT AS total_rows,
         (SELECT string_agg(DISTINCT created_by, ' | ') FROM f) AS created_by
) TO '/dev/stdout' (FORMAT CSV, HEADER true);
EOF

if [[ -n "$LIST_DIR" ]]; then
    mkdir -p "$LIST_DIR"
    SQL+="
COPY (SELECT substr(file_name, ${#DIR} + 2) AS path, bytes, num_rows, num_row_groups
      FROM f ORDER BY path)
TO '${LIST_DIR}/sf${SF}_${MODEL}.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);"
fi

row="$("$DUCKDB" -c "$SQL")"
if [[ ! -s "$OUT" ]]; then
    echo "$row" | head -1 > "$OUT"
fi
echo "$row" | tail -n +2 >> "$OUT"
echo "$row" | tail -n +2
