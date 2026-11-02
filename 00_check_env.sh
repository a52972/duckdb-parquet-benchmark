#!/usr/bin/env bash
# Checks the environment: DuckDB binary, cache drop, tools, disk and RAM.
# Usage: bash 00_check_env.sh
set -euo pipefail

echo "=== DuckDB Benchmark — Environment Check ==="
PASS=0; FAIL=0

ok()   { echo "  [OK]   $1"; PASS=$((PASS+1)); }
fail() { echo "  [FAIL] $1"; FAIL=$((FAIL+1)); }
info() { echo "  [INFO] $1"; }

[[ -f ./duckdb && -x ./duckdb ]] && ok "DuckDB binary" || fail "DuckDB binary missing or not executable"
./duckdb -c "SELECT 1" &>/dev/null && ok "DuckDB runs" || fail "DuckDB does not run"

echo ""
VERSION=$(./duckdb -csv -noheader -c "SELECT version()" 2>/dev/null || true)
info "DuckDB version: $VERSION"
[[ "$VERSION" == "v1.5.0" ]] || info "results in the article were produced with v1.5.0"

./duckdb -c "INSTALL tpch; LOAD tpch; SELECT 1" &>/dev/null \
  && ok "TPC-H extension" || fail "TPC-H extension unavailable"

command -v python3   &>/dev/null && ok "python3"          || fail "python3 not found"
[[ -x /usr/bin/time ]]           && ok "/usr/bin/time"    || fail "/usr/bin/time not found"
command -v strace    &>/dev/null && ok "strace"           || info "strace not found (only needed for --strace-runs)"

if [[ $EUID -eq 0 ]]; then
    ok "running as root (drop_caches)"
elif sudo -n /usr/local/sbin/drop-caches 2>/dev/null; then
    ok "drop_caches helper (sudo -n /usr/local/sbin/drop-caches)"
else
    fail "cannot drop the page cache — run with sudo or run 'sudo scripts/setup/setup_sudo.sh'"
fi

AVAIL_GB=$(df -BG . | awk 'NR==2{gsub("G",""); print $4}')
(( AVAIL_GB >= 25 )) && ok "Disk: ${AVAIL_GB} GB free (>=25 GB)" \
  || { (( AVAIL_GB >= 8 )) && ok "Disk: ${AVAIL_GB} GB — generate SF-10 in parts (--models)" \
    || fail "Disk: ${AVAIL_GB} GB free — need at least 8 GB"; }

RAM_GB=$(awk '/MemTotal/{printf "%d", $2/1024/1024}' /proc/meminfo)
(( RAM_GB >= 14 )) && ok "RAM: ${RAM_GB} GB" \
  || { (( RAM_GB >= 8 )) && ok "RAM: ${RAM_GB} GB (use --memory-limit below 10GB)" \
    || echo "  [WARN] RAM: ${RAM_GB} GB — 16 GB recommended for the full run (the smoke test needs less)"; }

info "CPU cores: $(nproc)"

echo ""
echo "=== $PASS passed, $FAIL failed ==="
(( FAIL == 0 )) || { echo "Fix the FAIL items before continuing."; exit 1; }
echo "Environment ready."
