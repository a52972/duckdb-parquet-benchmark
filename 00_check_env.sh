#!/usr/bin/env bash
set -euo pipefail

echo "=== DuckDB Benchmark — Environment Check ==="
PASS=0; FAIL=0

ok()   { echo "  [OK]   $1"; PASS=$((PASS+1)); }
fail() { echo "  [FAIL] $1"; FAIL=$((FAIL+1)); }
info() { echo "  [INFO] $1"; }

# DuckDB binary
[[ -f ./duckdb && -x ./duckdb ]] && ok "DuckDB binary" || fail "DuckDB binary missing or not executable"
./duckdb -c "SELECT 1" &>/dev/null && ok "DuckDB runs" || fail "DuckDB does not run"

echo ""
info "DuckDB version: $(./duckdb -csv -noheader -c "SELECT version()" 2>/dev/null)"

# TPC-H extension
./duckdb -c "INSTALL tpch; LOAD tpch; SELECT 1" &>/dev/null \
  && ok "TPC-H extension" || fail "TPC-H extension unavailable"

# System tools
command -v python3   &>/dev/null && ok "python3"          || fail "python3 not found"
[[ -x /usr/bin/time ]]           && ok "/usr/bin/time"    || fail "/usr/bin/time not found"
sudo -n true 2>/dev/null         && ok "sudo (no passwd)" \
  || { sudo -v 2>/dev/null && ok "sudo (requires passwd)"; } \
  || fail "sudo not available — needed to drop caches"

# Disk space
AVAIL_GB=$(df -BG . | awk 'NR==2{gsub("G",""); print $4}')
(( AVAIL_GB >= 50 )) && ok "Disk: ${AVAIL_GB} GB free (>=50 GB)" \
  || { (( AVAIL_GB >= 20 )) && ok "Disk: ${AVAIL_GB} GB — run SF-1 and SF-10 separately" \
    || fail "Disk: ${AVAIL_GB} GB free — need at least 20 GB"; }

# RAM
RAM_GB=$(awk '/MemTotal/{printf "%d", $2/1024/1024}' /proc/meminfo)
(( RAM_GB >= 10 )) && ok "RAM: ${RAM_GB} GB" \
  || { (( RAM_GB >= 6 )) && ok "RAM: ${RAM_GB} GB (minimum, set memory_limit carefully)" \
    || fail "RAM: ${RAM_GB} GB — 8+ GB strongly recommended"; }

info "CPU cores: $(nproc)"

echo ""
echo "=== $PASS passed, $FAIL failed ==="
(( FAIL == 0 )) || { echo "Fix the FAIL items before continuing."; exit 1; }
echo "Environment ready."
