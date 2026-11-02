#!/usr/bin/env bash
# Reproduces the study: tables from the published data, or the whole benchmark on a Linux machine.
# Usage: bash reproduce.sh tables | setup | data | layouts | smoke | core | experiments | analysis | all
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
chmod +x reproduce.sh 00_check_env.sh 01_generate_layouts.sh scripts/*/*.sh scripts/vm/host_cache/*.sh 2>/dev/null || true
export RESULTS_ROOT="${RESULTS_ROOT:-results/local}"
export BENCH_PLATFORM="${BENCH_PLATFORM:-baremetal}"
DUCKDB_VERSION=v1.5.0
DUCKDB_SHA256=d4aedabf9277fc1be1d7502a2210d3a9582d9e703352aead50eb307162e4295d
[[ -x .venv/bin/python ]] && export PATH="$PWD/.venv/bin:$PATH"
INV=(--inventory "$RESULTS_ROOT/inventories/bm_inventory_v2.csv"
     --inventory-files "$RESULTS_ROOT/inventories/bm_inventory_v2_files"
     --gen-log "$RESULTS_ROOT/inventories/bm_generation_log.csv")
say() { printf '\n=== [%s] %s ===\n' "$(date +%H:%M:%S)" "$*"; }
PY="${PYTHON:-python3}"   # Python >= 3.11 used to create .venv
check_python() {
    "$PY" -c 'import sys; sys.exit(sys.version_info < (3, 11))' 2>/dev/null \
        || { echo "Python >= 3.11 is needed ($PY is $("$PY" -V 2>&1)); run with PYTHON=/path/to/python3.11"; exit 1; }
}

tables() {
    say "tables and figures from the published measurements → results/v1.1/derived, figures/v1.1"
    [[ -x .venv/bin/python ]] || { check_python; "$PY" -m venv .venv && .venv/bin/pip install -q -r requirements.txt; }
    RESULTS_ROOT= .venv/bin/python analysis/run_all.py
}

setup() {
    say "system packages"
    if command -v apt-get >/dev/null; then
        sudo apt-get update -qq
        sudo apt-get install -y -qq python3 python3-venv strace bc time curl unzip >/dev/null
    else
        echo "not a Debian/Ubuntu system: install python3 (venv), strace, bc, GNU time, curl and unzip"
    fi
    say "DuckDB $DUCKDB_VERSION CLI"
    if [[ ! -x duckdb ]]; then
        curl -fsSLO "https://github.com/duckdb/duckdb/releases/download/$DUCKDB_VERSION/duckdb_cli-linux-amd64.zip"
        unzip -oq duckdb_cli-linux-amd64.zip && rm duckdb_cli-linux-amd64.zip && chmod +x duckdb
    fi
    echo "$DUCKDB_SHA256  duckdb" | sha256sum -c -
    say "Python environment (.venv)"
    check_python
    "$PY" -m venv .venv
    .venv/bin/pip install -q -r requirements.txt
    export PATH="$PWD/.venv/bin:$PATH"
    say "cache-drop helper (asks for your password once)"
    sudo -n /usr/local/sbin/drop-caches 2>/dev/null || sudo scripts/setup/setup_sudo.sh
    bash 00_check_env.sh
}

data() {
    say "TPC-H input data, SF-1 and SF-10"
    scripts/setup/make_data.sh 1 10
}

layouts() {
    say "layouts A1–A7, B2–B4, C1 at SF-1 and SF-10 (~16 min, ~17 GB)"
    ./01_generate_layouts.sh --sf 1 "${INV[@]}"
    ./01_generate_layouts.sh --sf 10 "${INV[@]}"
}

smoke() {
    say "smoke test: SF-1 only, 3 cold + 1 warm runs per combination (~10 min)"
    scripts/setup/make_data.sh 1
    ./01_generate_layouts.sh --sf 1 "${INV[@]}"
    python3 02_run_benchmark.py --run-id smoke --sf 1 --cold 3 --warm 1 --order interleaved --platform local
    python3 - "$RESULTS_ROOT/local/smoke/benchmark_results.csv" <<'PY'
import sys, pandas as pd
d = pd.read_csv(sys.argv[1]); bad = d[d.Status != "OK"]
print(f"{len(d)} runs, {len(bad)} not OK")
sys.exit(1 if len(bad) else 0)
PY
}

core() {
    say "five core campaigns (SF-1 and SF-10, 20 cold + 3 warm, interleaved; ~3.6 h)"
    scripts/baremetal/run_core_all.sh
}

experiments() {
    say "experiments E4, E7, E8, E9, E6, D1, D2, E10 (SF-10; ~2.5 h; up to ~18 GB of extra layouts at a time)"
    scripts/baremetal/run_e4_e7_e8_e9.sh
    scripts/baremetal/run_e6.sh
    scripts/baremetal/run_d1.sh
    scripts/baremetal/run_d1_strace.sh
    scripts/baremetal/run_d2.sh
    scripts/baremetal/run_e10.sh
}

analysis() {
    say "analysis of your runs ($RESULTS_ROOT), with the published VM runs as the second platform"
    mkdir -p "$RESULTS_ROOT/inventories"
    [[ -e "$RESULTS_ROOT/vm" ]] || ln -s "$(realpath results/v1.1/vm)" "$RESULTS_ROOT/vm"
    for f in inventory_original.csv inventory_v2.csv; do
        [[ -e "$RESULTS_ROOT/inventories/$f" ]] || cp "results/v1.1/inventories/$f" "$RESULTS_ROOT/inventories/"
    done
    python3 analysis/run_all.py
    echo "Tables: $RESULTS_ROOT/derived   Figures: $RESULTS_ROOT/figures"
}

all() {
    local free_gb; free_gb=$(df -BG --output=avail . | tail -1 | tr -dc 0-9)
    (( free_gb >= 40 )) || echo "WARNING: ${free_gb} GB free; the full run needs about 40 GB"
    setup; data; layouts; core; experiments; analysis
    say "finished"
}

case "${1:-}" in
    tables|setup|data|layouts|smoke|core|experiments|analysis|all) "$1" ;;
    *) sed -n '2,3p' "$0" | sed 's/^# //'; exit 1 ;;
esac
