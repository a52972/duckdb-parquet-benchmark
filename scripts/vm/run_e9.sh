#!/usr/bin/env bash
# E9: threads and memory limit.
# Usage: scripts/vm/run_e9.sh
set -euo pipefail
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$BASE_DIR"

Q="A1:Q1,A1:Q4,A4:Q1,A4:Q4,A6:Q1,A6:Q4,A7:Q1,A7:Q4,B3:Q1_hive,B3:Q4_hive,B4:Q1_hive,B4:Q4_hive"
for cfg in "1 10" "2 10" "4 10" "4 8" "4 4"; do
    read -r t m <<< "$cfg"
    python3 02_run_benchmark.py --platform vm-vmware --run-id "e9_t${t}_m${m}" --sf 10 \
        --models A1,A4,A6,A7,B3,B4 --queries "$Q" --no-baseline \
        --threads "$t" --memory-limit "${m}GB" --cold 10 --warm 0
done
echo "E9 finished: $(date)"
