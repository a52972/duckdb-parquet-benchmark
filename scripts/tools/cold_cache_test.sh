#!/usr/bin/env bash
# Checks that dropping the page cache gives a cold read.
# Usage: scripts/tools/cold_cache_test.sh <file> [label] [out.csv]
set -euo pipefail
export LC_ALL=C  # printf %.3f and bc need a dot as decimal separator
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
F="$(realpath "$1")"; LABEL="${2:-$(basename "$F")}"
OUT="${3:-$BASE_DIR/results/local/e9_cold_cache/cold_cache.csv}"
mkdir -p "$(dirname "$OUT")"
[[ -s "$OUT" ]] || echo "timestamp,boot_time,label,file,bytes,step,drop_caches,flags,seconds,MB_per_s,host_flush_s" > "$OUT"

BYTES=$(stat -c %s "$F")
BOOT=$(uptime -s)
drop() { sudo -n /usr/local/sbin/drop-caches; }
DD=$(command -v gnudd || command -v dd)

read_once() {  # step drop(yes/no) dd-flags [flush]
    [[ "$2" == yes ]] && drop
    local fs=""
    if [[ "${4:-}" == flush ]]; then fs=$($HOST_FLUSH_CMD); fi
    local t0 t1 s
    t0=$(date +%s.%N)
    $DD if="$F" of=/dev/null bs=4M $3 status=none
    t1=$(date +%s.%N)
    s=$(echo "$t1 - $t0" | bc -l)
    printf '%s,%s,%s,%s,%s,%s,%s,%s,%.3f,%.1f,%s\n' "$(date -u +%FT%TZ)" "$BOOT" "$LABEL" "$F" \
        "$BYTES" "$1" "$2" "${3:-none}" "$s" "$(echo "$BYTES / 1000000 / $s" | bc -l)" "$fs" | tee -a "$OUT"
}

read_once 1_after_drop_first yes ""
read_once 2_after_drop        yes ""
read_once 3_warm_no_drop      no  ""
read_once 4_direct_after_drop yes "iflag=direct"
if [[ -n "${HOST_FLUSH_CMD:-}" ]]; then
    read_once 5a_after_guest_drop_and_host_flush yes "" flush
    read_once 5b_after_guest_drop_and_host_flush yes "" flush
fi
