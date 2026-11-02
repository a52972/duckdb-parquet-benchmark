#!/usr/bin/env bash
# Measures the host-cache effect on a freshly written file inside the VM.
# Usage: scripts/vm/host_cache/host_cache_freshfile_test.sh [size_MB] [out.csv]
set -euo pipefail
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
SIZE_MB="${1:-3072}"
OUT="${2:-$BASE_DIR/results/local/vm/host_cache_freshfile.csv}"
F="$BASE_DIR/layouts/_freshfile_tmp.bin"
mkdir -p "$(dirname "$OUT")" "$(dirname "$F")"
trap 'rm -f "$F"' EXIT
[[ -s "$OUT" ]] || echo "timestamp,boot_time,size_MB,step,dd_flags,host_flush,seconds,MB_per_s,flush_s,standby_before_MB,standby_after_MB" > "$OUT"

echo "writing $SIZE_MB MB (O_DIRECT, fsync)…"
dd if=/dev/urandom of="$F" bs=4M count=$(( SIZE_MB / 4 )) oflag=direct conv=fsync status=none
BYTES=$(stat -c %s "$F"); BOOT=$(uptime -s)

read_once() {  # step flags flush(yes/no)
    sudo -n /usr/local/sbin/drop-caches
    local fl="" fs="" sb="" sa=""
    if [[ "$3" == yes ]]; then
        fl=$(FLUSH_VERBOSE=1 scripts/vm/host_cache/host_flush.sh); read -r fs sb sa <<< "$fl"
    fi
    local t0 t1 s
    t0=$(date +%s.%N)
    dd if="$F" of=/dev/null bs=4M $2 status=none
    t1=$(date +%s.%N)
    s=$(echo "$t1 - $t0" | bc -l)
    printf '%s,%s,%s,%s,%s,%s,%.3f,%.1f,%s,%s,%s\n' "$(date -u +%FT%TZ)" "$BOOT" "$SIZE_MB" "$1" \
        "${2:-none}" "$3" "$s" "$(echo "$BYTES / 1000000 / $s" | bc -l)" "$fs" "$sb" "$sa" | tee -a "$OUT"
}

cd "$BASE_DIR"
read_once R1_drop_only        ""             no
read_once R2_drop_host_flush  ""             yes
read_once R3_drop_only        ""             no
read_once R4_drop_host_flush  ""             yes
read_once R5_direct_drop_only "iflag=direct" no
read_once R6_direct_host_flush "iflag=direct" yes
