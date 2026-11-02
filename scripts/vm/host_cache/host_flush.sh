#!/usr/bin/env bash
# Asks the Windows host (flush_server.ps1) to empty its file cache; used with --pre-cold-cmd.
# Usage: scripts/vm/host_cache/host_flush.sh
set -euo pipefail
if [[ -n "${FLUSH_DIR:-}" ]]; then DIR="$FLUSH_DIR"
elif [[ -d /mnt/hgfs/flush ]]; then DIR=/mnt/hgfs/flush
else DIR=/mnt/hgfs/duckdb-flush; fi
TIMEOUT="${FLUSH_TIMEOUT:-60}"
[[ -d "$DIR" && -w "$DIR" ]] || { echo "host_flush: $DIR not mounted or not writable" >&2; exit 2; }

id="$(date +%s%N)_$$"
ack="$DIR/ack_$id"
echo "$(date -u +%FT%TZ)" > "$DIR/req_$id"

deadline=$(( $(date +%s) + TIMEOUT ))
until [[ -s "$ack" ]]; do
    if (( $(date +%s) >= deadline )); then
        rm -f "$DIR/req_$id"
        echo "host_flush: no ack from the host after ${TIMEOUT}s (is flush_server.ps1 running?)" >&2
        exit 3
    fi
    sleep 0.05
done
reply="$(cat "$ack")"; rm -f "$ack"
[[ "$reply" == ok* ]] || { echo "host_flush: host reported: $reply" >&2; exit 4; }
r="${reply#ok }"; r="${r//,/.}"   # decimal comma from a non-English Windows locale
if [[ "${FLUSH_VERBOSE:-0}" == 1 ]]; then echo "$r"; else echo "${r%% *}"; fi
