#!/usr/bin/env bash
# Records hardware, OS and software versions.
# Usage: scripts/setup/capture_env.sh [output_file]
set -uo pipefail

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
OUT="${1:-$BASE_DIR/results/local/env.txt}"
mkdir -p "$(dirname "$OUT")"

section() { printf '\n===== %s =====\n' "$1"; }
{
    echo "Captured: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    section "uname -srvmo";           uname -srvmo   # no host name
    section "/etc/os-release";       cat /etc/os-release
    section "systemd-detect-virt";   systemd-detect-virt 2>&1
    section "lscpu";                 lscpu
    section "free -h";               free -h
    section "/proc/meminfo (head)";  head -5 /proc/meminfo
    section "lsblk";                 lsblk -o NAME,SIZE,TYPE,ROTA,DISC-GRAN,MODEL,VENDOR,TRAN,SCHED,MOUNTPOINT
    section "block queue settings"
    for d in /sys/block/sd* /sys/block/nvme* /sys/block/vd*; do
        [[ -d "$d" ]] || continue
        echo "$(basename "$d"): rotational=$(cat "$d/queue/rotational") scheduler=$(cat "$d/queue/scheduler") read_ahead_kb=$(cat "$d/queue/read_ahead_kb") nr_requests=$(cat "$d/queue/nr_requests")"
    done
    section "df -h /";               df -h /
    section "mount (root fs)";       findmnt -no SOURCE,FSTYPE,OPTIONS /
    section "vm sysctls"
    for k in swappiness dirty_ratio dirty_background_ratio vfs_cache_pressure; do
        echo "vm.$k = $(cat /proc/sys/vm/$k)"
    done
    section "transparent hugepages"; cat /sys/kernel/mm/transparent_hugepage/enabled
    section "cpufreq governor";      cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>&1
    section "DuckDB";                "$BASE_DIR/duckdb" -csv -noheader -c "SELECT version(), current_setting('threads'), current_setting('memory_limit')"
    section "Python";                python3 --version
    section "strace";                strace -V 2>&1 | head -1
    section "dmidecode -t system"
    if [[ $EUID -eq 0 ]]; then dmidecode -t system | grep -vE "Serial Number|UUID"; else sudo -n /usr/sbin/dmidecode -t system 2>&1 | grep -vE "Serial Number|UUID" || echo "(skipped: needs sudo)"; fi
} > "$OUT" 2>&1

if [[ -n "${SUDO_UID:-}" ]]; then chown "$SUDO_UID:$SUDO_GID" "$OUT"; fi
echo "Environment → $OUT"
