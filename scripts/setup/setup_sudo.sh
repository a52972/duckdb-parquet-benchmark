#!/usr/bin/env bash
# Installs a helper so cold runs can drop the page cache without a password.
# Usage: sudo scripts/setup/setup_sudo.sh    (undo: --remove)
set -euo pipefail
[[ $EUID -eq 0 ]] || { echo "run with sudo" >&2; exit 1; }
USER_NAME="${SUDO_USER:?run with sudo from the user account}"
HELPER=/usr/local/sbin/drop-caches
RULE=/etc/sudoers.d/duckdb-benchmark

if [[ "${1:-}" == "--remove" ]]; then
    rm -f "$HELPER" "$RULE"; echo "removed $HELPER and $RULE"; exit 0
fi

command -v strace >/dev/null || apt-get install -y strace

cat > "$HELPER" <<'EOS'
#!/bin/sh
sync
echo 3 > /proc/sys/vm/drop_caches
EOS
chown root:root "$HELPER"; chmod 0755 "$HELPER"

TMP=$(mktemp)
echo "$USER_NAME ALL=(root) NOPASSWD: $HELPER, /usr/sbin/dmidecode -t system" > "$TMP"
visudo -cf "$TMP" >/dev/null
install -o root -g root -m 0440 "$TMP" "$RULE"; rm -f "$TMP"
echo "OK: $USER_NAME can run 'sudo -n $HELPER' and 'sudo -n /usr/sbin/dmidecode -t system'"
