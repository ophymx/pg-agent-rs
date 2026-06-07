#!/bin/sh
# Pre-remove scriptlet for pg-agent-rs packages.
#
# Skips on upgrades — the old binary is replaced in place while
# systemd holds the file descriptor on the running process. The new
# package's postinstall script handles the restart.
#
# Arg convention:
#   .deb prerm:   "$1" is "remove" | "purge" | "upgrade" | "deconfigure"
#   .rpm %preun:  "$1" is the number of instances after this
#                 transaction completes (1 → upgrade, 0 → removal)

set -e

case "$1" in
    upgrade|1)
        # Old version's preremove during an upgrade — do nothing.
        # postinstall on the new package handles the restart.
        exit 0
        ;;
esac

if ! command -v systemctl >/dev/null 2>&1; then
    exit 0
fi

# Real removal — stop the service before its binary disappears.
if systemctl is-active --quiet pg_agentd.service 2>/dev/null; then
    systemctl stop pg_agentd.service >/dev/null 2>&1 || true
fi

exit 0
