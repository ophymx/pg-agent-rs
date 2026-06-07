#!/bin/sh
# Post-remove scriptlet for pg-agent-rs packages.
#
# Runs AFTER the files are gone. Two responsibilities, both only on
# a real removal:
#   1. disable the unit so a subsequent reboot doesn't try to start
#      the (now-deleted) binary
#   2. daemon-reload so systemd forgets the unit entirely
#
# Arg convention:
#   .deb postrm:  "$1" is "remove" | "purge" | "upgrade" | "abort-*" |
#                 "disappear"
#   .rpm %postun: "$1" is the number of instances after this
#                 transaction (1 → upgrade, 0 → full removal)

set -e

case "$1" in
    upgrade|1)
        # Upgrade tail — leave the service state alone; the new
        # package's postinstall will daemon-reload and restart.
        exit 0
        ;;
esac

if ! command -v systemctl >/dev/null 2>&1; then
    exit 0
fi

systemctl disable pg_agentd.service >/dev/null 2>&1 || true
systemctl daemon-reload >/dev/null 2>&1 || true

exit 0
