#!/bin/sh
# Post-install scriptlet for pg-agent-rs packages.
#
# Behaviour matrix:
#   fresh install      → daemon-reload, no enable, no start
#                        (Ansible enables after staging config.toml)
#   upgrade, running   → daemon-reload + restart (pick up new binary)
#   upgrade, stopped   → daemon-reload, leave stopped
#
# Bare `systemctl` — no deb-systemd-helper or dh_* magic. Same script
# runs from both the .deb postinst (`$1 = "configure"`) and the .rpm
# %post (`$1 = 1` install / `$1 = 2` upgrade); we don't actually need
# to distinguish, because the restart-if-active check covers both.

set -e

# The config directory Ansible writes config.toml into. Created here
# rather than shipped as a packaged directory: neither cargo-deb nor
# cargo-generate-rpm has a file-less directory asset, and an empty dir
# is not worth a placeholder file. mkdir -p is idempotent, so upgrades
# and reinstalls are no-ops.
mkdir -p /etc/pg_agent
chmod 0755 /etc/pg_agent

if ! command -v systemctl >/dev/null 2>&1; then
    # Container build or non-systemd host. Nothing further to do.
    exit 0
fi

# Pick up the (possibly updated) unit file.
systemctl daemon-reload >/dev/null 2>&1 || true

# Restart-if-running: an upgrade with the service active picks up
# the new binary transparently. New installs see is-active=false →
# no-op, leaving the service off until Ansible enables it.
if systemctl is-active --quiet pg_agentd.service 2>/dev/null; then
    systemctl restart pg_agentd.service >/dev/null 2>&1 || true
fi

exit 0
