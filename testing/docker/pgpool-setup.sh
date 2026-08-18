#!/bin/bash
# Configure and start the local pgpool-II, in the shape
# docs/pgpool-hook-contract.md §4 proposes for agent-led failover:
# watchdog OFF, failover_command as an advisory poke,
# follow_primary_command EMPTY, detach_false_primary on,
# auto_failback off.
#
# Plays BOOTSTRAP.md Phase 1.4 (Ansible renders pgpool config) + Phase
# 3.1 (operator unmasks and starts). Run on every node after
# `pg_agentctl cluster init`.
#
# Harness deviations from BOOTSTRAP, deliberate and noted:
#   - enable_pool_hba = off and trust auth everywhere. Avoids the
#     pg_enc / .pgpoolkey / AES pool_passwd machinery, which is
#     orthogonal to the failover behavior under test.
set -euo pipefail

# This cell's layout — see provision.sh and cluster::Facts. pgpool's
# config directory and unit name are both family-specific
# (/etc/pgpool2 + pgpool2.service on Debian, /etc/pgpool-II +
# pgpool-II.service on RHEL); nothing else here is.
# shellcheck disable=SC1091
[ -r /etc/pg-agent-matrix/env ] && . /etc/pg-agent-matrix/env
PG_VERSION="${PG_VERSION:-17}"
PG_HOME="${PG_HOME:-/var/lib/postgresql}"
PG_LOG="${PG_LOG:-/var/log/postgresql/postgresql-${PG_VERSION}-main.log}"
PGPOOL_UNIT="${PGPOOL_UNIT:-pgpool2}"
PGPOOL_CONF_DIR="${PGPOOL_CONF_DIR:-/etc/pgpool2}"

PCP_PASSWORD="${PCP_PASSWORD:-pcpsecret}"
CONF="$PGPOOL_CONF_DIR/pgpool.conf"
# Where pgpool keeps pgpool_status, and therefore the file the restart
# below has to delete. Set EXPLICITLY rather than left to the package
# default, which is one more thing the two families disagree about.
LOGDIR="$(dirname "$PG_LOG")"

mkdir -p "$PGPOOL_CONF_DIR"
install -d -o postgres -g postgres "$LOGDIR"

# --- base config -------------------------------------------------------
cat > "$CONF" <<EOF
# --- harness base (see pgpool-setup.sh) ---
listen_addresses = '*'
port = 9999
socket_dir = '/var/run/postgresql'
pcp_listen_addresses = '*'
pcp_port = 9898
pcp_socket_dir = '/var/run/postgresql'
# Explicit for the same reason as logdir: the compiled-in default is
# /var/run/pgpool/pgpool.pid on RHEL and /var/run/postgresql on Debian,
# and RHEL's directory comes from a tmpfiles.d entry that has not run
# against this container's tmpfs /run — pgpool then exits 3 at startup
# with "could not open pid file", on a loop, until systemd gives up.
pid_file_name = '/var/run/postgresql/pgpool.pid'
# pgpool 4.6 renamed this to work_dir and warns when it sees the old
# name, but still honours it; Debian's 4.5 knows only `logdir`. The old
# name is the one both understand.
logdir = '${LOGDIR}'

backend_clustering_mode = 'streaming_replication'
enable_pool_hba = off
# Empty = do not use a pool_passwd file at all, which is the honest
# statement of this harness's auth deviation (trust everywhere, no
# pg_enc/AES machinery). Left at its default, pgpool resolves the
# relative name against its config directory and tries to CREATE
# /etc/pgpool-II/pool_passwd as the postgres user — root-owned on RHEL,
# so it exits 3 before serving anything.
pool_passwd = ''
log_destination = 'stderr'
logging_collector = off
log_min_messages = 'info'

# Streaming-replication check: how pgpool LEARNS which backend is
# primary. Kept (hook-contract §4) — we cut the causing, not the
# learning.
sr_check_period = 2
sr_check_user = 'pgpool'
sr_check_password = ''
sr_check_database = 'postgres'

health_check_period = 2
health_check_timeout = 3
health_check_user = 'pgpool'
health_check_password = ''
health_check_database = 'postgres'
# The router must outwait a cold-booting database (G10): pgpool starts
# seconds after the blip while the agent's cold-start reconcile is
# still bringing PostgreSQL through crash recovery (~12 s observed).
# With 1-retry tolerance the health check detached the primary during
# that window, and with auto_failback off + PCP not listening during
# startup's find_primary_node loop, pgpool wedged until
# search_primary_node_timeout. ~22 s of retry tolerance covers the
# window; detach speed is ROUTING convergence, not failover authority
# (the lease owns that), so nothing safety-relevant slows down.
health_check_max_retries = 10
health_check_retry_delay = 2
# Backstop, not the fix: a genuinely dead primary bounds pgpool's
# startup search at 30 s (default 300) and it comes up degraded with
# PCP listening — reachable by the attach fan-out instead of wedged.
search_primary_node_timeout = 30
connect_timeout = 3000

# Per-instance routing reactions: self-limiting, kept on.
failover_on_backend_error = on
detach_false_primary = on
# Slots are in use, so pgpool's own caveat applies; the agent owns
# reattach.
auto_failback = off

# The whole point: no watchdog. Each instance is uncoordinated, and
# the agent is the authority.
use_watchdog = off

recovery_user = 'postgres'
recovery_password = ''
recovery_1st_stage_command = 'recovery_1st_stage'

EOF

# --- backends + canonical hook block, from the agent itself ------------
# This IS the deployed contract: failover_command as the advisory poke,
# follow_primary_command empty, decision-critical settings included.
# No overrides — check-hooks must pass on this file verbatim.
runuser -u postgres -- pg_agentctl gen-pgpool >> "$CONF"

# --- PCP auth ----------------------------------------------------------
printf 'pgpool:%s\n' "$(pg_md5 "$PCP_PASSWORD")" > "$PGPOOL_CONF_DIR/pcp.conf"
chmod 0644 "$PGPOOL_CONF_DIR/pcp.conf"
printf '*:9898:pgpool:%s\n' "$PCP_PASSWORD" > "$PG_HOME/.pcppass"
chown postgres:postgres "$PG_HOME/.pcppass"
chmod 0600 "$PG_HOME/.pcppass"

# --- start (BOOTSTRAP Phase 3.1) --------------------------------------
# Discard any cached backend status: this is a config-changing restart,
# and down-status is otherwise sticky with no watchdog leader to correct
# it (measured in S11 / hook-contract §5.3). Equivalent to `pgpool -D`.
# Stop FIRST: pgpool rewrites pgpool_status from its in-memory map on
# shutdown, so an rm while it runs is silently undone by the restart's
# stop phase — a freshly "cleared" instance then boots with the stale
# backend states anyway (bit E3 on its first run: a mid-repair "down"
# survived the rm and wedged the 3-backends-up wait).
systemctl stop "${PGPOOL_UNIT}.service" 2>/dev/null || true
rm -f "$LOGDIR/pgpool_status"
systemctl unmask "${PGPOOL_UNIT}.service"
# Enabled, not just started: the router must come back on its own
# after a node reboot (G10's site power blip) — pgpool is
# systemd-managed in this deployment shape, not agent-managed.
systemctl enable "${PGPOOL_UNIT}.service"
systemctl start "${PGPOOL_UNIT}.service"
echo "pgpool-setup: started ${PGPOOL_UNIT} on $(hostname)"
