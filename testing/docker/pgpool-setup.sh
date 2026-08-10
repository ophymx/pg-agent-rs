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
#   - failover_command routes through /usr/local/bin/failover-probe so
#     each invocation's argv is recorded before pg_agentc runs — that
#     recording is the measurement for hook-contract §5.1.
set -euo pipefail

PCP_PASSWORD="${PCP_PASSWORD:-pcpsecret}"
CONF=/etc/pgpool2/pgpool.conf
FOLLOW_PRIMARY="${FOLLOW_PRIMARY:-}"   # non-empty only for the §5.4 probe

# --- failover argv recorder -------------------------------------------
cat > /usr/local/bin/failover-probe <<'EOF'
#!/bin/sh
# Record this invocation, then run the real hook client. One line per
# firing: "<iso8601> <hostname> <argv...>".
printf '%s %s %s\n' "$(date -Is)" "$(hostname)" "$*" >> /var/log/failover-probe.log
exec /usr/bin/pg_agentc failover "$@"
EOF
chmod 0755 /usr/local/bin/failover-probe
: > /var/log/failover-probe.log
chmod 0666 /var/log/failover-probe.log

# --- base config -------------------------------------------------------
cat > "$CONF" <<EOF
# --- harness base (see pgpool-setup.sh) ---
listen_addresses = '*'
port = 9999
socket_dir = '/var/run/postgresql'
pcp_listen_addresses = '*'
pcp_port = 9898
pcp_socket_dir = '/var/run/postgresql'

backend_clustering_mode = 'streaming_replication'
enable_pool_hba = off
log_destination = 'stderr'
logging_collector = off
log_min_messages = 'info'

# Streaming-replication check: how pgpool LEARNS which backend is
# primary. Kept (hook-contract §4) — we cut the causing, not the
# learning.
sr_check_period = 5
sr_check_user = 'pgpool'
sr_check_password = ''
sr_check_database = 'postgres'

health_check_period = 5
health_check_timeout = 5
health_check_user = 'pgpool'
health_check_password = ''
health_check_database = 'postgres'
health_check_max_retries = 2
health_check_retry_delay = 1
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
runuser -u postgres -- pg_agentctl gen-pgpool >> "$CONF"

# --- hook overrides for the target contract ----------------------------
cat >> "$CONF" <<EOF

# --- target hook contract (docs/pgpool-hook-contract.md §4) ---
# Later directives win in pgpool.conf, so these override the canonical
# block above.
#
# failover_command: kept as an advisory poke, routed through the probe
# so every firing is recorded.
failover_command = '/usr/local/bin/failover-probe %d %h %p %D %m %H %M %P %r %R %N %S'
# follow_primary_command: MUST be empty. A non-empty value makes pgpool
# degenerate every healthy standby after a primary failover.
follow_primary_command = '$FOLLOW_PRIMARY'
# Watchdog is off, so these never fire; kept blank to match.
wd_escalation_command = ''
wd_de_escalation_command = ''
EOF

# --- PCP auth ----------------------------------------------------------
printf 'pgpool:%s\n' "$(pg_md5 "$PCP_PASSWORD")" > /etc/pgpool2/pcp.conf
chmod 0644 /etc/pgpool2/pcp.conf
printf '*:9898:pgpool:%s\n' "$PCP_PASSWORD" > /var/lib/postgresql/.pcppass
chown postgres:postgres /var/lib/postgresql/.pcppass
chmod 0600 /var/lib/postgresql/.pcppass

# --- start (BOOTSTRAP Phase 3.1) --------------------------------------
# Discard any cached backend status: this is a config-changing restart,
# and down-status is otherwise sticky with no watchdog leader to correct
# it (measured in S11 / hook-contract §5.3). Equivalent to `pgpool -D`.
rm -f /var/log/postgresql/pgpool_status
systemctl unmask pgpool2.service
systemctl restart pgpool2.service
echo "pgpool-setup: started on $(hostname)"
