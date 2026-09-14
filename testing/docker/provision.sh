#!/bin/bash
# Per-node provisioning for the acceptance cluster. Idempotent; runs at
# every boot before PostgreSQL / pg_agentd (see provision.service).
# Plays the role Ansible plays in production (BOOTSTRAP.md Phase 1).
set -euo pipefail

# --- this cell's layout ------------------------------------------------
# Read from disk, not the environment: systemd hands its units a clean
# env, so the image's ENV is invisible here even though `docker exec`
# sees it. The Dockerfiles write this file; see cluster::Facts for the
# key set and why the image is the authority on it.
#
# The defaults below are Debian's, so this script still works against
# an image built before the facts file existed. They are NOT a fallback
# worth relying on: on a RHEL image with no facts file every path here
# would be wrong in a way that looks like PostgreSQL is broken.
# shellcheck disable=SC1091
[ -r /etc/pg-agent-matrix/env ] && . /etc/pg-agent-matrix/env
PG_VERSION="${PG_VERSION:-17}"
PG_FAMILY="${PG_FAMILY:-debian}"
PG_UNIT="${PG_UNIT:-postgresql@${PG_VERSION}-main}"
PGDATA="${PGDATA:-/var/lib/postgresql/${PG_VERSION}/main}"
PG_BIN="${PG_BIN:-/usr/lib/postgresql/${PG_VERSION}/bin}"
PG_CONF_DIR="${PG_CONF_DIR:-/etc/postgresql/${PG_VERSION}/main}"
PG_HOME="${PG_HOME:-/var/lib/postgresql}"
PG_LOG="${PG_LOG:-/var/log/postgresql/postgresql-${PG_VERSION}-main.log}"
PGPOOL_UNIT="${PGPOOL_UNIT:-pgpool2}"
PGPOOL_CONF_DIR="${PGPOOL_CONF_DIR:-/etc/pgpool2}"

# The installation prefix is the parent of bin/ — that is exactly the
# distinction pg_install_prefix draws (config.rs PostgresConfig docs).
PG_PREFIX="${PG_BIN%/bin}"

NODE_ID="${HOSTNAME#db}"
case "$NODE_ID" in
    0|1|2) ;;
    *) echo "provision: hostname $HOSTNAME does not look like dbN" >&2; exit 1 ;;
esac
MARKER=/var/lib/pg-agent-provisioned

echo "provision: node id $NODE_ID ($HOSTNAME), $PG_FAMILY PostgreSQL $PG_VERSION"

# --- pgpool node id (agent's implicit local-id source) -----------------
# In pgpool's OWN config dir, which differs by family — the whole point
# of this file is that pgpool and the agent read the same one.
mkdir -p "$PGPOOL_CONF_DIR"
echo "$NODE_ID" > "$PGPOOL_CONF_DIR/pgpool_node_id"

# --- TLS material (mounted read-only at /certs by compose) -------------
mkdir -p /etc/pg_agent/tls
install -m 0644 /certs/ca.crt /etc/pg_agent/tls/ca.crt
install -m 0644 "/certs/${HOSTNAME}.crt" /etc/pg_agent/tls/node.crt
install -m 0600 "/certs/${HOSTNAME}.key" /etc/pg_agent/tls/node.key
chown -R postgres:postgres /etc/pg_agent/tls

# --- agent config ------------------------------------------------------
cat > /etc/pg_agent/config.toml <<EOF
listen = "0.0.0.0"

[tls]
ca_cert = "/etc/pg_agent/tls/ca.crt"
cert    = "/etc/pg_agent/tls/node.crt"
key     = "/etc/pg_agent/tls/node.key"

[[pool]]
id       = 0
hostname = "db0"

[[pool]]
id       = 1
hostname = "db1"

[[pool]]
id       = 2
hostname = "db2"

[postgres]
# Every path written explicitly rather than left to the agent's
# defaults. Those defaults name Debian's PostgreSQL 17 layout, and this
# matrix runs 15/16/17 across two packaging families whose directory
# conventions agree on nothing. A cell that silently fell back to the
# Debian 17 answers would fail in a way that looks like a product bug.
pg_install_prefix = "${PG_PREFIX}"
data_dir          = "${PGDATA}"
user_home         = "${PG_HOME}"
archive_dir       = "${PG_HOME}/archive"
service           = "${PG_UNIT}.service"

# Container-to-container replication without client certs.
[postgres.replication]
sslmode = "disable"

[pcp]
# pgpool's unit is pgpool2.service on Debian and pgpool-II.service on
# RHEL. The agent only ever manages it through this name.
pgpool_service = "${PGPOOL_UNIT}.service"

# Fresh-cluster bootstrap: all three nodes initdb as TL1 primaries, so
# peer evidence is unavailable/contradictory until ClusterInit shapes
# the cluster. 0 disables the quorum gate; the timeline comparison
# still fires when peers answer. See testing/README.md "findings".
[startup]
phantom_check_required_peers = 0

# pgpool stays masked in phase 1; don't let the supervisor fight that.
[supervisor]
pgpool = false

# Timing only. There is no switch here any more: consensus is not
# optional, so the daemon joins the lease or refuses to start. A
# leftover \`enabled = true\` would still load (with a WARN); an
# \`enabled = false\` would fail the config load outright.
#
# Test-friendly timing. Invariants:
# leader_ttl >= loop_wait + 2*retry_timeout (10 >= 1+4);
# retry_timeout > election_timeout (2s > 1s).
#
# leader_ttl deliberately stays at 10 s. A partition-time promotion
# legitimately stalls ~5-7 s on its first probe of the dead peer
# (FETCH_WAL_SETUP_TIMEOUT before the cooldown kicks in, finding 14);
# a tighter ttl would put rival deposal inside a healthy promotion
# window - the churn finding 13 exists to prevent. The suite's speed
# comes from cadence and detection, not from shaving the safety
# window.
[raft]
loop_wait_secs      = 1
retry_timeout_secs  = 2
leader_ttl_secs     = 10
election_timeout_ms = 1000
EOF

# --- RHEL: create the cluster the package does not create for you ------
# Debian's postgresql-common initdb's a `main` cluster in the package's
# postinst, which is why the Debian path here has nothing to do. RHEL
# ships the software and leaves the data directory to the operator, so
# this is the step that has no Debian counterpart rather than a
# different spelling of one.
if [ "$PG_FAMILY" = "rhel" ] && [ ! -f "$PGDATA/PG_VERSION" ]; then
    echo "provision: initdb $PGDATA (RHEL has no packaged cluster)"
    "/usr/pgsql-${PG_VERSION}/bin/postgresql-${PG_VERSION}-setup" initdb
fi

# --- PostgreSQL config -------------------------------------------------
# On Debian PG_CONF_DIR is /etc/postgresql/<v>/main and postgresql.conf
# already carries `include_dir = 'conf.d'`. On RHEL the config lives
# INSIDE PGDATA and initdb writes no include_dir at all, so the drop-in
# directory has to be created and wired up once.
mkdir -p "$PG_CONF_DIR/conf.d"
if [ "$PG_FAMILY" = "rhel" ]; then
    if ! grep -q "^include_dir = 'conf.d'" "$PG_CONF_DIR/postgresql.conf"; then
        printf "\ninclude_dir = 'conf.d'\n" >> "$PG_CONF_DIR/postgresql.conf"
    fi
fi

cat > "$PG_CONF_DIR/conf.d/10-pg-agent-acceptance.conf" <<EOF
listen_addresses = '*'
# The retention floor slots structurally cannot provide (finding 22): a
# slot created at promotion cannot retroactively protect segments
# written before it, and a standby whose replay trails inside one of
# those needs exactly those. validate-env warns below 512MB.
wal_keep_size = '512MB'
# Replication liveness detection. PostgreSQL's 60s defaults dominated
# the suite's runtime: a severed walreceiver held 'streaming' for a
# full minute before the wedge clock could even start (68s observed),
# the primary kept counting severed standbys as ack sources for the
# same minute (58s to reach sync_commit=blocked), and a partitioned
# primary's shutdown drained walsenders toward it (47s). Three waits,
# one knob, ~30% of the run. These are DETECTION-latency knobs, not
# safety ones — every assertion they gate is about event order, not
# duration — so the test cluster detects in 15s instead of 60s.
#
# The status interval must stay well under the timeout or a HEALTHY
# walsender starts timing out: the standby only replies every
# wal_receiver_status_interval, and the 10s default would leave 5s of
# margin against a 15s timeout. 2s keeps the margin comfortable under
# G11's write load and mid-basebackup.
wal_sender_timeout = '15s'
wal_receiver_timeout = '15s'
wal_receiver_status_interval = '2s'
# The agent writes standby recovery settings to \$PGDATA/myrecovery.conf
# (SPEC §5.10, pgpool convention); PostgreSQL only reads it if the main
# config includes it. Ansible owns this line in production.
include_if_exists = '${PGDATA}/myrecovery.conf'
EOF

if [ "$PG_FAMILY" = "rhel" ]; then
    # Give the harness the one server log path it tails on both
    # families. Debian gets this for free — pg_ctlcluster redirects the
    # postmaster's stderr into /var/log/postgresql/postgresql-<v>-main.log
    # — while RHEL's unit lets stderr go to the journal.
    #
    # The collector rather than the journal on purpose: journald rate
    # limits (10k messages / 30 s by default) and DROPS the excess, and
    # this suite asserts on the ORDER of specific log lines under G11's
    # write load. A silently dropped line would read as a safety
    # violation. A file cannot rate limit.
    #
    # Rotation is off in all three of its forms so the path stays
    # valid for the whole run rather than becoming a stale inode that
    # `tail -F` has to notice.
    cat >> "$PG_CONF_DIR/conf.d/10-pg-agent-acceptance.conf" <<EOF
logging_collector = on
log_directory = '$(dirname "$PG_LOG")'
log_filename = '$(basename "$PG_LOG")'
log_rotation_age = 0
log_rotation_size = 0
log_truncate_on_rotation = off
unix_socket_directories = '/var/run/postgresql, /tmp'
EOF
fi
chown -R postgres:postgres "$PG_CONF_DIR/conf.d"

# PostgreSQL is AGENT-managed: the OS must never autostart it. Debian's
# generator starts every 'auto' cluster at boot through the postgresql
# meta-service — and enabling pgpool2 pulls that in via its
# Wants=postgresql.service — which is exactly the path that raced the
# agent's cold-start reconciliation out of G10 (PostgreSQL was up 2 s
# before the agent, so the reconcile correctly no-op'd and the product
# path went untested). 'manual' closes autostart while leaving explicit
# `systemctl start postgresql@<ver>-main` (provision bootstrap, recover,
# cold start) untouched.
#
# RHEL needs no equivalent: its unit is a plain per-version service
# with no generator and no meta-service, and the Dockerfile disables it.
if [ "$PG_FAMILY" = "debian" ]; then
    echo manual > "$PG_CONF_DIR/start.conf"
fi

# The same principle one level deeper: the OS must not RESURRECT it
# either. PGDG's RHEL unit ships `Restart=on-failure` active, while
# Debian's ships it commented out — so a SIGKILLed postmaster stays
# dead on one family and is back within a second on the other, with no
# agent involvement at all.
#
# That difference silently invalidated the crash scenarios on the RHEL
# cell: G9 kills the primary's postmaster and waits for the lease to
# depose it, but systemd handed the primary straight back, nothing was
# ever deposed, and the next thirteen scenarios ran against a cluster
# no assertion expected (32 failures, all of them downstream of this
# one fact).
#
# Written for BOTH families, not just RHEL: the drop-in is identical
# either way, and a suite whose crash shape depends on which distro it
# booted is a suite that proves less than it appears to. This does not
# touch the agent's own `systemctl start` — recover, cold start and
# provisioning bootstrap all still work.
#
# This started as the TEST cluster's uniformity, before the product
# had anything to say about it. It is now also what BOOTSTRAP §1.1
# tells a real deployment to write — same filename, same content — and
# `validate-env` refuses to start the daemon without it. Provisioning
# writes it here for the same reason Ansible does there, which is why
# the two stayed identical rather than the fixture keeping a private
# workaround. See testing/FINDINGS.md finding 29.
install -d "/etc/systemd/system/${PG_UNIT}.service.d"
cat > "/etc/systemd/system/${PG_UNIT}.service.d/10-agent-managed.conf" <<'EOF'
# pg-agent-acceptance: PostgreSQL's lifecycle belongs to the agent.
# systemd may neither start it at boot nor restart it after a crash.
[Service]
Restart=no
EOF
systemctl daemon-reload

HBA="$PG_CONF_DIR/pg_hba.conf"
if ! grep -q "pg-agent-acceptance" "$HBA"; then
    cat >> "$HBA" <<'EOF'
# pg-agent-acceptance: replication + rewind + pgpool sr_check/health
# check across the compose network. Trust everywhere: this is an
# isolated test network, and pool_passwd/AES auth is orthogonal to the
# failover behavior under test (see testing/README.md deviations).
host    replication     all             0.0.0.0/0               trust
host    all             all             0.0.0.0/0               trust
EOF
fi

# --- filesystem bits ---------------------------------------------------
install -d -o postgres -g postgres "${PG_HOME}/archive"
# Both exist already on Debian; on RHEL the log directory is this
# harness's invention and the socket directory is created by a
# tmpfiles.d entry that has no reason to have run yet.
install -d -o postgres -g postgres "$(dirname "$PG_LOG")"
install -d -o postgres -g postgres /var/run/postgresql

# --- first-boot cluster shaping ---------------------------------------
if [ ! -e "$MARKER" ]; then
    if [ "$NODE_ID" = "0" ]; then
        echo "provision: bootstrap primary — starting PostgreSQL"
        systemctl start "${PG_UNIT}.service"
        until runuser -u postgres -- "$PG_BIN/pg_isready" -q; do sleep 0.5; done
        runuser -u postgres -- "$PG_BIN/psql" -v ON_ERROR_STOP=1 <<'SQL'
DO $$ BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'repl') THEN
    CREATE ROLE repl WITH LOGIN REPLICATION;
  END IF;
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'pgpool') THEN
    CREATE ROLE pgpool WITH LOGIN;
  END IF;
END $$;
CREATE EXTENSION IF NOT EXISTS pgpool_recovery;
SQL
        runuser -u postgres -- "$PG_BIN/psql" -d template1 -v ON_ERROR_STOP=1 \
            -c 'CREATE EXTENSION IF NOT EXISTS pgpool_recovery;'
    else
        echo "provision: standby node — PostgreSQL stays down until ClusterInit"
    fi
    touch "$MARKER"
else
    # Subsequent boots: PostgreSQL stays down here ON PURPOSE. The
    # agent's cold-start reconciliation (finding 21) owns bringing it
    # back — standby-shaped pgdata starts unconditionally, a
    # primary-shaped one only when the persisted lease still names
    # this node. Starting it from provisioning would preempt exactly
    # the product path G10 exists to exercise (and did, masking the
    # cold-start behavior entirely on the first G10 run).
    echo "provision: subsequent boot — PostgreSQL left to the agent's cold-start reconcile"
fi

echo "provision: done"
