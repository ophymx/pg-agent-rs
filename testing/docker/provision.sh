#!/bin/bash
# Per-node provisioning for the acceptance cluster. Idempotent; runs at
# every boot before PostgreSQL / pg_agentd (see provision.service).
# Plays the role Ansible plays in production (BOOTSTRAP.md Phase 1).
set -euo pipefail

NODE_ID="${HOSTNAME#db}"
case "$NODE_ID" in
    0|1|2) ;;
    *) echo "provision: hostname $HOSTNAME does not look like dbN" >&2; exit 1 ;;
esac
MARKER=/var/lib/pg-agent-provisioned

echo "provision: node id $NODE_ID ($HOSTNAME)"

# --- pgpool node id (agent's implicit local-id source) -----------------
mkdir -p /etc/pgpool2
echo "$NODE_ID" > /etc/pgpool2/pgpool_node_id

# --- TLS material (mounted read-only at /certs by compose) -------------
mkdir -p /etc/pg_agent/tls
install -m 0644 /certs/ca.crt /etc/pg_agent/tls/ca.crt
install -m 0644 "/certs/${HOSTNAME}.crt" /etc/pg_agent/tls/node.crt
install -m 0600 "/certs/${HOSTNAME}.key" /etc/pg_agent/tls/node.key
chown -R postgres:postgres /etc/pg_agent/tls

# --- agent config ------------------------------------------------------
cat > /etc/pg_agent/config.toml <<'EOF'
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

# Container-to-container replication without client certs.
[postgres.replication]
sslmode = "disable"

# Fresh-cluster bootstrap: all three nodes initdb as TL1 primaries, so
# peer evidence is unavailable/contradictory until ClusterInit shapes
# the cluster. 0 disables the quorum gate; the timeline comparison
# still fires when peers answer. See testing/README.md "findings".
[startup]
phantom_check_required_peers = 0

# pgpool stays masked in phase 1; don't let the supervisor fight that.
[supervisor]
pgpool = false

# EXECUTE MODE FROM FIRST BOOT - the greenfield deployment shape the
# suite validates. This is the only supported shape: the pgpool-led
# path (shadow on / enabled off) is deleted.
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
enabled             = true
shadow              = false
loop_wait_secs      = 1
retry_timeout_secs  = 2
leader_ttl_secs     = 10
election_timeout_ms = 1000
EOF

# --- PostgreSQL config -------------------------------------------------
PGCONF_DIR=/etc/postgresql/17/main
mkdir -p "$PGCONF_DIR/conf.d"
cat > "$PGCONF_DIR/conf.d/10-pg-agent-acceptance.conf" <<'EOF'
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
# The agent writes standby recovery settings to $PGDATA/myrecovery.conf
# (SPEC §5.10, pgpool convention); PostgreSQL only reads it if the main
# config includes it. Ansible owns this line in production.
include_if_exists = '/var/lib/postgresql/17/main/myrecovery.conf'
EOF
chown -R postgres:postgres "$PGCONF_DIR/conf.d"

# PostgreSQL is AGENT-managed: the OS must never autostart it. Debian's
# generator starts every 'auto' cluster at boot through the postgresql
# meta-service — and enabling pgpool2 pulls that in via its
# Wants=postgresql.service — which is exactly the path that raced the
# agent's cold-start reconciliation out of G10 (PostgreSQL was up 2 s
# before the agent, so the reconcile correctly no-op'd and the product
# path went untested). 'manual' closes autostart while leaving explicit
# `systemctl start postgresql@17-main` (provision bootstrap, recover,
# cold start) untouched.
echo manual > "$PGCONF_DIR/start.conf"

HBA="$PGCONF_DIR/pg_hba.conf"
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
install -d -o postgres -g postgres /var/lib/postgresql/archive

# --- first-boot cluster shaping ---------------------------------------
if [ ! -e "$MARKER" ]; then
    if [ "$NODE_ID" = "0" ]; then
        echo "provision: bootstrap primary — starting PostgreSQL"
        systemctl start postgresql@17-main.service
        until runuser -u postgres -- pg_isready -q; do sleep 0.5; done
        runuser -u postgres -- psql -v ON_ERROR_STOP=1 <<'SQL'
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
        runuser -u postgres -- psql -d template1 -v ON_ERROR_STOP=1 \
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
