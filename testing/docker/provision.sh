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

# Shadow-mode HA loop with test-friendly timing.
# Invariants: leader_ttl >= loop_wait + 2*retry_timeout (10 >= 2+6);
# retry_timeout > election_timeout (3s > 1s).
[raft]
shadow              = true
loop_wait_secs      = 2
retry_timeout_secs  = 3
leader_ttl_secs     = 10
election_timeout_ms = 1000
EOF

# --- PostgreSQL config -------------------------------------------------
PGCONF_DIR=/etc/postgresql/17/main
mkdir -p "$PGCONF_DIR/conf.d"
cat > "$PGCONF_DIR/conf.d/10-pg-agent-acceptance.conf" <<'EOF'
listen_addresses = '*'
# The agent writes standby recovery settings to $PGDATA/myrecovery.conf
# (SPEC §5.10, pgpool convention); PostgreSQL only reads it if the main
# config includes it. Ansible owns this line in production.
include_if_exists = '/var/lib/postgresql/17/main/myrecovery.conf'
EOF
chown -R postgres:postgres "$PGCONF_DIR/conf.d"

HBA="$PGCONF_DIR/pg_hba.conf"
if ! grep -q "pg-agent-acceptance" "$HBA"; then
    cat >> "$HBA" <<'EOF'
# pg-agent-acceptance: replication + rewind across the compose network
host    replication     repl            0.0.0.0/0               trust
host    postgres        repl            0.0.0.0/0               trust
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
    # Subsequent boots: bring PostgreSQL up in whatever role its data
    # dir holds (primary or standby.signal).
    systemctl start postgresql@17-main.service || true
fi

echo "provision: done"
