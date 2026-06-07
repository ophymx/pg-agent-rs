# Cluster Bootstrap — Operator Flow

Companion to [SPEC.md](SPEC.md). The SPEC describes the agent in
isolation; this doc walks through how a fresh cluster actually comes up
end-to-end, who is responsible for what, and where `pg_agentctl cluster
init` fits.

> This is the **minimal-install** path. Three layers of work,
> performed in order:
>
> 1. **Ansible** provisions the OS, packages, configs, certs, users.
>    Per-node, once.
> 2. **`pg_agentctl cluster init`** performs replication setup
>    (CREATE ROLE, create slots, basebackup each standby, start them).
>    Per-cluster, once.
> 3. **Operator** starts pgpool, creates app databases, points
>    applications at HAProxy. Per-cluster, once.

ClusterInit is the **smallest** of the three layers. It does only
what requires PG to be live and the cluster's mTLS mesh to be
operational — everything else happens before or after.

---

## Phase 1 — Ansible (per-node provisioning)

Pseudo-code for what each playbook step accomplishes. The
configuration files mentioned here are written by Ansible templates
*before* any service starts.

### 1.1 OS prerequisites

> **Important: mask `pgpool2.service` BEFORE installing pgpool2.**
> The Debian package's postinst (`dh_installsystemd` boilerplate)
> both *enables* and *starts* `pgpool2.service` at install time. If
> we let that happen, pgpool comes up before the standbys are
> streaming, fails to attach to its backends, and the unit ends up in
> `failed` state by the time `cluster init` runs. Masking the unit
> first makes the postinst's `deb-systemd-invoke start` a no-op AND
> blocks the boot-time start until we explicitly unmask in Phase 3.1.
>
> Confirmed against `pgpool2 4.6.1-2 trixie/main` — the postinst
> contains both `deb-systemd-helper enable 'pgpool2.service'` and
> `deb-systemd-invoke start 'pgpool2.service'`.

```
# Mask pgpool BEFORE installing — order matters.
systemctl mask pgpool2.service

apt install postgresql-17 pgpool2 pg-agent-rs haproxy
useradd -r pgagent                              # optional shared group
open firewall ports:
  5432   (PG, peer-mesh only)
  9999   (pgpool client port — behind HAProxy)
  9701   (pg-agent peer mTLS)
  9702   (pg-agent /healthz, plain HTTP, HAProxy only)
  9898   (PCP, loopback only)
```

PostgreSQL DOES auto-start at install (the `postgresql-17` package's
postinst runs `pg_createcluster 17 main`, which initdb's + starts an
empty cluster). That's fine — on the chosen primary the empty cluster
IS the cluster; on the standbys ClusterInit's defensive `peer.stop()`
step shuts it down before basebackup wipes pgdata.

`pg_agentd` will also auto-start at install (our own `.deb` follows
the same `dh_installsystemd` pattern). That's also fine — we want the
agent up everywhere so the mesh is reachable when the operator runs
`cluster init`.

The *only* unit that needs masking is `pgpool2.service`.

### 1.2 mTLS material

Generated from a central CA Ansible owns (Vault, step-ca, etc.).
Distributed to each node:

```
/etc/pg_agent/tls/ca.crt              # the CA cert
/etc/pg_agent/tls/node.crt            # leaf with SAN = this node's hostname
/etc/pg_agent/tls/node.key            # mode 0600 postgres:postgres
/etc/pg_agent/tls/replication/ca.crt  # if replication uses TLS too
/etc/pg_agent/tls/replication/node.crt
/etc/pg_agent/tls/replication/node.key
```

Two separate sets because the cluster-internal mesh (pg_agent peer
RPCs) and PostgreSQL replication can have different trust roots. They
*can* be the same CA — operator's choice.

### 1.3 PostgreSQL config

Identical content on every node (Debian layout: configs in `/etc/`,
PGDATA in `/var/lib/postgresql/17/main/`):

```
/etc/postgresql/17/main/postgresql.conf
  data_directory     = '/var/lib/postgresql/17/main'
  hba_file           = '/etc/postgresql/17/main/pg_hba.conf'
  listen_addresses   = '*'
  wal_level          = replica
  max_wal_senders    = N+2           # one per peer + slack
  max_replication_slots = N+2
  archive_mode       = on
  archive_command    = 'test ! -f /var/lib/postgresql/archive/%f && cp %p /var/lib/postgresql/archive/%f'
  restore_command    = 'pg_agentc restore-wal %f %p'
  ssl                = on
  ssl_ca_file        = '/etc/pg_agent/tls/replication/ca.crt'
  ssl_cert_file      = '/etc/pg_agent/tls/replication/node.crt'
  ssl_key_file       = '/etc/pg_agent/tls/replication/node.key'
  include_if_exists  = 'myrecovery.conf'
```

```
/etc/postgresql/17/main/pg_hba.conf
  local   all   postgres                          peer
  local   all   all                               md5
  hostssl replication  repl  <each-peer-cidr>     cert  clientcert=verify-full
  hostssl all          all   <app-cidr>           md5
```

The key line for cluster operation is `hostssl replication repl … cert`
— ClusterInit assumes `repl` can connect via mTLS to every other
node. If Ansible doesn't set this up correctly, basebackup fails.

### 1.4 pgpool config

Ansible renders these from `pg_agentctl gen-pgpool` (a CLI subcommand
the agent provides — generates the canonical pgpool.conf from the
agent's `[[pool]]` so both stay in sync):

```
/etc/pgpool2/pgpool.conf
  backend_hostname0 = ...                # one per [[pool]] entry
  failover_command  = 'pg_agentc failover %d %h %p %D %m %H %M %P %r %R %N %S'
  follow_primary_command = 'pg_agentc follow_primary ...'
  recovery_1st_stage_command = 'recovery_1st_stage'  # exec'd from $PGDATA
  wd_escalation_command  = 'pg_agentc escalation'
  ...

/etc/pgpool2/pool_passwd
  app_user:md5<hash>                # Ansible generates via pg_md5
  pgpool:md5<hash>                  # for PCP

/etc/pgpool2/pcp.conf
  pgpool:md5<hash>                  # pcp admin user

/etc/pgpool2/.pcppass               # pg_agentd reads this
  *:9898:pgpool:<plaintext>         # mode 0600 postgres:postgres

/etc/pgpool2/pgpool_node_id         # per-host: the integer node id
  1                                 # mode 0644; matches [[pool]].id for THIS host
```

`pgpool_node_id` is the single source of truth that both pgpool and
pg_agent read. Writing it once per node (Ansible's per-host inventory
already knows the id) means the two tools can never drift on "which
backend am I?".

`pool_passwd` is where pgpool's client auth state lives. Ansible
populates it with the app user's hashed password. The agent doesn't
touch this file (it's a pgpool concern).

### 1.5 pg-agent config

```
/etc/pg_agent/config.toml
  agent_port  = 9701
  unix_socket = "/run/pg_agentd/pg_agentd.sock"
  state_dir   = "/var/lib/postgresql/pg_agent"

  # No node_id / node_id_file here — every node gets the SAME config.toml.
  # The agent resolves local_node_id from /etc/pgpool2/pgpool_node_id
  # (which Ansible writes per-host in Phase 1.4 — see below), so pg_agent
  # and pgpool share one source of truth. Falls back to hostname match
  # against [[pool]] if the pgpool file isn't present.

  [tls]
  ca_cert = "/etc/pg_agent/tls/ca.crt"
  cert    = "/etc/pg_agent/tls/node.crt"
  key     = "/etc/pg_agent/tls/node.key"

  [[pool]]
  id = 0
  hostname = "pg1.example.com"

  [[pool]]
  id = 1
  hostname = "pg2.example.com"

  [[pool]]
  id = 2
  hostname = "pg3.example.com"

  [postgres]
  port      = 5432
  pghome    = "/usr/lib/postgresql/17"
  data_dir  = "/var/lib/postgresql/17/main"
  archive_dir = "/var/lib/postgresql/archive"
  service   = "postgresql@17-main.service"
  repl_user = "repl"

  [postgres.replication_tls]
  ca_cert = "/etc/pg_agent/tls/replication/ca.crt"
  cert    = "/etc/pg_agent/tls/replication/node.crt"
  key     = "/etc/pg_agent/tls/replication/node.key"
  sslmode = "verify-full"

  [pcp]
  user     = "pgpool"
  port     = 9898
  pcp_pass_file  = "/etc/pgpool2/.pcppass"
  pgpool_service = "pgpool2.service"

  [healthz]
  enabled = true
  listen  = "0.0.0.0"
  port    = 9702
```

The same config.toml goes on every node — the agent resolves
`local_node_id` from hostname at startup.

### 1.6 Initialise the chosen primary's PG instance

```
on the chosen primary only:
  pg_dropcluster 17 main --stop   # if it exists from a prior attempt
  pg_createcluster 17 main        # fresh initdb
  systemctl start postgresql@17-main
```

The Debian `postgresql-17` package may have done `pg_createcluster` at
install time. If so, just ensure it's running.

PG on the **standbys** is NOT started yet. Their `$PGDATA` is empty
(or leftover from a previous attempt — ClusterInit will deal with that
by stopping and re-basebackup'ing).

### 1.7 Start pg_agentd everywhere

```
on every node:
  systemctl enable --now pg_agentd.service
```

Each agent at boot:
- Loads `/etc/pg_agent/config.toml`, resolves local node id by hostname
- Validates config (mTLS material readable, paths absolute, etc.)
- Connects to systemd D-Bus, PG Unix socket, builds PCP CLI
- Repairs hook symlinks in `$PGDATA` (creates them on the chosen primary
  whose pgdata exists; standbys without pgdata yet will get repaired
  later by `peer.basebackup`'s tail step)
- Creates `state_dir/{replay,maintenance}/`
- Binds Unix socket + peer TCP + healthz listeners
- Calls `sd_notify(READY=1)` → systemd considers the service started

### 1.8 Optional preflight

```
on every node:
  pg_agentctl preflight
```

Per SPEC §14: checks TLS material readability, polkit rule, .pcppass
permissions, pool reachability via the peer mTLS mesh, PostgreSQL
running, pg_hba.conf has the repl entries we expect, etc. Operator
fixes anything that reports `ERR`.

This is the last gate before ClusterInit.

---

## Phase 2 — `pg_agentctl cluster init`

Operator runs this **once**, from any node (or a workstation):

```
pg_agentctl cluster init --primary pg1.example.com
# or:
pg_agentctl cluster init --primary pg1.example.com --only-node-id 2
```

`pg_agentctl` dials the Unix socket of the local pg_agentd, which
forwards to the primary's `LocalServer::ClusterInit`. (If you're
running it from a workstation, you SSH to the primary and dial the
socket there — `pg_agentctl` doesn't speak gRPC over the network.)

The primary's `ClusterInit` handler does **only this**:

```
1. Refuse if local node is in recovery
   (defensive — ClusterInit only runs on the primary)

2. db.create_replication_role("repl")
   CREATE ROLE repl WITH LOGIN REPLICATION
   42710 (already exists) → ok

3. For each non-local pool entry (or the one in --only-node-id):
   a. db.create_slot(node.slot_name())          # "node1", "node2", ...
   b. peer.stop()                               # defensive — basebackup
                                                # needs empty pgdata
   c. peer.basebackup(opts)                     # streams primary into
                                                # standby's pgdata
                                                # (post-step: hook
                                                # symlink repair)
   d. peer.configure_standby(opts)              # writes myrecovery.conf +
                                                # standby.signal
   e. peer.start()                              # brings standby up as
                                                # streaming replica

4. Return ClusterInitResponse {
     ok: <true iff every standby succeeded>,
     standbys: [ {node_id, hostname, ok, message}, ... ]
   }
```

That's the entire scope. ClusterInit:

- Does NOT start pgpool.
- Does NOT create app users / app databases / app passwords.
- Does NOT populate `pool_passwd` / `pcp.conf` / `.pcppass`.
- Does NOT configure HAProxy.
- Does NOT install or repair TLS material.
- Does NOT install OS packages.
- Does NOT touch `/etc/postgresql/17/main/*.conf` (those are Ansible's).
- Does NOT call `pcp_attach_node` (pgpool isn't running yet).

It only touches the bits that need PG-the-primary to be live: the
replication role, the slots, the standbys' data directories.

---

## Phase 3 — Operator (post-ClusterInit)

After `cluster init` returns `ok: true`:

### 3.1 Unmask + start pgpool

```
on every pgpool host:
  systemctl unmask pgpool2.service
  systemctl enable --now pgpool2.service
```

pgpool attaches to all backends, sees them up + streaming, and begins
accepting connections on port 9999.

If pgpool is co-located with PG (the SPEC-assumed layout), every host
in `[[pool]]` runs pgpool. HAProxy fronts them.

This is the natural cut between "playbook 1: install + bootstrap"
and "playbook 2: activate" if Ansible drives both phases. A
`cluster_init_done: true` variable (set after the operator confirms
step 2 succeeded) gates this unmask block, so the playbook is
idempotent and re-runnable.

### 3.2 Verify

```
pg_agentctl cluster status     # not implemented yet — roadmap v1.x
# meanwhile:
for node in pg1 pg2 pg3; do
  curl -s http://$node:9702/healthz | jq .
done
```

Every node should report `is_postgres_running=true`,
`is_pgpool_running=true`, and on standbys
`is_in_recovery=true, replication_state=streaming`.

### 3.3 Create app database + users (operator's concern)

```
psql -h <haproxy> -p 9999 -U postgres postgres <<EOF
  CREATE DATABASE app;
  CREATE ROLE app_user LOGIN PASSWORD '...';
  GRANT CONNECT ON DATABASE app TO app_user;
EOF
```

The app user's MD5 hash also needs to be in `/etc/pgpool2/pool_passwd`
on every pgpool host — Ansible put it there in Phase 1.4. If the app
user is created after-the-fact, the operator re-runs the Ansible
playbook (or appends to pool_passwd by hand and reloads pgpool).

### 3.4 Point applications at HAProxy

The app's connection string targets HAProxy's frontend (port 5432 →
HAProxy → pgpool:9999 → PG). Out of scope.

---

## Credentials inventory

| Identity | Where used | Who creates | How it authenticates | Where its credential lives |
|---|---|---|---|---|
| `postgres` (PG superuser) | initdb default | `pg_createcluster` (Debian package) | `local … peer` in `pg_hba.conf` | n/a (peer auth from `postgres` OS user) |
| `repl` (PG replication role) | replication / basebackup / rewind | **ClusterInit** (`db.create_replication_role`) | mTLS client cert (`hostssl replication repl … cert`) | `/etc/pg_agent/tls/replication/` |
| Pgpool admin (`pgpool`) | PCP commands (`pcp_attach_node`) | Ansible | md5 in `pcp.conf` | `/etc/pgpool2/.pcppass` (read by agent) |
| App user(s) | application traffic | **Operator** (psql) | md5 in `pg_hba.conf` + `pool_passwd` | `/etc/pgpool2/pool_passwd` (Ansible-managed) |
| pg-agent peer mesh | inter-node RPC | Ansible (mints from CA) | mTLS client cert + SAN allowlist | `/etc/pg_agent/tls/` |

**Three CAs, in principle, all separate:**
- `/etc/pg_agent/tls/ca.crt` — the agent peer mesh
- `/etc/pg_agent/tls/replication/ca.crt` — PG replication
- (HAProxy's CA if it does TLS, but that's the app layer)

Most deployments use the **same** CA for the first two — simpler key
management. Three separate CAs is unusual but supported.

---

## In scope / out of scope for `cluster init`

### In scope

- `CREATE ROLE repl WITH LOGIN REPLICATION` on the primary (idempotent).
- Create the standby slot on the primary, one per standby.
- Stop, basebackup, configure_standby, start — for each standby.
- Collect per-standby results, return aggregate.

### Out of scope (Ansible's job)

- Installing packages, opening firewall ports, creating OS users.
- Writing every config file mentioned in Phase 1.
- Minting + distributing TLS material.
- Populating `pool_passwd`, `pcp.conf`, `.pcppass`.
- Setting up `pg_hba.conf` entries for cert-auth replication.
- `pg_createcluster` on the chosen primary.

### Out of scope (operator's job, post-ClusterInit)

- Starting pgpool.
- Creating app databases and app users.
- Setting / rotating app user passwords.
- Configuring HAProxy backends.
- Pointing applications at the front-end.

---

## Re-run + recovery scenarios

### "ClusterInit failed partway through; some standbys are up, some aren't"

Re-run the same command. Idempotent steps:

- `create_replication_role` already done → 42710 → ok.
- `create_slot` for a node whose slot already exists → 42710 → ok.
- `peer.stop()` on an already-stopped node → no-op.
- `peer.basebackup()` on a half-populated `$PGDATA` → **REFUSES** (pg_basebackup
  declines a non-empty target).
- Workaround: SSH to the bad standby, `rm -rf /var/lib/postgresql/17/main/*`,
  then re-run `cluster init --only-node-id <that-one>`.

A future v1.x improvement: `cluster init --force` that wipes the
standby's pgdata via `peer.stop() + clear` before basebackup. Today
it's manual.

### "Need to add a new standby to an existing cluster"

```
1. Ansible: provision the new node (configs, certs, packages).
2. Update [[pool]] in config.toml on EVERY node to include the new entry.
3. Restart pg_agentd on every node so the new pool config takes effect.
4. Run: pg_agentctl cluster init --primary <primary> --only-node-id <new-id>
5. pg_agentctl gen-pgpool --write && systemctl reload pgpool2
   (regenerate pgpool.conf with the new backend block)
```

`cluster init` and `recovery_1st_stage` cover overlapping ground.
ClusterInit is preferred for "first time bringing this node up";
RecoveryFirstStage is what pgpool fires automatically when a previously-
known node failed and now needs to come back.

### "Need to rotate the repl user password"

The repl user uses mTLS client certs, not passwords. Rotation is a cert
rotation: Ansible mints new certs, copies them in place, sends SIGHUP
to `pg_agentd` (which the cert reloader picks up) and to `postgres`
(which `pg_reload_conf()`s). No `cluster init` involvement.

If you ever switch to password auth (not recommended), the password
lives in `~postgres/.pgpass` and is Ansible's responsibility.

---

## Open questions

Things I'm not 100% sure about — let's resolve before implementing ClusterInit.

1. **`peer.stop()` on a standby that's never been started.** The
   systemd unit may exist but report "inactive (dead)". Calling
   `systemctl stop` on it is a no-op; we should make sure
   `DbusSystemd::stop_postgres` tolerates that. Currently the systemd
   handler waits for a `JobRemoved` signal — would it fire for a
   no-op stop?

2. **`pg_basebackup` against a primary whose `pg_hba.conf` doesn't
   yet permit the connection.** The basebackup will fail with
   `pg_basebackup: error: connection to server failed`. Worth a
   preflight item: "pg_hba on the chosen primary permits the
   configured peer hosts." Already on the preflight list?

3. **App user creation timing.** If `pool_passwd` lacks the app user
   at the time pgpool starts, pgpool refuses connections from that
   user. Should we recommend Ansible pre-populate `pool_passwd` with
   the expected app users (Phase 1.4) so step 3.3 only does the PG-
   side `CREATE ROLE`? Or accept the two-step "create user → re-run
   Ansible" dance?

4. **`pcp_attach_node` in ClusterInit?** Currently we deliberately
   don't call it — pgpool isn't running yet, so there's no PCP
   endpoint to attach to. But if the operator runs ClusterInit
   *after* pgpool is already up (e.g., adding a new standby), we'd
   need to attach. The current Go impl matches SPEC §5.7: never
   attaches. Adding a standby to a running cluster would need
   `pg_agentctl cluster attach <id>` as a separate step. Acceptable?

5. **Failure ordering: drop the slot if basebackup/configure/start
   fails?** The other slot-creating flows (FollowPrimary,
   RecoveryFirstStage) drop the slot on mid-flow failure. ClusterInit
   currently doesn't — per SPEC §5.7 it just collects the failure in
   the response. Inconsistent with the others; should we add the
   same cleanup pattern, or is the operator-driven nature (you'll
   re-run the command manually) reason to keep it simple?
