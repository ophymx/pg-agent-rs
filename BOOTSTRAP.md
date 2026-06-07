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

`pg_agentd` does NOT auto-start at install — our `.deb` is built
with `dh_installsystemd --no-enable`. The agent needs `config.toml`
in place + a valid mTLS bundle before it can do anything useful, and
those are written in Phases 1.2–1.6. Phase 1.7 turns the service on
once Ansible has staged everything.

`pgpool2.service` is the *only* unit that needs explicit masking
(its upstream postinst would auto-enable + auto-start).

### 1.2 mTLS material

Two cert populations, in two different homes. Ansible owns both —
pg-agent only reads them.

**Cluster-internal mesh (pg_agent peer RPCs):**

```
/etc/pg_agent/tls/ca.crt              # the CA cert
/etc/pg_agent/tls/node.crt            # leaf with SAN = this node's hostname
/etc/pg_agent/tls/node.key            # mode 0600 postgres:postgres
```

pg-agent reads these paths explicitly (declared in its config.toml).

**PostgreSQL replication TLS — libpq defaults, NOT under /etc/pg_agent:**

```
~postgres/.postgresql/root.crt        # CA for verifying primary's cert
~postgres/.postgresql/postgresql.crt  # leaf, presented as replication client
~postgres/.postgresql/postgresql.key  # mode 0600 postgres:postgres
```

This is the path libpq looks at by default. pg_basebackup, pg_rewind,
and PostgreSQL's own walreceiver pick these up automatically — pg-agent
does not need to know they exist and does not name them in conninfo.
The agent only chooses `sslmode` (default `verify-full`); the paths
are libpq's problem.

The two CAs *can* be the same — operator's choice. Symmetric with how
`.pcppass` (Phase 1.4) and `.pgpass` live in `~postgres/` and are
picked up via libpq's default search; one home, one owner (Ansible),
multiple readers (PostgreSQL, pgpool, pg-agent).

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
  ssl_ca_file        = '/var/lib/postgresql/.postgresql/root.crt'
  ssl_cert_file      = '/var/lib/postgresql/.postgresql/postgresql.crt'
  ssl_key_file       = '/var/lib/postgresql/.postgresql/postgresql.key'
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

~postgres/.pcppass                  # pg_agentd reads this via libpq default
  *:9898:pgpool:<plaintext>         # mode 0600 postgres:postgres

/etc/pgpool2/pgpool_node_id         # per-host: the integer node id
  1                                 # mode 0644; matches [[pool]].id for THIS host
```

`~postgres/.pcppass` is libpq/pgpool's default search location when
the `postgres` user runs `pcp_*` commands — same convention as
`~/.pgpass` for libpq. pg-agent doesn't carry a config field for the
path; it relies on the default. Symmetric with the replication TLS
material in Phase 1.2.

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
  pg_install_prefix = "/usr/lib/postgresql/17"
  data_dir  = "/var/lib/postgresql/17/main"
  archive_dir = "/var/lib/postgresql/archive"
  service   = "postgresql@17-main.service"
  repl_user = "repl"

  # [postgres.replication] section is optional.
  # sslmode defaults to "verify-full". Cert paths come from libpq's
  # own search (~postgres/.postgresql/) — pg-agent does NOT name them.
  # [postgres.replication]
  # sslmode = "verify-full"

  [pcp]
  user     = "pgpool"
  port     = 9898
  pgpool_service = "pgpool2.service"
  # No .pcppass field — pg-agent calls pcp_* binaries as the postgres
  # user; libpq picks up ~postgres/.pcppass automatically.

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
  # pg-agent's .deb deliberately does NOT auto-enable at install time.
  # Ansible writes config.toml first (steps 1.5 above), then turns the
  # service on.
  systemctl enable --now pg_agentd.service
```

The pg-agent Debian package is built with `dh_installsystemd
--no-enable`, which means the postinst installs the unit file but
doesn't enable it (so it stays off across reboots) and doesn't start
it. Ansible is responsible for the activation in this step. On
upgrade, dh_installsystemd's restart-if-running default still applies
— a running agent picks up new binaries via a restart.

Each agent at boot:
- Loads `/etc/pg_agent/config.toml`, resolves local node id from
  `/etc/pgpool2/pgpool_node_id` (or hostname fallback)
- Validates config (mTLS material readable, paths absolute, sslmode in
  libpq's set)
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

### 3.3 Create app database + users via Ansible

Declare app users + databases in the Ansible inventory (or
playbook-level vars / vault), then re-run the playbook with the
new entries:

```yaml
# inventory: group_vars/databases.yml
postgres_databases:
  - name: app
postgres_users:
  - name: app_user
    db: app
    password: "{{ vault_app_user_password }}"
```

Ansible's `postgres` role does three things in one pass:
1. `CREATE DATABASE` + `CREATE ROLE … LOGIN PASSWORD` on the cluster
2. Writes the md5 of the password into
   `/etc/pgpool2/pool_passwd` on every pgpool host
3. Reloads `pgpool2.service` (or restarts if `pool_passwd` semantics
   require it on that pgpool version)

Don't `CREATE ROLE` from `psql` by hand — `pool_passwd` ends up
stale on the pgpool side and connections refuse for a reason that's
non-obvious from PG's perspective. See [resolved decisions](#resolved-design-decisions) above.

### 3.4 Point applications at HAProxy

The app's connection string targets HAProxy's frontend (port 5432 →
HAProxy → pgpool:9999 → PG). Out of scope.

---

## Credentials inventory

| Identity | Where used | Who creates | How it authenticates | Where its credential lives |
|---|---|---|---|---|
| `postgres` (PG superuser) | initdb default | `pg_createcluster` (Debian package) | `local … peer` in `pg_hba.conf` | n/a (peer auth from `postgres` OS user) |
| `repl` (PG replication role) | replication / basebackup / rewind | **ClusterInit** (`db.create_replication_role`) | mTLS client cert (`hostssl replication repl … cert`) | `~postgres/.postgresql/` (libpq default) |
| Pgpool admin (`pgpool`) | PCP commands (`pcp_attach_node`) | Ansible | md5 in `pcp.conf` | `~postgres/.pcppass` (libpq default) |
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

## Resolved design decisions

### `peer.stop()` on a never-started standby — works as a no-op

systemd's `StopUnit` D-Bus call accepts an already-inactive unit: the
job is submitted, processed as a no-op, and `JobRemoved` fires with
`result = "done"`. Our `DbusSystemd::stop_postgres` is wired exactly
for that — `job_result_ok("done") → true → Ok(())`. No code change.

Precondition: the unit must EXIST. The Debian `postgresql-17`
package's `pg_createcluster 17 main` step creates the templated unit
instance, so on a standard install the unit is present even before
its first start. If the cluster was never created (e.g., manual
install without `pg_createcluster`), `StopUnit` returns "Unit not
loaded" — surfaced as a clear startup error.

### `pg_basebackup` against a misconfigured `pg_hba.conf` — preflight catches it

SPEC §14 already lists `pg_hba.conf` checks under "TLS / pg_hba":

> verify `pg_hba.conf` has `hostssl replication <repl_user> … cert
> clientcert=verify-full` (or equivalent).

Running `pg_agentctl preflight` on every node before `cluster init`
is Phase 1.8 of this doc; an `ERR` from that check stops the
operator before basebackup gets a chance to fail less informatively.

### App user creation — Ansible owns it, not psql

Don't recommend the operator do `CREATE ROLE app_user PASSWORD ...`
in psql by hand. Two reasons:

1. The password also has to land in `/etc/pgpool2/pool_passwd` as
   md5, and on every pgpool host. The psql path leaves `pool_passwd`
   stale.
2. Operators forget what they typed. Reproducing the cluster from
   the Ansible inventory should always recover the same state.

**Pattern:** declare app users in the Ansible inventory (or a
secrets vault). Ansible's postgres role does both `CREATE ROLE` AND
writes the md5 hash to `pool_passwd` AND reloads pgpool. The
operator's only manual step is the inventory edit + Ansible run.

Phase 3.3 above is shorthand for "re-run the Ansible playbook with
the new app user in the inventory." Updated to say so.

### `pcp_attach_node` in ClusterInit — deliberately omitted

ClusterInit runs BEFORE pgpool starts (Phase 2 of this doc). There's
no PCP endpoint to attach to. Matches SPEC §5.7 + §17 invariant #6
(`pcp_attach_node` is `FollowPrimary`-only).

**Adding a standby to a running cluster** is a separate use case
covered by `pg_agentctl cluster init --only-node-id <id>` followed
by a manual `pcp_attach_node` (or a future `pg_agentctl cluster
attach <id>` wrapper — see [ROADMAP.md](ROADMAP.md) v1.x "Cluster
control plane").

### Slot cleanup on ClusterInit failure — drop, for consistency

Earlier ambiguity: ClusterInit was operator-driven, so leaving
slots around for forensic value might be OK. Resolved as: drop the
slot on mid-flow failure, same pattern as `FollowPrimary` and
`RecoveryFirstStage`. Three reasons:

1. **WAL pinning.** A standby that never came up still ties up the
   primary's WAL via the slot. The operator might not notice for
   hours; meanwhile the primary's pg_wal grows unbounded.
2. **Consistency.** All three slot-creating flows now follow the
   same shape: drop on mid-flow failure, queue a `DropSlotCleanup`
   maintenance intent if the drop itself fails. No special case.
3. **Re-run idempotency.** `db.create_slot` is 42710-idempotent, so
   re-running cluster init after a failure creates a fresh slot
   regardless of whether the old one survived. Dropping costs
   nothing; pinning WAL costs the operator real space.

SPEC §5.7 to be updated to spell out the cleanup pattern when
ClusterInit lands.
