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
# Mask pgpool BEFORE installing — order matters. (See "first-deploy-only"
# note below — wrapping this unconditionally re-masks the unit underneath
# the operator after Phase 3.1 and takes the cluster offline.)
systemctl mask pgpool2.service

# `--no-install-recommends` + explicit list keeps Ansible from pulling
# pgpool2 transitively (pg-agent-rs's Recommends: includes pgpool2) and
# running its auto-start postinst before the mask is in place.
apt install --no-install-recommends postgresql-17 pgpool2 pg-agent-rs haproxy

# pg_agentd runs as User=postgres (matches libpq's default search paths
# for ~/.pgpass, ~/.pcppass, ~/.pgpoolkey, and ~/.postgresql/). No
# dedicated pgagent OS user.

open firewall ports:
  5432   (PG, peer-mesh only)
  9999   (pgpool client port — behind HAProxy)
  9701   (pg-agent peer mTLS)
  9702   (pg-agent /healthz, plain HTTP, HAProxy only)
  9898   (PCP, loopback only)
```

> **The `systemctl mask pgpool2.service` step is first-deploy-only.**
> Running it unconditionally on every Ansible play will re-mask the
> unit underneath the operator after they've completed Phase 3.1 (the
> unmask + start), taking the cluster offline on the next redeploy.
> Gate with a state check:
>
> ```
> when: ansible_facts.services['pgpool2.service'].status in ['', 'masked']
> # only mask if the unit is uninstalled (post-install hasn't run yet)
> # or already masked. Skip on enabled / disabled / static.
> ```
>
> The cleanest pattern is to read `systemctl is-enabled pgpool2.service`
> in a `check_mode` task and gate the mask on its stdout — no
> separate `cluster_init_done` flag to maintain across plays.

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

> **Ansible must `mkdir /etc/pg_agent/tls`.** The `.deb` lays down
> `/etc/pg_agent/` itself but **not** the `tls/` subdirectory.
> Create it as `0700 postgres:postgres` before any cert-renewer
> writes there; otherwise the renewer fails and the daemon's
> `validate-env` ExecStartPre rejects the missing material.

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
  local   all   all                               scram-sha-256
  hostssl replication  repl  <each-peer-cidr>     cert  clientcert=verify-full
  hostssl all          all   <app-cidr>           scram-sha-256
```

PG 14+ defaults to `password_encryption = scram-sha-256`, so the
backend stores SCRAM verifiers. Using `md5` in `pg_hba.conf` against
SCRAM-stored passwords gives the user "no pg_hba.conf entry" /
authentication-failed errors with no obvious thread to pull on.

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
  enable_pool_hba = on                   # required (see pool_hba.conf below)
  pool_hba_file   = '/etc/pgpool2/pool_hba.conf'   # pin explicitly
  pool_passwd     = 'pool_passwd'        # relative → /etc/pgpool2/pool_passwd
  ...

/etc/pgpool2/pool_passwd
  app_user:AES<base64>             # Ansible generates via pg_enc -k ...
  pgpool:AES<base64>               # for PCP + sr_check

/etc/pgpool2/pool_hba.conf         # NOT shipped by the .deb's stub
  local   all   all                            trust
  hostssl all   all   <peer-cidr>              scram-sha-256
  hostssl all   all   <app-cidr>               scram-sha-256

/etc/pgpool2/pcp.conf
  pgpool:md5<hash>                  # pcp admin user

~postgres/.pgpoolkey                # AES decryption key for pool_passwd
  <random 32-byte secret>           # mode 0600 postgres:postgres

~postgres/.pcppass                  # pg_agentd reads this via libpq default
  *:9898:pgpool:<plaintext>         # mode 0600 postgres:postgres

/etc/pgpool2/pgpool_node_id         # per-host: the integer node id
  1                                 # mode 0644; matches [[pool]].id for THIS host
```

#### `pool_passwd` must be AES (not md5)

PostgreSQL 14+ defaults to `password_encryption = scram-sha-256`, so
the backend stores SCRAM verifiers, not md5 hashes. SCRAM auth on the
client←pgpool→backend path requires pgpool to know the **plaintext**
password — md5 entries are one-way and pgpool can't recover the
plaintext to do SCRAM downstream. Symptom of getting this wrong is
`WARNING: could not get the password for user:pgpool` on every
`sr_check` tick, followed by every backend being marked `down`.

Generate entries with `pg_enc -k <keyfile> -u <user> <password>`. A
valid line looks like `pgpool:AES<base64>...`, **not**
`pgpool:md5<hex>...`.

> **Render `pgpool.conf` BEFORE running `pg_enc`.** `pg_enc -f
> pgpool.conf` reads the `pool_passwd = ...` directive out of the
> config to learn where to write. If `pgpool.conf` doesn't exist yet
> or points at the wrong path, `pg_enc` silently no-ops (exits 0 with
> nothing written) — and an Ansible `creates:`-style gate on the
> output file's existence then latches the failure across subsequent
> deploys. After `pg_enc`, assert `pool_passwd` has the expected
> number of lines before declaring the task `changed_when:` clean.

#### `~postgres/.pgpoolkey` — NOT under /etc/pgpool2/

pgpool reads the AES decryption key from `$HOME/.pgpoolkey` of the
user it runs as. With the Debian unit's `User=postgres` that's
`/var/lib/postgresql/.pgpoolkey` — **not** any path under
`/etc/pgpool2/`. The "obvious" admin path doesn't work; symptom is
`unable to decrypt password from pool_passwd` / `verify the valid
pool_key exists` on every auth attempt.

Mode `0600 postgres:postgres`; the same file goes on every node
(Ansible's secrets vault is the source of truth). The `POOL_KEY` /
`POOL_KEY_DIR` env vars are the alternative if the admin really wants
the key under `/etc/`, but `~postgres/.pgpoolkey` is what the upstream
package expects.

#### `pool_hba.conf` — required when `enable_pool_hba = on`

The Debian `pgpool2` `.deb`'s stub `pool_hba.conf` only covers
loopback. With `enable_pool_hba = on` (a sane and recommended
default), LAN clients hit `FATAL: client authentication failed,
DETAIL: no pool_hba.conf entry for host "10.0.0.x"...`. Pin
`pool_hba_file` in `pgpool.conf` so the deployment doesn't ride on
the deb's compile-time default, and render a `pool_hba.conf` that
matches the network the cluster actually serves.

#### the rest

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
populates it with the app user's AES-encrypted password. The agent
doesn't touch this file (it's a pgpool concern).

### 1.5 pg-agent config

The `.deb` ships an annotated sample at
**`/usr/share/pg_agent/config.toml.sample`**. Render Ansible's
template against that file (every field the daemon reads is listed
there, with defaults called out), then write the rendered result
to `/etc/pg_agent/config.toml`.

The minimum every deployment needs:

```
# /etc/pg_agent/config.toml

[tls]
ca_cert = "/etc/pg_agent/tls/ca.crt"
cert    = "/etc/pg_agent/tls/node.crt"
key     = "/etc/pg_agent/tls/node.key"

[[pool]]
id       = 0
hostname = "pg1.example.com"

[[pool]]
id       = 1
hostname = "pg2.example.com"

[[pool]]
id       = 2
hostname = "pg3.example.com"
```

Everything else has a working default for the Debian PG 17 layout
(`pg_install_prefix = /usr/lib/postgresql/17`, `data_dir =
/var/lib/postgresql/17/main`, `service = postgresql@17-main.service`,
`agent_port = 9701`, `healthz.port = 9702`, …). See the sample
file for the full list, and override only what your host differs on.

The same `config.toml` goes on every node — the agent resolves
`local_node_id` by reading `/etc/pgpool2/pgpool_node_id` (the same
file pgpool reads, written once per host by Ansible in Phase 1.4),
falling back to hostname match against `[[pool]]` if the pgpool file
isn't present. So **no `node_id` / `node_id_file` field** in this
config in normal deployments.

### 1.6 Initialise the chosen primary's PG instance

```
on the chosen primary only:
  pg_dropcluster 17 main --stop   # if it exists from a prior attempt
  pg_createcluster 17 main        # fresh initdb
  systemctl start postgresql@17-main
```

The Debian `postgresql-17` package may have done `pg_createcluster` at
install time. If so, just ensure it's running.

> **The Ansible idiom for this step is "stat `PG_VERSION` → run
> `pg_createcluster` if missing."** That stays idempotent across
> reruns without an operator step:
>
> ```yaml
> - name: postgres cluster exists
>   ansible.builtin.stat:
>     path: /var/lib/postgresql/17/main/PG_VERSION
>   register: pg_version_file
>
> - name: pg_createcluster 17 main
>   ansible.builtin.command: pg_createcluster 17 main
>   when: not pg_version_file.stat.exists
> ```

PG on the **standbys** is NOT started yet. Their `$PGDATA` is empty
(or leftover from a previous attempt — ClusterInit will deal with that
by stopping and re-basebackup'ing).

### 1.7 Validate env, then start pg_agentd everywhere

```
on every node:
  # The polkit rule grants the postgres user the systemctl verbs the
  # agent dispatches over D-Bus (start/stop/reload postgresql + pgpool2).
  # The .deb does NOT ship this file — it's pure Ansible. See SPEC §10.5
  # for the canonical content; the matching grant on pg_agentd's targets
  # is what lets the agent's RemoteStart / Stop / Reload RPCs succeed
  # without a setuid shim. validate-env (below) refuses to pass if the
  # file is missing.
  copy /etc/polkit-1/rules.d/50-pg-agent.rules   # mode 0644 root:root

  # Optional belt-and-braces — the unit also runs this as
  # ExecStartPre, so a broken env will fail-fast either way.
  pg_agentd validate-env --json   # → Ansible parses, fails the play on ERR

  # pg-agent's .deb deliberately does NOT auto-enable at install time.
  # Ansible writes config.toml first (steps 1.5 above), then turns the
  # service on.
  systemctl enable --now pg_agentd.service
```

`pg_agentd validate-env` is the `nginx -t` equivalent: load + validate
the same config the daemon would load, then walk SPEC §14's localhost
checklist (TLS material readability, `pgpool_node_id` consistency,
`.pcppass` perms, PostgreSQL running, `pg_hba.conf` has the repl
entries we expect, `pgpool_recovery` extension installed, recovery
tool binaries present). Exit 0 iff every row is `OK` or `WARN` — Ansible
parses `--json` and fails the play on `has_errors`.

Each node's validate-env passes on its own merits: there's no cross-node
dependency at this stage. Cluster-wide mesh validation happens after
Phase 2 via `pg_agentctl cluster status`.

The systemd unit also runs `pg_agentd validate-env` as `ExecStartPre=`,
so an environment that drifts after a config edit (or a `.deb` upgrade
that lands new path defaults) refuses to start — journalctl gets the
clear "validate-env: N error(s) — FAIL" line instead of a half-broken
daemon. The Ansible task above is the early-warning gate; the
`ExecStartPre=` is the safety net.

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
and "playbook 2: activate" if Ansible drives both phases. The
cleanest gate is the same `systemctl is-enabled pgpool2.service`
check that gates the mask in Phase 1.1 — running this block when
the unit reports `masked` is the unmask path, and running it again
later (already `enabled`) is a no-op. No `cluster_init_done`
inventory variable to maintain across plays.

### 3.2 Verify

```
pg_agentctl cluster status
# or, equivalent JSON for Ansible:
pg_agentctl --json cluster status

# /healthz is the external-LB-facing probe — same data, plain HTTP:
for node in pg1 pg2 pg3; do
  curl -s http://$node:9702/healthz | jq .
done
```

`cluster status` should show one primary + (N-1) standbys, every
row `reachable`, standbys with `streaming` and finite lag. `/healthz`
on every node should report `is_postgres_running=true`,
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
HAProxy → pgpool:9999 → PG). Worked example so operators don't have
to derive the two load-bearing constraints from first principles:

```
# /etc/haproxy/haproxy.cfg
frontend pg_in
  bind *:5432
  mode tcp
  default_backend pg_pool

backend pg_pool
  mode tcp
  balance roundrobin
  # /healthz on 9702 is the agent's probe — plain HTTP, NOT for routing
  # (pgpool stays the routing layer; this just keeps backends out of
  # rotation when their agent is unreachable). For real wire-protocol
  # routing health, use `option pgsql-check user pgpool` instead.
  option httpchk GET /healthz
  http-check expect status 200

  server db0 db0.example.com:9999 check port 9702
  server db1 db1.example.com:9999 check port 9702
  server db2 db2.example.com:9999 check port 9702
```

Two load-bearing constraints (SPEC §13.1):

1. **No PROXY protocol** on the backend (`send-proxy`, `send-proxy-v2`).
   Pgpool doesn't parse PROXY headers; bytes get treated as garbage
   protocol and connections drop.

2. **No backend TLS** on the `server` line (no `ssl verify required`,
   no `ca-file`, no `sni`). Pgpool terminates TLS for the client
   itself (postgres-protocol SSL upgrade); stacking haproxy↔pgpool
   TLS on top means the client's `ClientHello` arrives encrypted
   inside the haproxy tunnel and pgpool drops it as garbage. Symptom
   is `server closed the connection unexpectedly` with **nothing**
   useful in either log and a green health check throughout — no
   thread to pull on. HAProxy must stay a pure L4 forwarder.

---

## Credentials inventory

| Identity | Where used | Who creates | How it authenticates | Where its credential lives |
|---|---|---|---|---|
| `postgres` (PG superuser) | initdb default | `pg_createcluster` (Debian package) | `local … peer` in `pg_hba.conf` | n/a (peer auth from `postgres` OS user) |
| `repl` (PG replication role) | replication / basebackup / rewind | **ClusterInit** (`db.create_replication_role`) | mTLS client cert (`hostssl replication repl … cert`) | `~postgres/.postgresql/` (libpq default) |
| Pgpool admin (`pgpool`) | PCP commands (`pcp_attach_node`) | Ansible | md5 in `pcp.conf` (PCP's own format) | `~postgres/.pcppass` (libpq default) |
| Pgpool `pgpool` user — backend auth | `sr_check`, `health_check` against backends | Ansible | AES in `pool_passwd` (decrypted with `~postgres/.pgpoolkey`) → SCRAM to PG | `/etc/pgpool2/pool_passwd` (Ansible-managed via `pg_enc`) |
| App user(s) | application traffic | **Operator** (psql) | scram-sha-256 in `pg_hba.conf` + AES in `pool_passwd` | `/etc/pgpool2/pool_passwd` (Ansible-managed via `pg_enc`) |
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

## Ansible patterns worth knowing

Operational gotchas collected from real deploys — none are bugs in
pg-agent itself; all are about how Ansible interacts with the
surrounding services.

### Cleanup deletions go in `post_tasks`, not `pre_tasks`

If the Ansible play uses a cert-renewer (vault-cert-agent, certbot,
etc.) that watches its output files and reloads haproxy / pgpool on
change, mid-play deletions race the rerender. Concretely: a
`pre_tasks` step that removes an orphan CA file referenced by the
on-disk haproxy.cfg will trigger the renewer-driven reload against
the still-old config, and haproxy exits with
`Couldn't open the ca-file '…' (No such file or directory)` —
with no other useful diagnostic in either log.

Move cleanup deletions to `post_tasks` so they run after the role
has rerendered the config that referenced them. Safer pattern: only
delete files the role can re-derive from inventory on the next
play.

### Don't gate `pg_enc` on the output file's mere existence

`pg_enc -f /etc/pgpool2/pgpool.conf -u <user> <password>` exits 0
even when nothing was written (e.g., it parsed `pool_passwd =`
out of the config and the file resolves somewhere the playbook
isn't expecting, or the entry already exists). An Ansible
`creates: /etc/pgpool2/pool_passwd` gate then latches the failure:
every subsequent deploy sees the empty file, skips regen, and
pgpool silently fails every auth.

Assert the row count instead. After running `pg_enc`, fail the
task if `wc -l < /etc/pgpool2/pool_passwd` doesn't match the
inventory's expected user list.

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

### `pg_basebackup` against a misconfigured `pg_hba.conf` — validate-env catches it

SPEC §14 already lists `pg_hba.conf` checks under "TLS / pg_hba":

> verify `pg_hba.conf` has `hostssl replication <repl_user> … cert
> clientcert=verify-full` (or equivalent).

`pg_agentd validate-env` (Phase 1.7) runs this check, plus the systemd
unit's `ExecStartPre=` re-runs it on every start — an `ERR` from
either path stops the deploy before basebackup gets a chance to fail
less informatively.

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
