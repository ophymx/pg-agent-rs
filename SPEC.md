# pg-agent-rs — SPEC

A Rust port of [`pg_agent`](../pg_agent), the daemon that replaces pgpool-II's
shell-script hooks (`failover.sh`, `follow_primary.sh`, `recovery_1st_stage`,
`pgpool_remote_start`, `escalation.sh`) and the SSH-based remote execution
they depend on.

This spec is distilled from the Go implementation. It does not document Go
internals — it describes **what the Rust implementation must do**, what
choices are fixed by the on-the-wire / on-disk contract, and what choices the
Rust port is free to make differently.

---

## 1. System shape

Three binaries, one daemon and two CLIs, deployed on every PostgreSQL backend
node (which also runs pgpool-II and HAProxy):

| Binary        | Role | Loads config? | Talks to peers? |
|---------------|------|---------------|-----------------|
| `pg_agentd`   | Daemon. Owns local PostgreSQL operations + cluster coordination. Serves a Unix-socket RPC for local callers and an mTLS TCP RPC for peer agents. Plus an HTTPS `/healthz` listener. | yes | yes |
| `pg_agentc`   | One-shot hook client. Marshals pgpool's positional argv into a single gRPC call on the local Unix socket, then exits. Also `pg_agentc status`. **No config. No node resolution. No PostgreSQL logic.** | no | no |
| `pg_agentctl` | Operator CLI. `print-hooks`, `check-hooks`, `gen-pgpool`, `preflight`, `maintenance {list,show,retry}`, `cluster init`. May dial peer agents. | yes | yes |

The hook client must stay tiny — if it can reach the socket, it works. Every
new piece of operator functionality goes in `pg_agentctl`, not `pg_agentc`.

The shared positional-argument schemas (one per pgpool/PostgreSQL hook) live
in a single crate so the dispatcher (`pg_agentc`) and the validator/printer
(`pg_agentctl`) cannot drift.

### 1.1 Cluster layout (unchanged from Go)

```
clients → HAProxy (TCP LB, 5432) → Pgpool-II (9999) → PostgreSQL backends
                                          ↑
                            watchdog (leader election, no VIP)
```

| Node    | Pgpool | PCP  | WD   | Heartbeat  | Agent gRPC | Agent /healthz |
|---------|--------|------|------|------------|------------|----------------|
| serverN | 9999   | 9898 | 9000 | 9694/udp   | 9701 mTLS  | 9702 HTTPS     |

VIP management is intentionally absent — HAProxy replaces it. The
`Escalation`/`DeEscalation` watchdog hooks are no-ops in this deployment. The
`RemoveVip` peer RPC is reserved in the proto for forward compatibility but
returns `Unimplemented`.

**Co-location assumption:** pgpool-II runs on every PostgreSQL backend. The
agent reaches its local pgpool via PCP on `localhost` and manages
`pgpool2.service` via systemd. A separate-middleware topology is out of
scope.

### 1.2 No SSH

The reference scripts SSH between nodes to run `pg_ctl promote`,
`pg_basebackup`, `pg_rewind`, `pg_ctl start`, and `ip addr del`. Every one of
those is replaced by a gRPC call (over mTLS) to a peer's `PgAgentPeer`
service, which runs the operation locally on the peer. There is no
`authorized_keys`, no `ssh-keygen`, no `StrictHostKeyChecking=no`.

---

## 2. Recommended Rust crates

The Rust port is greenfield; the proto + on-disk contracts are fixed, the
implementation language is not. Default choices:

| Concern                          | Crate                                              | Notes |
|----------------------------------|----------------------------------------------------|-------|
| async runtime                    | `tokio` (full)                                     | multi-threaded scheduler |
| gRPC server + client             | `tonic` + `tonic-build`, `prost`                   | matches the Go grpc surface; supports server-streaming for `Basebackup`/`Rewind`/`FetchWal` |
| protovalidate (field constraints) | `protovalidate` (Rust port) or hand-rolled        | only one rule in use today — see §3.3. A hand-rolled regex check is acceptable if `protovalidate` lags. |
| TLS                              | `rustls` + `tokio-rustls`                          | for both peer mTLS and /healthz server-only TLS |
| TOML config                      | `toml` + `serde`                                   | matches BurntSushi/toml semantics |
| CLI                              | `clap` (derive)                                    | one binary per crate inside the workspace |
| Logging                          | `tracing` + `tracing-subscriber` (JSON or fmt)     | replace Go's `log/slog` |
| PostgreSQL client (local DB)     | `tokio-postgres` + `deadpool-postgres`             | direct port of pgx — keep the pool single-host (Unix socket) |
| systemd D-Bus                    | `zbus` (async)                                     | replace `coreos/go-systemd/v22/dbus`; talk to `org.freedesktop.systemd1` |
| sd_notify                        | `sd-notify` crate, or the documented `NOTIFY_SOCKET` envelope written directly | for `READY=1` / `STOPPING=1` |
| Atomic snapshot pointers         | `arc-swap` (`ArcSwap<T>`, `ArcSwapOption<T>`)      | for `CertReloader` bundle and `HealthSnapshotter` snapshot |
| HTTP server for /healthz         | `axum` or `hyper` directly                         | request path does no async I/O |
| Concurrent peer map              | `tokio::sync::Mutex<HashMap<…>>` or `dashmap`      | small N (3 peers); a `parking_lot::Mutex` is fine |
| Signal handling                  | `tokio::signal::unix` (`SIGINT`, `SIGTERM`, `SIGHUP`) | drives shutdown + cert reload |
| Subprocess                       | `tokio::process::Command`                          | drives `pg_basebackup`, `pg_rewind`, `pcp_attach_node`, `pcp_node_count` |
| Error model                      | `thiserror` for typed errors, `anyhow` only at binary entrypoints | mirror the named errors used in the Go agent (`ErrInsecureRemotePeer`, `ErrReplicationTLSPartial`, etc.) |

### 2.1 Suggested workspace layout

```
pg-agent-rs/
├── Cargo.toml                  # workspace
├── crates/
│   ├── pg-agent-proto/         # .proto + tonic-build output (re-exports)
│   ├── pg-agent-hookspec/      # positional-arg schemas (no proto dep)
│   ├── pg-agent-core/          # Agent, config, peers, db, systemd,
│   │                           # pgstandby, walstore, maintenance, healthz,
│   │                           # certreload, preflight (no transport)
│   ├── pg-agentd/              # daemon binary
│   ├── pg-agentc/              # hook-client binary
│   └── pg-agentctl/            # operator CLI binary
└── proto/                      # .proto files copied/maintained verbatim
```

`pg-agent-core` should expose traits for every external collaborator (see
§4) so unit tests can swap in in-process fakes and multi-node functional
tests can swap in cross-process HTTP-observable fakes (mirroring the Go
`agent/fakes` vs `agent/funcfakes` split).

---

## 3. Protocol surface

Two gRPC services. Both are versioned by the `.proto` file; both must stay
wire-compatible with the existing Go implementation so a mixed-version
cluster works during a rolling upgrade.

Common messages (`common.proto`):

```proto
message OpResult {
  bool   ok      = 1;
  string message = 2;
}

message OpProgress {
  int64  bytes_done  = 1;
  int64  bytes_total = 2;  // 0 = unknown
  string phase       = 3;  // "streaming" | "rewinding" | "done" | …
  string message     = 4;
}

message NodeStatus {
  bool   is_running              = 1;
  bool   is_in_recovery          = 2;  // true = standby
  bool   is_ready                = 3;  // accepting conns AND every probe succeeded
  int64  replication_lag_bytes   = 4;  // 0 if primary/unknown
  string replication_state       = 5;  // "streaming" | "catchup" | "" (primary)
  bool   is_postgres_running     = 6;
  bool   is_pgpool_running       = 7;
  bool   is_postgres_status_ok   = 8;  // false = systemd unreachable; _running is unknown
  bool   is_pgpool_status_ok     = 9;
}

message GetStatusRequest {}
message NodeConfigRequest {}
message NodeConfigResponse {
  int32  pg_port     = 1;
  string pg_data_dir = 2;
}
```

### 3.1 `PgAgentLocal` — Unix socket, no auth

Filesystem permissions on the socket are the access control. The socket is
`0600 postgres:postgres` under the systemd-managed `RuntimeDirectory=pg_agentd`.

```proto
service PgAgentLocal {
  rpc Failover         (FailoverRequest)      returns (OpResult);
  rpc FollowPrimary    (FollowPrimaryRequest) returns (OpResult);
  rpc Recovery1stStage (RecoveryRequest)      returns (OpResult);
  rpc RemoteStart      (RemoteStartRequest)   returns (OpResult);
  rpc Escalation       (EscalationRequest)    returns (OpResult);   // no-op
  rpc RestoreWal       (RestoreWalRequest)    returns (OpResult);

  rpc GetStatus        (GetStatusRequest)     returns (NodeStatus);
  rpc GetNodeConfig    (NodeConfigRequest)    returns (NodeConfigResponse);

  rpc ClusterInit      (ClusterInitRequest)      returns (ClusterInitResponse);

  rpc ListMaintenance  (ListMaintenanceRequest)  returns (ListMaintenanceResponse);
  rpc GetMaintenance   (GetMaintenanceRequest)   returns (MaintenanceIntent);
  rpc RetryMaintenance (RetryMaintenanceRequest) returns (OpResult);
}

message NodeRef {
  int32  id       = 1;   // pgpool node ID, -1 == unset
  string hostname = 2;
  int32  pg_port  = 3;   // informational; agent uses config-sourced value
  string pg_data  = 4;   // informational; same
}

// Hook arg layout — see §6 for token mapping.
message FailoverRequest {
  NodeRef detached    = 1;
  NodeRef new_main    = 2;
  NodeRef old_primary = 3;
  NodeRef old_main    = 4;
}
message FollowPrimaryRequest {
  NodeRef detached    = 1;
  NodeRef new_primary = 2;
  NodeRef old_main    = 3;
  NodeRef old_primary = 4;
}
message RecoveryRequest    { NodeRef standby = 1; NodeRef primary = 2; }
message RemoteStartRequest { NodeRef target = 1; }
message EscalationRequest  {}

message RestoreWalRequest {
  // ^([0-9A-F]{24}|[0-9A-F]{8}\.history)$
  string wal_file  = 1;
  string dest_path = 2;  // absolute, must resolve inside PGDATA
}

// Maintenance — durable retry queue for failed peer DropSlot calls.
message MaintenanceIntent {
  string id            = 1;
  string op            = 2;
  string status        = 3;  // "pending" | "done" | "abandoned"
  bytes  payload       = 4;
  int32  attempts      = 5;
  string last_error    = 6;
  string created_at    = 7;  // RFC3339Nano UTC
  string updated_at    = 8;
  string next_retry_at = 9;  // RFC3339Nano UTC, empty when unset
}
message ListMaintenanceRequest    { repeated string statuses = 1; }
message ListMaintenanceResponse   {
  repeated MaintenanceIntent       intents = 1;
  repeated SkippedMaintenanceIntent skipped = 2;
}
message SkippedMaintenanceIntent  { string path = 1; string error = 2; }
message GetMaintenanceRequest     { string id = 1; }
message RetryMaintenanceRequest   { string id = 1; }

// One-time bootstrap.
message ClusterInitRequest         { optional int32 only_node_id = 1; }
message ClusterInitStandbyResult   { int32 node_id = 1; string hostname = 2; bool ok = 3; string message = 4; }
message ClusterInitResponse        { bool ok = 1; string message = 2; string repl_user = 3; repeated ClusterInitStandbyResult standbys = 4; }
```

### 3.2 `PgAgentPeer` — mTLS TCP, port 9701

Clients are other agents. Mutual TLS, cert from `[tls]`, SAN must be in the
pool allowlist (see §7).

```proto
service PgAgentPeer {
  rpc Start            (StartRequest)            returns (OpResult);
  rpc Stop             (StopRequest)             returns (OpResult);
  rpc Reload           (ReloadRequest)           returns (OpResult);
  rpc ReloadPgpool     (ReloadPgpoolRequest)     returns (OpResult);
  rpc Promote          (PromoteRequest)          returns (OpResult);
  rpc CreateSlot       (CreateSlotRequest)       returns (OpResult);
  rpc DropSlot         (DropSlotRequest)         returns (OpResult);
  rpc ConfigureStandby (ConfigureStandbyRequest) returns (OpResult);
  rpc Basebackup       (BasebackupRequest)       returns (stream OpProgress);
  rpc Rewind           (RewindRequest)           returns (stream OpProgress);
  rpc FetchWal         (FetchWalRequest)         returns (stream WalChunk);
  rpc RemoveVip        (RemoveVipRequest)        returns (OpResult);   // codes::Unimplemented
  rpc GetStatus        (GetStatusRequest)        returns (NodeStatus);
  rpc GetNodeConfig    (NodeConfigRequest)       returns (NodeConfigResponse);
}

message CreateSlotRequest        { string slot_name = 1; }       // min_len 1
message DropSlotRequest          { string slot_name = 1; }       // min_len 1
message ConfigureStandbyRequest  { string primary_host = 1; int32 primary_port = 2; string repl_user = 3; string slot_name = 4; }
message BasebackupRequest        { string primary_host = 1; int32 primary_port = 2; string repl_user = 3; string slot_name = 4; }
message RewindRequest            { string primary_host = 1; int32 primary_port = 2; string repl_user = 3; }
message FetchWalRequest          { string wal_file = 1; }        // ^([0-9A-F]{24}|[0-9A-F]{8}\.history)$
message WalChunk                 { bytes data = 1; }             // chunk size: 1 MiB
message RemoveVipRequest         { string address = 1; string device = 2; }
```

### 3.3 Validation

The Go code installs a `protovalidate` interceptor on both servers (unary +
stream first-message). The only constraints actually used are:

- `string.min_len = 1` on slot names, hostnames, replication user, intent ids,
  dest paths.
- `int32.gt = 0` on PostgreSQL ports.
- A pattern on `wal_file`: `^([0-9A-F]{24}|[0-9A-F]{8}\.history)$`.

If a Rust protovalidate crate is awkward, validate by hand in the handlers
and return `tonic::Status::invalid_argument(...)`. The constraints must be
checked **before** any handler logic runs.

### 3.4 Status codes used

| gRPC code              | When |
|------------------------|------|
| `InvalidArgument`      | validation failure; `dest_path` outside `PGDATA`; `wal_file` rejected by filename whitelist |
| `FailedPrecondition`   | `Basebackup` called while PostgreSQL is running |
| `NotFound`             | `FetchWal` for a WAL segment that is absent in the peer's archive; `GetMaintenance` / `RetryMaintenance` for missing id |
| `Unimplemented`        | `RemoveVip` |
| `Internal`             | everything else that fails inside a handler |

Hook RPCs (`PgAgentLocal.Failover`, etc.) usually return `OpResult { ok=false, message=... }` rather than a gRPC error when the failure is operationally normal (`FollowPrimary` skipping a stopped target, `Failover` with no candidates, `RestoreWal` not finding a segment). Reserve gRPC errors for unrecoverable / system-level failures.

---

## 4. Dependency-injection seams

All external collaborators are pulled out behind traits so unit tests can
fake them and the daemon can be exercised end-to-end in multi-node
functional tests without touching real PostgreSQL or systemd. Translate the
following Go interfaces to Rust traits, every method `async fn` returning
`Result<…, AgentError>`:

| Trait              | Real impl                          | Surface |
|--------------------|------------------------------------|---------|
| `LocalDb`          | tokio-postgres pool to local Unix socket | `promote()`, `checkpoint()`, `create_slot(name)`, `drop_slot(name)`, `is_in_recovery()`, `replication_lag()` → `(bytes, state)`, `setting(name)`, `extension_exists(name)`, `role_exists(name)`, `create_replication_role(name)` |
| `Peers`            | mTLS gRPC pool                     | `client(node) -> PeerClient`, `close()` |
| `PgStandby`        | subprocess + filesystem            | `basebackup(opts, progress_cb)`, `rewind(opts, progress_cb)`, `write_recovery_conf(opts)` |
| `Pcp`              | `pcp_attach_node` / `pcp_node_count` subprocess | `attach_node(id)`, `node_count() -> int` |
| `Systemd`          | zbus to `systemd1`                 | `start_postgres()`, `stop_postgres()`, `status_postgres()`, `status_pgpool()`, `reload_or_restart_postgres()`, `reload_or_restart_pgpool()` |
| `ReplayMarkerStore` | dotfiles under `$PGDATA`          | `has(op, key)`, `mark_done(op, key)`, `sweep(now)` |
| `WalStore`         | filesystem (archive dir + PGDATA)  | `open_archive(wal_file) -> AsyncRead`, `write_restore(dest_path, src)` |
| `MaintenanceStore` | one JSON file per intent under `<agent_dir>/maintenance/` | `append(op, payload)`, `list_pending()`, `list(statuses…)`, `get(id)`, `mark_attempt(id, err, next_retry_at)`, `mark_done(id)`, `mark_abandoned(id, err)`, `reschedule(id, when)` |

A `NodeIntrospection` trait (`get_status`, `get_node_config`) is satisfied by
`Agent` itself; `LocalServer` and `PeerServer` both delegate `GetStatus` /
`GetNodeConfig` to it so there is one canonical implementation.

### 4.1 LocalDb queries (verbatim)

```sql
-- promote
SELECT pg_promote();

-- checkpoint
CHECKPOINT;

-- create slot (treat SQLSTATE 42710 as success → idempotent)
SELECT pg_create_physical_replication_slot($1);

-- drop slot
SELECT pg_drop_replication_slot($1);

-- recovery / lag
SELECT pg_is_in_recovery();
SELECT coalesce(pg_wal_lsn_diff(pg_last_wal_receive_lsn(),
                                pg_last_wal_replay_lsn()), 0);
SELECT coalesce(status, '') FROM pg_stat_wal_receiver LIMIT 1;  -- empty when no receiver

-- preflight helpers
SELECT current_setting($1, true);
SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname=$1);
SELECT EXISTS (SELECT 1 FROM pg_roles  WHERE rolname=$1);

-- replication role (treat 42710 as success)
CREATE ROLE "<name>" WITH LOGIN REPLICATION;  -- name validated against the same regex used for repl_user
```

The local pool connects as `postgres` via Unix socket (peer auth):

```
host=<socket_dir> port=<pg_port> user=postgres dbname=postgres
```

---

## 5. Workflows

All coordination logic runs inside `pg_agentd`. The hook client is purely a
forwarder. Each workflow maps 1:1 to a `PgAgentLocal` RPC; the agent then
dispatches per-step calls to peer agents over `PgAgentPeer`.

### 5.1 `Failover(detached, new_main, old_primary, old_main)`

1. If `new_main.id == -1` → no candidates available. Log critical error,
   return `OpResult { ok=false, message="no standby candidates available" }`.
   Do **not** error the RPC.
2. Compute idempotency replay key:
   `detached={id},new_main={id},old_primary={id}`. If
   `replay.has("failover", key)` → return ok with "already processed".
3. Resolve `detached`, `new_main`, `old_primary` from topology
   (hostname-authoritative — see §8.2).
4. **Standby down** (`detached.id != old_primary.id`):
   - Drop the slot locally with a best-effort cleanup context (30s timeout,
     decoupled from the hook ctx).
   - On error: enqueue `drop_slot_cleanup` maintenance intent; still return
     `ok=true` with a descriptive message.
   - `replay.mark_done(...)`.
5. **Primary down** (`detached.id == old_primary.id`):
   - `peers[new_main].Promote()`.
   - `peers[new_main].DropSlot(detached.slot_name)`.
   - On DropSlot failure: enqueue maintenance intent; still mark done.

The slot name is always `node{id}` (e.g. `node2`).

### 5.2 `FollowPrimary(detached, new_primary, …)`

Called by pgpool per-down-non-primary in a forked child (concurrent
invocations targeting different nodes are normal). Also fires on
`pcp_promote_node`. Not called when only a standby went down.

1. Replay key: `detached={id},new_primary={id}`. If already done → skip.
2. Resolve both nodes.
3. `peers[detached].GetStatus()`. If `!is_running` → return ok with "skipping",
   mark replay done. The node is presumed down for a deliberate reason.
4. `peers[detached].Stop()`.
5. Local `Checkpoint` (see notes §2: makes the slot consistent with the WAL
   stream a basebackup would start from).
6. Local `CreateSlot(detached.slot_name)` (slot is created on the **new
   primary**, i.e. local node).
7. Try `peers[detached].Rewind(new_primary)`. Drain progress stream until
   `phase == "done"` or error.
8. If rewind failed: `peers[detached].Basebackup(new_primary, slot=detached.slot_name)`.
9. `peers[detached].ConfigureStandby(new_primary)` — writes
   `$PGDATA/myrecovery.conf` + `$PGDATA/standby.signal` on the standby.
10. `peers[detached].Start()`.
11. Local `pcp.attach_node(detached.id)`. (Slot is now in use by a running
    standby — do **not** drop it if this step fails.)
12. `replay.mark_done(...)`.

On any failure between step 6 and 10, drop the slot via a best-effort
cleanup context; on cleanup failure enqueue the drop_slot_cleanup
maintenance intent. On `pcp_attach` failure (step 11): do not drop the slot.

### 5.3 `Recovery1stStage(standby, primary)`

Invoked indirectly: pgpool calls the `pgpool_recovery` PostgreSQL extension
on the primary, which exec's `$PGDATA/recovery_1st_stage` (a symlink to
`pg_agentc`).

1. Replay key: `primary={id},standby={id}`. If done → skip.
2. Resolve primary as **local** node (`resolve_local_node` — refuse if not
   local) and standby normally.
3. Local `Checkpoint`.
4. Local `CreateSlot(standby.slot_name)`.
5. `peers[standby].Basebackup(primary, slot=standby.slot_name)` — drain
   progress until `phase == "done"`.
6. `peers[standby].ConfigureStandby(primary)` — must follow basebackup
   because basebackup wipes `$PGDATA` first.
7. **No** `pcp_attach_node` — pgpool drives re-attachment after stage 2.
8. `replay.mark_done(...)`.

Cleanup rule: drop the slot on any failure after step 4.

### 5.4 `RemoteStart(target)`

Invoked by `pgpool_recovery` on the primary.

1. `db.is_in_recovery()`. If true → refuse with `ok=false, message="local node is not the primary (in recovery)"`.
2. Resolve target. `peers[target].Start()`.
3. Discards pgpool's positional `$2` (the primary's PGDATA) — see notes §11.

### 5.5 `Escalation()` / `DeEscalation()`

Both map to the same RPC (`Escalation`). No-op: HAProxy replaces VIP
management. Returning ok keeps watchdog happy.

### 5.6 `RestoreWal(wal_file, dest_path)`

Invoked by PostgreSQL on a standby via
`restore_command = 'pg_agentc restore-wal %f %p'`. `dest_path` is
**authoritative** — it is the local backend telling the agent where to drop
the segment (see notes §6).

1. Validate `wal_file` against the regex above.
2. For each non-local pool entry, in order:
   - Open `PgAgentPeer.FetchWal(wal_file)` on the peer (server stream of
     `WalChunk`). Bound the call with a per-peer 30s timeout that honours the
     parent ctx.
   - `wal.write_restore(dest_path, stream)`:
     - Reject `dest_path` outside `PGDATA` with `InvalidArgument`
       (`ErrDestOutsidePgData`) → abort entire `RestoreWal` (every peer
       would fail identically).
     - Write to a hidden temp file in the same dir
       (`."<base>"-<8 hex>"`), `O_EXCL`, mode `0600`. Copy. Close (syncs).
       Rename onto `dest_path`. Remove temp on any error.
   - On `NotFound` from the peer → try next peer.
   - On context deadline / cancel → try next peer.
   - On success → return `ok=true`.
3. If no peer had the segment → return `ok=false` so the caller exits
   non-zero, which is the signal PostgreSQL uses to pause and retry.

### 5.7 `ClusterInit(only_node_id?)`

Operator one-shot, dispatched via `pg_agentctl cluster init`. Must run on the
chosen primary.

1. Refuse if local node is in recovery.
2. `db.create_replication_role(repl_user)` (idempotent, SQLSTATE 42710 → ok).
3. For each non-local pool entry (or just the requested one):
   - `db.create_slot(slot_name)`
   - `peers[node].Stop()` (defensive — basebackup needs an empty pgdata)
   - `peers[node].Basebackup(primary=local, slot=…)` → drain
   - `peers[node].ConfigureStandby(primary=local, slot=…)`
   - `peers[node].Start()`
4. Collect per-node results; overall ok iff every standby succeeded.

Authentication uses replication client certs (`[postgres.replication_tls]`),
so the request carries no password.

### 5.8 Peer RPC handlers (what a peer does for someone else)

| RPC                 | Action |
|---------------------|--------|
| `Start`             | `systemd.StartUnit($pg_service, "replace")` |
| `Stop`              | `systemd.StopUnit($pg_service, "replace")` |
| `Reload`            | `systemd.ReloadOrRestartUnit($pg_service, "replace")` |
| `ReloadPgpool`      | `systemd.ReloadOrRestartUnit($pgpool_service, "replace")` |
| `Promote`           | `SELECT pg_promote()` |
| `CreateSlot`        | `pg_create_physical_replication_slot(name)`, SQLSTATE 42710 ok |
| `DropSlot`          | `pg_drop_replication_slot(name)` |
| `ConfigureStandby`  | validate (`primary_host` regex, port>0, repl_user regex, slot regex). Write `$PGDATA/myrecovery.conf` (template — see §5.10) and create empty `$PGDATA/standby.signal`. Both files mode `0640`. |
| `Basebackup`        | refuse if PostgreSQL is running (`FailedPrecondition`). Clear `$PGDATA` contents. Exec `<pghome>/bin/pg_basebackup --pgdata <data> --dbname '<conninfo>' --wal-method=stream --checkpoint=fast --no-password [--slot <name>] [--progress]`. Scan stderr line-by-line (split on `\r` *or* `\n`), forward `done/total kB` lines as `OpProgress { phase="streaming", bytes_done=done*1024, bytes_total=total*1024 }`, log other lines, capture last ~4 KiB into the error tail if the subprocess exits non-zero. Final `OpProgress { phase="done" }`. |
| `Rewind`            | clear `$PGDATA/pg_replslot/*` before. Exec `<pghome>/bin/pg_rewind --target-pgdata <data> --source-server '<conninfo with dbname=postgres>' --no-password --progress`. Same scanner. After success, clear `$PGDATA/pg_replslot/*` again (notes §3). Final `OpProgress { phase="done" }`. |
| `FetchWal`          | validate filename. Open `<archive_dir>/<wal_file>` (after `filepath.Localize`-equivalent rejection of `..`/absolute paths). Stream 1 MiB chunks. `NotFound` if absent. |
| `RemoveVip`         | always `Unimplemented`. |
| `GetStatus` / `GetNodeConfig` | delegate to `NodeIntrospection`. |

### 5.9 `GetStatus` implementation

Concurrent queries (best-effort; carry `_status_ok` flags so callers know
whether a `false` means "stopped" or "unknown"):

- `systemd.status_postgres()`
- `systemd.status_pgpool()`
- `db.is_in_recovery()`
- `db.replication_lag()` → `(bytes, state)`

`is_ready` is true iff every probe succeeded (no errors). A standby that
can't report lag isn't ready. An unreachable systemd makes the role
indistinguishable from "down" → not ready.

### 5.10 `myrecovery.conf` template

Rendered by `PgStandby::write_recovery_conf`. Single-quoted values; the
agent rejects any conninfo that contains `'`, `\r`, or `\n` after rendering
(defense in depth — inputs are already validated):

```
# managed by pg_agent
primary_conninfo = '<conninfo>'
primary_slot_name = '<slot>'
restore_command = 'pg_agentc restore-wal %f %p'
```

The leading comment matters — operators should be able to grep for it. The
file is included from `postgresql.conf` via
`include_if_exists = 'myrecovery.conf'` (provisioning is outside the
agent's scope).

`<conninfo>` is built by `PgReplicationTls::conninfo(host, port, user, dbname)`:

- Always: `host=<h> port=<p> user=<u>` (plus ` dbname=<d>` for `pg_rewind`).
- When `[postgres.replication_tls]` is configured (all three of ca/cert/key):
  append ` sslmode=<effective> sslrootcert=<ca> sslcert=<cert> sslkey=<key>`.
  Default `sslmode = "verify-full"`.
- Partial config (1 or 2 paths) → reject at config load
  (`ErrReplicationTLSPartial`).
- `sslmode` outside libpq's set (`disable|allow|prefer|require|verify-ca|verify-full`)
  → `ErrReplicationTLSSSLMode`.
- Cert paths must match `^/[A-Za-z0-9._/-]+$` → `ErrReplicationTLSBadPath`.

### 5.11 Input validation regexes

Defense in depth — every value that flows into a subprocess arg, a libpq
conninfo, or `CREATE ROLE` SQL is matched against one of:

```
primary_host : ^[A-Za-z0-9._:-]+$        (DNS, IPv4, IPv6 literal w/o brackets)
repl_user    : ^[A-Za-z0-9_.-]+$
slot_name    : ^[A-Za-z0-9_.-]+$
ssl_path     : ^/[A-Za-z0-9._/-]+$       (absolute, no shell metachars)
wal_file     : ^([0-9A-F]{24}|[0-9A-F]{8}\.history)$
```

A hostname containing a space could otherwise inject a second `key=value`
into libpq's conninfo and redirect a basebackup to an attacker host.

### 5.12 Replay markers (idempotency)

`Failover`, `FollowPrimary`, `Recovery1stStage` each derive a stable key
from their request and check `replay_marker_store.has(op, key)` before
doing any work. On success they `mark_done(op, key)`. Markers are stored as
dotfiles under `$PGDATA`:

```
.pg_agent_idem_<op>_<sha256(op|key) hex>.done
```

Content is the RFC3339Nano UTC timestamp of completion. The maintenance
worker sweeps markers older than 24h (`DefaultReplayMarkerRetention`).
`op` is sanitised to `[a-z0-9_-]` only.

### 5.13 Maintenance queue (durable drop-slot retries)

A failed peer `DropSlot` after a successful failover / follow_primary /
recovery_1st_stage **must not** propagate as an RPC error to pgpool. The
slot is queued via `MaintenanceStore.append("drop_slot_cleanup", payload)`
and retried on a 30s sweep tick. Payload:

```json
{
  "slot_name": "node2",
  "target_hostname": "server3",
  "cause": "rpc_error|standby_down_local_drop_error|follow_primary_cleanup_drop_error|recovery_1st_stage_cleanup_drop_error",
  "last_error": "<error>"
}
```

Worker behaviour:

- Sweep interval: 30s.
- Per-op timeout: 30s (so one wedged peer can't stall the whole sweep).
- Per-intent attempt budget: 5. On exhaustion → `MarkAbandoned`.
- Backoff: 30s base, ×2 each attempt, capped at 10 min.
- `NextRetryAt` is honoured — operator-forced retries use `Reschedule(now)`
  without consuming an attempt slot.
- Storage layout: one JSON file per intent under `<agent_dir>/maintenance/`.
  Filename = intent id = `<unix_nano>-<sanitised_op>-<seq>.json`. Writes are
  atomic via temp+rename in the same directory.
- Terminal intents (done/abandoned) are pruned by `list_pending` once they
  exceed retention (the daemon configures this at 24h).
- Obsolete ops `rewind_restore_replslot` and
  `rewind_delete_quarantine_slots` (from older Go versions) are silently
  `MarkDone`'d so an upgrade doesn't show old intents as failing.

The same worker also calls `replay_marker_store.sweep(now)` on the same
cadence.

### 5.14 Best-effort cleanup contexts

When a hook RPC is being cancelled (deadline, peer reset), cleanup work
must still complete. Use a detached, time-bounded context:

```rust
// Go: bestEffortCleanupContext(parent) →
//      context.WithTimeout(context.WithoutCancel(parent), 30s)
// Rust: tokio::time::timeout(Duration::from_secs(30), async {…})
//       inside a tokio::spawn that does NOT inherit the parent cancellation.
```

30s budget is enough for one local `DropSlot` and one maintenance append.

---

## 6. Hook contract (positional args / format tokens)

The hookspec crate is the single source of truth for these layouts; both
`pg_agentc` (parsing pgpool's argv) and `pg_agentctl` (printing/validating
`pgpool.conf`) consume it. There must be no other place a token order is
written down.

### 6.1 pgpool-templated hooks

Operator-configurable format strings — order is whatever appears in
`pgpool.conf`. The canonical lines `pg_agentctl print-hooks` emits:

```
failover_command           = 'pg_agentc failover       %d %h %p %D %m %H %M %P %r %R %N %S'
follow_primary_command     = 'pg_agentc follow_primary %d %h %p %D %m %H %M %P %r %R %N %S'
wd_escalation_command      = 'pg_agentc escalation'
wd_de_escalation_command   = 'pg_agentc de_escalation'
```

Token meaning (pgpool tokens, *not* postgres tokens):

| Token | Meaning |
|-------|---------|
| `%d %h %p %D` | detached node id / host / port / pgdata |
| `%m %H %r %R` | new main id / host / port / pgdata |
| `%M`          | old main id |
| `%P %N %S`    | old primary id / host / port |

`%m / %H` carry different semantics across the two hooks (notes §9): in
`failover_command` they are the smallest-id surviving node ("new main"); in
`follow_primary_command` they explicitly mean "new primary". Agent code
should verify primary status with `pg_is_in_recovery()` rather than trust
the token blindly.

### 6.2 Fixed-arg hooks (pgpool_recovery C extension)

The pgpool_recovery extension exec's these directly with a fixed argv
layout. The operator has no `pgpool.conf` knob for the order.

```
recovery_1st_stage : $1=primary_pgdata $2=standby_host $3=standby_pgdata
                     $4=primary_port   $5=standby_id   $6=standby_port
                     $7=primary_host
pgpool_remote_start: $1=standby_host   $2=primary_pgdata   (see notes §11)
```

Both are installed as symlinks under `$PGDATA/recovery_1st_stage` and
`$PGDATA/pgpool_remote_start` pointing at `pg_agentc`. The client detects
symlink invocation via `argv[0]` basename. The daemon creates / repairs
these symlinks during startup (refuses if a non-`pg_agentc` symlink is in
the way, and refuses if a regular file/dir is in the way).

### 6.3 PostgreSQL hook

```
postgresql.conf:
restore_command = 'pg_agentc restore-wal %f %p'
```

`%f` is the WAL filename, `%p` is the destination path (these are
postgres's tokens, not pgpool's — they happen to collide visually with
`%p=port` for pgpool, so the hookspec keeps them in separate enum types).

---

## 7. mTLS and certificate handling

### 7.1 When TLS is required

- Required whenever any pool hostname resolves to a non-loopback IP.
  Checked at startup; an unresolvable hostname is conservatively treated as
  remote.
- Loopback-only pools (e.g. `127.0.0.x` in functional tests) may run
  plaintext.
- `--dev` (CLI flag) combined with `allow_insecure_remote_peer = true` in
  config is the **only** way to disable the check. Either alone is a hard
  error.

### 7.2 Where TLS material lives

```toml
[tls]
ca_cert = "/etc/pg_agent/ca.crt"
cert    = "/etc/pg_agent/node.crt"
key     = "/etc/pg_agent/node.key"
```

All three must be set together. Partial config rejected at startup.

### 7.3 Peer mTLS rules

- TLS 1.3 minimum, both inbound and outbound.
- Inbound: standard chain verification against the CA, **plus** a custom
  SAN check that requires at least one of the peer cert's DNS SANs or IP
  SANs to match an entry in the pool allowlist. The allowlist is derived
  from `[[pool]] hostname` plus the IPs every hostname resolves to.
- Outbound: chain verification against the CA, server hostname check.
- Connection lifetime: cap each TCP connection at 12h with a 5-minute grace
  for in-flight streams (so SIGHUP-rotated certs make it onto the wire
  within ~12h without tearing down healthy connections).

### 7.4 Cert reloading

`SIGHUP` triggers a hot reload — re-read all three files from disk and
swap the active bundle atomically. Use `arc-swap::ArcSwap<CertBundle>` with
rustls's `ResolvesServerCert` / `ResolvesClientCert` callbacks (or hand-rolled
callbacks reading the latest bundle on every handshake). The previous
bundle stays in use on reload failure; log the error.

Only `[tls]` paths are hot-reloadable. Changes to `[[pool]]`, listen
address, node id, etc. require a restart.

The `pg_agentd.service` unit ships `ExecReload=/bin/kill -HUP $MAINPID`, so
`systemctl reload pg_agentd` is the operator interface.

### 7.5 `/healthz` TLS

The healthz HTTPS listener reuses the same cert (server-only — no client
cert is required). HAProxy probes it without presenting a cert. The cert
reloader's `ServerOnlyConfig`/equivalent serves it.

---

## 8. Configuration

### 8.1 File layout (`/etc/pg_agent/config.toml`)

```toml
agent_port  = 9701                                # mTLS peer port
unix_socket = "/run/pg_agentd/pg_agentd.sock"     # local socket
# listen = "0.0.0.0"                              # bind addr (peer listener)

# node_id      = 0                                # explicit local node id
# node_id_file = "/etc/pgpool2/pgpool_node_id"    # or via file
# agent_dir    = "/var/lib/postgresql/pg_agent"   # state root

# allow_insecure_remote_peer = false              # requires --dev to actually disable

[tls]
ca_cert = "/etc/pg_agent/ca.crt"
cert    = "/etc/pg_agent/node.crt"
key     = "/etc/pg_agent/node.key"

[[pool]]
id       = 0
hostname = "server1"
[[pool]]
id       = 1
hostname = "server2"
[[pool]]
id       = 2
hostname = "server3"

[postgres]
# port       = 5432
# pghome     = "/usr/lib/postgresql/17"
# data_dir   = "/var/lib/postgresql/17/main"
# socket_dir = "/var/run/postgresql"
# repl_user  = "repl"
# home       = "/var/lib/postgresql"
# archive_dir = "/var/lib/postgresql/archive"
# service    = "postgresql@17-main.service"

[postgres.replication_tls]
# ca_cert = "/etc/pg_agent/repl-ca.crt"
# cert    = "/etc/pg_agent/repl-node.crt"
# key     = "/etc/pg_agent/repl-node.key"
# sslmode = "verify-full"

[pcp]
# user           = "pgpool"
# port           = 9898
# pgpool_service = "pgpool2.service"

[healthz]
# enabled = true
# listen  = "0.0.0.0"
# port    = 9702
```

### 8.2 Defaults (apply when omitted)

```
agent_port            = 9701
unix_socket           = /run/pg_agentd/pg_agentd.sock
agent_dir             = <postgres.home>/pg_agent       (after pg defaults)

postgres.port         = 5432
postgres.pghome       = /usr/lib/postgresql/17
postgres.data_dir     = /var/lib/postgresql/17/main
postgres.socket_dir   = /var/run/postgresql
postgres.repl_user    = repl
postgres.home         = /var/lib/postgresql
postgres.archive_dir  = /var/lib/postgresql/archive
postgres.service      = postgresql@17-main.service

pcp.user              = pgpool
pcp.port              = 9898
pcp.pgpool_service    = pgpool2.service

healthz.enabled       = true
healthz.listen        = 0.0.0.0
healthz.port          = 9702
```

### 8.3 Validation

- `pool` must be non-empty.
- Node ids must be unique and ≥ 0; hostnames must be unique.
- Local node id must resolve (see §8.4) and be present in the pool.
- `[postgres.replication_tls]` is all-or-nothing; sslmode and cert paths
  validated as in §5.10.

### 8.4 Local-node id resolution

In priority order, first hit wins:

1. `node_id` field at the root of `config.toml`.
2. `node_id_file` field — file containing the integer.
3. `<agent_dir>/node_id` — same convention as pgpool's `pgpool_node_id`.
4. Hostname fallback: `os::hostname()` matched against `[[pool]].hostname`.

Sources 1 and 2 are errors if they point at an id that isn't in the pool.
Sources 3 and 4 are best-effort; if none match, the daemon fails startup
with a "local node not found" error.

### 8.5 Peer listen address

`PeerListenAddr()` returns `host:agent_port` where host is:

- `config.listen` if set,
- `"0.0.0.0"` if TLS is configured,
- `"127.0.0.1"` otherwise.

Override via `PG_AGENTD_LISTEN`.

### 8.6 Hostname-authoritative node resolution

When the agent receives a `NodeRef` from a hook, it must look the node up
**by hostname first** (pgpool's authoritative view), falling back to id only
when hostname is empty. All operational parameters (`pg_data`, `pg_port`,
service names) come from `config.toml` — the wire `pg_data` / `pg_port`
fields are informational. Drift between argv and config is logged at WARN
("NodeRef X does not match config; using config value"); the agent's config
always wins.

### 8.7 Config precedence

Highest wins, last:

1. Config file
2. Env vars (see below) — `ApplyEnvOverrides`
3. CLI flags

If `--config` is not given and the default file is missing, the daemon
falls back to `DefaultConfig()` and warns. This is intentional for dev — a
bare `pg_agentd` runs with built-in defaults (empty pool → no peers, useful
for local smoke tests).

### 8.8 Environment variables

| Var                       | Field |
|---------------------------|-------|
| `PG_AGENTD_SOCKET`        | `unix_socket` |
| `PG_AGENTD_PORT`          | `agent_port` |
| `PG_AGENTD_LISTEN`        | `listen` |
| `PG_AGENTD_PCP_USER`      | `pcp.user` |
| `PG_AGENTD_PCP_PORT`      | `pcp.port` |
| `PG_AGENTD_TLS_CA_CERT`   | `tls.ca_cert` |
| `PG_AGENTD_TLS_CERT`      | `tls.cert` |
| `PG_AGENTD_TLS_KEY`       | `tls.key` |
| `PG_AGENTD_CONFIG`        | path to `config.toml` (read by the systemd unit, not the binary) |
| `PG_AGENTC_TIMEOUT`       | hook client per-RPC timeout (default 30 min) |
| `PG_AGENTCTL_TIMEOUT`     | operator CLI per-RPC timeout (default 5 min) |

---

## 9. Health endpoint (`/healthz`)

Separate HTTPS listener on port 9702. Status code is the contract; JSON
body is informational.

| Path                | 200 iff |
|---------------------|---------|
| `/healthz`          | snapshot is fresh — pure liveness for the agent process |
| `/healthz/primary`  | snapshot fresh, postgres reachable, role=primary, pgpool reachable |
| `/healthz/replica`  | snapshot fresh, postgres reachable, role=replica, WAL receiver active, pgpool reachable |

### 9.1 Mechanism

- A background "snapshotter" probes postgres and pgpool every **1 s**,
  each sub-probe with a **500 ms** timeout, in parallel.
- Latest snapshot is published via `ArcSwap<HealthSnapshot>` (initially
  None).
- Handler is hot-path-cheap: atomic load + small struct read + JSON marshal.
- **No DB or PCP calls happen on the request path.**
- A snapshot older than **30 s** flips `/healthz` to 503 (the
  snapshotter goroutine is wedged → process is wedged). Sub-probes that
  fail still stamp a fresh timestamp with `reachable: false` in the body.
- HEAD is supported alongside GET; method != GET/HEAD → 405.

### 9.2 Snapshot body (informational JSON)

```json
{
  "role": "primary|replica|unknown",
  "ready": true,
  "pgpool":   { "reachable": true,  "backends_up": 3, "error": "" },
  "postgres": { "reachable": true,  "in_recovery": false, "error": "" },
  "replication": { "lag_bytes": 0, "wal_receiver_state": "" },
  "snapshot_age_ms": 137
}
```

### 9.3 TLS

Reuses the peer cert (`[tls]`). Server-only TLS (no client cert). If TLS is
not configured, refuse to bind unless `--dev` is set (so a botched cert
install can't silently expose role/lag over the network).

---

## 10. Filesystem & deployment

### 10.1 Unix socket

`/run/pg_agentd/pg_agentd.sock`, mode `0600`, owner `postgres:postgres`.
Created on each start (after removing any stale socket from a prior run).
Provided by systemd via `RuntimeDirectory=pg_agentd`. Every legitimate
caller (`pg_agentc`, the `pgpool_recovery` extension running inside the
PostgreSQL backend, the `restore_command` PostgreSQL forks) already runs as
the `postgres` OS user, so file-mode access control is sufficient.

If a non-`postgres` user must read the socket, add a shared group
(`pgagent`) and switch the socket to `0660 postgres:pgagent` (out of scope).

### 10.2 `$PGDATA` symlinks

```
$PGDATA/recovery_1st_stage  -> /usr/bin/pg_agentc
$PGDATA/pgpool_remote_start -> /usr/bin/pg_agentc
```

Created/repaired by `pg_agentd` at startup. Rules:

1. Missing → create.
2. Symlink whose target basename is `pg_agentc` → replace.
3. Symlink pointing elsewhere → **error** (refuse to overwrite operator
   state).
4. Regular file/dir in the way → **error**.

`pg_agentd` locates `pg_agentc` first as a sibling of its own executable
(Debian package layout) and falls back to `PATH`.

### 10.3 Persistent state

```
<agent_dir>/
├── node_id                # optional — see §8.4
└── maintenance/
    └── <intent-id>.json   # one per intent, atomic temp+rename writes

$PGDATA/
└── .pg_agent_idem_<op>_<sha256>.done   # replay markers (dotfiles)
```

`<agent_dir>` defaults to `<postgres.home>/pg_agent` and is created with
mode `0700` by the daemon at startup.

### 10.4 systemd unit

`pg_agentd.service` essentials:

```ini
[Unit]
After=network.target postgresql.service
Wants=network.target
Before=pgpool2.service

[Service]
Type=notify                                # daemon sends READY=1 + STOPPING=1
EnvironmentFile=-/etc/default/pg_agentd
RuntimeDirectory=pg_agentd
RuntimeDirectoryMode=0755
ExecStart=/bin/sh -ec 'exec /usr/bin/pg_agentd ${PG_AGENTD_CONFIG:+-config "$PG_AGENTD_CONFIG"}'
ExecReload=/bin/kill -HUP $MAINPID         # cert rotation
Restart=on-failure
RestartSec=5s
TimeoutStartSec=30s
TimeoutStopSec=30s
User=postgres
Group=postgres
PrivateTmp=true
ProtectHome=read-only
ProtectHostname=true
ProtectKernel{Logs,Modules,Tunables}=true
RestrictRealtime=true
RestrictSUIDSGID=true

[Install]
WantedBy=multi-user.target
```

- `Type=notify`: daemon must send `READY=1` via `sd_notify` once both
  listeners are bound and gRPC servers have accepted them. On shutdown,
  send `STOPPING=1` before graceful drain. No-op when `NOTIFY_SOCKET` is
  unset (dev runs outside systemd).
- Order: `pg_agentd` must come up before `pgpool2`. Both depend on
  `network-online.target` (the upstream unit uses `network.target` — keep
  matching).
- The package does **not** auto-enable or auto-start the service —
  deployment tooling does that.

### 10.5 polkit rule

Installed at `/usr/share/polkit-1/rules.d/50-pg-agent.rules`. Grants the
`postgres` user `org.freedesktop.systemd1.manage-units` for any
`postgresql@*.service` and `pgpool2.service`, verbs:
`start`/`stop`/`reload`/`restart`/`reload-or-restart`/`try-restart`/
`reload-or-try-restart`. No `sudo`, no shell exec.

### 10.6 Binary placement & permissions

```
/usr/bin/pg_agentd    0755 root:root
/usr/bin/pg_agentc    0755 root:root          # world-executable for postgres backend
/usr/bin/pg_agentctl  0755 root:root
```

The `pg_agentc` symlinks in `$PGDATA` need execute on the target, not on
the link.

---

## 11. Subprocess invocations

The agent shells out to a small, fixed set of binaries. Subprocess
construction is the same shape in Rust — `tokio::process::Command` with
`kill_on_drop(true)`.

### 11.1 `pg_basebackup` (Peer.Basebackup)

```
<pghome>/bin/pg_basebackup
  --pgdata    <data_dir>
  --dbname    <conninfo>          # built by ReplicationTLS::conninfo, dbname omitted
  --wal-method=stream
  --checkpoint=fast
  --no-password
  [--slot <slot>]
  [--progress]                    # only when a progress cb is provided
```

`--write-recovery-conf` is **intentionally omitted** — `ConfigureStandby`
is the single source of truth for recovery configuration; letting
pg_basebackup append to `postgresql.auto.conf` would split it across two
files and conflict with `ALTER SYSTEM`.

Pre-step: clear `$PGDATA` contents (the directory itself stays). PostgreSQL
must not be running on this node (refused by `PeerServer::Basebackup` →
`FailedPrecondition`).

### 11.2 `pg_rewind` (Peer.Rewind)

```
<pghome>/bin/pg_rewind
  --target-pgdata <data_dir>
  --source-server <conninfo with dbname=postgres>
  --no-password
  --progress
```

Pre + post: `rm -rf $PGDATA/pg_replslot/*` (notes §3).

### 11.3 `pcp_attach_node` / `pcp_node_count` (PCP impl)

```
pcp_attach_node -h localhost -p <pcp_port> -U <pcp_user> -n <id> -w
pcp_node_count  -h localhost -p <pcp_port> -U <pcp_user> -w
```

`-w` = no password prompt; auth is via `~postgres/.pcppass` (mode `0600`,
format `localhost:<port>:<user>:<password>`). The agent never reads or
handles the PCP password directly.

### 11.4 Progress scanner (shared)

Both pg_basebackup and pg_rewind print progress to stderr with carriage
returns (`\r`) for in-place updates. The scanner:

- Reads stderr byte stream, splits on `\r` *or* `\n`.
- Lines matching `^(\d+)/(\d+) (kB|KB)` (with anything after) → forward as
  `(done * 1024, total * 1024)` to the progress callback.
- Other lines → log at INFO, append to a stderr-tail buffer capped at
  ~4 KiB (drop overflow).
- On non-zero exit: error message ends with `; stderr: <tail>` so the
  caller can see the real reason instead of a generic "subprocess exited".

The streaming RPC then converts each callback into
`OpProgress { phase = "streaming" | "rewinding", bytes_done, bytes_total }`
and sends a final `OpProgress { phase = "done" }` on success.

---

## 12. Daemon lifecycle

1. Parse CLI flags (`--config`, `--socket`, `--dev`, `--version`).
2. Load config (or fallback to `DefaultConfig` with a warning).
3. Apply env overrides; apply CLI overrides; apply `--dev` rules.
4. Create `<agent_dir>` and `<agent_dir>/maintenance` (mode `0700`).
5. Compose runtime: `Topology`, `ServeSettings`, `PostgresRuntime`.
6. Build `CertReloader` if TLS configured; fail fast if not and remote
   peers are present and `--dev` is not set.
7. Spawn a SIGHUP handler task that calls `CertReloader::reload()`.
8. Open `LocalDb` pool to local PostgreSQL.
9. Build `PeerTransport` (CA pool, SAN allowlist), then `PeerPool`.
10. Build `PgStandby`, `PcpCli`, `Systemd`, `ReplayMarkerStore`, `WalStore`,
    `MaintenanceStore`.
11. Construct `Agent` with all deps.
12. Repair `$PGDATA` hook symlinks (fail fast on conflicts).
13. `Agent::serve(ctx)`:
    a. Bind Unix socket (chmod 0600, remove stale), start `LocalServer`.
    b. Bind TCP peer addr, start `PeerServer` with mTLS.
    c. Start `MaintenanceWorker` background task (initial sweep + 30s ticker).
    d. Start `/healthz` HTTPS listener (snapshot loop ticking at 1 s).
    e. `sd_notify("READY=1")`.
    f. Wait for SIGINT/SIGTERM. On shutdown: `sd_notify("STOPPING=1")`,
       gracefully stop both gRPC servers, shut down healthz with a 5 s
       grace window.

---

## 13. Operator CLI (`pg_agentctl`)

`pg_agentctl <subcommand> [flags]`.

| Subcommand                                         | Behaviour |
|----------------------------------------------------|-----------|
| `print-hooks`                                      | Emit canonical `pgpool.conf` and `postgresql.conf` hook lines. |
| `check-hooks <pgpool.conf>`                        | Parse the given file (`key = 'value'` lines, single-quote stripping, `#` comment trimming). For every entry in the canonical list: missing/wrong → `ERR`, exact match → `OK`. Exit 0 iff all rows are `OK`. |
| `gen-pgpool [--write <path>] [--config <path>]`    | Build `pg_agent.conf` include fragment by querying every pool member via `GetNodeConfig` (local via Unix socket, peers via mTLS) for live `pg_port` / `pg_data_dir`. Emits `backend_hostname{i} / backend_port{i} / backend_data_directory{i} / backend_flag{i} = ALLOW_TO_FAILOVER`, then the canonical hook block. Stdout by default; `--write` does atomic temp+rename. |
| `preflight [--config <path>] [--skip-db]`          | Run the preflight checks (see §14). Exit 0 if no `ERR` rows. |
| `maintenance list [--status pending|done|abandoned]` | Tabular dump of `ListMaintenance`. Surfaces `Skipped` files to stderr. |
| `maintenance show <id>`                            | `GetMaintenance(id)`; pretty-print fields and JSON payload. |
| `maintenance retry <id>`                           | `RetryMaintenance(id)`. Refuses non-pending intents. |
| `cluster init [--only-node <id>] [--config <path>]` | `ClusterInit({only_node_id})`. Long deadline — overridable via `PG_AGENTCTL_TIMEOUT`. |
| `help` / `version`                                 | as usual |

For RPCs that dial peers (`gen-pgpool`, `cluster init`) the CLI builds its
own `PeerPool` from config (it does not go through the local daemon).
Maintenance + `gen-pgpool`'s local-node query go through the Unix socket.

---

## 14. Preflight checks

Validates the runtime environment has the prereqs `pg_agentd` assumes.
Each check is independent and idempotent. Each emits a `Check { name,
status: OK|WARN|ERR, detail }`. Run as the `postgres` user so the
mode-`0600` files are readable.

Filesystem (always):

- **TLS material** — if `[tls]` unset and only loopback peers exist → WARN.
  If set: each of `ca_cert`/`cert`/`key` exists, is a regular file, has
  mode that postgres can read; cert parses; cert has SANs covering every
  non-local pool hostname; expiry > now + 30 d.
- **polkit rule** — `/usr/share/polkit-1/rules.d/50-pg-agent.rules` exists
  and is readable.
- **pgpool_node_id** — `/etc/pgpool2/pgpool_node_id` exists and matches
  the agent's resolved local node id.
- **.pcppass** — `/var/lib/postgresql/.pcppass` mode `0600`, contains a line
  `localhost:<pcp_port>:<pcp_user>:*`.
- **.pgpass** — contains entries for `repl` and `postgres` users on the
  configured port.
- **pcp.conf** — `/etc/pgpool2/pcp.conf` exists and contains `pgpool` user
  with an md5 hash.
- **pool_passwd** — `/etc/pgpool2/pool_passwd` contains entries for
  `pgpool` and `postgres`.
- **Recovery tools** — `<pghome>/bin/pg_basebackup` and
  `<pghome>/bin/pg_rewind` exist and are executable.

DB-backed (skipped with a WARN if `--skip-db` or DB unreachable):

- **Settings**: `wal_log_hints = on`, `hot_standby = on`,
  `max_replication_slots ≥ pool size`, `max_wal_senders ≥ pool size`.
- **TLS / pg_hba**: if `[postgres.replication_tls]` is configured, verify
  `listen_addresses` includes non-loopback, `ssl=on`, server certs exist,
  and the SSL CA on the primary trusts the configured replication client
  cert; verify `pg_hba.conf` has `hostssl replication <repl_user> … cert
  clientcert=verify-full` (or equivalent).
- **Extension**: `CREATE EXTENSION pgpool_recovery` is in place in the
  `postgres` database.
- **Roles**: `repl`, `pgpool`, `postgres` exist; `pgpool` has `pg_monitor`.
- **pg_hba.conf**: `scram-sha-256` or `md5` for the required (database,
  user) tuples from `samenet`.

Report format:

```
OK    tls material: ca_cert
WARN  tls material: expires in 21d
ERR   pgpool_node_id: file says 0, config says 1
…
preflight: 1 error(s), 1 warning(s) — FAIL
```

---

## 15. `pg_agentc` (hook client)

Tiny on purpose.

- Read socket path from `PG_AGENTD_SOCKET` (default
  `/run/pg_agentd/pg_agentd.sock`).
- Read RPC timeout from `PG_AGENTC_TIMEOUT` (default `30m`).
- Determine hook name and args:
  - If `argv[0]` basename is `recovery_1st_stage` or `pgpool_remote_start`
    (symlink invocation): hook = basename, args = `argv[1..]`.
  - Else: hook = `argv[1]`, args = `argv[2..]`.
- Subcommands: `failover`, `follow_primary`, `recovery_1st_stage` (also via
  symlink), `pgpool_remote_start` (also via symlink), `escalation`,
  `de_escalation`, `restore-wal`, `status`, `help`, `version`.
- Parse positional args via the hookspec schema; any arg-count mismatch is
  a fatal user error.
- Dial `unix://<socket>` with insecure credentials (filesystem perms = auth)
  and issue the corresponding RPC.
- Exit 0 iff `OpResult.ok` is true; otherwise print the message and exit 1.
- `pg_agentc status` queries `GetStatus` and prints a human-readable
  summary: role, postgres state (running/stopped/unknown — `unknown`
  whenever `is_postgres_status_ok` is false), pgpool state, ready, and (if
  replica) replication state + lag bytes.
- `pg_agentc config ...` should print "moved to pg_agentctl" and exit 1.

---

## 16. Testing

Functional multi-node tests should work the same way they do today:

- Three loopback IPs (`127.0.0.0/8`), one daemon per IP, exercised end-to-end.
- A `faked-agentd` test helper binary linked against cross-process fakes:
  same `Agent` wiring as `pg_agentd`, but `LocalDb` / `Systemd` /
  `PgStandby` etc. swapped for HTTP-observable fakes that expose
  `GET/DELETE /<service>/calls`, `POST /<service>/errors`,
  `PUT /<service>/state`. Tests poll those endpoints to assert peers were
  called.
- Env toggles like `FAKED_AGENTD_REAL_PEERS=1` and `FAKED_AGENTD_REAL_FS=1`
  control which collaborators stay real.
- Linux-only by design (the loopback-range trick).

Unit tests use in-process fakes (one struct per trait with call-log
vectors and configurable `Err` fields). Keep the in-process and
cross-process fakes in separate crates so a Rust test can't accidentally
pull in the HTTP machinery for a pure unit test.

---

## 17. Invariants (must preserve)

The Rust port has freedom to refactor everything **except** these. They are
the load-bearing decisions that make this system safe to drop into a live
cluster:

1. **Drop-slot failures are not RPC errors.** The hook RPC returns ok and
   the failed cleanup goes through the maintenance queue. Surfacing the
   error to pgpool would cause it to loop.
2. **Cleanup runs even after hook ctx cancels.** Use a detached, bounded
   (≤ 30 s) context for any cleanup that runs after the primary success.
3. **`$PGDATA/myrecovery.conf` is the single source of recovery config.**
   Never use `pg_basebackup --write-recovery-conf`.
4. **`pg_basebackup` is refused while PostgreSQL is running.** And `$PGDATA`
   contents must be cleared right before the subprocess starts. Otherwise
   pg_basebackup will refuse / corrupt a live datadir.
5. **`pg_rewind` clears `$PGDATA/pg_replslot/*` before *and* after.** Before:
   stale slot dirs from this node's pre-rewind role. After: slot dirs
   pg_rewind copied from the source's role would crash recovery.
6. **`pcp_attach_node` is `FollowPrimary`-only.** Not Recovery1stStage.
7. **`RemoteStart` asserts the local node is the primary.** Pgpool's
   `pgpool_recovery` extension is supposed to call it on the primary; if
   we're a standby, refuse.
8. **`Failover` with `new_main.id == -1` is a logged failure, not a panic.**
   No safe action; return ok=false.
9. **`RestoreWal.dest_path` is authoritative.** Unlike every other hook
   field, this one comes from the local PostgreSQL backend, not pgpool,
   and the agent uses it verbatim (after sanitising the segment name and
   bounding the path to `$PGDATA`).
10. **Hostname-authoritative node resolution.** Topology lookups prefer
    `NodeRef.hostname`; `NodeRef.id` is fallback only.
11. **mTLS required for non-loopback peers.** `allow_insecure_remote_peer`
    requires both the config field and the `--dev` CLI flag — neither
    alone disables it.
12. **`pg_agentd.service` starts before `pgpool2.service`.** Otherwise the
    first failover hook fires against a missing Unix socket.
13. **The Unix socket is `0600 postgres:postgres`.** No auth beyond that.
14. **The hook client carries no config and resolves no nodes.** Adding any
    state to `pg_agentc` is a design regression.
15. **Cert reload is hot, but only `[tls]` is.** Other config changes need a
    full restart.

---

## 18. Out of scope (for v1 of the port)

- VIP management (`RemoveVip`, `if_up_cmd`, etc.) — HAProxy makes it
  unnecessary; the proto entry stays for forward compatibility.
- SRV-based pool discovery (notes §14) — `[[pool]]` stays the source of
  truth; SRV is a future enhancement.
- A pure-Rust replacement for `pg_basebackup` / `pg_rewind` — subprocess for
  now.
- A pure-Rust PCP protocol client — subprocess `pcp_attach_node` /
  `pcp_node_count` for now (the protocol is simple TCP text and could be
  ported later).
- A separate-middleware topology (pgpool not co-located with PostgreSQL).
- `if_up_cmd` / `if_down_cmd` / `arping_cmd` — all replaced by HAProxy.
