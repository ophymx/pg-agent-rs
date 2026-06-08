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
| `pg_agentd`   | Daemon (`pg_agentd serve`, the default). Owns local PostgreSQL operations + cluster coordination. Serves a Unix-socket RPC for local callers and an mTLS TCP RPC for peer agents. Plus a plain-HTTP `/healthz` listener (see §9.2 for why no TLS). Also `pg_agentd validate-env` (see §14). | yes | yes |
| `pg_agentc`   | One-shot hook client. Marshals pgpool's positional argv into a single gRPC call on the local Unix socket, then exits. Also `pg_agentc status`. **No config. No node resolution. No PostgreSQL logic.** | no | no |
| `pg_agentctl` | Operator CLI. `print-hooks`, `check-hooks`, `gen-pgpool`, `maintenance {list,show,retry}`, `cluster {init,status}`. May dial peer agents. | yes | yes |

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
| serverN | 9999   | 9898 | 9000 | 9694/udp   | 9701 mTLS  | 9702 HTTP      |

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
| TLS                              | `rustls` + `tokio-rustls`                          | peer mTLS only — `/healthz` is plain HTTP (see §9.2) |
| TOML config                      | `toml` + `serde`                                   | matches BurntSushi/toml semantics |
| CLI                              | `clap` (derive)                                    | one binary per crate inside the workspace |
| Logging                          | `tracing` + `tracing-subscriber` (JSON or fmt)     | replace Go's `log/slog` |
| PostgreSQL client (local DB)     | `tokio-postgres` + `deadpool-postgres`             | direct port of pgx — keep the pool single-host (Unix socket) |
| systemd D-Bus                    | `zbus` (async)                                     | replace `coreos/go-systemd/v22/dbus`; talk to `org.freedesktop.systemd1` |
| sd_notify                        | `sd-notify` crate, or the documented `NOTIFY_SOCKET` envelope written directly | for `READY=1` / `STOPPING=1` |
| Atomic snapshot pointers         | `arc-swap` (`ArcSwap<T>`, `ArcSwapOption<T>`)      | for `CertReloader` bundle and `HealthSnapshotter` snapshot |
| HTTP server for /healthz         | `axum`                                             | request path does no async I/O; one route |
| Concurrent peer map              | `tokio::sync::Mutex<HashMap<…>>` or `dashmap`      | small N (3 peers); a `parking_lot::Mutex` is fine |
| Signal handling                  | `tokio::signal::unix` (`SIGINT`, `SIGTERM`, `SIGHUP`) | drives shutdown + cert reload |
| Subprocess                       | `tokio::process::Command`                          | drives `pg_basebackup`, `pg_rewind`, `pcp_attach_node`, `pcp_node_info -a` |
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

Two gRPC services. Both are versioned by the `.proto` file. Wire
compatibility is required **across Rust deployments** (so rolling upgrades
work between any two pg-agent-rs versions) but not back to the original Go
agent — pg-agent-rs is a successor, not a peer; mixed Go/Rust clusters are
not a supported topology.

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
  rpc RecoveryFirstStage (RecoveryRequest)    returns (OpResult);
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
| `LocalDb`          | tokio-postgres pool to local Unix socket | `promote()`, `checkpoint()`, `create_slot(name)`, `drop_slot(name)`, `is_in_recovery()`, `replication_lag()` → `ReplicationLag { bytes, state }`, `setting(name)`, `extension_exists(name)`, `role_exists(name)`, `create_replication_role(name)` |
| `PeerRegistry`     | mTLS gRPC pool (`PeerPool`)        | `client(node) -> PeerClient`, `close()` |
| `StandbyOps`       | subprocess + filesystem            | `basebackup(opts, progress_cb)`, `rewind(opts, progress_cb)`, `write_recovery_conf(opts)` |
| `Pcp`              | `pcp_attach_node` / `pcp_node_info` subprocess | `attach_node(id)`, `node_info_all() -> Vec<NodeInfo>`, `node_count() -> int` (legacy / preflight only — `/healthz` uses `node_info_all`) |
| `Systemd`          | zbus to `systemd1`                 | `start_postgres()`, `stop_postgres()`, `status_postgres()`, `status_pgpool()`, `reload_or_restart_postgres()`, `reload_or_restart_pgpool()` |
| `ReplayMarkerStore` | JSON files under `<state_dir>/replay/` | `has(op, key)`, `mark_done(op, key)`, `sweep(now)` |
| `WalStore`         | filesystem (archive dir + PGDATA)  | `open_archive(wal_file) -> AsyncRead`, `write_restore(dest_path, src)` |
| `MaintenanceStore` | one JSON file per intent under `<state_dir>/maintenance/` | `append(op, payload)`, `list_pending()`, `list(statuses…)`, `get(id)`, `mark_attempt(id, err, next_retry_at)`, `mark_done(id)`, `mark_abandoned(id, err)`, `reschedule(id, when)` |

A `NodeInfo` trait (`get_status`, `get_node_config`) is satisfied by
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
2. Resolve `detached`, `new_main`, `old_primary` from topology
   (hostname-authoritative — see §8.2).
3. **Standby down** (`detached.id != old_primary.id`):
   - Drop the slot locally with a best-effort cleanup context (30s timeout,
     decoupled from the hook ctx).
   - On error: enqueue `drop_slot_cleanup` maintenance intent; still return
     `ok=true` with a descriptive message.
4. **Primary down** (`detached.id == old_primary.id`):
   - `peers[new_main].Promote()`.
   - `peers[new_main].DropSlot(detached.slot_name)`.
   - On DropSlot failure: enqueue maintenance intent; still mark done.

> No replay marker for `Failover` — every operation in this flow is
> naturally near-idempotent: `pg_promote()` on an already-primary fails
> harmlessly, slot drops have 42710-ignore baked in (and failure routes
> to the maintenance queue rather than re-running). The worst a re-fire
> does is generate a few lines of warn-level log noise. See §5.12 for
> the rationale on which hooks earn a replay marker.

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

### 5.3 `RecoveryFirstStage(standby, primary)`

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

Cleanup rule: drop the slot on any per-standby failure between
`create_slot` and `start`. If the drop itself fails, append a
`DropSlotCleanup` maintenance intent and continue to the next
standby. Same pattern as `FollowPrimary` and `RecoveryFirstStage` —
slots that survive a half-completed flow pin WAL on the primary
indefinitely.

**No** `pcp_attach_node` — pgpool isn't running yet during initial
bootstrap, and adding-to-a-running-cluster is the future
`pg_agentctl cluster attach <id>` command (see ROADMAP v1.x).

Authentication uses replication client certs (`[postgres.replication]`
+ libpq's `~postgres/.postgresql/` defaults), so the request carries
no password.

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
| `Basebackup`        | refuse if PostgreSQL is running (`FailedPrecondition`). Clear `$PGDATA` contents. Exec `<pg_install_prefix>/bin/pg_basebackup --pgdata <data> --dbname '<conninfo>' --wal-method=stream --checkpoint=fast --no-password [--slot <name>] [--progress]`. Scan stderr line-by-line (split on `\r` *or* `\n`), forward `done/total kB` lines as `OpProgress { phase="streaming", bytes_done=done*1024, bytes_total=total*1024 }`, log other lines, capture last ~4 KiB into the error tail if the subprocess exits non-zero. Final `OpProgress { phase="done" }`. |
| `Rewind`            | clear `$PGDATA/pg_replslot/*` before. Exec `<pg_install_prefix>/bin/pg_rewind --target-pgdata <data> --source-server '<conninfo with dbname=postgres>' --no-password --progress`. Same scanner. After success, clear `$PGDATA/pg_replslot/*` again (notes §3). Final `OpProgress { phase="done" }`. |
| `FetchWal`          | validate filename. Open `<archive_dir>/<wal_file>` (after `filepath.Localize`-equivalent rejection of `..`/absolute paths). Stream 1 MiB chunks. `NotFound` if absent. |
| `RemoveVip`         | always `Unimplemented`. |
| `GetStatus` / `GetNodeConfig` | delegate to `NodeInfo`. |

### 5.9 `GetStatus` implementation

Concurrent queries (best-effort; carry `_status_ok` flags so callers know
whether a `false` means "stopped" or "unknown"):

- `systemd.status_postgres()`
- `systemd.status_pgpool()`
- `db.is_in_recovery()`
- `db.replication_lag()` → `ReplicationLag { bytes, state }`

`is_ready` is true iff every probe succeeded (no errors). A standby that
can't report lag isn't ready. An unreachable systemd makes the role
indistinguishable from "down" → not ready.

### 5.10 `myrecovery.conf` template

Rendered by `StandbyOps::write_recovery_conf`. Single-quoted values; the
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

`<conninfo>` is built by `PgReplicationConfig::conninfo(host, port, user, dbname)`:

- Always: `host=<h> port=<p> user=<u> sslmode=<m>` (plus ` dbname=<d>` for
  `pg_rewind`). The agent ALWAYS emits `sslmode=`; libpq's own default
  is `prefer`, which silently accepts an MITM, so secure-by-default
  requires us to force the operator's hand.
- `sslmode` defaults to `verify-full` when `[postgres.replication]` is
  absent. Operators relax this only deliberately (`disable` for
  dev/test, `require` if the CA isn't distributable, etc.).
- `sslmode` outside libpq's set (`disable|allow|prefer|require|verify-ca|verify-full`)
  → `ErrReplicationSslMode`.

The conninfo does **not** carry `sslcert=`, `sslkey=`, or `sslrootcert=`.
libpq picks those up from its own defaults
(`~postgres/.postgresql/postgresql.crt`, `…/postgresql.key`,
`…/root.crt`) or from `PGSSLCERT` / `PGSSLKEY` / `PGSSLROOTCERT` env
vars on the `postgresql@*.service` unit. Ansible provisions cert
material into libpq's expected locations; pg-agent does NOT own those
paths. Symmetric with `.pcppass` and `.pgpass`, which both live in the
postgres user's home and are also picked up via libpq's default
search.

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

**Scope:** only `FollowPrimary` and `RecoveryFirstStage` carry replay
markers. Both flows run `pg_basebackup` (conditionally for `FollowPrimary`,
unconditionally for `RecoveryFirstStage`), which **wipes `$PGDATA` before
streaming the primary's data**. Re-running a fully-completed flow would
clobber the healthy standby's data dir with a fresh basebackup. The marker
makes the second invocation a fast no-op.

`Failover` deliberately has no marker — its operations are
naturally-near-idempotent (see §5.1's note).

**Stored under `<state_dir>/replay/`** (NOT under `$PGDATA`). Operators
inspecting `$PGDATA` should see PostgreSQL's files, not agent bookkeeping.
The agent's own state lives in `<state_dir>` by design.

**Filename:** `<sanitised_op>_<sha256(op|key) hex>.json`
**Content:** `{"op": "...", "key": "...", "completed_at": "RFC3339Nano UTC"}`

`op` is sanitised to `[a-z0-9_-]` for the filename prefix; the SHA-256 of
`op|key` keeps the name a fixed length regardless of how long the caller's
key gets. JSON content (rather than a bare timestamp) makes each marker
self-describing — `cat <state_dir>/replay/*.json | jq` is a working
incident-review tool with no other infrastructure.

**Retention:** the maintenance worker sweeps markers whose `completed_at`
is older than 24h (`DefaultReplayMarkerRetention`), on the same 30s tick.
A bad file (read error, JSON parse error) is logged and kept — a single
corrupt marker must not block the sweep, and operator intervention beats
silent deletion.

**What the marker does NOT protect:** in-progress re-entry. If a flow is
running concurrently with a retry, both attempts race. The second will
typically fail (e.g. basebackup refuses a non-empty target, or
`peers[detached].Stop()` makes the target unhealthy mid-flow). Fully
preventing in-progress re-entry would require a distributed lock; the
mark-after-success model trades that for simplicity, which is the right
call for our threat model (re-fires after success are far more common
than re-fires during execution).

### 5.13 Maintenance queue (durable retry of failed cleanups)

**Scope today: failed `DropSlot` retry only.** The queue's only
production use is recovering from a peer (or local) `DropSlot` call that
failed *after* the primary success of a Failover / FollowPrimary /
RecoveryFirstStage — leaving an orphan replication slot that pins WAL on
the primary and eventually fills the disk. The hook RPC must still return
`ok=true` to pgpool (the cluster's authoritative side already succeeded);
the cleanup goes here.

The design accommodates additional intent types but adding one is a
**deliberate choice**, not a default. Plausible future candidates
(`pcp_attach_node` retry, `peer Start` retry, `ReloadPgpool` retry) stay
"bubble up as error" until a real operational need surfaces. Resist the
temptation to start enqueueing every possible failure mode.

**Payload is a typed Rust enum**, not an opaque JSON blob:

```rust
#[serde(tag = "op", rename_all = "snake_case")]
enum MaintenancePayload {
    DropSlotCleanup {
        slot_name: String,        // e.g. "node2"
        target_hostname: String,  // node hosting the slot
        cause: &'static str,      // code-level reason; one of:
                                  //   "rpc_error"
                                  //   "standby_down_local_drop_error"
                                  //   "follow_primary_cleanup_drop_error"
                                  //   "recovery_1st_stage_cleanup_drop_error"
        initial_error: String,    // error message at enqueue time
    },
}
```

Adding a new variant forces the worker's `match` to handle it (compile
error if missing) — the polymorphism stays type-safe end to end with no
runtime "unsupported op" branch.

**Worker behaviour:**

- Sweep interval: 30 s.
- Per-op timeout: 30 s (so one wedged peer can't stall the whole sweep).
- Per-intent attempt budget: **5 retries** (the initial fail-then-enqueue
  isn't counted). On exhaustion → `MarkAbandoned`.
- Backoff: 30 s base, ×2 each attempt, capped at 10 min — so a struggling
  intent waits 30 s → 60 s → 2 min → 4 min between attempts, abandoning
  ~8 min after first retry.
- `NextRetryAt` is honoured — operator-forced retries use
  `reschedule(now)` without consuming an attempt slot.
- Dispatch: the worker resolves `target_hostname` against the `NodePool`;
  local target → `LocalDb::drop_slot`, remote → `PeerClient::drop_slot`.
- Storage: one JSON file per intent under `<state_dir>/maintenance/`,
  named `<unix_nano>-<sanitised_op>-<seq>.json`. Writes are atomic via
  temp + rename in the same directory.
- Terminal intents (done/abandoned) are pruned by `list_pending` once
  they exceed retention (default 24 h).

The same worker also calls `replay_marker_store.sweep(now)` on every
tick — one timer, two janitor jobs.

**Deliberately *not* present** (vs. the Go version): the silent
migration of `rewind_restore_replslot` / `rewind_delete_quarantine_slots`
intents. Those op strings were leftovers from a removed feature in the
Go implementation; there is no installed base for pg-agent-rs, so an
intent file with an unknown op surfaces as an error rather than being
quietly retired.

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

- Required whenever any pool hostname is not a literal loopback string
  (`localhost`, `ip6-localhost`, `127.0.0.x`, or `::1`). Pure string
  check — no DNS lookup, no startup blocking.
- Loopback-only pools (e.g. `127.0.0.x` in functional tests) run plaintext
  without any flags.
- For non-loopback pools, the `--dev` CLI flag is the **only** way to
  disable the check. Deliberately CLI-only — a config-file knob could be
  committed by accident; a flag has to be passed every time the process
  starts.

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

### 7.5 `/healthz` does not use TLS

The healthz listener is **plain HTTP** — `CertReloader` has nothing to
do with it. See §9.2 for the reasoning. Mentioned here because earlier
revisions of this SPEC and the Go version did wire the cert reloader to
healthz; new readers who reach for that pattern should know we
deliberately don't.

---

## 8. Configuration

### 8.1 File layout (`/etc/pg_agent/config.toml`)

```toml
agent_port  = 9701                                # mTLS peer port
unix_socket = "/run/pg_agentd/pg_agentd.sock"     # local socket
# listen = "0.0.0.0"                              # bind addr (peer listener)

# node_id      = 0                                # explicit local node id
# node_id_file = "/etc/pgpool2/pgpool_node_id"    # or via file
# state_dir    = "/var/lib/postgresql/pg_agent"   # agent-owned persistent state


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
# port               = 5432
# pg_install_prefix  = "/usr/lib/postgresql/17"     # contains bin/pg_basebackup
# data_dir           = "/var/lib/postgresql/17/main" # $PGDATA
# socket_dir         = "/var/run/postgresql"
# repl_user          = "repl"
# user_home          = "/var/lib/postgresql"         # postgres OS user's home
# archive_dir        = "/var/lib/postgresql/archive"
# service            = "postgresql@17-main.service"
#
# Three distinct paths above:
#   pg_install_prefix → where the binaries live (bin/pg_basebackup etc.)
#   data_dir          → $PGDATA (PostgreSQL's data files)
#   user_home         → the `postgres` OS user's home (where libpq finds
#                       .postgresql/, .pcppass, .pgpass)

[postgres.replication]
# sslmode = "verify-full"   # default; pg-agent always emits sslmode= in
                            # conninfo. Cert paths come from libpq's
                            # defaults under ~postgres/.postgresql/.

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
state_dir                    = <postgres.user_home>/pg_agent  (after pg defaults)

postgres.port                = 5432
postgres.pg_install_prefix   = /usr/lib/postgresql/17        # contains bin/
postgres.data_dir            = /var/lib/postgresql/17/main   # $PGDATA
postgres.socket_dir          = /var/run/postgresql
postgres.repl_user           = repl
postgres.user_home           = /var/lib/postgresql           # postgres OS user
postgres.archive_dir         = /var/lib/postgresql/archive
postgres.service             = postgresql@17-main.service
postgres.replication.sslmode = verify-full

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
- `[postgres.replication]` is all-or-nothing; sslmode and cert paths
  validated as in §5.10.

### 8.4 Local-node id resolution

In priority order, first hit wins:

1. `node_id` field at the root of `config.toml`.
2. `node_id_file` field — file containing the integer.
3. `/etc/pgpool2/pgpool_node_id` if present — pgpool's own node-id file.
   Sharing this file between pg_agent and pgpool means one Ansible step
   writes one file both tools read; the two can never drift.
4. Hostname fallback: `os::hostname()` matched against `[[pool]].hostname`.

Sources 1 and 2 are errors if they point at an id that isn't in the pool.
Source 3 is "missing → fall through, present-but-broken → error" — an
absent file means pgpool isn't deployed (or `pg_agent` is running
standalone), but a malformed one is a configuration mistake. Source 4 is
best-effort. If none match, the daemon fails startup with a "local node
not found" error.

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

Separate **plain-HTTP** listener on port 9702. Status code is the
contract; JSON body is informational only.

### 9.1 What `/healthz` answers

> *"Can this pgpool field queries right now?"*

That is the entire contract. There is **one path** (`/healthz`); there is
**no `/healthz/primary` or `/healthz/replica`** — pgpool is the
role-aware routing layer in this architecture (see §1.1), so HAProxy's
job is "pick any healthy pgpool" rather than "pick the primary". The
JSON body still carries role + lag + sub-probe state for operators, but
it is not part of the status-code contract.

| Status | Meaning |
|--------|---------|
| `200`  | Snapshot is fresh (`age < 30 s`) AND **at least one backend has `status == "up"`** (or `"waiting"`, which is also routable per pgpool docs). The cluster can field queries through this pgpool. |
| `503`  | Snapshot is stale (probe loop wedged → process is wedged) OR the last `pcp_node_info -a` probe failed (pgpool unreachable) OR every backend is `down`. |
| `405`  | Method other than GET / HEAD. |

The "at least one backend up" gate matters because a pgpool with PCP
answering but every backend `down` will accept connections from HAProxy
and then return `"no available backend"` errors to clients. The current
gate makes `/healthz` honest about that case.

The stale-gate stays: without it a wedged probe loop could keep
returning 200 forever based on its last successful probe. Fresh AND
at-least-one-up is the joint condition.

### 9.2 Why plain HTTP, not HTTPS

The body contains operational state (role, lag, in-recovery, configured
backends) — nothing an attacker on the same network couldn't infer by
probing pgpool's wire protocol directly. **Error strings are filtered
from the body** (logged to tracing instead), so the body has no schema /
role / path leakage. Given that, TLS at the agent's listener buys
encryption of "lag is 0, role is primary" — worth essentially nothing —
while costing operators the friction of CA-trust gymnastics in every
monitoring tool (blackbox-exporter, k8s probes, HAProxy `httpchk`).

Operators who need TLS (zero-trust LAN, public exposure via tunnel) put
a reverse proxy in front. That's the standard pattern for making a
plain-HTTP service HTTPS; we don't bake the TLS path into the agent
itself.

Net: no `[tls]` interaction with the healthz listener at all.
`CertReloader` exists solely for the peer mTLS port (9701).

### 9.3 Mechanism

- A background **snapshotter** probes postgres and pgpool every **1 s**,
  each sub-probe with a **500 ms** timeout, concurrently.
- The pgpool sub-probe uses `pcp_node_info -a` (one subprocess
  invocation; returns one line per backend with status code, role,
  replication delay, etc. — see `pcp-node-info.html`). This replaces
  the `pcp_node_count` we used in earlier revisions: same subprocess
  cost, much richer signal (per-backend up/down, role, replication
  state), and lets `/healthz` answer "can this pgpool actually field
  queries" instead of just "is PCP answering".
- Latest snapshot lives in `ArcSwap<HealthSnapshot>` (initially `None`).
- An **initial synchronous probe runs before `sd_notify::ready()`** so
  the listener is fresh-and-true from the very first request after the
  daemon says READY. Without this, there's a ~1 s window where
  `/healthz` returns 503 because the snapshot hasn't landed yet —
  which would make HAProxy briefly mark the node down right after
  startup.
- Handler is hot-path-cheap: atomic load + small struct read + JSON marshal.
- **No DB or PCP calls happen on the request path.**
- HEAD is supported alongside GET; everything else → 405.
- Sub-probes that fail still stamp a fresh `timestamp` with
  `reachable: false` in the body — so the stale-gate distinguishes
  "probe loop wedged" (no fresh timestamp) from "pgpool just answered
  and said it's down" (fresh timestamp, `reachable: false`).

### 9.4 Snapshot body (informational JSON)

```json
{
  "ready": true,
  "snapshot_age_ms": 137,
  "role": "primary",
  "postgres":    { "reachable": true, "in_recovery": false },
  "pgpool": {
    "reachable": true,
    "backends": [
      { "id": 0, "hostname": "server1", "role": "primary", "status": "up",      "replication_state": "none"      },
      { "id": 1, "hostname": "server2", "role": "standby", "status": "up",      "replication_state": "streaming" },
      { "id": 2, "hostname": "server3", "role": "standby", "status": "down",    "replication_state": "none"      }
    ]
  },
  "replication": { "lag_bytes": 0, "wal_receiver_state": "" }
}
```

- `role` is one of `"primary"`, `"replica"`, `"unknown"`.
  Serialised from a typed `HealthRole` enum — no stringly-typed
  constants in code.
- `pgpool.backends` is a per-backend array, one element per
  `[[pool]]` entry, with the pgpool-side view of each backend. Fields
  are projected from `pcp_node_info -a`'s 11-field output (a curated
  subset — full structure available via `Pcp::node_info_all()` for
  consumers that want more, e.g. the eventual `/metrics` endpoint).
- `status` is `"up"` / `"waiting"` / `"down"` — the textual form of
  pgpool's status code. `"up"` and `"waiting"` both count toward the
  readiness gate; `"down"` does not.
- The previous `backends_configured` count is implicit in
  `backends.len()`.
- **No `postgres.error` / `pgpool.error` strings.** Sub-probe failures
  set `reachable: false`; the error message is logged via `tracing` for
  operator review. The body carries operational state, not failure
  diagnostics — which avoids leaking PostgreSQL error contents (schema
  names, role names, file paths) over an unauthenticated endpoint.

### 9.5 Snapshot struct shape

The struct mirrors the JSON body (nested, not flat):

```rust
struct HealthSnapshot {
    timestamp: DateTime<Utc>,
    role: HealthRole,
    postgres: PostgresProbe,        // reachable, in_recovery
    pgpool: PgpoolProbe,            // reachable, backends: Vec<BackendStatus>
    replication: ReplicationProbe,  // lag_bytes, wal_receiver_state
}

struct PgpoolProbe {
    reachable: bool,
    /// Per-backend snapshot from `pcp_node_info -a`. Empty when
    /// `reachable == false`.
    backends: Vec<BackendStatus>,
}

struct BackendStatus {
    id: i32,
    hostname: String,
    role: String,              // "primary" | "standby" | "main" | "replica" | "unknown"
    status: String,            // "up" | "waiting" | "down"
    replication_state: String, // "streaming" | "catchup" | "none" | ""
}
```

The full 11-field `NodeInfo` returned by `Pcp::node_info_all()` is
available to other consumers (preflight, the future `/metrics`
endpoint, `pg_agentctl cluster status`); the snapshot body carries
only the projected subset above. Avoids the `postgres_*` / `pgpool_*`
prefix soup the Go version carried; lets `#[serde(rename_all =
"snake_case")]` handle the wire shape automatically.

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
<state_dir>/
├── node_id                          # optional — see §8.4
├── maintenance/
│   └── <intent-id>.json             # one per intent, atomic temp+rename
└── replay/
    └── <op>_<sha256>.json           # idempotency markers (see §5.12)

$PGDATA/                             # no agent files — left to PostgreSQL
```

`<state_dir>` defaults to `<postgres.user_home>/pg_agent` and is created with
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
<pg_install_prefix>/bin/pg_basebackup
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
<pg_install_prefix>/bin/pg_rewind
  --target-pgdata <data_dir>
  --source-server <conninfo with dbname=postgres>
  --no-password
  --progress
```

Pre + post: `rm -rf $PGDATA/pg_replslot/*` (notes §3).

### 11.3 `pcp_*` PCP impl

```
pcp_attach_node -h localhost -p <pcp_port> -U <pcp_user> -n <id> -w
pcp_node_info   -h localhost -p <pcp_port> -U <pcp_user> -w -a
pcp_node_count  -h localhost -p <pcp_port> -U <pcp_user> -w
```

`-w` = no password prompt; auth is via `~postgres/.pcppass` (mode `0600`,
format `localhost:<port>:<user>:<password>`). The agent never reads or
handles the PCP password directly.

`pcp_node_info -a` dumps all backends in one subprocess invocation — one
line per backend, 11 space-separated fields per `pcp-node-info.html`
(hostname, port, status code, weight, status name, actual status,
role, actual role, replication delay, replication state, sync state)
followed by a `last_status_change` timestamp the agent currently
discards. This is what `/healthz` consumes (see §9) and what
`pg_agentctl cluster status` will use.

`pcp_node_count` returns the count of backends defined in `pgpool.conf`
(not the count currently up — per upstream docs). Kept in the `Pcp`
trait for preflight and operator use; not on the `/healthz` hot path.

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
4. Create `<state_dir>` and `<state_dir>/maintenance` (mode `0700`).
5. Compose runtime: `Topology`, `ServeSettings`, `PostgresRuntime`.
6. Build `CertReloader` if TLS configured; fail fast if not and remote
   peers are present and `--dev` is not set.
7. Spawn a SIGHUP handler task that calls `CertReloader::reload()`.
8. Open `LocalDb` pool to local PostgreSQL.
9. Build `PeerTransport` (CA pool, SAN allowlist), then `PeerPool`.
10. Build `StandbyOps`, `PcpCli`, `Systemd`, `ReplayMarkerStore`, `WalStore`,
    `MaintenanceStore`.
11. Construct `Agent` with all deps.
12. Repair `$PGDATA` hook symlinks (fail fast on conflicts).
13. `Agent::serve(ctx)`:
    a. Bind Unix socket synchronously (chmod 0600, remove stale).
    b. Bind TCP peer addr synchronously.
    c. Bind `/healthz` TCP listener synchronously (plain HTTP, port 9702).
    d. Run **one synchronous probe** to seed the healthz snapshot so
       the listener is fresh-and-true from the very first request.
    e. Spawn the `LocalServer`, `PeerServer`, `MaintenanceWorker`, and
       healthz serve loops on their respective bound listeners (the
       background snapshot loop also starts here, ticking at 1 s).
    f. `sd_notify::ready()` — only after every listener is bound AND
       the initial snapshot is in place. See SPEC §9.3 and the
       `sdnotify` module docs for the race this ordering prevents.
    g. Wait for SIGINT/SIGTERM. On shutdown: `sd_notify::stopping()`,
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
| `maintenance list [--status pending|done|abandoned]` | Tabular dump of `ListMaintenance`. Surfaces `Skipped` files to stderr. |
| `maintenance show <id>`                            | `GetMaintenance(id)`; pretty-print fields and JSON payload. |
| `maintenance retry <id>`                           | `RetryMaintenance(id)`. Refuses non-pending intents. |
| `cluster init [--only-node <id>] [--config <path>]` | `ClusterInit({only_node_id})`. Long deadline — overridable via `PG_AGENTCTL_TIMEOUT`. |
| `cluster status [--config <path>]`                 | Fan-out `GetStatus` to every pool member (local via Unix socket, peers via mTLS) and render a topology table: id, hostname, role (primary/standby), PG state, pgpool state, lag bytes, replication state, last-seen. Also serves as the mesh-level mTLS reachability check that preflight doesn't cover. Exit 0 iff every node responded successfully. |
| `help` / `version`                                 | as usual |

For RPCs that dial peers (`gen-pgpool`, `cluster init`, `cluster status`)
the CLI builds its own `PeerPool` from config (it does not go through
the local daemon). Maintenance + the local-node query in `gen-pgpool` /
`cluster status` go through the Unix socket.

### 13.1 Ansible integration

Cluster deployment is expected to be driven by Ansible. The agent's job is
to **give Ansible good seams where they help and stay out of the way
otherwise**. Concretely:

**What Ansible owns** (the agent never does these — duplicating them would
fight the deployment tooling):

- Installing the `.deb` / binaries themselves.
- Writing `/etc/pg_agent/config.toml`, `/etc/default/pg_agentd`,
  `/etc/polkit-1/rules.d/50-pg-agent.rules`, TLS cert material.
- Enabling / starting `pg_agentd.service` and `pgpool2.service`. The
  package explicitly does **not** auto-enable (SPEC §10.4).
- Writing `pgpool.conf`, `postgresql.conf`, `pg_hba.conf`, `pool_passwd`,
  `.pgpass`, `.pcppass`, the `pgpool_node_id` file.
- `CREATE EXTENSION pgpool_recovery` and replication-role bootstrap (or
  delegate the role part to `pg_agentctl cluster init` — operator's call).

**What the agent exposes that Ansible plays well with:**

- **Stable exit codes** across every `pg_agentctl` subcommand:
  - `0` — success / clean / no action needed
  - `1` — work to do, hard failure, or any check returned `ERR`
  - `2` — usage / argument error
  This lets Ansible `register:` + `failed_when:` cleanly.

- **`--json` everywhere** that produces output an operator would parse.
  Stable schemas:

  ```
  pg_agentctl --json print-hooks       # for the `template` module
  pg_agentctl --json maintenance list
  pg_agentctl --json cluster status
  pg_agentd   validate-env --json      # localhost env validation (§14)
  ```

- **No interactive prompts, ever.** Destructive commands take `--force`
  rather than reading from stdin.

- **Idempotent by default.** Re-running `cluster init`, `maintenance
  retry`, or a future `cluster pause` against an already-correct state is
  a no-op, not an error. This makes `changed_when:` honest.

- **Atomic file writes** for everything operator-facing
  (`gen-pgpool --write`, the daemon's symlink repair). Partial writes
  never appear under target paths.

- **SIGHUP reload, not restart**, for cert rotation. The systemd unit's
  `ExecReload=/bin/kill -HUP $MAINPID` makes the Ansible
  `ansible.builtin.service: state=reloaded` idiom Just Work.

- **`pg_agentd validate-env` is the universal post-deploy assertion.**
  Running it as a task after every config change gives Ansible a
  single-call "is this node ready?" probe. The JSON output is structured
  per-check so playbooks can conditionally remediate (e.g. install the
  polkit rule iff that check is `ERR`). The systemd unit also wires it
  as `ExecStartPre=`, so the daemon refuses to start with a broken
  environment — Ansible's explicit task is the early-warning gate, the
  unit's `ExecStartPre=` is the safety net.

**Recommended playbook shape:**

```yaml
- name: pg-agent validate-env
  ansible.builtin.command: pg_agentd validate-env --json
  register: validate_env
  changed_when: false
  failed_when: (validate_env.stdout | from_json).has_errors

- name: render pgpool include from live cluster
  ansible.builtin.command: pg_agentctl gen-pgpool --write /etc/pgpool2/pg_agent.conf
  register: gen
  changed_when: "'wrote' in gen.stderr"
  notify: reload pgpool
```

**Anti-patterns the agent should never adopt** (because they make
Ansible's life harder):

- Reading state from environment variables that aren't documented.
- Writing to paths outside `<state_dir>` / `$PGDATA` / `/run/pg_agentd/`
  unless explicitly told to (e.g. `gen-pgpool --write <path>`).
- Bundling its own service-management of pgpool / postgres beyond the
  D-Bus calls already specified.
- Auto-creating directories Ansible owns (`/etc/pg_agent/`,
  `/etc/pgpool2/`).

**HAProxy configuration gotchas** (deployment-time, not agent-time —
included here so the playbook author has the matching context):

- **Do NOT enable PROXY protocol** (`send-proxy`, `send-proxy-v2`) on the
  HAProxy backend that fronts pgpool. Pgpool-II does not understand
  PROXY protocol headers (verified against pgpool 4.6 docs — no
  `proxy_protocol` config, no PROXY header parsing); PostgreSQL itself
  also doesn't support it natively as of PG 17. Enabling PROXY on the
  HAProxy side would prepend bytes pgpool reads as garbage during the
  startup phase, dropping every client connection.
- Client identity is consequently lost at the first proxy hop. Postgres
  and pgpool both see the previous hop's IP. The intended pattern is
  `pg_hba.conf` rules using `samenet` for the `pgpool` / `postgres` /
  `repl` users (which the preflight check verifies in §14); real
  client IP visibility for audit / debugging lives in HAProxy's access
  log, correlated with pgpool / postgres logs via timestamps.
- HAProxy's backend health-check on pgpool should be `option pgsql-check
  user pgpool` (a real wire-protocol probe). Don't use the agent's
  `/healthz` for routing — pgpool is the role-aware routing layer in
  this architecture (see §1.1 and §9.1).

---

## 14. Environment validation (`pg_agentd validate-env`)

Validates the **localhost** runtime environment has the prereqs `pg_agentd`
assumes. Each check is independent and idempotent. Each emits a `Check {
name, status: OK|WARN|ERR, detail }`. Run as the `postgres` user so the
mode-`0600` files are readable.

Lives on the daemon binary (`pg_agentd validate-env`), not on
`pg_agentctl`, because the daemon is the authority on what counts as a
valid environment — same loader, same projections, no chance of drift
between the validator and the consumer. Modeled after `nginx -t` /
`sshd -t` / `caddy validate`: one binary owns the definition.

Scoped to localhost on purpose — it runs in Ansible's per-host loop and
must pass on each node independently of the others' readiness, and is
wired into the systemd unit as `ExecStartPre=` so the daemon refuses
to start with a broken environment. The network-level twin (peer mTLS
reachability) lives in `pg_agentctl cluster status`, which runs once
after every daemon is up. The two answer different questions:
`validate-env` asks "is this node set up correctly to participate in
a cluster?"; `cluster status` asks "are the nodes that exist actually
reaching each other?".

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
- **Recovery tools** — `<pg_install_prefix>/bin/pg_basebackup` and
  `<pg_install_prefix>/bin/pg_rewind` exist and are executable.

DB-backed (skipped with a WARN if `--skip-db` or DB unreachable):

- **Settings**: `wal_log_hints = on`, `hot_standby = on`,
  `max_replication_slots ≥ pool size`, `max_wal_senders ≥ pool size`.
- **TLS / pg_hba**: if `[postgres.replication]` is configured, verify
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
validate-env: 1 error(s), 1 warning(s) — FAIL
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
  `StandbyOps` etc. swapped for HTTP-observable fakes that expose
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
6. **`pcp_attach_node` is `FollowPrimary`-only.** Not RecoveryFirstStage.
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
11. **mTLS required for non-loopback peers.** `--dev` CLI flag is the
    only escape hatch. No config-file knob — the choice has to be re-made
    at every startup, so it can't be silently committed.
12. **`pg_agentd.service` starts before `pgpool2.service`.** Otherwise the
    first failover hook fires against a missing Unix socket.
13. **The Unix socket is `0600 postgres:postgres`.** No auth beyond that.
14. **The hook client carries no config and resolves no nodes.** Adding any
    state to `pg_agentc` is a design regression.
15. **Cert reload is hot, but only `[tls]` is.** Other config changes need a
    full restart.
16. **Hook symlinks repaired after every `pg_basebackup`.** Upstream
    `pg_basebackup` silently skips non-tablespace symlinks (per the PG
    docs: *"Other symbolic links and special device files are skipped"*).
    The hook symlinks under `$PGDATA` (`recovery_1st_stage`,
    `pgpool_remote_start`) are therefore lost during basebackup and must
    be re-created before a subsequent promotion can fire its hooks. The
    repair runs at the tail of `StandbyOps::basebackup` (same handler
    that did the wipe — no chance for an orchestrator to forget). Rewind
    modifies `$PGDATA` in place and does NOT need the repair.

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
