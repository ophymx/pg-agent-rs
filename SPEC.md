# pg-agent-rs — SPEC

The daemon that replaces pgpool-II's shell-script hooks (`failover.sh`,
`follow_primary.sh`, `recovery_1st_stage`, `pgpool_remote_start`,
`escalation.sh`) and the SSH-based remote execution they depend on — and
that owns the promotion decision pgpool cannot safely make.

This describes what the system does and which choices are load-bearing.
Where a contract lives in a file that is itself authoritative — `proto/`
for the wire, `--help` for the CLI surface, the `DEFAULT_*` consts for
configuration defaults — this document says what the contract *means*
rather than restating it.

---

## 1. System shape

Three binaries, one daemon and two CLIs, deployed on every PostgreSQL backend
node (which also runs pgpool-II and HAProxy):

| Binary        | Role | Loads config? | Talks to peers? |
|---------------|------|---------------|-----------------|
| `pg_agentd`   | Daemon (`pg_agentd serve`, the default). Owns local PostgreSQL operations + cluster coordination. Serves a Unix-socket RPC for local callers, an mTLS TCP RPC for peer agents and the consensus plane, and a plain-HTTP `/healthz` listener (see §9.2 for why no TLS). Also `pg_agentd validate-env` (see §14). | yes | yes |
| `pg_agentc`   | One-shot hook client. Marshals pgpool's positional argv into a single gRPC call on the local Unix socket, then exits. Also `pg_agentc status`. **No config. No node resolution. No PostgreSQL logic.** | no | no |
| `pg_agentctl` | Operator CLI (§13). May dial peer agents, but only ever through the local daemon. | yes | yes |

The hook client must stay tiny — if it can reach the socket, it works. Every
new piece of operator functionality goes in `pg_agentctl`, not `pg_agentc`.

The shared positional-argument schemas (one per pgpool/PostgreSQL hook) live
in a single crate so the dispatcher (`pg_agentc`) and the validator/printer
(`pg_agentctl`) cannot drift.

### 1.1 Cluster layout

```
clients → HAProxy (TCP LB, 5432) → Pgpool-II (9999) → PostgreSQL backends
                                                              ↕
                                         agent mesh: peer RPC + Raft (9701)
```

| Node    | Pgpool | PCP  | Agent gRPC + Raft | Agent /healthz |
|---------|--------|------|-------------------|----------------|
| serverN | 9999   | 9898 | 9701 mTLS         | 9702 HTTP      |

The agents *are* the quorum — there is no separate consensus box and no
external DCS (docs/promotion-authority.md §5).

pgpool's watchdog is **off** (§5.15): it was running quorum-only with no
VIP, and what it provided — single execution of the hooks, a quorum gate
on failover, and backend-status sync between instances — is either
subsumed by the lease or paid for explicitly (the attach fan-out).

VIP management is intentionally absent; HAProxy replaces it. The
`Escalation`/`DeEscalation` hooks are therefore no-ops that never fire.

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

## 2. Where each contract actually lives

This document drifted once by restating things that live elsewhere, so:
when the two disagree, the artifact wins, and the fix is to delete the
restatement rather than sync it.

| Contract | Authority | What this doc adds |
|---|---|---|
| Wire format | `proto/*.proto` | why each service exists, and what must not cross between them (§3) |
| CLI surface | `--help` | what each command is for, and which ones are destructive (§13, §15) |
| Config defaults | the `DEFAULT_*` consts and `config.toml.sample` | which fields are load-bearing, and why (§8) |
| Hook argv layout | `pg-agent-hookspec` | the token semantics pgpool documents badly (§6) |
| Promotion, quorum commit | `docs/` | the behavioral contract only (§5.15) |
| Failure discoveries | `testing/FINDINGS.md` | cited by number wherever a rule exists because of one |

Section numbers are referenced from code comments, so they stay stable
when a section is removed.

---

## 3. Protocol surface

Three gRPC services, defined in `proto/`. **The `.proto` files are the
contract** — message fields, comments and all. This section says what
each service is *for* and which properties must not drift; it does not
restate the schema.

Wire compatibility is required across pg-agent-rs versions so a rolling
agent upgrade works (the acceptance suite measures the restart window
against `leader_ttl` on every run).

### 3.1 `PgAgentLocal` — Unix socket, no auth

`proto/pgagent_local.proto`. Filesystem permissions on the socket are the
access control: `0600 postgres:postgres` under the systemd-managed
`RuntimeDirectory=pg_agentd`. Every legitimate caller already runs as
`postgres`.

Three families of caller:

- **pgpool/PostgreSQL hooks**, via `pg_agentc`: `Failover`,
  `FollowPrimary`, `RecoveryFirstStage`, `RemoteStart`, `Escalation`,
  `RestoreWal`. Workflows in §5.
- **Operator commands**, via `pg_agentctl`: `ClusterInit`,
  `ClusterStatus`, `ClusterRecover`, `ClusterHandoff`,
  `GetPgpoolBackends`, the `*Maintenance` trio, the `*InflightOp` quartet,
  `AllowAsync`, `SetPause`. Surface in §13.
- **Status**: `GetStatus`, `GetNodeConfig`, delegated to `NodeInfo` (§4).

### 3.2 `PgAgentPeer` — mTLS TCP, port 9701

`proto/pgagent_peer.proto`. Clients are other agents. Mutual TLS, cert
from `[tls]`, SAN must be in the pool allowlist (§7).

**This service carries work, never authority.** `Start`, `Stop`,
`StartPgpool`, `AttachNode`, `Promote`, `CreateSlot`, `DropSlot`,
`ConfigureStandby`, `Basebackup`, `Rewind`, `FetchWal`, plus the two
status RPCs. Each does one thing locally on the receiver at the
requester's instruction. Handler behaviour in §5.8.

The distinction is load-bearing: the original defect was that a work RPC
(`Failover`) *was* the role decision. Role assignment lives in §3.3 and
nowhere else.

### 3.3 `PgAgentRaft` — mTLS TCP, port 9701, separate connection

`proto/pgagent_raft.proto`. The consensus plane: openraft's
`AppendEntries` / `Vote` / `InstallSnapshot`, plus `Propose` and
`ReadState` for forwarding a lease write or a linearizable read to
whichever node currently leads Raft.

Three properties that must hold (docs/promotion-authority.md §5):

- **Same listener, same certs, same SAN allowlist** as `PgAgentPeer`, so
  the mTLS gate guarding the peer plane's mutating RPCs guards promotion
  authority unchanged. An unauthenticated consensus port is an
  unauthenticated promotion authority; `validate-env` refuses to start
  without mTLS for this reason.
- **A separate connection** from the work plane. `AppendEntries`
  heartbeats are small, frequent and latency-critical; `Basebackup`
  streams gigabytes. Sharing an HTTP/2 connection lets a saturated
  basebackup starve heartbeats at the TCP layer and trigger a spurious
  election during a recovery.
- **Forwarding is one hop, never two.** The leader-side handlers refuse
  rather than re-forward; a chain makes latency unbounded in exactly the
  churny conditions where the `retry_timeout` budget is tightest.

Frames are opaque serialized openraft types rather than protobuf-mirrored
fields, so `grpcurl` sees a blob and debugging goes through agent logs
and openraft metrics. This is a deliberate trade — mirroring would
re-declare a large slice of openraft's internals every upgrade to buy
interop that cannot arise, since both ends are the same binary at the
same version.

### 3.4 Validation

Validated by hand in the handlers, **before any handler logic runs**,
returning `tonic::Status::invalid_argument(...)`:

- non-empty slot names, hostnames, replication user, intent ids, dest
  paths;
- PostgreSQL ports > 0;
- `wal_file` matching `^([0-9A-F]{24}|[0-9A-F]{8}\.history)$`.

The value-shape regexes every input is matched against are in §5.11.

### 3.5 Status codes used

| gRPC code              | When |
|------------------------|------|
| `InvalidArgument`      | validation failure; `dest_path` outside `PGDATA`; `wal_file` rejected by filename whitelist |
| `FailedPrecondition`   | `Basebackup` called while PostgreSQL is running |
| `NotFound`             | `FetchWal` for a WAL segment that is absent in the peer's archive; `GetMaintenance` / `RetryMaintenance` / `GetInflightOp` for a missing id |
| `Internal`             | everything else that fails inside a handler |

Hook RPCs (`PgAgentLocal.Failover`, etc.) usually return `OpResult { ok=false, message=... }` rather than a gRPC error when the failure is operationally normal (`FollowPrimary` skipping a stopped target, `Failover` with no candidates, `RestoreWal` not finding a segment). Reserve gRPC errors for unrecoverable / system-level failures.

---

## 4. Dependency-injection seams

Every external collaborator sits behind a trait so no handler calls
PostgreSQL, systemd, PCP, a `pg_*` binary, or a peer unmediated. Unit
tests swap in-process fakes; the composition root
(`pg-agentd/src/main.rs`) is the only place concrete types appear.

Method lists below are indicative, not exhaustive — the trait definitions
are authoritative. What matters here is which collaborator each seam
owns, because that boundary is a design decision rather than an
implementation detail.

| Trait              | Real impl                          | Surface |
|--------------------|------------------------------------|---------|
| `LocalDb`          | tokio-postgres pool to local Unix socket | `promote()`, `checkpoint()`, `create_slot(name)`, `drop_slot(name)`, `is_in_recovery()`, `replication_lag()` → `ReplicationLag { bytes, state }`, `setting(name)`, `extension_exists(name)`, `role_exists(name)`, `create_replication_role(name)` |
| `PeerRegistry`     | mTLS gRPC pool (`PeerPool`)        | `client(node) -> PeerClient`, `close()` |
| `StandbyOps`       | subprocess + filesystem            | `basebackup(opts, progress_cb)`, `rewind(opts, progress_cb)`, `write_recovery_conf(opts)` |
| `Pcp`              | `pcp_attach_node` / `pcp_detach_node` / `pcp_node_info` subprocess | `attach_node(id)`, `detach_node(id)`, `node_info_all() -> Vec<NodeInfo>`, `node_count() -> int` (preflight only — `/healthz` uses `node_info_all`) |
| `Systemd`          | zbus to `systemd1`                 | start / stop / status / reload-or-restart for the configured PostgreSQL and pgpool units, each waiting on `JobRemoved` for real job completion rather than trusting the call to return |
| `ReplayMarkerStore` | JSON files under `<state_dir>/replay/` | `has(op, key)`, `mark_done(op, key)`, `sweep(now)` |
| `WalStore`         | filesystem (archive dir + PGDATA)  | `open_archive(wal_file) -> AsyncRead`, `write_restore(dest_path, src)` |
| `MaintenanceStore` | one JSON file per intent under `<state_dir>/maintenance/` | `append(op, payload)`, `list_pending()`, `list(statuses…)`, `get(id)`, `mark_attempt(id, err, next_retry_at)`, `mark_done(id)`, `mark_abandoned(id, err)`, `reschedule(id, when)` |
| `InflightOpStore` | one JSON file per op under `<state_dir>/inflight_ops/` | `begin(payload, phase, exclusive)`, `update_phase`, `complete`, `abandon`, `find(op, key)`, `get(id)`, `list(statuses…)`, `sweep(now)`. Also the **slot-ownership authority**: `owner_of_node` / `owner_of_slot` gate every destructive slot path (§5.1 step 3, `PgAgentPeer.DropSlot`, the maintenance worker) |
| `ConsensusStore`   | `RaftConsensusStore` over the openraft handle; `InMemoryConsensusStore` with fault injection for tests | `read_state() -> ClusterState` (linearizable; **`Err` = unknown, never vacant**), `try_takeover(candidate, expected)` (lease CAS, terms are fencing tokens), `release(holder, term)`, `set_paused(…)`, `set_switchover(…)` — see docs/promotion-authority.md §5 |

A `NodeInfo` trait (`get_status`, `get_node_config`) is satisfied by
`Agent` itself; `LocalServer` and `PeerServer` both delegate `GetStatus` /
`GetNodeConfig` to it so there is one canonical implementation.

The `ConsensusStore` seam earns its keep twice: it let the HA loop be
written and fault-tested against a deterministic in-memory store before
openraft existed in the tree, and it keeps partition cases as ordinary
unit tests rather than a lab exercise.

### 4.1 The local database connection

The local pool connects as `postgres` over the Unix socket (peer auth):

```
host=<socket_dir> port=<pg_port> user=postgres dbname=postgres
```

Two conventions hold across every `LocalDb` method, and callers depend on
both:

- **SQLSTATE 42710 (`duplicate_object`) is success.** `create_slot` and
  `create_replication_role` are idempotent so a re-run of any
  orchestration is safe. (The cost of that choice is in TODO.md: a
  pre-existing slot keeps its stale `restart_lsn`.)
- **Identifiers reaching SQL are regex-validated first** (§5.11), never
  escaped-and-hoped.

The queries themselves live in `pgman::localdb`, with the role-awareness
that matters — a standby's flush position and timeline come from
different sources than a primary's, and getting that wrong has twice
produced a silent selection defect (findings 4 and 24).

---

## 5. Workflows

All coordination logic runs inside `pg_agentd`. The hook client is purely a
forwarder. Each workflow maps 1:1 to a `PgAgentLocal` RPC; the agent then
dispatches per-step calls to peer agents over `PgAgentPeer`.

### 5.1 `Failover(detached, new_main, old_primary, old_main)`

> **The primary-down branch is advisory, period.** pgpool's failure
> report is a hint, not an order — the handler logs the announcement
> and returns `ok=true` ("advisory") without touching cluster state.
> Promotion is the HA loop's decision (§5.15): the lease holder is
> watched for `leader_ttl`, a quorum-serialized CAS picks the
> successor, and the winner's executor promotes. Only the standby-down
> slot hygiene below is real work — slot lifecycle is mechanism, not
> authority. (The pgpool-led promote path this hook once carried was
> deleted wholesale; see
> [docs/promotion-authority.md](docs/promotion-authority.md) §10
> step 7.)

1. If `new_main.id == -1` → no candidates available. Log critical error,
   return `OpResult { ok=false, message="no standby candidates available" }`.
   Do **not** error the RPC.
2. **Primary down** (`detached.id == old_primary.id`): answer the
   advisory (log + `ok=true`) before the replay-marker check — the
   advisory is stateless and re-firing it is free. Nothing else runs.
3. Resolve `detached`, `new_main`, `old_primary` from topology
   (hostname-authoritative — see §8.2).
4. **Standby down** (the only path that reaches here):
   - **Cross-op consult:** if an `inflight_ops` entry owns `detached`
     (recovery, follow_primary, or handoff — matched via
     `InflightPayload::target_node_id`), retain the slot and return
     `ok=true` naming the op. Such an orchestration stops its target's
     PostgreSQL deliberately, which is what fired this hook; dropping
     the slot would destroy the one it just created and leave a standby
     that can never stream. Ownership covers `InProgress` ops **and**
     completed ops whose rebuilt node has not yet been observed alive:
     pgpool's hook is a delayed reaction, so it routinely arrives after
     the orchestration finished but before the rebuilt standby reaches
     `streaming` — a window in which the precondition check below also
     reads the node as legitimately down. A completed op's ownership
     ends at an **event**, not a clock: the consult itself observes the
     slot, and the first time it finds the slot active (the rebuilt
     node came up) it records a durable *discharge* on the op — from
     then on a destructive request is the ordinary standby-down case
     and proceeds immediately. `CROSS_OP_GRACE` (120s) survives only as
     the backstop for a node that never comes up, so its slot does not
     stay protected forever.
   - **Precondition** (defense in depth — see
     [docs/promotion-authority.md](docs/promotion-authority.md) §3): if
     `detached` is reachable, running, in recovery, and
     `replication_state == "streaming"`, the failure report is provably
     wrong — refuse with `ok=false` rather than break healthy
     replication. Unreachable/unverifiable → log and proceed.
   - Drop the slot locally with a best-effort cleanup context (30s timeout,
     decoupled from the hook ctx).
   - On error: enqueue `drop_slot_cleanup` maintenance intent; still return
     `ok=true` with a descriptive message.

> The standby-down branch carries a replay marker (key
> `detached={id},new_main={id},old_primary={id}`) so a re-fired hook
> skips as "already processed". The primary-down advisory answers
> *before* the marker check and never writes one — a stale marker from
> the same key shape must not mask the advisory. See §5.12.

The slot name is always `node{id}` (e.g. `node2`).

### 5.2 `FollowPrimary(detached, new_primary, …)`

> **Not wired into pgpool**: `follow_primary_command` is
> empty in the canonical contract (§5.15 / hook-contract §2 — a
> non-empty value makes pgpool degenerate healthy standbys), and the
> executor's follow path replaces this flow for lease-driven role
> changes. The RPC and handler remain for `pcp_promote_node`-style
> direct invocation and for the planned "follow_primary unification"
> (TODO.md), which will converge it with the executor's driver.

When invoked, the behavior is: per-down-non-primary in a forked child
(concurrent invocations targeting different nodes are normal).

1. Replay key: `detached={id},new_primary={id}`. If already done → skip.
2. Resolve both nodes.
3. `peers[detached].GetStatus()`. If `!is_running` → return ok with "skipping",
   mark replay done. The node is presumed down for a deliberate reason.
4. `peers[detached].Stop()`.
5. Local `Checkpoint` — makes the slot consistent with the WAL stream a
   basebackup would start from.
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

Journaled in `inflight_ops` as a `recovery` op (key
`primary={id},standby={id}`) across the ladder
`started → slot_created → data_copied → standby_configured`.

1. Dedup: if a `recovery` op with the same key completed within
   `RECOVERY_DEDUP_WINDOW` (24h) → skip with a distinct message.
   `bypass_replay_marker=true` on the request overrides this; a
   *concurrent* duplicate is rejected structurally by `inflight.begin`.
2. Resolve primary as **local** node (`resolve_local_node` — refuse if not
   local) and standby normally.
3. `inflight.begin(Recovery{…}, "started")`. **This is what makes the
   orchestration visible to `Failover`** — see the cross-op consult in
   §5.1 step 3.
4. Local `Checkpoint`.
5. Local `CreateSlot(standby.slot_name)` → phase `slot_created`.
6. `peers[standby].Basebackup(primary, slot=standby.slot_name)` — drain
   progress until `phase == "done"` → phase `data_copied`.
7. `peers[standby].ConfigureStandby(primary)` — must follow basebackup
   because basebackup wipes `$PGDATA` first → phase `standby_configured`.
8. **No** `pcp_attach_node` — pgpool drives re-attachment after stage 2.
9. `inflight.complete(...)`.

Cleanup rule: drop the slot on any failure after step 5, and mark the
op `Abandoned` so a retry isn't rejected as a duplicate of a run that
died. No resume driver yet: `ResumeInflightOp` refuses a `recovery` op
and directs the operator to `cluster recover`, which restarts from a
known state rather than re-entering a ladder whose `$PGDATA` may be
half-copied.

### 5.4 `RemoteStart(target)`

Invoked by `pgpool_recovery` on the primary.

1. `db.is_in_recovery()`. If true → refuse with `ok=false, message="local node is not the primary (in recovery)"`.
2. Resolve target. `peers[target].Start()`.
3. Discards pgpool's positional `$2` (the primary's PGDATA): every
   operational path comes from `config.toml`, never from hook argv
   (§8.6).

### 5.5 `Escalation()` / `DeEscalation()`

Both map to the same RPC (`Escalation`), which is a no-op — HAProxy
replaces VIP management.

**Vestigial.** `use_watchdog = off` means the `wd_*` hooks never fire and
`gen-pgpool` does not emit them, so nothing invokes this path in a
supported deployment. The RPC, the hookspec constants and the `pg_agentc`
dispatch arm survive only because removing a proto RPC is a wire-compat
decision rather than code hygiene (TODO.md).

### 5.6 `RestoreWal(wal_file, dest_path)`

Invoked by PostgreSQL on a standby via
`restore_command = 'pg_agentc restore-wal %f %p'`. `dest_path` is
**authoritative** — unlike every other hook field it comes from the local
PostgreSQL backend rather than from pgpool, so it is the backend telling
the agent where to drop the segment (§17 invariant 9).

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
       (`."<base>"-<8 hex>"`), `O_EXCL`, mode `0600`. Copy. **`fsync` before
       the rename** — closing does not sync, and renaming over still-dirty
       page cache lets a host crash leave a correctly-named segment full of
       zeros, which PostgreSQL reads as end-of-WAL and stops recovery on.
       Rename onto `dest_path`. Remove temp on any error. The parent
       directory is deliberately not synced: losing the rename just means
       `restore_command` asks for the segment again.
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
| `StartPgpool`       | `systemd.StartUnit($pgpool_service, "replace")`. Idempotent. Used by `cluster recover` to bring pgpool back on a freshly re-cloned target — and only *after* the target's PostgreSQL start succeeded, because a pgpool with a dead local backend health-checks it down and fires `failover_command` at the node just rebuilt. |
| `Promote`           | `SELECT pg_promote()` |
| `CreateSlot`        | `pg_create_physical_replication_slot(name)`, SQLSTATE 42710 ok |
| `AttachNode`        | Attach a backend on the RECEIVER's local pgpool (hook-contract §3: attach is per-instance — `cluster recover` fans this out to every member). Finding-16 semantics server-side: attach only a backend this map holds down; when the request's `primary_node_id` backend is down here too, attach it first (a standby attach into a primary-less map blocks in `find_primary_node_repeatedly`). pcp failure → `ok=false`, not a gRPC error — the caller's fan-out is best-effort per instance. |
| `DropSlot`          | **Ownership guard first:** if `inflight_ops::owner_of_slot_observing` reports an op owns this slot (`InProgress`, or `Done`, undischarged, within the `CROSS_OP_GRACE` backstop — an active slot discharges the op instead, §5.1 step 4), return `ok=true` with a "retained" message and do nothing. `ok=true` rather than an error is deliberate — the caller's cleanup is genuinely obsolete, and an error keeps a maintenance intent retrying against a slot now in legitimate use. Otherwise `pg_drop_replication_slot(name)`. The guard lives here because callers are plural and some are stale (a queued `drop_slot_cleanup` on another node retries with backoff), and only the slot's host knows whether it is spoken for. |
| `ConfigureStandby`  | validate (`primary_host` regex, port>0, repl_user regex, slot regex). Write `$PGDATA/myrecovery.conf` (template — see §5.10) and create empty `$PGDATA/standby.signal`. Both files mode `0640`. |
| `Basebackup`        | refuse if PostgreSQL is running (`FailedPrecondition`). Clear `$PGDATA` contents. Exec `<pg_install_prefix>/bin/pg_basebackup --pgdata <data> --dbname '<conninfo>' --wal-method=stream --checkpoint=fast --no-password [--slot <name>] [--progress]`. Scan stderr line-by-line (split on `\r` *or* `\n`), forward `done/total kB` lines as `OpProgress { phase="streaming", bytes_done=done*1024, bytes_total=total*1024 }`, log other lines, capture last ~4 KiB into the error tail if the subprocess exits non-zero. Final `OpProgress { phase="done" }`. |
| `Rewind`            | clear `$PGDATA/pg_replslot/*` before. Exec `<pg_install_prefix>/bin/pg_rewind --target-pgdata <data> --source-server '<conninfo with dbname=postgres>' --no-password --progress`. Same scanner. After success, clear `$PGDATA/pg_replslot/*` again (§17 invariant 5). Final `OpProgress { phase="done" }`. |
| `FetchWal`          | validate filename. Open `<archive_dir>/<wal_file>` (after `filepath.Localize`-equivalent rejection of `..`/absolute paths). Stream 1 MiB chunks. `NotFound` if absent. Chunks are read into a `BytesMut` and shipped as `bytes::Bytes` (`WalChunk.data` carries the prost `bytes` override) so neither side copies a chunk out of its transport buffer. The service negotiates **zstd** — a 16 MiB segment compresses well, and one closed early by `archive_timeout` is mostly zero padding; zstd rather than gzip because tonic hardcodes gzip to level 6, slow enough to bottleneck a LAN. Compression is per-service in tonic, so the small peer RPCs ride along. |
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

**Scope:** two handlers still carry replay markers: `FollowPrimary`
(which runs `pg_basebackup` conditionally — **wiping `$PGDATA` before
streaming**; re-running a completed flow would clobber a healthy
standby's data dir, and the marker makes the second invocation a fast
no-op) and `Failover`'s standby-down branch (§5.1's note: a re-fired
hook skips as "already processed"; the primary-down advisory answers
before the marker check and never writes one).

`RecoveryFirstStage` **moved off markers** to `inflight_ops` (§5.3): it
gets the same post-completion dedup, plus two things a binary marker
cannot provide — rejection of *concurrent* duplicates, and visibility of
an in-flight orchestration to `Failover`'s cross-op consult (§5.1 step
3), without which stopping a recovery target made pgpool fire a hook
that dropped the slot the recovery had just created.

`Failover`'s standby-down branch carries a marker (§5.1's note); the
primary-down advisory is stateless, answers before the marker check,
and never writes one.

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

> **Queued drops are stale instructions.** An intent records what looked
> true when it was queued and retries with exponential backoff, so it
> can execute minutes later — long enough for an orchestration to have
> re-created that slot and started streaming through it. The worker
> therefore runs the same `inflight_ops::owner_of_slot` guard as
> `PgAgentPeer.DropSlot` before executing, and drops the intent
> (`mark_done`) when an op owns the slot. The check is needed in *both*
> places: the worker calls `LocalDb::drop_slot` directly when the target
> is the local node, bypassing the RPC entirely.

**Scope today: failed `DropSlot` retry only.** The queue's only
production use is recovering from a peer (or local) `DropSlot` call that
failed *after* the primary success of a Failover / FollowPrimary /
RecoveryFirstStage — leaving an orphan replication slot that pins WAL on
the primary and eventually fills the disk. The hook RPC must still return
`ok=true` to pgpool (the cluster's authoritative side already succeeded);
the cleanup goes here.

The design accommodates additional intent types but adding one is a
**deliberate choice**, not a default. Plausible future candidates
(`pcp_attach_node` retry, `peer Start` retry, `StartPgpool` retry) stay
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

**An intent file with an unknown op is an error**, never quietly
retired. There is no installed base to migrate, so a payload the worker
cannot match is a bug in this build rather than a leftover from an older
one.

### 5.14 Best-effort cleanup contexts

When a hook RPC is being cancelled (deadline, peer reset), cleanup work
must still complete. Use a detached, time-bounded context:

`tokio::time::timeout(30s, …)` inside a `tokio::spawn` that does **not**
inherit the parent cancellation. 30 s is enough for one local `DropSlot`
and one maintenance append.

---

### 5.15 Lease-driven roles (agent-led failover)

Always active — every daemon joins consensus and runs the loop, or
fails to start. The full design, its invariants, and its history live
in [docs/promotion-authority.md](docs/promotion-authority.md); this
section is the behavioral contract.

**Decision layer** (`ha` module): every `loop_wait`, each node performs
a linearizable read of the replicated lease and emits one decision.
"Cannot read" is *unknown*, never vacant; a holder that cannot confirm
its lease within `retry_timeout` decides to demote. A dead holder is
watched for `leader_ttl` before any candidate proposes a CAS takeover.
Candidate selection is STRICT flush-max (docs/quorum-commit.md §4):
any reachable peer with more flushed WAL outranks, byte-for-byte, and
node id breaks exact ties only. There is no lag-tolerance knob: the
former `max_lag_on_failover_bytes` band was both an acknowledged-write
hole under quorum commit and finding 15's wedge cause, so it is not a
config field. Terms are fencing tokens, minted monotonically; the lease
is seeded by `ClusterInit` at bootstrap, and a candidacy that is still
receiving WAL **freezes first** — detaching from the deposed primary so
positions stop moving — because a moving stream has no stable order
(finding 23).

A candidate that cannot see the holder consults the cluster before
deposing it: every node reports how long ago it last observed each peer
*serving as primary* (`NodeStatus.peer_primary_seen_age_ms`), and a
candidate stands down while any reachable member has watched that holder
serve within `leader_ttl`. Self-clearing — a genuinely dead holder ages
out of every witness's map within one ttl.

**Execution layer** (`roleexec` module): every decision is handed to
the executor after logging.

- Winning a takeover → journaled promotion (`inflight_ops` `promote`
  op), `pg_promote(false)` + a poll bounded by `leader_ttl`.
- Holding as primary → **quorum-commit convergence**
  (docs/quorum-commit.md): `synchronous_standby_names = ANY 1
  (members minus self)` armed at the first-standby-attached event and
  repaired on membership drift — never auto-disarmed; the operator's
  `cluster allow-async --confirm` (journaled) is the only disarm, and
  the next attach re-arms over it. Acknowledging a commit thereby
  requires a standby that follows the lease: a deposed primary's
  commits hang unacknowledged the moment its standbys re-point,
  independent of fence latency. `/healthz` reports `sync_commit:
  armed|disarmed|blocked|n/a` (`blocked` = armed with no connected
  standby — commits hanging — a page).
- Holding as primary → **pgpool self-attach convergence** (finding 16):
  a probe — spawned off the tick, single-flight, 10 s cadence — reads
  the local pgpool's map and re-attaches this node's own backend if
  marked down. `failover_on_backend_error` can degenerate the winner on
  its own instance after promotion, and `auto_failback off` makes that
  permanent; in a pgpool-routed deployment self-attach is part of what
  "promote" means. Probe failures are debug-level (pgpool legitimately
  down is routine). The cross-instance fan-out — converging the *other*
  instances' maps after a promotion — is still open (TODO.md); today
  only `cluster recover` fans out, via the `AttachNode` peer RPC.
- A demote decision → **fence**: stop local PostgreSQL. Never gated on
  journaling. A node running as primary while another holds the lease
  is fenced the same way.
- A holder change → re-point the local standby: replication slot
  ensured on the holder (peer RPC), recovery config rewritten, reload
  (`primary_conninfo` is reloadable). This replaces pgpool's
  `follow_primary_command`. A confirmed follow is then VERIFIED, not
  trusted: if it has not reached `streaming` after `leader_ttl`, the
  executor logs at error, sets `/healthz follow_wedged=true`, and
  re-attempts (finding 15 — structurally unreachable since candidacy
  went strict flush-max, kept as defense in depth).
- **Demote policy: stop and wait.** A fenced node stays stopped;
  rejoining (rewind/reclone) is `cluster recover` — operator-driven,
  never automatic.

Cluster-wide, `cluster pause` suspends the execution layer on every
member: the loop keeps observing and logging but stops acting — no
takeover, no fence, no re-point. It is replicated through consensus, so
it survives agent restarts, and it does **not** protect a primary that
dies while paused; nothing promotes in its place until `cluster resume`.

**pgpool's place** (the §6 contract of
[docs/promotion-authority.md](docs/promotion-authority.md)): watchdog
off, `failover_command` a notify-only poke, `follow_primary_command`
empty, `detach_false_primary` on, `auto_failback` off. pgpool remains
the router and learns the primary through `sr_check`; it commands
nothing. `pg_agentctl gen-pgpool` emits this contract and `check-hooks`
verifies it, including the decision-critical settings.

## 6. Hook contract (positional args / format tokens)

The hookspec crate is the single source of truth for these layouts; both
`pg_agentc` (parsing pgpool's argv) and `pg_agentctl` (printing/validating
`pgpool.conf`) consume it. There must be no other place a token order is
written down.

### 6.1 pgpool-templated hooks

Operator-configurable format strings — order is whatever appears in
`pgpool.conf`. The canonical contract (`pg_agentctl print-hooks`) sets
`failover_command` and leaves `follow_primary_command` empty; the
command lines `pg_agentc` can parse, and their token order:

```
failover_command           = 'pg_agentc failover       %d %h %p %D %m %H %M %P %r %R %N %S'
follow_primary_command     = ''   # MUST stay empty (hook-contract §2)
```

(`pg_agentc follow_primary %d %h %p %D %m %H %M %P %r %R %N %S` remains
a parseable command for the `FollowPrimary` RPC's sake, but no contract
wires it into `pgpool.conf`.)

Token meaning (pgpool tokens, *not* postgres tokens):

| Token | Meaning |
|-------|---------|
| `%d %h %p %D` | detached node id / host / port / pgdata |
| `%m %H %r %R` | new main id / host / port / pgdata |
| `%M`          | old main id |
| `%P %N %S`    | old primary id / host / port |

`%m / %H` carry different semantics across the two hooks: in
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
pgpool_remote_start: $1=standby_host   $2=primary_pgdata   (discarded)
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
do with it. See §9.2 for the reasoning. Mentioned here because it is a
natural thing to reach for, and the answer is that we deliberately don't:
`CertReloader` exists solely for the peer/consensus port (9701).

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

[raft]
# Timing only — there is no switch here (§5.15). Defaults satisfy both
# invariants in §8.3; change them only with a measured reason.
# loop_wait_secs      = 10
# retry_timeout_secs  = 10
# leader_ttl_secs     = 30
# election_timeout_ms = 5000

[startup]
# How many peers must answer the phantom-primary check before a
# primary-shaped node is allowed to come up. Default 1. Set to 0 only
# on a cluster where the quorum gate cannot be met by construction
# (the acceptance harness does this at bootstrap, when no peer has a
# database yet).
# phantom_check_required_peers = 1

[supervisor]
# The agent ensures pgpool2.service is running, once after the
# phantom-primary verdict resolves and continuously thereafter on a
# rate-limited cadence. Disable if pgpool's lifecycle is managed
# externally. Default true.
# pgpool = true
```

The `.deb`/`.rpm` ship an annotated sample at
`/usr/share/pg_agent/config.toml.sample` listing every field the daemon
reads; it is the operator-facing companion to this section.

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

raft.loop_wait_secs          = 10
raft.retry_timeout_secs      = 10
raft.leader_ttl_secs         = 30
raft.election_timeout_ms     = 5000

startup.phantom_check_required_peers = 1
supervisor.pgpool                    = true
```

Every PostgreSQL/pgpool path default above is Debian's. On the RHEL
family an operator sets five fields explicitly (`pg_install_prefix`,
`data_dir`, `service`, `user_home`, `pcp.pgpool_service`) and everything
else works — the `rocky9-pg16` matrix cell runs the full suite that way.
Auto-detecting them is on the roadmap. `node_id_file` is **not** on that
list: the agent probes both families' spellings of pgpool's own node-id
file (§8.4), because it is pgpool's file rather than a field anyone sets.

### 8.3 Validation

- `pool` must be non-empty.
- Node ids must be unique and ≥ 0; hostnames must be unique.
- Local node id must resolve (see §8.4) and be present in the pool.
- `[postgres.replication]` is all-or-nothing; sslmode and cert paths
  validated as in §5.10.
- `[raft]` (docs/promotion-authority.md §5): `leader_ttl >= loop_wait +
  2 * retry_timeout`, and `retry_timeout > election_timeout`. Violating
  either is a config error, because each converts routine events into
  spurious failovers. The block is timing only — consensus itself is
  not configurable. The obsolete `enabled` key is refused rather than
  ignored: `false` fails the load, `true` loads with a warning to
  delete the line. Decisions are logged every `loop_wait` on the `ha`
  tracing target.

### 8.4 Local-node id resolution

In priority order, first hit wins:

1. `node_id` field at the root of `config.toml`.
2. `node_id_file` field — file containing the integer.
3. pgpool's own node-id file, probed at **both** family spellings in
   order: `/etc/pgpool2/pgpool_node_id` (Debian) then
   `/etc/pgpool-II/pgpool_node_id` (RHEL). Sharing this file between
   pg_agent and pgpool means one Ansible step writes one file both tools
   read; the two can never drift. Probing only one spelling was finding
   28 — invisible when it fires, because source 4 quietly answers
   correctly on any host named like its pool entry.
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
  "replication": { "lag_bytes": 0, "wal_receiver_state": "" },
  "sync_commit": "armed",
  "follow_wedged": false
}
```

- `role` is one of `"primary"`, `"replica"`, `"unknown"`.
  Serialised from a typed `HealthRole` enum — no stringly-typed
  constants in code.
- `sync_commit` is `"armed"` / `"disarmed"` / `"blocked"` / `"n/a"`
  (docs/quorum-commit.md §5), primaries only. `"armed"` means
  acknowledged commits are on ≥ 2 nodes. `"disarmed"` means they are
  single-copy promises — bootstrap before the first standby attaches, or
  the operator's `allow-async` hatch. **`"blocked"` means commits are
  currently hanging** for want of a standby, and is a page.
- `follow_wedged` is the executor's tripwire: a follow it confirmed has
  not reached `streaming` past the grace window, so redundancy is
  degraded until the node is rebuilt. Structurally unreachable in the
  designed flows since candidacy went strict flush-max — if it ever
  trips, that is a new finding.
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

The full 11-field `NodeInfo` returned by `Pcp::node_info_all()` is
available to other consumers (preflight, `pg_agentctl cluster status`, a
future `/metrics` endpoint); the body carries only the projected subset
above. `HealthSnapshot` mirrors this shape — nested rather than flat, so
`#[serde(rename_all = "snake_case")]` handles the wire form and there is
no `postgres_*` / `pgpool_*` prefix soup to keep in sync by hand.

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
├── replay/
│   └── <op>_<sha256>.json           # idempotency markers (see §5.12)
├── inflight_ops/
│   └── <op-id>.json                 # phased orchestration journal
└── raft/
    └── raft.redb                    # consensus log + state machine

$PGDATA/                             # no agent files — left to PostgreSQL
```

`raft/` is the only one that is not disposable-by-design at the file
level — and even it is recoverable from peers: stop the agent, delete the
directory, restart, let Raft re-replicate. That is what makes the storage
engine a low-stakes choice, and the acceptance suite executes the
procedure rather than trusting it.

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

`/etc/polkit-1/rules.d/50-pg-agent.rules`, mode `0644 root:root`. Grants
the `postgres` user `org.freedesktop.systemd1.manage-units` for the
PostgreSQL and pgpool units, verbs:
`start`/`stop`/`reload`/`restart`/`reload-or-restart`/`try-restart`/
`reload-or-try-restart`. No `sudo`, no shell exec.

**Ansible ships this file; the package does not**, and `validate-env`
does not check it (§14). A missing or non-matching rule surfaces as peer
operations failing with
`org.freedesktop.DBus.Error.InteractiveAuthorizationRequired` — polkit's
fallback is to ask a human, which no daemon can answer.

**Match unit patterns, not literal names.** A rule pinned to
`postgresql@17-main.service` covers no other version, and one naming only
`pgpool2.service` covers neither RHEL's `pgpool-II.service` nor a
source-built `pgpool.service`. Both were real defects (findings 27, 28).
The rule must accept `/^postgresql@\d+-main\.service$/` (Debian's
per-cluster template), `/^postgresql-\d+\.service$/` (RHEL's per-version
unit), `postgresql.service`, and all three pgpool spellings.
`testing/docker/50-pg-agent.rules` is the reference content.

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

Pre + post: `rm -rf $PGDATA/pg_replslot/*` (§17 invariant 5).

### 11.3 `pcp_*` PCP impl

```
pcp_attach_node -h localhost -p <pcp_port> -U <pcp_user> -w -n <id>
pcp_detach_node -h localhost -p <pcp_port> -U <pcp_user> -w -n <id>
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
discards. This is what `/healthz` consumes (§9), and what the executor's
self-attach probe reads to decide whether the local pgpool has degenerated
this node's own backend (§5.15). `pg_agentctl cluster status` does not use
it — that fans `GetStatus` out over the peer mesh instead, so it reports
what the *agents* see rather than what one pgpool's map says.

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
2. Load config (or fall back to defaults with a warning); apply env
   overrides, CLI overrides, `--dev` rules.
3. Create `<state_dir>` and its `maintenance/`, `replay/`,
   `inflight_ops/` subdirectories (mode `0700`).
4. Build `CertReloader` if TLS is configured; fail fast if it is not,
   remote peers are present, and `--dev` is not set. Spawn the SIGHUP
   handler that reloads it.
5. Open the `LocalDb` pool; build `PeerTransport` (CA pool, SAN
   allowlist) and `PeerPool`; build `StandbyOps`, `PcpCli`, `Systemd`
   and the three file-backed stores.
6. Build the Raft runtime over `<state_dir>/raft/` and compose
   `HaWiring` — the consensus store, the HA loop and the executor as one
   value, so neither "consensus without executors" nor "executors
   without consensus" is a state that can be spelled.
7. Repair `$PGDATA` hook symlinks (fail fast on conflicts).
8. `Agent::serve`:
   a. Bind the Unix socket (chmod 0600, remove stale), the TCP peer
      listener, and the plain-HTTP healthz listener — all synchronously.
   b. Run **one synchronous probe** to seed the healthz snapshot.
   c. Spawn `LocalServer`, `PeerServer` (carrying `PgAgentRaft` on the
      same listener), `MaintenanceWorker`, the healthz serve + snapshot
      loops, and — if enabled — the pgpool supervisor.
   d. `sd_notify::ready()`, only after every listener is bound and the
      initial snapshot is in place (§9.3).
   e. **Phantom-primary check**, after the subsystems are up so peers
      can answer: if any peer reports a higher timeline, or asserts
      primary on this node's own timeline, stop local PostgreSQL rather
      than serve on a timeline the cluster has moved past. Unverifiable
      evidence stops it too — conservatively, and tunable via
      `[startup] phantom_check_required_peers`.
   f. **Cold-start reconciliation**, synchronously and *before* the HA
      loop spawns, so a primary coming up through crash recovery never
      races the loop's fence. Standby-shaped `$PGDATA` (has
      `standby.signal`) starts unconditionally — divergence lands in the
      follow-wedge tripwire, never in a serving primary. Primary-shaped
      `$PGDATA` starts only on a quorum-fresh lease read naming this
      node; a deposed ex-holder reads `holder != self` and stays down.
      Uninitialized `$PGDATA` is never touched. Without this, an
      established cluster never returns from a full-site power blip:
      PostgreSQL is agent-managed, the demote policy ignores `Down`
      instances, and a holder whose PostgreSQL is down can only tick
      `WouldDemote` (finding 21). The quorum poll is bounded at 10 s
      because the unit ships `TimeoutStartSec=30s`.
   g. Spawn the HA loop and executor.
   h. Wait for SIGINT/SIGTERM. On shutdown: `sd_notify::stopping()`,
      gracefully stop the gRPC servers, shut down healthz with a 5 s
      grace window.

---

## 13. Operator CLI (`pg_agentctl`)

**`--help` is the authoritative surface** — flags, defaults and exact
spellings live in the clap definitions and are not restated here. This is
what each command is *for*, and which ones can hurt you.

| Command | Purpose |
|---|---|
| `print-hooks` | Emit the canonical `pgpool.conf` + `postgresql.conf` hook lines (§6). |
| `check-hooks <pgpool.conf>` | Verify a deployed conf against that canonical list, including the decision-critical settings (`use_watchdog`, `detach_false_primary`, `auto_failback`, `failover_on_backend_error`). Exit 0 iff every row is `OK`. |
| `gen-pgpool` | Render the backend block from live cluster values plus the canonical hook block. `--write` is atomic temp+rename. **Refuses to render if any node is unreachable** — better a clear error than a silently mis-sized pool. |
| `cluster init` | One-time bootstrap (§5.7): replication role, slots, basebackup each standby, seed the lease, form Raft membership. Run **on the chosen primary**; refused in recovery. Idempotent. |
| `cluster status` | Fan out `GetStatus`; render a topology table. Also the mesh-level mTLS reachability check `validate-env` deliberately does not cover. Exit 0 iff every node answered. |
| `cluster recover --target <id>` | Reclone a broken standby **from** the local primary. The supported repair for a fenced ex-primary or a wedged follow. `--stop-target-pg` stops the target's PostgreSQL first; without it, a target still running PostgreSQL is refused rather than dying eight layers deeper. |
| `cluster handoff --target <id>` | Planned role swap: promote the target, demote the local primary to follow it. Refuses on lag above one WAL segment unless `--allow-lag`. **Destructive on failure** — see the Ctrl-C caveat in TODO.md. |
| `cluster pause --reason <why>` / `cluster resume` | Suspend automatic role decisions cluster-wide for planned work. Replicated through consensus, so it survives agent restarts and applies on every member. Does **not** stop PostgreSQL or protect a primary that dies while paused: nothing will promote in its place until you resume. `--reason` is required — the next person to find a cluster that is not failing over needs to know it was deliberate. |
| `cluster allow-async --confirm` | **Emergency only.** Disarm quorum commit on the current primary so commits stop waiting for a standby ack (§5.15). Journaled, shouted in `/healthz`, and re-armed automatically at the next standby attach. |
| `maintenance list \| show \| retry` | The durable cleanup-retry queue (§5.13). |
| `ops list \| show \| resume \| abandon` | The in-flight orchestration journal. `resume` verifies the cluster still matches the recorded phase and refuses on divergence; `abandon` clears an unrecoverable op out of the way. |

**Every** subcommand routes through the local daemon over the Unix
socket. The daemon owns the `PeerPool`, the cert material, and any
fan-out; the CLI never imports `PeerPool`, `CertReloader` or TLS
material. Operators can therefore run it from a workstation with socket
access over SSH, with no TLS material on disk.

### 13.1 What the CLI guarantees to automation

Cluster deployment is driven by Ansible ([BOOTSTRAP.md](BOOTSTRAP.md) is
the playbook-shaped walkthrough, including who owns which file and the
HAProxy constraints). The agent's job is to give Ansible good seams and
otherwise stay out of the way. Four properties are contract, not
convention:

- **Stable exit codes** on every subcommand: `0` success / clean / no
  action needed, `1` work to do or hard failure or any check `ERR`, `2`
  usage error. This is what makes `register:` + `failed_when:` honest.
- **`--json` wherever output would otherwise be parsed** — `print-hooks`,
  `maintenance list`, `cluster status`, and `pg_agentd validate-env`.
  Stable schemas.
- **No interactive prompts, ever.** Destructive commands take an explicit
  flag (`--confirm`, `--stop-target-pg`, `--allow-lag`) rather than
  reading stdin.
- **Idempotent by default.** Re-running `cluster init`, `maintenance
  retry`, or `cluster pause` against an already-correct state is a no-op,
  not an error — which is what makes `changed_when:` meaningful.

Plus **atomic file writes** for anything operator-facing (`gen-pgpool
--write`, the daemon's symlink repair), and **SIGHUP reload rather than
restart** for cert rotation, so `service: state=reloaded` Just Works.

`pg_agentd validate-env` is the universal post-deploy assertion: one call
per host, structured per-check so a playbook can remediate conditionally.
The systemd unit wires it as `ExecStartPre=` too — Ansible's explicit
task is the early-warning gate, the unit's is the safety net. Mind §14's
list of what it does *not* cover.

Things the agent deliberately never does, because they would fight the
deployment tooling: read undocumented environment variables, write
outside `<state_dir>` / `$PGDATA` / `/run/pg_agentd/` unless told to,
manage pgpool or PostgreSQL beyond the D-Bus calls specified here, or
auto-create directories Ansible owns (`/etc/pg_agent/`, `/etc/pgpool2/`).

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

- **`tls material`** — if `[tls]` is unset and only loopback peers exist
  → WARN. If set: each of `ca_cert`/`cert`/`key` exists, is a regular
  file, is readable by postgres; the cert parses; its SANs cover every
  non-local pool hostname; expiry is more than 30 d away.
- **`pgpool_node_id`** — pgpool's node-id file (either family spelling,
  §8.4) exists and matches the agent's resolved local node id.
- **`postgres user_home` / `.pcppass` / `.postgresql`** — the postgres
  home resolves, `.pcppass` is present and mode `0600` (absent → WARN,
  `pcp_attach_node` would prompt), libpq's cert directory exists.
- **`pg_basebackup` / `pg_rewind`** — present and executable under
  `pg_install_prefix`.
- **`recovery conf include`** — the effective `postgresql.conf` (walked
  the way PostgreSQL walks it: `include`, `include_if_exists`,
  `include_dir`) has an include that **resolves to**
  `$PGDATA/myrecovery.conf`. Asserting the include merely *names* that
  file is not enough: PostgreSQL resolves a relative include against the
  directory of the referencing file, so on the Debian layout
  `include_if_exists = 'myrecovery.conf'` points into `/etc` at a file
  nothing ever writes — it parses, PostgreSQL starts clean, and the
  standby silently never streams. ERR when absent, ERR when it resolves
  elsewhere (both paths named), WARN for a plain `include` (PostgreSQL
  refuses to start when the file is absent, which is a primary's normal
  state).
- **`postgres unit: restart policy`** — the *effective* `Restart=` for
  the configured unit (via `systemctl show`, so a drop-in counts) must be
  `no`; anything else is ERR, with the drop-in path in the message.
  `LoadState` is read alongside it because `systemctl show` answers for a
  nonexistent unit by printing defaults — and the default is
  `Restart=no`, so without that a typo'd unit name would report a clean
  bill of health.
- **`raft: …`** — five refusals rather than warnings, because each one's
  failure mode only becomes visible during an outage: a pool smaller than
  three, an unresolved local node id, no mTLS (the consensus plane shares
  the peer listener, so this would expose lease takeover to anyone who
  can reach the port), an unwritable state dir (a vote that cannot be
  persisted is a vote that can be cast twice after a crash), and a
  leftover obsolete `[raft] enabled` key.

DB-backed (skipped with a WARN row if the DB is unreachable):

- **`setting: …`** — `wal_level ≥ replica`, `hot_standby = on`,
  `max_replication_slots` and `max_wal_senders` ≥ pool size + 2, and
  `wal_keep_size` ≥ 512 MB (WARN). The agent does not manage
  `wal_keep_size` — it is static deployment config, unlike
  `synchronous_standby_names` — but it is the only thing covering WAL
  written *before* a slot exists at promotion, so the check says plainly
  when that gap is left open (finding 22).
- **`extension: pgpool_recovery`** — installed in the `postgres` database.
- **`role: …`** — the replication role and `pgpool` exist.

**What it deliberately does not check**, because operators reasonably
assume otherwise: the polkit rule, `pcp.conf`, `pool_passwd`, and
`pg_hba.conf`. Each of those fails later and less informatively, and
covering them is open work — until then Ansible should assert them
directly. See BOOTSTRAP.md Phase 1.7.

Report format:

```
OK    tls material            ca_cert
WARN  tls material            expires in 21d
ERR   pgpool_node_id          file says 0, config says 1
…
validate-env: 1 error(s), 1 warning(s) — FAIL
```

`--json` emits `{"checks": [{name, status, detail}…], "has_errors":
bool}`. The shape is stable; `has_errors` is what Ansible gates on.

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

Two layers, and the split is deliberate.

**Unit tests** use in-process fakes — one struct per trait (§4) with
call-log vectors and configurable `Err` fields, in the same file as the
trait. No mocking framework. This is where decision logic, parsers and
state machines are pinned, including the consensus store's fault
injection.

**Acceptance tests** boot the real artifacts: the packaged `.deb`/`.rpm`,
the packaged unit with its `ExecStartPre`, the polkit rule, real mTLS,
real PostgreSQL replication, on three systemd containers. See
[testing/README.md](testing/README.md).

The load-bearing property of the acceptance layer is that it asserts on
**event order**, not on sampled state: scenario windows are cursors into
one merged event log, "did X happen" awaits that log, and absence claims
cover windows bounded by awaited events rather than instants. A whole
class of defect — and of vacuous pass — is only visible that way.

There is no middle layer of process-level fakes, and deliberately so:
every bug the acceptance suite has found lived in the interaction with a
real dependency (systemd's job semantics, libpq's include resolution,
pgpool's backend map, a partitioned peer's TCP behaviour), which a fake
would have modelled away. See [testing/FINDINGS.md](testing/FINDINGS.md)
— finding 4 states the general case: no unit test can catch that class,
because the stubs don't run SQL.

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
6. **`RecoveryFirstStage` never calls `pcp_attach_node`.** pgpool drives
   re-attachment after its own stage 2, and attaching underneath it
   races that. Other paths legitimately attach — `FollowPrimary`,
   `cluster recover`'s fan-out, and the executor's self-attach (§5.15) —
   but each attaches on an instance it owns, and with the watchdog off
   an attach reaches exactly the one instance it was sent to.
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

## 18. Out of scope

- **VIP management** (`if_up_cmd`, `if_down_cmd`, `arping_cmd`,
  `delegate_IP`) — HAProxy is the entry point. Supporting a
  watchdog-managed VIP instead is on the roadmap, and would mean turning
  the watchdog back on, which reintroduces a second thing with opinions
  about failover.
- **A separate-middleware topology** — pgpool is assumed co-located with
  PostgreSQL on every backend. The agent reaches its local pgpool via PCP
  on `localhost` and manages the unit via systemd.
- **SRV-based pool discovery** — `[[pool]]` is the source of truth.
- **Pure-Rust replacements for `pg_basebackup` / `pg_rewind` / PCP** —
  subprocess for now. PCP is simple TCP text and could be ported; the
  data-path tools are a larger bet (roadmap).
- **Non-systemd init systems** — the agent drives PostgreSQL through
  systemd over D-Bus, waiting on `JobRemoved` for genuine job completion.
  See TODO.md for what a second `ServiceManager` impl would cost.
