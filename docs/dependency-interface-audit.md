# Dependency-interface audit — readiness for the HA loop

**Status:** scoping companion to
[promotion-authority.md](promotion-authority.md) and
[pgpool-hook-contract.md](pgpool-hook-contract.md). Audited 2026-08 at
workspace v0.7.3. Nothing here is implemented; line numbers will drift.

The question this answers: **do the interfaces to our external
dependencies (PostgreSQL, PCP, pg_* tools, systemd, the peer mesh)
support building the HA loop, or does that work start with plumbing?**

**Verdict up front:** the seam architecture the design assumes already
exists — every external dependency sits behind an injectable
`#[async_trait]` trait with hand-written test doubles, and no handler
calls postgres, systemd, PCP, a pg binary, or a peer unmediated. What
is missing is specific and enumerable: a demote primitive (none exists),
a self-promotion call path (never exercised), `pcp_detach_node`
(unimplemented), an N-way WAL comparator (data on the wire, logic
scattered), timeouts on the safety-critical calls, and retry/backoff
infrastructure. openraft/redb are absent from the tree, as expected.

---

## 1. The seams (all in `crates/pg-agent-core/src/`)

Eleven traits, all `Send + Sync`, all injected as `Arc<dyn _>`; the
composition root (`crates/pg-agentd/src/main.rs`, `run_serve`) is the
only place concrete types appear.

| Trait | Module | Prod impl | Mechanism | Test doubles |
|---|---|---|---|---|
| `LocalDb` | `localdb.rs:43` | `PgLocalDb` | tokio-postgres + deadpool over Unix socket, `NoTls` | 6 |
| `Pcp` | `pcp.rs:33` | `PcpCli` | subprocess (`pcp_attach_node`, `pcp_node_count`, `pcp_node_info -a`), `.pcppass` via `-w` | 3 |
| `Systemd` | `systemd.rs:57` | `DbusSystemd` | zbus D-Bus system bus + polkit rule; job-completion wait via `JobRemoved` subscription | 4 |
| `StandbyOps` | `pgstandby.rs:142` | `StandbyExec` | subprocess (`pg_basebackup`, `pg_rewind` only — no `pg_ctl`/`pg_controldata`) | 4 |
| `PeerRegistry` / `PeerClient` | `peers.rs:48/66` | `PeerPool` / `PeerChannel` | tonic gRPC over rustls mTLS, 12 peer RPCs | 3 |
| `WalStore` | `walstore.rs:60` | `FileWalStore` | filesystem | 3 |
| `ReplayMarkerStore` | `replay_markers.rs:49` | file-backed JSON | filesystem | 2 |
| `MaintenanceStore` | `maintenance.rs:138` | file-backed JSON | filesystem | 2 |
| `InflightOpStore` | `inflight_ops.rs:243` | file-backed JSON | filesystem | 2 |
| `NodeInfo` | `agent.rs:55` | `Agent` | — | 3 |

Test style: hand-written stubs in same-file `#[cfg(test)]` modules
(`AtomicUsize` call counters + `Mutex<Option<anyhow::Error>>` failure
injection); no mocking framework. Real-integration precedent exists:
`peers.rs` tests stand up a genuine mTLS `PeerServer` with
`rcgen`-generated certs; `pgstandby.rs` tests drive the real subprocess
runner against a shell-script `pg_basebackup` stub.

## 2. What the HA loop needs that already works

- **Role + WAL queries exist and are on the wire.** `LocalDb` has
  `is_in_recovery`, `current_wal_lsn` (role-aware, parsed to u64),
  `timeline_id`, `replication_lag`; `NodeStatus.current_wal_lsn` /
  `timeline_id` are proto fields populated by `Agent::get_status`.
  The candidate-comparison *data* needs no new plumbing.
- **Promote exists** as `pg_promote()` SQL (`localdb.rs:131`), exposed
  via the `PgAgentPeer::Promote` RPC with a 300 s client timeout.
- **Periodic-loop idiom is established.** Three loops share the
  `loop { select! { cancelled => return, sleep => tick } }` shape;
  `PgpoolSupervisor` (`pgpool_supervisor.rs`) is the closest template —
  `Arc<Self>` + `run(shutdown)`, `pub tick_once()` for deterministic
  tests, cooldown and give-up counters.
- **The Raft transport substrate is real.** Server side: one
  `add_service` site in `peerserver.rs` — adding `PgAgentRaftServer` on
  the same listener/certs/SAN-allowlist is free. Client side:
  `connect_mtls` + the `Endpoint` construction in `dial()` are directly
  reusable for a separate Raft connection.
- **Config/persistence patterns to copy:** `[startup]`/`[supervisor]`
  blocks in `config.rs` for `[raft]`; `create_state_subdirs` for
  `<state_dir>/raft/`; `panic = "abort"` already set (the design doc's
  Raft-member caveat applies as written).

## 3. Gap list

Ordered roughly by how early the sequencing (promotion-authority §10)
hits them.

> **Update (2026-08-08):** items 3 (`pcp_detach_node`), 6 (timeouts:
> deadpool pool + statement_timeout, systemd job-wait ceiling,
> basebackup/rewind stall watchdog, PCP call timeout), 7
> (`retry::retry_result`; value-predicate retries stay bespoke), 9 (the
> reactive-failover lag gate), and the comparator half of 4 (the
> `cluster_view` module: status fan-out + lexicographic `(timeline,
> lsn)` ordering, now shared by the phantom-primary check and the lag
> gate) have landed. Still open from 4: jittered backoff and the
> node-id tiebreak, which belong to the HA loop itself.
>
> **Update (2026-08-09):** the consensus-store trait (item 2 — the
> `consensus` module with `InMemoryConsensusStore`), the `[raft]` config
> block (item 7's config half), and the shadow-mode HA loop itself
> (item 6, including item 4's backoff + tiebreak, in the `ha` module)
> have landed. The daemon is no longer structurally reactive when
> `[raft] shadow = true`. Still open: demote/self-promote primitives
> (items 1, 2, 9 of "does not exist"), the `PgAgentRaft` proto +
> channel factoring, and everything openraft.

**Does not exist at all:**

1. **Demote primitive** — the design's most safety-critical component.
   Today "demotion" exists only as the 8-phase reclone inside
   `run_handoff_from_phase` (`localserver.rs:2427`), which requires a
   known new primary and always copies data. "Stop the write path with
   no successor known" (demote on quorum loss) has no interface. The
   ingredients exist (`sd.stop_postgres()`, `write_recovery_conf` +
   `standby.signal`) but the operation does not.
2. **Self-promotion path.** `LocalDb::promote` is reachable only via
   the inbound peer RPC — the local node has never promoted itself. The
   HA loop's "winner promotes" is a new call-graph edge and must be
   wired into `inflight_ops` journaling like the existing two sites.
3. **`pcp_detach_node`** — not in the `Pcp` trait. Also relevant to the
   hook-contract §3 attach fan-out: attach exists, detach does not, and
   *every* PCP mutation becomes per-instance under watchdog-off.
4. **N-way candidate comparison.** Two partial consumers of WAL state
   exist — `verify_primary_at_startup` (timeline-only, one-shot) and
   `cluster_handoff`'s lag gate (one target, `MAX_HANDOFF_LAG_BYTES`) —
   but there is no "(timeline, lsn) lexicographic compare across all
   reachable peers", no jittered backoff, no node-id tiebreak.
   `verify_primary_at_startup`'s fan-out is the piece to generalize.
5. **Consensus-store trait, `PgAgentRaft` proto, `[raft]` config,
   `ClusterInit` membership bootstrap, `validate-env` raft checks** —
   all greenfield, each with a clear sibling pattern to imitate
   (`InflightOpStore` is the nearest trait shape; `build.rs` needs a
   fourth proto file registered).

**Exists but needs hardening before a loop depends on it:**

6. **No timeouts on safety-critical calls.** `LocalDb` has no statement
   timeout and a default deadpool (a hung backend blocks forever);
   `Systemd`'s `JobRemoved` wait is unbounded; `basebackup`/`rewind`
   have kill-on-drop plumbing built for caller-imposed deadlines that
   no caller imposes; `Pcp` has none. An HA loop with a `loop_wait`
   budget must wrap every dependency call in `tokio::time::timeout` —
   or the loop stalls exactly when it matters.
7. **No retry/backoff infrastructure.** Two hand-rolled retries exist
   (`stop_postgres_with_retry`, `verify_primary_with_retries_params`);
   `retry_timeout` semantics need a real home.
8. **`PeerPool` can't serve a `RaftNetwork`.** It caches
   `Arc<dyn PeerClient>`, not `Channel`; `dial()` wants factoring to
   hand out a raw channel a second pool can consume. Its 12 h
   redial-on-access policy is also wrong for a consensus plane (a
   mid-election redial costs an election timeout) — the Raft pool needs
   its own lifecycle and much tighter timeouts than 5 s/30 s/300 s.
9. **Reactive `Failover` lag gate** (sequencing step 1): the LSN logic
   to reuse sits in `cluster_handoff` (`localserver.rs:1449`); the
   `Failover` handler consults none of it.
10. **`/healthz` role reporting** is derived from `pg_is_in_recovery()`
    with an `Unknown` variant — the shape is right for a lease-backed
    answer, and the existing 503-on-stale contract is the natural hook
    for "quorum unreachable".

## 4. Reading the gaps against the sequencing

Steps 1–2 (lag gate, precondition validator) touch only existing seams —
no plumbing prerequisites. Step 4 (store trait + in-memory impl) and
step 5 (HA loop, shadow mode) are where gaps 1, 2, 4, 6, 7 land; the
timeout/retry hardening (6–7) is best treated as part of step 5's
definition of done rather than deferred to cutover, because shadow mode
only proves the loop's decisions if the loop actually completes its
ticks under fault injection. Gap 8 (channel factoring) belongs to step 6
(openraft) and is small. Gap 3 (`pcp_detach_node` + attach fan-out)
belongs to step 7 (pgpool config contract cutover) alongside
[pgpool-hook-contract.md](pgpool-hook-contract.md) §3.
