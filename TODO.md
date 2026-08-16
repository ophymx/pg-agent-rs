# TODO

Open work that surfaced during recent feature shipping but isn't yet
scheduled. Items roughly in priority order within each section.

## Active

### ~~Quorum commit — make the lease's terms bind writes~~ — DONE (all four phases)

- **Design:** [docs/quorum-commit.md](docs/quorum-commit.md). The gap:
  terms fence *promotion* flawlessly (the acceptance auditor proves it
  every run) but nothing at the write path checks one — a deposed
  primary's acknowledged commits die with its timeline (findings 17,
  18). The mechanism: `synchronous_standby_names = ANY 1` managed by
  the executor, so acknowledging a commit requires a standby that
  follows the lease — a primary that loses the lease loses the ability
  to acknowledge within one follow-convergence, independent of fence
  latency.
- **Phases** (each shippable alone, §8): (1) `application_name` in
  `primary_conninfo` + `last_flush_lsn` in `NodeStatus`;
  (2) candidacy key → flush position + G7 rework — **finding 19
  validated this premise live**: the replay-paused standby that "won
  wrongly" held every byte flushed and lost nothing; (3) executor
  arms `ANY 1` on first-standby-attach, healthz `sync_commit`
  tri-state, `allow-async` escape hatch; (4) acceptance sentinel
  write-survival asserts — the suite's first data-survival checks.

### ~~`cluster recover` races pgpool's failover hook and loses its slot~~ — FIXED

> Closed by the `inflight_ops` migration: `recovery_first_stage` now
> journals a `Recovery` op across a `started → slot_created →
> data_copied → standby_configured` ladder, and every path that can
> destroy a slot consults `inflight_ops::owner_of_node/owner_of_slot`
> first — an op owns its target while `InProgress`, and for
> `CROSS_OP_GRACE` (120s) after completing.
>
> Four such paths existed, each surfaced by re-running the acceptance
> suite after closing the previous one (testing/README.md finding 9):
> `failover`'s standby-down branch; the same branch reached by a
> *late* hook after the recovery completed; a stale
> `drop_slot_cleanup` intent retried from another node over the peer
> RPC (guard now in `PeerServer::drop_slot`, since only the slot's
> host knows it is spoken for); and that same intent when the target
> is local, where the maintenance worker calls `db.drop_slot` directly
> and bypasses the RPC guard.
>
> The acceptance suite's `repair_cluster` no longer detaches first, so
> S10 exercises the live race on every run (58/58 green). Original
> report retained below for the reasoning; the replay-marker dedup it
> describes is also gone (superseded by `RECOVERY_DEDUP_WINDOW` over
> the journal).

### (historical) `cluster recover` races pgpool's failover hook

- **Where:** `crates/pg-agent-core/src/localserver.rs::cluster_recover`
  / `recovery_first_stage` (creates the slot) vs. `failover`'s
  standby-down branch (drops it).
- **Reproduced** in the docker acceptance suite, 2026-08-09:

  ```
  20:08:18  cluster_recover: stopping postgres on target (--stop-target-pg) target=db1
  20:08:18  recovery_1st_stage: running                      # creates slot node1
  20:08:21  failover: standby down, dropping replication slot detached=db1 slot=node1
  20:08:23  recovery_1st_stage: complete ... slot=node1
  20:08:45  peer: DropSlot slot=node1
  ```

  `cluster recover --target N --stop-target-pg` stops the target's
  PostgreSQL. pgpool sees that backend go down and fires
  `failover_command` on **every** instance; the agent's standby-down
  branch does its job and drops the detached node's replication slot —
  which is the slot the in-flight recovery just created. `cluster
  recover` then reports `OK: recovery complete`, the standby starts,
  and PostgreSQL fails with `could not start WAL streaming: ERROR:
  replication slot "node1" does not exist`. The recovery is silently
  useless.
- **Why the existing guards miss it:** the precondition check
  (§5.1 step 3) correctly proceeds — the standby really *is* down, we
  stopped it. `failover` does have a cross-op consult, but only against
  in-flight **handoff** ops in `inflight_ops`; `recovery_first_stage`
  still uses binary replay markers, so there is no in-flight record for
  it to find. This is the concrete cost of the "migrate replay markers
  to `inflight_ops`" item below.
- **Fix shape (needs deciding):**
  1. *Preferred:* journal recovery in `inflight_ops` (a `Recovery {
     target_node_id }` payload), and have `failover`'s standby-down
     branch skip the slot drop when an `InProgress` op targets the
     detached node — the same shape the handoff consult already uses.
  2. *Or:* have `cluster_recover` `pcp_detach_node` the target on every
     pgpool instance before stopping it, so the backend is already down
     in pgpool's view and going down cannot fire a fresh hook. Cheaper,
     but the detach itself fires the hook once (acceptance S8), so the
     ordering only works because the slot does not exist yet at that
     point — fragile in a way (1) is not.
- **Operator workaround today:** detach the target everywhere first,
  then recover, then re-attach. The acceptance harness does exactly
  this in `repair_cluster`.

### `cluster recover` should fan out the pgpool attach (hook-contract §3)

- **Where:** `crates/pg-agent-core/src/localserver.rs` recovery/attach
  tail; `crate::pcp` only talks to the local pgpool.
- **Why:** with the watchdog gone, backend status does not propagate —
  attach is per-instance, and hook-contract §3 priced exactly this
  fan-out obligation. Today recover attaches only through the local
  pcp, so after a failover + rejoin every OTHER instance still routes
  around the recovered node until an operator attaches it there (the
  greenfield acceptance run surfaced it: a later detach-fired hook
  arrived with `%m = -1` because the primary's own instance believed
  no standby was alive). Deployment-relevant.
- **Fix shape:** recover's attach step loops the pool via a peer RPC
  (`StartPgpool`-style: each agent attaches on its own instance), or a
  dedicated `AttachNode` peer RPC; the harness's
  `pcp_attach_everywhere` documents the interim operator action.
- ~~**Also (finding 16, urgency): the executor's post-promote step must
  ensure the winner's own backend is attached on its local pgpool.**~~
  — DONE: `roleexec` now runs a convergent self-attach probe on
  primary-holder ticks (spawned off-tick, single-flight, 10 s cadence)
  and after each promotion; a backend the local pgpool marks down is
  re-attached. Convergent rather than one-shot because the observed
  degeneration (`failover_on_backend_error` on a transient error) hit
  ~20 s *after* the promotion. The cross-instance fan-out above is
  still open — self-attach fixes only the winner's own instance.

### Executor: detect a wedged follow (finding 15) — URGENCY UPGRADED

> Greenfield acceptance runs show the diverged survivor is the COMMON
> post-takeover case, not the rare one: both surviving standbys stream
> the same WAL until the primary dies, so the takeover loser is a coin
> flip to be past the winner's fork point — and the light follow wedges
> every time it is. In production this is failover MTTR: redundancy
> stays degraded until an operator notices and runs `cluster recover`.
> Detection (below) is the minimum; the rewind-only auto-repair is
> likely worth pulling forward.

### (details) Executor: detect a wedged follow (finding 15)

- **Where:** `crates/pg-agent-core/src/roleexec.rs` `converge_follow` /
  `crates/pgman/src/instance.rs` `state()`.
- **Why:** after a failover, a surviving standby can be a few bytes
  ahead of the new primary's fork point (candidate selection samples
  moving WAL positions — testing/README.md finding 15). The light
  follow rewrites the conf and reloads successfully, the executor marks
  the holder confirmed, and PostgreSQL loops "new timeline forked off
  before current recovery point" underneath — `Standby { streaming:
  false }` forever, silently.
- **Fix shape:** the executor re-checks `state()` on `Following` ticks
  even when confirmed; `Standby { streaming: false }` persisting past a
  grace (≈ leader_ttl) clears the confirmation, logs at error naming
  the likely divergence, and surfaces in `/healthz`. Actually *fixing*
  it needs `pg_rewind` — `rebuild_as_standby` exists and is
  deliberately operator-gated (demote policy); an opt-in
  auto-rewind-only mode (never the basebackup fallback, bounded blast
  radius) is the eventual closure.

### ~~`gen-pgpool` emits hooks the agent-led target contract forbids~~ — FIXED

> Closed at the cutover (promotion-authority §10 step 7): the canonical
> block IS the agent-led contract now — `follow_primary_command` empty,
> `wd_*` hooks gone, decision-critical settings included. The
> pre-cutover block briefly survived behind `gen-pgpool --legacy` /
> `check-hooks --legacy`; both flag and block were then deleted with
> the rest of the pgpool-led path (greenfield deployment made them dead
> code). Original report below.

### (historical) `gen-pgpool` emits hooks the agent-led target contract forbids

- **Where:** `crates/pg-agent-hookspec/src/lib.rs::pgpool_hooks()` (the
  canonical block), consumed by `gen-pgpool` and `check-hooks`.
- **Why:** under [docs/pgpool-hook-contract.md](docs/pgpool-hook-contract.md)
  §4, `follow_primary_command` must be **empty** (a non-empty value
  makes pgpool degenerate every healthy standby after a primary
  failover) and the two `wd_*` escalation hooks never fire once
  watchdog is off. The canonical block still emits all three, so the
  intended configuration is reported as drift by `check-hooks` —
  confirmed in the acceptance suite (testing/README.md finding 8).
- **Fix shape:** a target-contract mode for both tools (e.g.
  `gen-pgpool --agent-led`), or flip the canonical block outright at
  the cutover (promotion-authority §10 step 7) and keep the current
  block behind a legacy flag. Needs deciding *with* the cutover, not
  before it: while pgpool still drives failover the current block is
  correct.

### `validate-env`: assert `include_if_exists` for `myrecovery.conf`

- **Where:** `crates/pg-agent-core/src/preflight.rs` (a new `fs_*` check).
- **Why:** a standby reads `$PGDATA/myrecovery.conf` only if
  `postgresql.conf` includes it (BOOTSTRAP.md Phase 1.2; Ansible owns
  the line). When the include is missing, `ConfigureStandby` still
  reports success and the standby starts with no `primary_conninfo` —
  it just never streams. Found while building the docker acceptance
  harness (testing/README.md finding 6); it is exactly the class of
  silent localhost misconfiguration `validate-env` exists to catch.
- **Fix shape:** grep the effective `postgresql.conf` (+ `conf.d/`) for
  an `include_if_exists`/`include` naming `myrecovery.conf`; ERR when
  absent. Cheap, local, no DB round-trip.

### `cluster_recover` reports OK + attaches pgpool even when PG start failed

- **Where:** `crates/pg-agent-core/src/localserver.rs::cluster_recover` (recovery_1st_stage path). Observed live 2026-06-12: recover --target 2 returned `OK: recovery complete for db2.home.ophymx.com; postgres start failed: ...; pgpool started; attached node 2 in pgpool`. The peer start error was concatenated into the message but the response was `ok=true` and `pcp_attach_node` ran anyway.
- **Why it bites:** pgpool now routes to a backend whose PG is down. Health-check eventually flags it, but in the meantime any write trying that backend errors out, and the operator sees `READY=yes` ish lines in `cluster status` that misrepresent the real state.
- **Fix shape:** treat "start failed" as terminal for the recover. Don't `pcp_attach_node`. Return `ok=false` with the underlying systemd error verbatim so the operator immediately sees what to fix. The slot + basebackup work that DID succeed stays on disk; the next `cluster recover` re-run picks up from there if we ever wire recover into `inflight_ops` (currently uses replay markers).
- **Pairs with:** the recover-completion auto-attach work from 0.4.0 — the auto-attach is correct when the start succeeded; it just needs to be gated on `start_ok`.

### ~~`recovery_first_stage`: migrate replay markers to `inflight_ops`~~ — DONE

> Landed. `recovery_1st_stage` is journaled as a phased `Recovery` op;
> (a) operators can see the phase a stuck recover reached, (c) the
> 24 h dedup window is now per-op (`RECOVERY_DEDUP_WINDOW`) and
> independent of failover/follow_primary's markers. **(b) resume is
> still not implemented** — `resume_inflight_op` refuses a `Recovery`
> op and points the operator at `cluster recover`, which restarts the
> orchestration from a known state rather than re-entering a ladder
> whose `$PGDATA` may be half-copied. Wiring a real resume driver
> (skip already-completed phases) is the remaining piece.
>
> The migration also closed the recover/failover slot race above,
> which was its most urgent motivation.

### (historical) `recovery_first_stage`: migrate replay markers to `inflight_ops`

- **Where:** `crates/pg-agent-core/src/localserver.rs::recovery_first_stage` + the in-flight ops substrate from 0.6.0.
- **Why:** 0.7.2 fixed the silent-skip bug by adding `bypass_replay_marker` and a distinguishable skip message, but the underlying contract is still a binary marker with 24h global TTL. A more honest model treats recovery_first_stage as a phased orchestration (checkpoint → create_slot → basebackup → configure_standby) and journals each phase so (a) operators can see what step a stuck recover is at, (b) resume is possible after a crash, and (c) the per-op retention can be tuned without affecting failover/follow_primary's markers.
- **Pairs with:** the `follow_primary` unification item — both are state-changing operations that today use the binary replay marker for dedup; converging them both onto `inflight_ops` lets the operator surface be uniform across all the cluster-shape RPCs.

Two related issues around handoff's replication-slot management on the new primary. Both have narrow trigger conditions but the fixes are small and defensive.

**(a) Both-paths-fail drops the slot before recovery can use it**

- **Where:** `crates/pg-agent-core/src/localserver.rs` cluster_handoff `if let Err(bb_err)` branch.
- **Symptom:** when rewind AND basebackup both fail, the handler drops the slot on the new primary as cleanup. But $PGDATA is empty (basebackup wiped it) AND the new primary now has no slot pinning its WAL. By the time the operator runs `cluster recover --target N --stop-target-pg`, segments needed for a cheap rebuild may have been recycled.
- **Likelihood:** both data-copy paths failing is uncommon (usually a shared cause: disk/network), but the worst-case time-to-recovery makes the cleanup hurtful.
- **Fix sketch:** keep the slot until cluster recover completes (or operator-confirms via `ops abandon`). The slot pin is doing useful work even when handoff has given up.

**(b) `create_slot` idempotency masks stale `restart_lsn`**

- **Where:** `crates/pg-agent-core/src/peerserver.rs:435-447`, `localdb.rs:147-164`.
- **Symptom:** server-side `create_slot` treats SQLSTATE 42710 (duplicate_object) as success without touching the existing slot. If a prior failed handoff left a slot with an old `restart_lsn`, a re-run silently reuses it. If rewind succeeds (doesn't use the slot during copy), local later streams via `primary_slot_name='nodeX'` pointing at a slot whose `restart_lsn` predates available WAL on the new primary → "requested WAL segment has already been removed."
- **Likelihood:** narrow path — requires a prior handoff to have failed *after* create_slot succeeded *but before* the local node started streaming, then a re-run where rewind succeeds (basebackup would have implicitly refreshed the slot). Possible but unusual.
- **Fix sketch:** drop-then-create in the RPC (always reset `restart_lsn` to current), OR have the caller inspect the existing slot's `restart_lsn` and decide whether to drop. (a) and (b) compose: if the RPC owns the lifecycle correctly, both stop being problems.

### Design: pre-execution cluster-state validation pattern

> **Landed in part (post-0.7.3):** `crates/pg-agent-core/src/preconditions.rs`
> — `validate_cluster_preconditions(intent)` with the
> `detached`-is-actually-down check, wired into both `Failover` branches
> (primary-down: refuse when the announced-failed primary is reachable
> and running as primary; standby-down: refuse when the announced-failed
> standby is reachable and streaming). Labeled defense-in-depth in the
> module docs, per the caveat below, which remains in force. Still open
> from the sketch: converging the other handlers' ad-hoc preflights
> (`follow_primary`, `cluster_recover`, `cluster_handoff`) onto the
> intent enum, and the slot-state-consistency check.

(Generalisation of review HIGH #6 to the broader principle. **Confirmed in production:** on 2026-06-11, pgpool's `failover_command` announced `detached=db1, new_main=db0` after db1's pg_agentd briefly restarted due to the shutdown race above. db1 was actually still primary and healthy — pgpool's quorum just couldn't reach the daemon during the restart window. Our handler trusted pgpool and promoted db0, creating split-brain.)

- **Where:** every cluster-state-changing RPC handler (`failover`, `follow_primary`, `cluster_recover`, `cluster_handoff`, future switchover/pause/resume). Today each handler has ad-hoc preflight checks; some are comprehensive (`cluster_handoff`'s six refusal cases) and some assume the caller did the right thing (`failover` trusts pgpool's `new_main` pick without verifying the announced `detached` is actually down).
- **Concrete failover hole:** when pgpool announces `detached=X, new_main=Y, old_primary=X` with `X == old_primary`, the handler MUST verify that X is actually down before promoting Y. Check `peer.get_status(detached)` and refuse if `is_postgres_running && !is_in_recovery` — the supposed "failed" primary is alive and well, so pgpool's announcement is wrong and promoting Y would create split-brain. This single check would have prevented the 2026-06-11 incident.
- **Fix sketch:** lift a shared `validate_cluster_preconditions(intent: ClusterIntent) -> Result<(), ValidationError>` that every state-changing handler calls before the destructive phases. The intent describes what's about to happen (target, expected current state, etc.) and the validator checks invariants:
  - local node's role matches the intent's expectation of it (primary vs standby)
  - target's reachability + role match (e.g. recover expects standby-down-or-broken; handoff expects healthy standby; failover expects standby-about-to-promote)
  - **`detached` is actually down** when the intent says so (the missing check above)
  - no conflicting in-flight orchestration (already done structurally via `inflight.list(InProgress)` in 0.6.0; this generalises to "no recent terminal orchestration that this command would conflict with")
  - slot state consistency (e.g. failover dropping a slot expects the slot to NOT be active)
- **Why now:** every command added past 0.6.0 will re-invent its own preflight. A shared validator means the consistency story is consistent across handlers and one place to look when something refuses. The 2026-06-11 split-brain wouldn't have happened with the `detached`-is-actually-down check alone.
- **Caveat — this is defense in depth, not the fix.** The `detached`-is-actually-down check closes the known trigger but not the class: under a real partition, `get_status(detached)` is itself unreachable, and both branches are wrong (refuse → unavailable during the partition we exist to survive; promote → the original bug). Split-brain is structurally reachable as long as promotion authority lives in pgpool's `failover_command`. See [docs/promotion-authority.md](docs/promotion-authority.md). Land this anyway — it's cheap and it helps — but don't record it as closing the issue.

## Deferred (acknowledged, low priority, listed so they don't get lost)

### Ctrl-C during basebackup wipes $PGDATA

- `crates/pg-agent-core/src/localserver.rs` cluster_handoff rewind→basebackup branch, `pgstandby.rs` basebackup driver. Tonic drops the server-side request future on client disconnect → `Command::kill_on_drop(true)` SIGKILLs the basebackup subprocess → $PGDATA is empty (clear_pgdata_contents ran first), no journal phase advancement past `local_stopped`. Real failure mode, but requires the operator to actively cancel a long destructive operation that the documentation tells them to leave alone. 0.6.0's inflight journal makes the stuck state visible (`pg_agentctl ops list`) and `cluster recover --target N --stop-target-pg` is the documented recovery. Fix would be detaching orchestration from the gRPC request lifetime (spawn the destructive phases in a task that survives client disconnect) — non-trivial and only buys protection against an unforced operator error.

### `MAX_HANDOFF_LAG_BYTES = 16 MiB` is hardcoded

- `crates/pg-agent-core/src/config.rs:425`. Described as "one WAL segment" but PG's `wal_segment_size` is set at initdb time from 1 MiB to 1 GiB. Fix: query `current_setting('wal_segment_size')` at startup, store the effective threshold. Cosmetic on default clusters; only matters on tuned deployments.

### Replay marker 24h TTL surprises long-gap re-runs (non-handoff ops)

- `crates/pg-agent-core/src/replay_markers.rs`. Handoff moved to `inflight_ops` (7d retention) in 0.6.0. `failover`, `recovery_first_stage`, `cluster_recover` still use 24h replay markers — an operator who re-runs `cluster recover --target N` 25 hours after a successful run will trigger the destructive reclone again. Mitigated by each handler's own state checks (basebackup refuses non-empty pgdata, slot create is duplicate-OK, etc.) so the failure mode is soft. Fix: bump retention to 7 days to match inflight_ops, or migrate these handlers to `inflight_ops` too if the contract grows phased state.

### Peer channel pool: evict on transport error (finding 20)

- `crates/pg-agent-core/src/peers.rs` `client()` — channels are cached with age-based eviction only, so a connection broken by a partition keeps being served until `MAX_CONNECTION_AGE`, and the first RPC after the heal fails with a transport error (observed: G5b's `cluster recover` precheck dying on `http2 error`, leaving the fenced node unrebuilt). tonic redials on the next use, so callers that retry once succeed — but callers shouldn't have to know that. Fix shape: the `PeerChannel` wrapper marks its pool entry dead on tonic transport-class errors (connection refused / h2 gone / broken pipe), so the next `client()` redials; or replace age eviction with a health-checked pool. Cheap and localized.

### Fence latency: fast shutdown drains walsenders toward `wal_sender_timeout` (finding 17) — urgency drops once quorum commit lands

> With [docs/quorum-commit.md](docs/quorum-commit.md) implemented, the
> fence window can no longer lose *acknowledged* writes (the deposed
> primary's commits hang unacknowledged the moment its standbys
> re-point) — this item then becomes latency polish, not safety.

### Fence latency (detail)

- `crates/pg-agent-core/src/roleexec.rs` `fence` → `PostgresInstance::ensure_stopped` → systemd stop (fast shutdown). On a PARTITIONED primary — the fence's primary use case — the walsenders being drained point at exactly the unreachable peers, so "database system is shut down" lags up to `wal_sender_timeout` (44 s observed in the acceptance suite). Writes are refused from the shutdown *request* onward, so this is not a split-brain window — but the node's $PGDATA stays owned by the dying postmaster the whole time, delaying operator recover (guarded by `PeerServer::basebackup`'s settling wait) and stretching the fence's completion evidence. Fix shape: escalate the fence to an immediate-mode stop (SIGQUIT semantics) after a short fast-shutdown grace, or preemptively terminate walsenders before the stop. Crash-recovery cost is moot — demote policy recloneds/rewinds the fenced node on rejoin anyway.

### Escalation hook constants + Escalation RPC are vestigial

- `crates/pg-agent-hookspec/src/lib.rs` still defines `HOOK_ESCALATION` / `HOOK_DE_ESCALATION` (and `pg_agentc` still dispatches them, backed by the Escalation RPC) even though the watchdog — the only thing that ever fired `wd_escalation_command` — is off in the agent-led contract and the legacy hook block that referenced them is deleted. Kept for now because removing a proto RPC is a wire-compat decision, not a code-hygiene one. Decide separately whether to rip the constants, the `pg_agentc` dispatch arm, and the RPC together.

### `slot_name` captured at orchestration start (hypothetical)

- `crates/pg-agent-core/src/localserver.rs` cluster_handoff slot creation. `local.slot_name()` is `node{id}`. The `NodePool` is snapshotted at daemon startup and isn't reassigned at runtime — so this is a documented constraint rather than a bug. Worth noting before adding any "reload pool" path: a handoff that creates a slot under one local id then writes recovery_conf referring to a different id would silently break.

## Roadmap items surfaced (planning, not coding)

These came up multiple times during the recent feature work as "v2 / future" but aren't tracked elsewhere yet.

- **Maintenance-mode pause/resume.** Cluster-wide flag that disables reactive failover + pgpool-driven hooks so the operator can do planned work without races. Closes "operator stops pgpool for maintenance, pg-agent's supervisor restarts it" friction. Pairs with auto-resume gating.
- **Auto-resume on startup (opt-in).** `[startup] auto_resume_inflight_ops = true` so a crashed daemon picks up where it left off after a clean restart instead of waiting for operator-typed `ops resume <id>`. Gated off by default until verify-then-resume has cluster-trial mileage.
- **Cluster-state RPC + gossip plane.** Already on `ROADMAP.md` ("Shared cluster state (the foundation switchover and pause need)"). The per-daemon `inflight_ops` journal closes local race windows; cross-node coordination still depends on pgpool's hooks. Switchover/pause as proper features want cluster-wide consensus on "is anything in flight."
- **follow_primary unification.** The existing `PgAgentLocal::FollowPrimary` RPC handler (pgpool hook) and the post-handoff fan-out's `drive_follow_primary` share the same end state but currently coexist as two implementations. Converge them: orchestration shouldn't care which node pulled the trigger. Sub-steps: (a) add a `Checkpoint` peer RPC so the slot's `restart_lsn` can be freshened from any node, (b) migrate the existing RPC's binary replay marker to the inflight journal so it shares the phase ladder + resume, (c) collapse the cleanup helpers (`cleanup_slot_after_failure` vs `cleanup_peer_slot_after_failure`) into one that picks local-vs-peer based on the recorded `new_primary` id. End shape: one driver, two triggers (pgpool hook + handoff fan-out + future explicit `pg_agentctl follow-primary` CLI).
