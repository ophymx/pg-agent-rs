# TODO

Open work that surfaced during recent feature shipping but isn't yet
scheduled. Items roughly in priority order within each section.

## Active

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

### `recovery_first_stage`: migrate replay markers to `inflight_ops`

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

### `slot_name` captured at orchestration start (hypothetical)

- `crates/pg-agent-core/src/localserver.rs` cluster_handoff slot creation. `local.slot_name()` is `node{id}`. The `NodePool` is snapshotted at daemon startup and isn't reassigned at runtime — so this is a documented constraint rather than a bug. Worth noting before adding any "reload pool" path: a handoff that creates a slot under one local id then writes recovery_conf referring to a different id would silently break.

## Roadmap items surfaced (planning, not coding)

These came up multiple times during the recent feature work as "v2 / future" but aren't tracked elsewhere yet.

- **Maintenance-mode pause/resume.** Cluster-wide flag that disables reactive failover + pgpool-driven hooks so the operator can do planned work without races. Closes "operator stops pgpool for maintenance, pg-agent's supervisor restarts it" friction. Pairs with auto-resume gating.
- **Auto-resume on startup (opt-in).** `[startup] auto_resume_inflight_ops = true` so a crashed daemon picks up where it left off after a clean restart instead of waiting for operator-typed `ops resume <id>`. Gated off by default until verify-then-resume has cluster-trial mileage.
- **Cluster-state RPC + gossip plane.** Already on `ROADMAP.md` ("Shared cluster state (the foundation switchover and pause need)"). The per-daemon `inflight_ops` journal closes local race windows; cross-node coordination still depends on pgpool's hooks. Switchover/pause as proper features want cluster-wide consensus on "is anything in flight."
- **follow_primary unification.** The existing `PgAgentLocal::FollowPrimary` RPC handler (pgpool hook) and the post-handoff fan-out's `drive_follow_primary` share the same end state but currently coexist as two implementations. Converge them: orchestration shouldn't care which node pulled the trigger. Sub-steps: (a) add a `Checkpoint` peer RPC so the slot's `restart_lsn` can be freshened from any node, (b) migrate the existing RPC's binary replay marker to the inflight journal so it shares the phase ladder + resume, (c) collapse the cleanup helpers (`cleanup_slot_after_failure` vs `cleanup_peer_slot_after_failure`) into one that picks local-vs-peer based on the recorded `new_primary` id. End shape: one driver, two triggers (pgpool hook + handoff fan-out + future explicit `pg_agentctl follow-primary` CLI).
