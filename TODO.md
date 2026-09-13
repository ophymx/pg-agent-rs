# TODO

Open work only. Closed items live in git history; discoveries made by
the acceptance suite live in [testing/FINDINGS.md](testing/FINDINGS.md).

## Active

### Handoff's slot management on the new primary

Two narrow bugs, same area, both small and defensive.

**(a) Both-paths-fail drops the slot before recovery can use it.**
`localserver.rs` cluster_handoff's `if let Err(bb_err)` branch drops the
slot on the new primary when rewind AND basebackup both fail — but
`$PGDATA` is empty by then (basebackup wiped it) and nothing pins the
new primary's WAL. By the time the operator runs `cluster recover
--target N --stop-target-pg`, the segments needed for a cheap rebuild
may have been recycled. Keep the slot until recovery completes (or the
operator confirms via `ops abandon`): the pin is doing useful work even
after handoff has given up.

**(b) `create_slot` idempotency masks a stale `restart_lsn`.**
`peerserver.rs` / `localdb.rs` treat SQLSTATE 42710 as success without
touching the existing slot. A prior failed handoff can leave a slot whose
`restart_lsn` predates the WAL still on the new primary; a re-run where
rewind succeeds (rewind does not use the slot during copy) then streams
through it and fails with "requested WAL segment has already been
removed". Fix: drop-then-create in the RPC, or have the caller inspect
`restart_lsn` first. (a) and (b) compose — if the RPC owns the slot
lifecycle correctly, both stop being problems.

### Converge the remaining handlers onto `validate_cluster_preconditions`

`preconditions.rs` exists with the standby-down intent (refuse the slot
drop when the announced-failed standby is reachable and streaming).
Still ad-hoc: `follow_primary`, `cluster_recover`, `cluster_handoff`
each carry their own preflight. Lift them onto the intent enum so the
refusal story is uniform and lives in one place. The slot-state
consistency check (a failover dropping a slot expects the slot to be
inactive) is the one invariant from the original sketch that was never
implemented.

Defense in depth, explicitly — the class it narrows is closed at the
root by the lease CAS, not here. See
[docs/promotion-authority.md](docs/promotion-authority.md) §3.

### Cross-instance pgpool attach after a lease promotion

`cluster recover` fans the attach out to every member via the
`AttachNode` peer RPC, and the executor self-attaches the winner's own
backend on primary-holder ticks. The promotion path has no fan-out: after
a failover, the *other* instances' maps can still hold the new primary's
backend down, and with `auto_failback off` that is permanent until
something attaches it there. Give the executor the same fan-out recover
already has.

### `recovery_first_stage`: a real resume driver

The op is journaled across `started → slot_created → data_copied →
standby_configured`, but `resume_inflight_op` refuses a `Recovery` op and
points the operator at `cluster recover`, which restarts the ladder from
the top. Restarting is correct-if-wasteful, so this is ergonomics: skip
the phases already recorded complete, after verifying the cluster state
still matches the recorded phase.

### `follow_primary` unification

Three implementations share one end state: the `PgAgentLocal::FollowPrimary`
RPC handler (no longer wired into pgpool — the follow hook is empty
post-cutover; the RPC survives for direct invocation), the executor's
light-follow path, and the post-handoff fan-out's `drive_follow_primary`.
Converge them — orchestration should not care who pulled the trigger.

- (a) add a `Checkpoint` peer RPC so a slot's `restart_lsn` can be
  freshened from any node;
- (b) migrate the RPC's binary replay marker to the inflight journal, so
  it shares the phase ladder and resume (this also closes the
  `FollowPrimary` half of the replay-TTL item below);
- (c) collapse `cleanup_slot_after_failure` and
  `cleanup_peer_slot_after_failure` into one that picks local-vs-peer
  from the recorded `new_primary` id.

End shape: one driver, N triggers.

### Scheduled switchover

`ClusterState.switchover` and `ConsensusStore::set_switchover` exist and
replicate; nothing writes them and no executor honors them. What is
missing is the operator surface (`cluster switchover --to <id> [--at
<RFC3339>]`) and the executor's scheduled-handoff path. `cluster pause`
/ `resume` already proved the state-machine-field-plus-ctl-verb shape.

### Auto-resume of in-flight ops on startup (opt-in)

`[startup] auto_resume_inflight_ops = true` so a crashed daemon picks up
where it left off instead of waiting for an operator-typed `ops resume
<id>`. Gated off by default until verify-then-resume has cluster mileage.

## Deferred

Acknowledged, low priority, listed so they do not get re-derived.

### Ctrl-C during basebackup wipes `$PGDATA`

`localserver.rs` cluster_handoff rewind→basebackup branch, `pgstandby.rs`
basebackup driver. Tonic drops the server-side request future on client
disconnect → `Command::kill_on_drop(true)` SIGKILLs `pg_basebackup` →
`$PGDATA` is empty (the clear ran first) with no phase advancement past
`local_stopped`. Real, but requires the operator to cancel a long
destructive operation the documentation tells them to leave alone. The
stuck state is visible (`pg_agentctl ops list`) and `cluster recover
--target N --stop-target-pg` is the documented recovery. The fix —
detaching orchestration from the gRPC request lifetime — is non-trivial
and buys protection only against an unforced error.

### `MAX_HANDOFF_LAG_BYTES = 16 MiB` is hardcoded

`config.rs`. Described as "one WAL segment", but `wal_segment_size` is
set at initdb time and ranges 1 MiB–1 GiB. Fix: query
`current_setting('wal_segment_size')` at startup. Cosmetic on default
clusters; matters on tuned ones.

### Replay-marker 24 h TTL surprises long-gap re-runs

`replay_markers.rs`. Still on markers: `FollowPrimary` and `failover`'s
standby-down branch. The `FollowPrimary` one is the destructive-if-expired
case (conditional basebackup); failover's is soft. Closed by (b) of the
follow_primary unification above.

### Fence latency: fast shutdown drains walsenders toward `wal_sender_timeout`

`roleexec.rs` `fence` → `PostgresInstance::ensure_stopped`. On a
partitioned primary — the fence's whole use case — the walsenders being
drained point at exactly the unreachable peers, so "database system is
shut down" lags up to `wal_sender_timeout` (44 s observed). Writes are
refused from the shutdown *request* onward, so this is not a split-brain
window, and under quorum commit it cannot lose an acknowledged write; it
delays operator recover and stretches the fence's completion evidence.
Fix shape: escalate to an immediate-mode stop after a short fast-shutdown
grace, or terminate walsenders before the stop. Crash-recovery cost is
moot — a fenced node is recloned or rewound on rejoin anyway.

### The `Escalation` RPC and its hook constants are vestigial

`pg-agent-hookspec` still defines `HOOK_ESCALATION` / `HOOK_DE_ESCALATION`,
`pg_agentc` still dispatches them, and the `Escalation` RPC still backs
them — but watchdog is off in the agent-led contract, so
`wd_escalation_command` never fires. Kept because removing a proto RPC is
a wire-compat decision, not code hygiene. Decide the constants, the
dispatch arm and the RPC together.

### `slot_name` captured at orchestration start

`localserver.rs` cluster_handoff. `local.slot_name()` is `node{id}` and
the `NodePool` is snapshotted at daemon startup, so this is a documented
constraint rather than a bug. Worth knowing before adding any "reload
pool" path: a handoff that creates a slot under one local id then writes
recovery config naming a different one would break silently.

### Alpine / non-systemd support

Scoped 2026-08-18, not scheduled. Estimated ~3–4 focused weeks to a green
`alpine-pg16` matrix cell. Static musl already works; Alpine's packages
for PostgreSQL and pgpool-II exist. **The blocker is the init system.**
The agent drives PostgreSQL through systemd over D-Bus with a polkit
rule, waiting on `JobRemoved` for job completion; OpenRC's `rc-service`
offers no async completion signal, so "wait until the unit actually
finished" has to be rebuilt on polling — and fencing correctness rests on
knowing a stop completed. The honest fix there (verify the EFFECT —
postmaster gone, port closed — rather than trusting the service manager)
would strengthen the systemd path too and is worth stealing back
regardless.

Deferred on audience, not difficulty: production PostgreSQL overwhelmingly
runs on Debian-derived images, and container-native deployments reach for
an operator instead. **What would change the decision** is reframing it as
"not locked to systemd" rather than "runs on Alpine" — ~80% of the cost is
OpenRC, which would equally buy Gentoo, Devuan, and any non-systemd host.
