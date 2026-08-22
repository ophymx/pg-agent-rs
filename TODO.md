# TODO

Open work that surfaced during recent feature shipping but isn't yet
scheduled. Items roughly in priority order within each section.

## Active

### ~~The Rocky cell's 8 unexplained failures (finding 30)~~ — FIXED, and it was the suite

> Not timing, and not the product. A `docker exec` whose container
> restarts underneath it **stops delivering without dying** — no EOF,
> no exit — so `spawn_tail`, which only notices an exec that dies,
> reported db0's agent stream healthy while it carried under half of
> what db0 wrote. Measured: `db0/Agent = 2052 heard / 3901 written`,
> 1849 lines lost, every db0-named failure an await for a line that
> was written and never heard.
>
> Fixed with a per-node watchdog on `docker inspect
> {{.State.StartedAt}}` (3s poll) that forces a re-attach when the
> container restarts. The restart is the signal, taken from docker
> rather than inferred from silence — a stall detector based on "no
> lines for N seconds" would fire constantly on a healthy quiet
> stream, since the agent is nearly silent by design in steady state.
>
>     before   PASS=283 FAIL=8   1252s
>     after    PASS=290 FAIL=0    781s
>
> **The correlation the finding was named for had the arrow
> backwards.** 1252 − 781 = 471s, which is what eight awaits burning
> 60–90s budgets costs. Slow runs did not cause failures; failures
> caused slow runs. Every red run in that table was a clean ~750s run
> plus its own timeouts.
>
> Two instruments landed with it, and they are the durable part —
> either one alone would have found this years sooner than the three
> runs of hypothesis it actually took. `await_event` timeouts are now
> classified LATE / NEVER / MISSED by re-running the predicate against
> the finished log, and the audit asks each node what it WROTE and
> compares with what the run heard. See testing/README.md finding 30.

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

### ~~`cluster recover` should fan out the pgpool attach (hook-contract §3)~~ — DONE

> Closed via the `AttachNode` peer RPC: recover's attach tail fans out
> to every member, each agent attaching on its OWN instance with the
> finding-16 semantics server-side (only-if-down; primary's backend
> first into a primary-less map). Best-effort per member — a failed
> instance converges via operator pcp or the next recover. G4 asserts
> every instance routes to the recovered node. The harness's
> `pcp_attach_everywhere` remains as belt-and-braces map normalizer
> for scenario setup. Original report below.

### (historical) `cluster recover` should fan out the pgpool attach

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

### ~~Executor: detect a wedged follow (finding 15)~~ — DONE, and the cause is gone

> Two-part closure. **Cause eliminated:** candidacy's ±`max_lag_on_failover`
> tiebreak band let a lower-id node up to 16 MiB of flush BEHIND win —
> which was both the wedge's structural cause (the loser could be past
> the winner's fork point) and, under quorum commit, an
> acknowledged-write loss hole (an ANY-1 ack can live exactly in that
> delta). Selection is now STRICT flush-max, node id breaking exact
> ties only; livelock-free because flush positions are static against
> a dead primary. Loser replay ≤ loser flush ≤ winner flush = fork
> point ⇒ the light follow always lands. **Detection stays as defense
> in depth:** a confirmed follow not streaming past `leader_ttl` logs
> at error, sets `/healthz follow_wedged=true`, and clears the
> confirmation so the follow re-runs. Auto-rewind is no longer worth
> pulling forward — if the flag ever trips, that is a new finding, not
> this one. Original report below.

### (historical) Executor: detect a wedged follow (finding 15) — urgency note

> Greenfield acceptance runs show the diverged survivor is the COMMON
> post-takeover case, not the rare one: both surviving standbys stream
> the same WAL until the primary dies, so the takeover loser is a coin
> flip to be past the winner's fork point — and the light follow wedges
> every time it is. In production this is failover MTTR: redundancy
> stays degraded until an operator notices and runs `cluster recover`.
> Detection (below) is the minimum; the rewind-only auto-repair is
> likely worth pulling forward.

### (historical details) Executor: detect a wedged follow (finding 15)

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

### ~~`validate-env`: assert `include_if_exists` for `myrecovery.conf`~~ — DONE

> Landed as the `recovery conf include` check
> (`preflight.rs::fs_recovery_conf_include`). It walks the effective
> `postgresql.conf` — following `include`, `include_if_exists` and
> `include_dir` the way PostgreSQL does — and takes `config_file` /
> `data_directory` from the running server when there is one, falling
> back to layout probing (PGDATA first, then
> `/etc/postgresql/<ver>/<cluster>/`) for the `ExecStartPre=`
> invocation where PG is down.
>
> **The fix shape in the original report was not sufficient**, and
> that is the interesting part. "An include naming `myrecovery.conf`"
> passes on a config that can never work: PostgreSQL resolves a
> relative include against the directory of the *referencing file*,
> not `data_directory`, so on the Debian layout
> `include_if_exists = 'myrecovery.conf'` names a file under `/etc`
> that nothing ever writes. It parses, PG starts clean, the standby
> never streams. The check therefore asserts the include *resolves to*
> `$PGDATA/myrecovery.conf`, and reports the misresolving spelling as
> its own ERR with both paths named.
>
> **BOOTSTRAP.md §1.3 prescribed exactly that broken relative line**
> (the acceptance images always used the absolute form, which is why
> no run ever caught it) — corrected in the same change. Statuses:
> ERR absent, ERR resolves elsewhere, WARN plain `include` (PG refuses
> to start when the file is absent, which is a primary's normal
> state), WARN cannot locate a `postgresql.conf` at all.

### (historical) `validate-env`: assert `include_if_exists` for `myrecovery.conf`

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

### ~~RHEL's packaged unit resurrects a fenced postmaster (`Restart=on-failure`)~~ — FIXED

> Closed with (a) + (b), as the fix-shape note below argued: state it,
> and hand the operator the file.
>
> **(a)** `validate-env` grew a `postgres unit: restart policy` check.
> It reads the EFFECTIVE policy (`systemctl show <unit>
> --property=Restart --property=LoadState`) — effective, so a drop-in
> counts and a commented-out line in the packaged unit counts — and
> ERRs on anything but `no`, with the drop-in's path in the message.
> ERR rather than WARN, and with no opt-out, on the same footing as the
> pool-size and mTLS refusals: a deployment where PostgreSQL's
> lifecycle is half systemd's and half the lease's has no coherent
> answer to "who decides whether this node serves".
>
> `LoadState` is read alongside `Restart` because `systemctl show`
> answers for a unit that does not exist by printing DEFAULTS, and the
> default is `Restart=no` — so without it a typo'd unit name would
> report a clean bill of health. Verified against real systemd:
> `systemctl show definitely-not-a-unit.service` prints `Restart=no` /
> `LoadState=not-found` and exits 0. Six unit tests over the parse,
> including that one.
>
> **(b)** BOOTSTRAP §1.1 now ships the drop-in
> (`10-agent-managed.conf`, the same filename the acceptance suite
> writes) for BOTH families, with the reasoning inline.
>
> **The framing was sharpened in the process.** "A fenced node stays
> down" was never actually at risk: systemd does not restart a unit it
> stopped by an explicit stop job, so `ensure_stopped` is safe on
> either family. The hazard is a postmaster that dies on its OWN terms
> — crash, OOM, `kill -9` — on a node whose agent may have died with
> it, leaving nothing to fence it. That is G8/G9's shape, which is
> exactly where the suite found it.
>
> **(c) not taken.** Masking the unit for the fence's duration is still
> the only option config drift cannot undo, and the "unresolved" note
> below — whether systemd's restart wins is timing-dependent — is still
> unresolved. But (a) turns the drift into a startup refusal, which
> covers the same ground without the fence acquiring a systemd-state
> side effect it has to unwind on every path.

### (historical) RHEL's packaged unit resurrects a fenced postmaster

- **Where:** deployment-owned, so `crates/pg-agent-core/src/preflight.rs`
  is the place the product can speak about it; the fence itself is
  `roleexec.rs` → `PostgresInstance::ensure_stopped`.
- **What:** PGDG's `postgresql-<ver>.service` ships `Restart=on-failure`
  **active**; Debian's `postgresql@.service` ships the same line
  commented out. Disabling the unit — which BOOTSTRAP does — closes
  boot-time autostart, NOT `Restart=`. So on the RHEL family systemd
  will restart a postmaster that died badly, with no agent
  involvement, and the agent's "a fenced node stays down until an
  operator or the executor says otherwise" assumption is
  Debian-shaped.
- **How it surfaced:** the acceptance suite, not analysis
  (testing/README.md finding 29). G9 SIGKILLs the primary's postmaster
  and waits for the lease to depose it; on Rocky systemd handed the
  primary straight back inside a second, nothing was deposed, and the
  32 failures that followed all descend from that. The suite now
  writes a `Restart=no` drop-in so its crash shape is uniform — which
  fixes the harness and deliberately does **not** fix this.
- **Why it matters beyond the suite:** the fence exists to stop a
  deposed primary from serving. If systemd restarts that postmaster
  before the executor's next tick, the node is serving again on a
  timeline the cluster has moved past. Quorum commit means it cannot
  ACK anything (docs/quorum-commit.md §3), so this is not an
  acknowledged-write hole — but it is a node answering reads as a
  primary after being fenced, which is exactly what the fence was for.
- **Fix shape (needs deciding):** three candidates, not exclusive.
  (a) `validate-env` reads the effective `Restart=` for the configured
  PG unit (`systemctl show <unit> -p Restart --value`) and WARNs — or
  ERRs — when it is not `no`; cheap, local, and the same shape as
  every other silent-localhost check. (b) BOOTSTRAP ships the drop-in
  as part of the RHEL path, making it Ansible's job. (c) the fence
  masks the unit for the duration, which is the only option that
  cannot be undone by a config drift, and the most invasive.
  (a) + (b) together look right: state it, and hand the operator the
  file.
- **Unresolved:** the same cell passed 257/257 the day before with the
  identical unit file, so whether systemd's restart wins is
  timing-dependent (start rate limiting is the likely gate). Worth
  understanding before choosing (c) over (a).

### ~~`cluster_recover` reports OK + attaches pgpool even when PG start failed~~ — FIXED

> The target's PostgreSQL start is now a **gate**, not a best-effort
> post-step. On failure `cluster_recover` returns `ok=false` carrying
> the peer's error verbatim plus the re-run instruction, and nothing
> downstream runs: no local `pcp_attach_node`, no attach fan-out, and
> no pgpool start on the target either — a fresh pgpool there would
> health-check its own dead backend down and fire `failover_command`
> at the node just rebuilt, which is a slot-drop hook. The reclone's
> data and slot stay on disk and the op stays journaled, so the
> operator fixes the start failure and re-runs.
>
> Steps *past* the gate (pgpool start, attach, fan-out) stay
> best-effort and are still reported individually: past it the node is
> serving, and a stale routing map is a retryable convergence problem,
> not a reason to fail a completed recovery. Two tests pin the split —
> start failure attaches nowhere, post-gate failures still return
> `ok=true`.
>
> Phased re-entry (resuming the journaled `Recovery` op instead of
> restarting the ladder) is still the open piece, tracked under the
> `recovery_first_stage` item above.

### (historical) `cluster_recover` reports OK + attaches pgpool even when PG start failed

- **Where:** `crates/pg-agent-core/src/localserver.rs::cluster_recover` (recovery_1st_stage path). Observed live 2026-06-12: recover --target 2 returned `OK: recovery complete for db2.home.ophymx.com; postgres start failed: ...; pgpool started; attached node 2 in pgpool`. The peer start error was concatenated into the message but the response was `ok=true` and `pcp_attach_node` ran anyway.
- **Why it bites:** pgpool now routes to a backend whose PG is down. Health-check eventually flags it, but in the meantime any write trying that backend errors out, and the operator sees `READY=yes` ish lines in `cluster status` that misrepresent the real state.
- **Fix shape:** treat "start failed" as terminal for the recover. Don't `pcp_attach_node`. Return `ok=false` with the underlying systemd error verbatim so the operator immediately sees what to fix. The slot + basebackup work that DID succeed stays on disk; recovery is journaled in `inflight_ops` now, so a phased re-entry is designable (today the re-run restarts from the top, which is correct if wasteful).
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
> `detached`-is-actually-down check. Post-rip, only the STANDBY-DOWN
> intent survives (refuse the slot drop when the announced-failed
> standby is reachable and streaming): the primary-down variant was
> deleted with the pgpool-led promote path — promotion authority is the
> lease's, and the 2026-06-11 incident class below is closed at the
> root by the quorum CAS rather than narrowed by a precondition.
> Labeled defense-in-depth in the module docs. Still open from the
> sketch: converging the other handlers' ad-hoc preflights
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

### ~~Packaging: the binary has no glibc floor~~ — DONE (static musl)

The floor is gone: release builds now target
`x86_64-unknown-linux-musl` and the packaged binaries are `static-pie`
with no dynamic dependencies. Verified installing and running on Debian
12, Ubuntu 24.04, Rocky 9 and Alpine, with the full acceptance suite
green (257/257) against the static agent — mTLS, D-Bus, tokio-postgres
and musl peer-hostname resolution all exercised, including across a
full-cluster restart. Debian 12 rejoined the matrix as the cell that
catches a revert. NSS plugins are unsupported by construction, as
decided.

What remains from the original entry, unchanged in substance:

- ~~**Move off nfpm to Rust-native packaging**~~ — **done**:
  `cargo-deb` + `cargo-generate-rpm`, both configured from
  `crates/pg-agentd/Cargo.toml` metadata, `nfpm.yaml` deleted. The
  staging directory survived the move for a reason that outlived nfpm:
  neither tool expands environment variables in asset paths either, so
  the target triple still cannot be templated in. See
  packaging/README.md for the gotchas each tool brought (silently
  inlined script paths, `recommends` as a sub-table).
- ~~Rocky/RHEL near term~~ — **done**, see below. Alpine remains
  aspirational.

### Historical detail, kept for the reasoning

- **The defect (testing/README.md finding 26):** the `.deb` declares no
  `Depends` at all, so it installs happily on a distro whose glibc is
  older than the build host's and then dies at exec:
  `/usr/bin/pg_agentd: /lib/x86_64-linux-gnu/libc.so.6: version
  'GLIBC_2.39' not found`. Found by the OS matrix on Debian 12
  (bookworm, glibc 2.36); the same applies to RHEL 9 (glibc 2.34), which
  matters for the `.rpm` (testing gap item 9). A trixie-built package
  currently supports glibc >= 2.39 and says so nowhere.

- **Two real fixes, and they are not exclusive:**
  1. *Pick a baseline and hold it.* Build releases against the oldest
     glibc to be supported — an old build container (what `cross` and
     manylinux do) or `cargo-zigbuild --target
     x86_64-unknown-linux-gnu.2.28`, which gets a chosen floor from a
     modern host.
  2. *Remove the floor entirely* with `x86_64-unknown-linux-musl` —
     **the preferred direction.** This project is unusually well suited
     to it: the dependency set is already pure Rust where it counts
     (zbus for D-Bus, tokio-postgres + rustls rather than libpq or
     OpenSSL), so the usual musl blocker is absent, and the allocator
     penalty is irrelevant for a daemon that sleeps between one-second
     ticks. One artifact would then serve the `.deb`, the `.rpm`, and
     every matrix cell.

     **NSS plugins are declared unsupported** — a deliberate call, not
     an oversight. Discovery in the deployments this targets is DNS or
     `/etc/hosts` (Kubernetes CoreDNS, cloud DNS, the compose network
     the suite runs on), all of which musl reads natively; SSSD/LDAP/
     mDNS resolution is an on-prem-directory concern, and the agent
     runs as a fixed local user under systemd rather than looking up
     directory identities. Document it in the packaging README so it is
     a stated boundary rather than a surprise.

     What still needs checking is musl's DNS *implementation*, which is
     a separate question from NSS: older musl had no EDNS0/TCP
     fallback, so DNS answers over 512 bytes were truncated (a real
     Kubernetes headless-service failure), and its `search`/`ndots`
     handling differs from glibc's. musl 1.2.4+ addresses the
     truncation case. Pin a recent musl and smoke-test peer resolution
     inside a cluster before committing.

- **Prep for musl, do this first: drop the accidental `aws-lc-rs`.**
  `pg-agent-core` pins `rustls = { default-features = false, features =
  ["ring"] }`, but `tokio-rustls` is declared with defaults ON, and its
  default enables rustls's `aws-lc-rs`. Cargo features are additive, so
  the whole graph gets it — `target/release/build` really does contain
  aws-lc-sys build dirs today. It is the single most likely musl
  blocker (cmake + C toolchain) and it is being compiled for nothing.
  Fix: `default-features = false` + explicit `ring`/`tls12` on
  `tokio-rustls` (and check `tonic`, which reaches rustls the same
  way). Cuts build time now, removes a cross-compile hazard later.
  Validate with the full suite — the peer mesh is mTLS end to end.

- **Alpine — ASPIRATIONAL, long term. It is not a matrix cell, it is a
  port.** Worth knowing before the next matrix round: musl removes the
  *glibc floor*, it does not make Alpine work. Checked against
  `alpine:3.20` rather than assumed:
  - PostgreSQL **is** packaged: `postgresql14/15/16` (17 on newer
    releases), each with an `-openrc` service subpackage.
  - pgpool-II **is** packaged: `pgpool` 4.5.2, plus `pgpool-openrc`
    and `pgpool-static`. I expected packaging to be the blocker; it is
    not, which materially lowers the estimate.
  - Everything is musl (`musl-1.2.5` — already past the 1.2.4 DNS
    truncation fix noted above).

  The blocker is the INIT SYSTEM, and those `-openrc` subpackages are
  the tell. This agent drives PostgreSQL through systemd over D-Bus
  with a polkit rule (`Systemd` trait, zbus), waiting on `JobRemoved`
  signals for job completion, and assumes Debian's
  `postgresql@VER-main` template units. Alpine has neither systemd nor
  a D-Bus policy model, so support means a second implementation behind
  the existing trait boundary — and OpenRC's `rc-service` offers no
  async job-completion signal, so the "wait until the unit actually
  finished" contract (systemd.rs gotcha #2) has to be rebuilt on
  polling. A design change, not a build flag.

  **DEFERRED — scoped 2026-08-18, not scheduled.** Estimate is ~3–4
  focused weeks to "supported", where supported means a green
  `alpine-pg16` cell at 257/257. Recorded so it need not be re-derived:

  | work | est. | risk |
  |---|---|---|
  | static musl binary on Alpine | **done** | — |
  | `rc-service` impl behind the trait; rename `Systemd`→`ServiceManager` | 2–3d | low |
  | **rebuild job-completion semantics** | 3–5d | **high** |
  | readiness without `sd_notify` | 1d | low |
  | privilege model without polkit (sudoers, or run as root) | 1–2d | med |
  | logging without journald | 1–2d | med |
  | `.apk` via abuild/APKBUILD + OpenRC init script | 2–3d | low |
  | **harness + matrix cell** | 5–8d | **high** |

  Two items carry the risk, and neither is mechanical:

  - **Job completion.** systemd's `JobRemoved` is an EVENT with a
    result code. `rc-service stop` returns an exit code meaning *the
    stop script returned*, not that the postmaster is gone. Fencing
    correctness and the audit invariant "every fence of a serving node
    reached PostgreSQL shutdown" both rest on knowing a stop
    completed, so this is re-deriving a safety predicate on a weaker
    primitive. The honest fix — verify the EFFECT (postmaster gone,
    port closed, `pg_isready` refusing) rather than trust the service
    manager's word — would strengthen the systemd path too, and is
    worth stealing back regardless of whether Alpine ever happens.
  - **The harness.** 22 `systemctl` sites in `scenarios.rs`, 5
    `journalctl` in `events.rs`, 6 in the provisioning scripts. The
    facts-file pattern makes most of it mechanical, but G9's
    crash-shape death has no OpenRC equivalent for its only evidence
    (`Main process exited, code=killed` from the unit journal) and
    needs a different evidence source.

  Capability losses to accept up front: no watchdog, no `Type=notify`
  readiness gate, no journal.

  **Why deferred, and it is not a technical objection.** Alpine's draw
  is small images, but this agent supervises a service-managed
  PostgreSQL on a host and deploys via Ansible; container-native
  deployments reach for an operator instead. The population that
  benefits is bare-metal/VM Alpine shops, and in practice production
  PostgreSQL overwhelmingly runs on Debian-slim-derived images — the
  official `postgres` image is Debian-based by default, with Alpine as
  the explicitly secondary variant. Three weeks for a small audience.

  **What would change the decision:** framing it as "not locked to
  systemd" rather than "runs on Alpine". ~80% of the cost above is
  OpenRC, not Alpine, and it would equally buy Gentoo, Devuan, and any
  non-systemd host. If that becomes a goal, this stops being a
  single-distro port and the arithmetic changes.

  **Cheaper middle option if it is ever wanted quickly (~1 week):**
  ship the static binary plus an OpenRC init script as a tarball — no
  `.apk`, no matrix cell — labelled community/unverified. Defers both
  high-risk items entirely. The distinction that matters is that a
  matrix cell is what earns the word "supported".

- **Rocky/RHEL — DONE as a supported, tested platform.** The
  `rocky9-pg16` matrix cell installs the real `.rpm` on Rocky 9 and
  runs the full suite green (257/257), so gap item 9 closed with it.
  The mechanism needed no changes at all — systemd, D-Bus and polkit
  behave identically — which was the bet. What needed changing was
  every place something had *assumed* the layout instead of being told
  it.

  How it was resolved, since the answer was not the obvious one: the
  harness does not detect the distro and it does not branch on a
  version. Each image writes `/etc/pg-agent-matrix/env` with the ten
  facts that differ, and provisioning, the pgpool setup, and the
  harness all read that one file — the harness over `docker exec`
  (`cluster::Facts`), before any scenario runs. One authority per
  cell. The agent itself is already fully config-driven, so its
  RHEL support is `config.toml` values that provisioning writes from
  the same file.

  Found on the way: finding 28 (the agent probed pgpool's node-id file
  only at the Debian path — silent, because the hostname fallback
  covers for it), the polkit rule not matching `pgpool-II.service`,
  and three RHEL-only pgpool startup requirements (`pid_file_name`
  under a tmpfiles directory that never got created, `pool_passwd`
  which pgpool tries to CREATE in a root-owned directory, and no
  packaged `initdb`).

  **What remains is the operator ergonomics, not the capability:** the
  AGENT's compiled defaults (`DEFAULT_PG_SERVICE`,
  `DEFAULT_PG_DATA_DIR`, `DEFAULT_PG_INSTALL_PREFIX`,
  `DEFAULT_POSTGRES_USER_HOME`, `DEFAULT_PGPOOL_SERVICE`) are still
  Debian's, so a RHEL operator must set five path fields explicitly.
  That is the distro-profile work in ROADMAP.md — a convenience now
  rather than a blocker, and worth doing with `/etc/os-release`
  auto-detection since there is now a cell that would catch it
  regressing.

- **Move off nfpm to Rust-native packaging** (`cargo-deb` +
  `cargo-generate-rpm`), which is wanted anyway and pays for itself
  here: **cargo-deb derives `Depends` from the built binary's actual
  shared-library needs**, so the failure above becomes a clean apt
  refusal at install time instead of a cryptic runtime crash. The
  trade-off is that one `nfpm.yaml` covering both formats becomes two
  tool configs (both live in `Cargo.toml` metadata, which is the point).
  `cargo-dist` is the other candidate but is oriented at archives and
  installers rather than native system packages — worth re-checking its
  current `.deb`/`.rpm` support before choosing. If the musl route is
  taken, the packaging tool matters less (no shared-library deps left to
  compute), so decide the linking question first.

## Deferred (acknowledged, low priority, listed so they don't get lost)

### Ctrl-C during basebackup wipes $PGDATA

- `crates/pg-agent-core/src/localserver.rs` cluster_handoff rewind→basebackup branch, `pgstandby.rs` basebackup driver. Tonic drops the server-side request future on client disconnect → `Command::kill_on_drop(true)` SIGKILLs the basebackup subprocess → $PGDATA is empty (clear_pgdata_contents ran first), no journal phase advancement past `local_stopped`. Real failure mode, but requires the operator to actively cancel a long destructive operation that the documentation tells them to leave alone. 0.6.0's inflight journal makes the stuck state visible (`pg_agentctl ops list`) and `cluster recover --target N --stop-target-pg` is the documented recovery. Fix would be detaching orchestration from the gRPC request lifetime (spawn the destructive phases in a task that survives client disconnect) — non-trivial and only buys protection against an unforced operator error.

### `MAX_HANDOFF_LAG_BYTES = 16 MiB` is hardcoded

- `crates/pg-agent-core/src/config.rs:425`. Described as "one WAL segment" but PG's `wal_segment_size` is set at initdb time from 1 MiB to 1 GiB. Fix: query `current_setting('wal_segment_size')` at startup, store the effective threshold. Cosmetic on default clusters; only matters on tuned deployments.

### Replay marker 24h TTL surprises long-gap re-runs (non-handoff ops)

- `crates/pg-agent-core/src/replay_markers.rs`. Handoff moved to `inflight_ops` (7d retention) in 0.6.0; `recovery_first_stage` followed (24h dedup via `RECOVERY_DEDUP_WINDOW` on the journaled op, not a marker). Still on 24h replay markers: `FollowPrimary` and `failover`'s standby-down branch (SPEC §5.12). The `FollowPrimary` one is the destructive-if-expired case (conditional basebackup); failover's is soft (slot drops are guarded several ways over). Fix: fold `FollowPrimary` into `inflight_ops` as part of the follow_primary unification item.

### ~~Peer channel pool: evict on transport error (finding 20)~~ — FIXED

> `PeerChannel` now shares a poison flag with its pool entry and sets
> it on transport-class errors (Unavailable, h2/http2 breakage,
> connection reset/canceled-in-flight — unary AND mid-stream);
> `client()` treats a poisoned entry like an aged-out one and redials.
> Application errors never poison: they prove the connection works.
> Regression test drives a real accept-then-drop peer and asserts the
> second `client()` returns a fresh channel.

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
- **Shared cluster document on the raft state machine.** The raft lease landed and serializes role assignment (ROADMAP's gossip/LWW design is superseded — see the annotation there); what remains is the small operational document (`paused` flag, scheduled switchover) as additional replicated state, which the maintenance-mode item above needs. The HA loop already reads the state machine every tick, so the plumbing is a state-machine field plus two ctl verbs.
- **follow_primary unification.** The `PgAgentLocal::FollowPrimary` RPC handler (no longer wired into pgpool — the follow hook is empty post-cutover; the RPC survives for direct invocation), the executor's light-follow path, and the post-handoff fan-out's `drive_follow_primary` share the same end state but coexist as three implementations. Converge them: orchestration shouldn't care who pulled the trigger. Sub-steps: (a) add a `Checkpoint` peer RPC so the slot's `restart_lsn` can be freshened from any node, (b) migrate the RPC's binary replay marker to the inflight journal so it shares the phase ladder + resume (also closes the FollowPrimary half of the replay-TTL deferred item), (c) collapse the cleanup helpers (`cleanup_slot_after_failure` vs `cleanup_peer_slot_after_failure`) into one that picks local-vs-peer based on the recorded `new_primary` id. End shape: one driver, N triggers (executor, handoff fan-out, direct RPC, future `pg_agentctl follow-primary` CLI).
