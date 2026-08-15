# Acceptance testing — dockerized 3-node cluster

Proves cluster behavior end to end against the **real artifacts**: the
nfpm-built `.deb`, the packaged systemd unit (sd_notify, ExecStartPre
`validate-env`), the polkit rule, real mTLS between agents, and real
PostgreSQL 17 streaming replication — three systemd-booted Debian 13
containers on a compose network.

This harness is the substitute for live-cluster shadow validation
(promotion-authority §10 step 5): the live cluster's existing behavior
is not a useful oracle, so robustness is demonstrated here instead —
and this same harness is where step 6 (openraft) gets its
partition/restart acceptance scenarios.

## Run

```
testing/acceptance.sh              # build .deb, up, all scenarios, down
KEEP=1 testing/acceptance.sh       # keep the cluster running afterwards
SKIP_BUILD=1 testing/acceptance.sh # reuse dist/ .deb
```

On failure the cluster is kept for debugging (`docker exec -it pga-db0
bash`, `journalctl -u pg_agentd`).

## Layout

| File | Role |
|---|---|
| `compose.yaml` | 3 nodes (`db0..db2`), privileged + `cgroup: host` so systemd is PID 1 |
| `docker/Dockerfile` | debian:trixie + systemd + postgresql-17 + the `.deb`; pgpool2 installed but masked (BOOTSTRAP.md Phase 1.1) |
| `docker/provision.sh` | boot-time provisioning = Ansible's Phase-1 role: node id, TLS, config.toml, pg_hba, roles/extension, archive dir |
| `docker/pgpool-setup.sh` | operator-run pgpool config + start (BOOTSTRAP Phase 1.4 + 3.1), in the target hook-contract shape |
| `docker/50-pg-agent.rules` | polkit grant (postgres user → manage PG/pgpool units) |
| `gen-certs.sh` | one CA + per-node certs, SAN = compose hostname (matches the peer SAN allowlist) |
| `acceptance.sh` | scenario driver + assertions |

## Scenarios (phase 1)

- **S0** — all three daemons pass `validate-env` and come up; standby
  PostgreSQLs deliberately down pre-init.
- **S1** — `pg_agentctl cluster init` from db0: slots, basebackups,
  standbys streaming; `cluster status` renders the topology.
- **S2** — shadow HA loop steady state: db0 retains a self-claimed
  lease; db1/db2 adopt the observed primary and follow.
- **S3** — primary death: both standbys watch the holder for
  `leader_ttl`, then **exactly one** (db1, node-id tiebreak at equal
  WAL positions) shadow-takes the lease; db2 stands down. Primary
  returns → shadow converges back (nothing was actually promoted).
- **S3c** — lag gate with *real* WAL lag: replay paused on db2, ~80 MB
  of WAL generated, primary stopped, then the handler is handed the
  lagging node as `new_main`. Refused, naming db1 as the better
  candidate. Runs before pgpool exists so nothing else reacts to the
  primary going down.
- **S4** — the 2026-06-11 incident replayed: `pg_agentc failover`
  announcing the healthy primary as dead → refused
  ("running as primary"), nothing promoted.
- **S5** — agent restart on the primary: phantom check confirms, no
  conservative stop, PG untouched.

## Phase 2 — pgpool in the loop

pgpool-II 4.6 on all three nodes, configured per
[docs/pgpool-hook-contract.md](../docs/pgpool-hook-contract.md) §4:
watchdog **off**, `failover_command` as an advisory poke,
`follow_primary_command` **empty**, `detach_false_primary` on,
`auto_failback` off. `docker/pgpool-setup.sh` plays BOOTSTRAP Phase 1.4
+ 3.1 and is run by the driver after `cluster init`.

- **S6** — pgpool starts on all three; every instance shows all three
  backends up; `/healthz` reports ready.
- **S7** — post-cutover: the canonical block IS the agent-led contract,
  so the conf checks clean except the harness's deliberate
  failover-probe wrapper (drift detection proven on a known deviation),
  and `check-hooks --legacy` dissents — the two contracts are
  distinguishable. (Closes finding 8.)
- **S8** — detach does **not** propagate between instances, and the
  detach-fired hook is refused by the precondition check because the
  "failed" standby is still streaming (hook-contract §5, item 2).
- **S9** — real primary failover driven by pgpool, with every
  `failover_command` invocation recorded per instance (hook-contract
  §5, item 1). Asserts a single primary afterwards.

Run `PHASE=1 testing/acceptance.sh` to stop before pgpool comes up.

## Phase 3 — repair, the rest of the hook contract, and the baseline

- **S10** — post-failover repair with the agent's own commands
  (`cluster recover` per surviving node, then the pgpool attach
  fan-out). Asserts the cluster returns to one primary + two streaming
  standbys. This is the scenario that exposed finding 9.
- **S11** — `pgpool_status` is sticky across a pgpool restart
  (hook-contract §5.3): a detached backend stays detached, and only an
  explicit attach clears it.
- **S12** — `follow_primary_command` non-empty degenerates healthy
  standbys (hook-contract §5.4), the claim that made the contract say
  *remove* rather than *notify-only*. Sets the hook to `/bin/true`,
  fails the primary over, and counts the backends pgpool marks down.
- **S13** — **the split-brain baseline.** Partitions the primary off
  the network (`docker network disconnect`) and checks whether the
  majority promotes while the isolated node keeps running as primary.
  This scenario **records unsafety, and is expected to**: it is the
  empirical form of promotion-authority §2.1, and it is the regression
  test that must invert once the lease lands. **S13b** then shows the
  one mitigation that exists today — restarting the agent on the stale
  primary makes the phantom check stop it — and how narrow it is
  (nothing fires while the stale primary just keeps running).

Run `PHASE=1` to stop before pgpool, `PHASE=2` to stop before phase 3,
`PHASE=3` to stop before the raft phase.

## Phase 4 — consensus for real (promotion-authority step 6)

The same shadow loop, backed by the embedded Raft instead of a
process-local store: `enabled = true` is appended to the `[raft]` block
on all three nodes and the agents restarted, which routes the lease
through `PgAgentRaft` on the peer mTLS listener and a replicated redb
state machine. Still shadow — nothing promotes — so the assertions are
about the *decision stream*, and specifically about the two claims the
per-node phases could only simulate:

- **R0** — post-S13 repair, pgpool stopped (these scenarios are about
  the agent's own decisions), raft enabled. The daemon restart itself
  is the validate-env assertion: the packaged unit's ExecStartPre gate
  now runs the raft prerequisite checks. Before membership exists, all
  nodes must tick `StoreUnknown` — "cannot read is not vacant" holding
  on a real consensus store, where the store's answer genuinely is
  unknown rather than fault-injected.
- **R1** — `ClusterInit` forms raft membership; a second run reports
  "already formed" rather than erroring. `--only-node 99` matches no
  standby, so the run exercises exactly the membership bootstrap.
- **R2** — steady state through the quorum: the primary retains, both
  standbys follow, and — the difference from S2 — lease acquisition
  happens **once cluster-wide**, not once per private store. The suite
  counts acquisition events across all three journals and requires
  exactly one.
- **R3/R3b** — primary death: exactly one standby commits the takeover,
  now CAS-serialized by the quorum rather than emergent from the
  node-id tiebreak; the winner cycles `AwaitingPromotion` (nothing
  promotes in shadow). Primary returns, lease converges back.
- **R4/R4b** — **S13 inverted at the decision level.** The same
  partition that produced two primaries under pgpool: the isolated
  lease holder, unable to complete a linearizable read, decides
  `WouldDemote` ("quorum contact lost") without ever learning whether a
  takeover happened — and commits nothing. The majority commits a
  takeover, and every takeover comes at least `leader_ttl` after the
  previous one — sequential handoffs are *expected* in shadow mode
  (nothing promotes, so each winner's lease looks orphaned after its
  grace window and legally moves on), but a fresh holder's ttl of
  protection must hold (finding 13 is what happens when it does not).
  Shadow mode means these are log lines, but they are the decisions
  that make split brain unreachable once step 7 hands them executors.
  R4b confirms the partitioned raft node rejoins without a restart and
  PostgreSQL-level state needed no repair (exactly one primary
  throughout).
- **E3** — the production end-state: pgpool up in the agent-led
  contract, execute mode on. The primary dies; pgpool fires its
  notify-only `failover_command`, the handler answers **advisory** (no
  promotion from the hook), the lease promotes exactly one standby, and
  pgpool discovers it through `sr_check` — routing stays pgpool's,
  authority does not. `cluster recover` then rejoins the dead node with
  the pcp attach fan-out live.

## Phase 5 — EXECUTE (promotion-authority step 7)

`shadow = false`: the same decisions, now driving PostgreSQL. Everything
before this phase proves the loop decides correctly; this phase proves
the executors act on it.

- **E0** — executors attach. The holder retains; both standbys converge
  onto it through the executor's follow path (slot prep via peer RPC,
  conf rewrite, reload) — the mechanism that replaces pgpool's
  `follow_primary_command`.
- **E1/E1b** — primary death: the lease is deposed after `leader_ttl`
  and the takeover **actually promotes** (journaled — `ops list` shows
  the promote op). The surviving standby re-points onto the new primary
  and streams. The dead ex-primary is asserted to have stayed stopped —
  demote policy is never-automatic-rejoin — and is then rebuilt by the
  operator path (`cluster recover`).
- **E2/E2b** — **S13, executed.** The same partition that measured two
  primaries in phase 3: the isolated holder escalates quorum loss to a
  demote decision and the executor **fences it** (PostgreSQL actually
  stopped); the majority promotes exactly one standby. After the
  partition heals: **one primary on the wire**, no phantom-check
  restart, no operator, no coincidence — the regression test
  promotion-authority §2.1 said must invert, inverted. The fenced node
  stays down until `cluster recover` rejoins it.

## Still planned

- `detach_false_primary` storm behavior (hook-contract §5.5), which
  needs a false primary manufactured out of band.
- `.rpm` flavor on a RHEL-family image (ROADMAP distro matrix).

## Findings log

Discoveries made while building the harness — the kind of thing these
tests exist to surface. Promote items to TODO.md as they're triaged.

1. **Fresh-cluster bootstrap vs. the phantom-primary check.** Three
   fresh nodes all initdb as TL1 primaries. If all three PostgreSQLs
   start before `cluster init`, every agent's startup check sees peers
   asserting primary on its own timeline → SplitBrain verdict → all
   three PGs stopped. With standbys' PG down instead, the bootstrap
   primary sees zero peer evidence → Unverifiable (at the default
   `phantom_check_required_peers = 1`) → also stopped. The harness
   works around it (`phantom_check_required_peers = 0`, standby PG down
   until init), but BOOTSTRAP.md documents neither; a first-boot
   affordance (e.g. an explicit "uninitialized" marker, or `cluster
   init` clearing a hold) deserves design.
2. **BOOTSTRAP.md drift:** it shows `pg_agentctl cluster init --primary
   <host>`; the actual CLI has no `--primary` — init runs *on* the
   intended primary over the local socket (`--only-node` is the only
   targeting flag).
3. **Equal-WAL candidacy tiebreak** (fixed in the HA loop while writing
   S3): the node-id tiebreak originally fired only when a peer was
   strictly ahead, so the common clean-death case (all standbys at the
   same LSN) would have let every candidate proceed. Equality now
   funnels into the tiebreak; S3 asserts exactly one takeover.
4. **`timeline_id()` never worked on a standby — production bug, fixed.**
   `pg_walfile_name_offset()` refuses to run during recovery
   (`ERROR: recovery is in progress`), so `LocalDb::timeline_id()`
   returned `Err` on *every* standby, and `GetStatus` reported the
   documented "unknown" sentinel `timeline_id = 0`. Consequences, all
   silent:
   - The **phantom-primary startup check** treats a 0 as "peer gave no
     evidence". With the default `phantom_check_required_peers = 1`,
     a primary whose only peers are standbys could never reach a
     `Confirmed` verdict — restarting `pg_agentd` on the primary would
     render `Unverifiable` and *stop a healthy PostgreSQL*. The
     `STARTUP_CHECK_RETRIES` comment blames a rolling-deploy race for
     the 2026-06-12 incident; retries could never have fixed this,
     because the condition was permanent, not a race.
   - The **reactive-failover lag gate** requires a known
     `(timeline, lsn)` for the candidate, and candidates are standbys —
     so the gate skipped itself every time. Step 1 of the
     promotion-authority sequencing was effectively dead code in
     production.
   - Same for the **HA loop's** candidate comparison (visible in the
     pre-fix shadow logs as `local WAL position unknown`).
   Fix: on a standby, read `pg_stat_wal_receiver.received_tli`, falling
   back to `pg_control_checkpoint().timeline_id` when not streaming.
   No unit test can catch this class — the stubs don't run SQL. This
   suite is the regression test.
5. **Lease thrash without a promotion grace window — fixed.** S3's log
   showed the winner cycling `TookOver` → `WouldDemote`(released) →
   `TookOver` every two ticks, minting a new term each time. Cause:
   the loop released the lease the instant it observed "I hold it but
   I'm in recovery". That state is *expected* briefly after a takeover
   because `pg_promote()` is asynchronous — so in production a slow or
   failed promotion would have become a lease-thrash storm across
   candidates. Fix: `AwaitingPromotion` holds the lease through a grace
   window (= `leader_ttl`, so self-release and others' takeover
   eligibility mature together), then releases *and* backs off.
6. **`include_if_exists` is load-bearing and unverified.** A standby
   only reads `$PGDATA/myrecovery.conf` if `postgresql.conf` includes
   it (BOOTSTRAP.md Phase 1.2 documents the line; Ansible owns it).
   Without it, `ConfigureStandby` reports success and the standby comes
   up with no `primary_conninfo` at all — a silent no-op. Candidate for
   a `validate-env` check: assert the include is present.

7. **A `pcp_detach_node` on a healthy standby would have dropped its
   replication slot.** Detaching a backend fires that instance's
   `failover_command` with the standby as `%d`, which routes into the
   agent's standby-down branch — whose job is to drop the detached
   node's slot. So "detach a backend for maintenance" was, before the
   precondition check landed, a silent way to break a live standby's
   replication. S8 asserts the refusal. This makes the precondition
   work protective against routine operator actions, not just against
   false health reports.
8. **`gen-pgpool` emits hooks the target contract forbids.** Its
   canonical block still sets `follow_primary_command` (which must be
   empty — a non-empty value makes pgpool degenerate every healthy
   standby after a primary failover) and the two watchdog escalation
   hooks (which never fire with watchdog off). `check-hooks` correctly
   flags the deviation, which means at cutover the tools will report
   the *intended* configuration as drift. `gen-pgpool` needs a
   target-contract mode before step 7.
9. **`cluster recover` races pgpool's failover hook and loses its slot
   — FIXED via the `inflight_ops` migration.** The report described one
   race; the slot turned out to have **four** ways to die, each found
   by re-running S10 after closing the previous one:
   1. `failover` on the primary while the recovery is mid-flight →
      cross-op consult (`inflight_owner_of`).
   2. `failover` arriving *after* the recovery completes but before the
      rebuilt standby reaches `streaming` → `CROSS_OP_GRACE` (120 s).
      pgpool's hook lags its health check, so on a small cluster the
      recovery routinely finishes first, and in that window the node
      reads as legitimately down to the precondition check too.
   3. A **stale `drop_slot_cleanup` maintenance intent on another
      node**, retried over the peer RPC with exponential backoff —
      deleting the freshly-created slot once per backoff step
      (`01:44:19, :49, 01:45:49, 01:47:49`). No caller-side guard can
      fix this: the caller is a different node acting on an intent
      recorded before the recovery existed. Guard moved to
      `PeerServer::drop_slot`, since the node holding the slot is the
      only one that knows it is spoken for.
   4. The **same intent when the target is local** — the maintenance
      worker's `if is_local { db.drop_slot }` branch bypasses the peer
      RPC and therefore the server-side guard. This is the one that
      kept S10 red after (1)–(3), showing up as `pg_basebackup: could
      not send replication command "START_REPLICATION": ERROR:
      replication slot "node0" does not exist`.

   The rule now lives once, in `inflight_ops::owner_of_node` /
   `owner_of_slot`, with all four sites delegating. S10 exercises the
   live race (no detach-first workaround) and asserts the consult
   fires. Original report: `cluster recover --target N
   --stop-target-pg` stops the target's PostgreSQL; pgpool sees that
   backend go down and fires `failover_command`; the agent's
   standby-down branch drops the detached node's replication slot —
   the slot the in-flight recovery just created. Recovery reports
   `OK: recovery complete`, the standby starts, and PostgreSQL fails
   with `replication slot "nodeN" does not exist`. The precondition
   check does not help (the standby genuinely *is* down), and
   `failover`'s cross-op consult only knows about in-flight *handoff*
   ops because recovery still uses replay markers rather than
   `inflight_ops`. `repair_cluster` in this suite works around it the
   way an operator must today: detach the target everywhere first.
10. **The split-brain baseline is reproducible on demand.** Isolating
    the primary with `docker network disconnect` yields two primaries
    every time: majority-side promotion at ~t+82s while the isolated
    node keeps serving. Recorded with its timeline in
    promotion-authority §2.1. Note S13's polling window must exceed the
    full promotion latency (below) or the scenario silently reports "no
    split brain" — an earlier run did exactly that.
11. **Two timeout bugs on the failover critical path — both fixed.**
    The partition probe's ~80 s promotion latency decomposed into two
    defects, neither visible without a real unreachable peer:
    - The precondition check cost a flat **30 s**. An
      already-established peer channel to an isolated node does not
      fail fast — it hangs until the full request timeout — so every
      failover reacting to a genuine outage paid 30 s before
      proceeding. Now bounded by `PRECONDITION_TIMEOUT` (5 s):
      evidence we cannot get in five seconds is evidence we do not get.
    - `LONG_RPC_TIMEOUT` (300 s) was **silently capped at 30 s**. The
      endpoint-level `.timeout(DEFAULT_REQUEST_TIMEOUT)` installs a
      tower layer that cancels the response future regardless of any
      per-request `set_timeout`, so `Start`/`Stop`/`Promote` never got
      the budget they asked for. Observed as
      `promote db1: peer promote: Timeout expired` at exactly 30 s —
      *while the promotion had already succeeded server-side*, since
      `pg_promote()` alone waits up to 60 s by default. A failover that
      works reporting failure is worse than one that fails: it invites
      a retry against a node that is already primary. The channel
      ceiling is now `LONG_RPC_TIMEOUT`, with fast unary RPCs bounded
      client-side by `short_rpc`.

12. **The consensus plane repeated finding 11's bug — caught by R4's
    first real partition, fixed.** The HA tick forwards its
    linearizable read to the raft leader; when that leader is the node
    that just got isolated, the forwarded RPC sat on a cached channel
    whose only bound was the 30 s channel ceiling. One tick blocked
    **34 s** — the entire partition window — while the surviving nodes
    had re-elected a reachable leader within 1 s. The majority never
    started its holder-unhealthy clock, so the takeover the scenario
    exists to observe never happened. `Request::set_timeout` could not
    help: it writes a header for the server to honour, and the server
    is precisely who is unreachable. Fixed twice over: leader-forwarded
    RPCs are bounded client-side by `LEADER_RPC_TIMEOUT` (5 s), and the
    HA tick bounds `read_state` at `retry_timeout` regardless of what
    the store behind the trait does — the loop no longer trusts any
    store to fail fast. Regression-tested with a black-hole peer
    (accepts TCP, never answers) in `raftnet`; same client-side
    enforcement added to openraft's replication RPCs via `hard_ttl`.
    Worth naming the pattern after two occurrences: **any RPC whose
    failure the caller has a time budget for must carry a client-side
    deadline; header deadlines evaporate exactly when they matter.**

13. **A new lease holder inherited its predecessor's unhealthy clock —
    caught by R4's second run, fixed.** During the partition, db2
    legitimately took the lease after watching the dead holder for
    `leader_ttl`; seven seconds later db1 deposed it. db1's
    holder-unhealthy clock had been running against the *previous*
    holder and was never reset when the lease changed hands, so the
    brand-new holder started its life already past ttl in db1's eyes.
    In shadow this is churn in a log; at cutover it voids exactly the
    window the `grace = leader_ttl` design promises a fresh winner —
    `pg_promote()` is asynchronous, and a rival with an inherited clock
    can CAS the lease away mid-promotion, which is the lease-thrash
    storm of finding 5 wearing consensus clothes. Fixed by keying the
    clock to the holder it watched (`holder_unhealthy_since: (holder,
    since)`); unit-regression-tested, and R4's hysteresis assertion
    (no two takeovers within `leader_ttl`) is the acceptance-level
    guard. Found because the suite's first "exactly one takeover"
    assertion was *wrong* — sequential shadow handoffs are legal — and
    replacing it with the property the ttl actually promises is what
    exposed the 7-second gap as a violation rather than noise.

14. **A real promotion stalled 40 s on the partitioned peer — two
    primaries existed for ~2 s. Caught by E2's first run, fixed at
    three layers.** The E2 partition's takeover winner issued
    `pg_promote()` and PostgreSQL, finishing recovery, ran
    `restore_command` — which fans `FetchWal` out to peers *including
    the isolated one*. Three compounding defects: `fetch_wal` had no
    client-side bound at all (the one peer RPC that had escaped the
    finding 11/12 sweep — third occurrence of the class, and the one
    on the promotion-critical path); each back-to-back
    `restore_command` invocation re-paid the full 30 s per-peer
    timeout for the same dead peer; and `pg_promote()` defaults to
    `wait := true`, so the server-side wait sat *outside*
    `promote_and_wait`'s deadline entirely. Both blocked promotions
    completed within 120 ms of each other at the instant the partition
    healed; in the ~2 s before the loser's executor fenced it ("runs
    as primary but node 0 holds the lease"), two primaries served.
    The CAS + fence contained it — the containment working is worth as
    much as the bug — but the window is now closed at the source:
    `FETCH_WAL_SETUP_TIMEOUT` (5 s) bounds stream establishment,
    `RESTORE_WAL_PEER_COOLDOWN` (10 s) stops re-probing a peer that
    just failed, and `pg_promote(false)` puts the entire wait under
    the caller's deadline. Under partition a promotion now stalls
    ≤ ~7 s, inside every ttl. Bonus finding: the suite's own E2
    asserts were false-negatives — `journalctl | grep -q` under
    `set -o pipefail`, the exact footgun `log_has`'s comment warns
    about, re-learned and re-fixed with the helpers.

15. **A surviving standby diverged 120 bytes past the new primary's
    fork point and wedged — candidate selection is a sampled-position
    race.** In E2, db0 had been rebuilt seconds earlier (E1b) and was
    still catching up when the partition hit; at candidacy time its
    position read behind/unknown, so db2 legitimately won the CAS —
    but by promote time db0 had replayed slightly *more* of the old
    timeline. A standby ahead of the fork point cannot follow the new
    timeline by streaming: PostgreSQL loops "new timeline forked off
    before current recovery point" and the executor's light-follow,
    having successfully rewritten the conf and reloaded, believes it
    converged while the walreceiver flaps underneath. Patroni has the
    same fundamental race; the answer there and here is `pg_rewind` —
    which v1 demote policy reserves for the operator, so E2b now
    detects the non-streaming survivor and repairs it via `cluster
    recover` (the operator path, exercised). Product follow-ups in
    TODO.md: the executor should *detect* a wedged follow (standby
    confirmed-following but not streaming past a grace) and say so
    loudly, and opt-in auto-rewind is the eventual closure.
