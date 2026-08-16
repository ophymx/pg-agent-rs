# Acceptance testing — dockerized 3-node cluster

Proves cluster behavior end to end against the **real artifacts**: the
nfpm-built `.deb`, the packaged systemd unit (sd_notify, ExecStartPre
`validate-env`), the polkit rule, real mTLS between agents, and real
PostgreSQL 17 streaming replication — three systemd-booted Debian 13
containers on a compose network.

**The model under test is lease-driven roles.** `acceptance.sh` boots
every node in execute mode (`[raft] enabled = true, shadow = false`) —
the greenfield deployment shape — and runs deploy, failure, and
recovery scenarios entirely under the raft consensus lease, with pgpool
present strictly as the router it is post-cutover. There is no
migration narrative: the pgpool-led promote path is deleted from the
codebase, and the staged-migration suite that regression-tested it
went with it (see "The migration suite" below).

## Run

```
testing/acceptance.sh              # build .deb, up, greenfield suite, down
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
| `acceptance.sh` | thin launcher for the Rust harness below |
| `acceptance/` | the scenario driver (Rust, workspace member): tails every node's agent journal + PostgreSQL log into one ordered event log, holds live `tokio-postgres` connections to each node over the docker bridge (pg_hba trusts the isolated compose subnet), and asserts on event ORDER — scenario windows are event cursors, "did X happen" awaits the event log, and absence claims cover windows bounded by awaited events rather than sampled instants |

## Mainline scenarios (`acceptance.sh`, greenfield lease-driven)

- **G0** — greenfield boot: all nodes come up in execute mode through
  the validate-env gate (raft checks included). Pre-membership the loop
  ticks `StoreUnknown` — no quorum, no action: executors attached to a
  store that answers "unknown" do nothing.
- **G1/G1b** — `cluster init` does replication + raft membership + lease
  seeding in one operator command (idempotent on re-run); executors
  converge the standbys; pgpool comes up in the agent-led contract and
  `/healthz` reports ready.
- **G2** — `check-hooks` passes fully clean on the deployed conf: the
  canonical `gen-pgpool` block IS what's deployed, no overrides.
- **G2b** — pgpool stays a router: a detach neither propagates between
  instances nor breaks replication (the agent refuses the slot drop for
  a streaming standby); explicit attach clears it.
- **G3** — primary death: the failover hook answers **advisory**, the
  lease promotes exactly one standby (journaled), the survivor
  re-points, pgpool discovers the new primary via `sr_check`.
- **G4/G4b** — operator rejoin: the dead ex-primary stayed stopped
  (demote policy), `cluster recover` rebuilds it with the
  recover/failover-hook slot race guarded (finding 9's cross-op
  consult, unchanged under the lease — G4b exercises the live race on
  a running standby), diverged survivors repaired via the operator
  path (finding 15).
- **G5/G5b** — partition of the holder: the isolated node fences
  itself, the majority commits exactly one takeover (hysteresis
  asserted: no two takeovers within `leader_ttl`), **one primary
  during and after the partition**, rejoin via recover.
- **G6** — agent restarts are non-events: phantom check confirms, the
  lease survives, no takeover churn, replication uninterrupted.
- **G7** — **§2.2 under the lease**: real WAL lag (replay paused,
  ~24 MB written), primary killed — the caught-up standby wins, the
  lagging one stands down naming the gap. This is the candidate-
  selection defect the design exists to close, tested in the decision
  layer that now owns it.

---

# The migration suite (deleted)

The staged-migration suite validated the path from pgpool-led failover
to the lease: shadow-mode phases (S0–S13, including the split-brain
baseline), raft-in-shadow (R0–R4b), and execute-mode cutover (E0–E3).
It was deleted together with the pgpool-led promote path it
regression-tested — there was never a trusted pgpool-led deployment,
so the repo validates the greenfield shape only. It was the previous
incarnation of `testing/acceptance.sh` and lives in that file's git
history (last complete at commit `22b266b`).

The findings below keep their original scenario references (S…, R…,
E…) as provenance. Those scenarios are no longer runnable, but every
load-bearing behavior they proved is asserted by the greenfield suite
above.

## Still planned

Failure-coverage gaps, roughly ordered by expected bug yield (the
suite today covers clean, single, scripted failures on an idle
cluster; the discovery rate on new probes says these will pay):

1. **Holder AGENT death with PostgreSQL healthy** (`kill -9
   pg_agentd`, no restart): the one deposal path with no fence — the
   design's answer is ack starvation once the standbys re-point, which
   has never been observed. Highest value.
2. **Crash-shape death** (SIGKILL the postmaster): no shutdown
   checkpoint, none of the log events the harness and auditor key on,
   crash recovery on rejoin.
3. **Full-cluster cold restart of an ESTABLISHED cluster** (the site
   power blip): persisted lease in the raft store, three phantom
   checks racing, executors reconciling stale roles. G0/G1 only cover
   greenfield boot.
4. **Write load through the failover**: a continuous ledger upgrades
   data-survival from "one at-rest sentinel survived" to "every
   acknowledged row survived" (the actual quorum-commit invariant),
   and lets the deposed primary's hanging-commit behavior be OBSERVED
   rather than assumed.
5. **Built-but-never-entered states**: `sync_commit=blocked` (both
   standbys down → commits hang → recover → unblock), the
   `allow-async` disarm/auto-re-arm lifecycle, and a deliberately
   provoked `follow_wedged` to prove the tripwire fires.
6. **Asymmetric / partial partitions** (A-sees-B-not-vice-versa;
   agent-mesh-up-PG-mesh-down and inverse) — finding 18's class,
   found by accident once.
7. **Double faults + soak**: primary death mid-rebuild of the only
   other standby; agent restart during basebackup; an N-cycle
   failover loop (slot debris, timeline growth, term growth, leaks);
   disk-full on the WAL partition; raft-store deletion recovery
   (documented as rm-and-re-replicate, never exercised).
8. `detach_false_primary` storm behavior (hook-contract §5.5), which
   needs a false primary manufactured out of band.
9. `.rpm` flavor on a RHEL-family image (ROADMAP distro matrix).

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
    recover` (the operator path, exercised). RESOLVED twice over:
    candidacy is now strict flush-max (node id breaks exact ties
    only), which makes the wedge unreachable by construction — the
    loser's replay ≤ its flush ≤ the winner's flush = the fork point,
    so the light follow always lands (the old ±16 MiB tiebreak band
    that allowed a behind-node winner was also an acknowledged-write
    hole under quorum commit); and the executor detects any wedge
    that somehow still occurs — a confirmed follow not streaming past
    `leader_ttl` logs at error, sets `/healthz follow_wedged=true`,
    and re-attempts the follow. If that flag ever trips, it is a new
    finding.

16. **After a lease-driven promotion, the winner's own pgpool instance
    can blackhole the primary — and pcp operations wedge behind it.
    Caught by the greenfield suite's router scenario running
    post-failover.** ~20 s after a promotion, the new primary's own
    pgpool degenerated *its own primary backend*
    (`failover_on_backend_error` on a transient connection error),
    leaving `new primary node: -1` — and the §4 contract makes that
    permanent: `auto_failback off`, and pgpool never health-checks a
    down backend. That instance then routes no writes, and any
    subsequent attach on it enters `find_primary_node_repeatedly`
    (`search_primary_node_timeout`, 300 s) hunting for a primary its
    map doesn't contain — queueing every later pcp request behind it.
    The §4 annotation "failover_on_backend_error = on: per-instance
    routing reaction, self-limiting" is wrong for exactly the node
    that just won: it is not self-limiting there. **FIXED in
    `roleexec`:** the holder now runs a convergent self-attach probe
    (off-tick, single-flight, 10 s cadence) on primary-holder ticks
    and after each promotion, re-attaching its own backend when the
    local pgpool marks it down — convergent rather than post-promote
    one-shot because the degeneration hit ~20 s *after* promotion. G3
    asserts the winner's own pgpool ends up routing to it. The
    cross-instance attach fan-out (other nodes' instances) remains
    open in TODO.md; `pcp_attach_everywhere` still attaches the
    primary's backend first so failbacks can find a primary.

17. **A partitioned primary's fence takes up to `wal_sender_timeout`
    to complete — 44 s observed.** The fence's fast shutdown
    disconnects clients immediately (write service ends at "received
    fast shutdown request"), but the postmaster then drains walsenders
    whose peers are exactly the nodes the partition cut off, so the
    final "database system is shut down" lags tens of seconds. Two
    consequences, one benign, one real: no dual-primary window (writes
    were already refused — the audit's serving intervals end at the
    request line for this reason), but the node's $PGDATA stays owned
    that whole time, which is what made a too-eager recover race
    basebackup's pgdata clear against the dying postmaster (the
    settling guard in `PeerServer::basebackup` and the harness's
    shutdown-event await both exist because of this). Product
    follow-up in TODO.md: an immediate-mode fence escalation would
    close the latency; the fenced node is recloned/rewound on rejoin
    anyway, so crash-recovery cost on a node being demoted is moot.

18. **A healthy serving holder was deposed because one unreachable
    peer blinded the rival's whole status view — caught by the event
    auditor's dual-serving invariant on its first un-blinded run,
    fixed.** `collect_statuses` returned `Err` with NOTHING when any
    peer outlived the collective fan-out budget, and the HA loop
    mapped that to an empty peer view. During G5's partition the
    isolated node's probe blew the budget on some ticks, the healthy
    just-promoted holder went missing from the view, "missing" read as
    "unhealthy", and the rival's deposal clock ran to `leader_ttl` on
    pure blindness — then legally CAS'd the lease away and promoted,
    giving real concurrent serving until the deposed holder's executor
    fenced it (containment held, terms stayed unique — the audit's
    consensus invariants all passed while its serving-interval
    invariant flagged the overlap). Fourth member of the findings
    11/12/14 class, with a new lesson: it is not enough for evidence
    RPCs to be bounded — a bounded fan-out must degrade PER PEER, so
    one straggler costs exactly one peer's evidence rather than
    converting partial evidence into total blindness that a
    life-and-death clock then runs on. `collect_statuses` now always
    returns one view per node (stragglers as explicit `Err` entries),
    and the startup phantom check got the same upgrade for free.

19. **The replay-paused standby won G7's takeover — and factually
    held every byte.** Candidacy's lag gate acts only on positive
    evidence (refusing on absent evidence is the §3 unavailability
    branch), so one blipped status probe of the caught-up rival let
    the "lagging" node proceed, and it won the CAS race. Two lessons.
    Harness: repairs must target the ACTUAL primary
    (`pg.current_primary()`), not the scenario's predicted winner —
    assuming the winner cascaded four follow-on failures. Product:
    the deeper wrongness is the selection KEY, not the race — the
    "lagging" node had all 24 MB *flushed* (replay paused, receive
    flowing), so promotion replayed it and nothing was lost; replay-
    based candidacy misclassifies a data-complete node as lagging.
    docs/quorum-commit.md §4 (flush-position candidacy) is the fix,
    and this run validated its premise before a line of it was
    implemented. RESOLVED: candidacy now compares flush positions
    (quorum-commit phase 2), and G7 arranges genuine FLUSH lag by
    severing the standby's walreceiver path (PG port only — the agent
    stays reachable so the node still participates in candidacy and
    the lag gate is what refuses it).

20. **The peer channel pool serves a partition-broken connection until
    it ages out — the first RPC after a heal fails.** The pool evicts
    cached channels by `MAX_CONNECTION_AGE` only, never on error, so
    G5b's `cluster recover` of the healed ex-holder died on
    "peer get_status: http2 error" from the stale channel, the node
    was never rebuilt, and three scenarios cascaded. tonic redials
    underneath on the NEXT use, so a single retry succeeds — the
    harness's `cluster_recover` now retries once (as the operator it
    models would), and the repair fallback recovers any node whose
    PostgreSQL is unreachable instead of letting a stale follow event
    shield it. FIXED in the pool: `PeerChannel` poisons its cache
    entry on transport-class errors (unary and mid-stream) and
    `client()` redials past poisoned entries — post-heal first-RPCs no
    longer pay the broken-connection tax; the harness retry stays as
    operator-model belt-and-braces.
