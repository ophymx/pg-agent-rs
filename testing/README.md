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

### The OS / PostgreSQL matrix — on demand, not on every run

`acceptance.sh` above always runs the baseline cell (Debian 13,
PostgreSQL 17) and needs no environment. The matrix is a separate,
opt-in entry point that runs the whole suite once per cell:

```
testing/matrix.sh                  # every cell, sequentially
testing/matrix.sh noble-pg16       # one named cell
KEEP_GOING=1 testing/matrix.sh     # do not stop at the first failing cell
FAIL_FAST=1 testing/matrix.sh …    # stop a cell at its first failure
```

| cell | base | PostgreSQL | package |
|------|------|-----------|---------|
| `trixie-pg17`   | debian:trixie  | 17 | `.deb` |
| `noble-pg16`    | ubuntu:24.04   | 16 | `.deb` |
| `bookworm-pg15` | debian:bookworm| 15 | `.deb` |
| `rocky9-pg16`   | rockylinux:9   | 16 | `.rpm` |

The Debian-family cells pair each base with the PostgreSQL version it
ships **natively** (no PGDG repo), so a build failure means "this
pairing does not exist" rather than "an external repo moved". The Rocky
cell cannot follow that rule and does not pretend to: RHEL ships no
pgpool-II from any of its own repos, so PGDG is the only source — which
is also what RHEL deployments actually use.

The three Debian-family cells vary the PostgreSQL version against one
layout. `rocky9-pg16` varies the **layout**, which is the part that had
never been tested: a per-version unit instead of a per-cluster
template, `postgresql.conf` inside `PGDATA` instead of under `/etc`, no
packaged `initdb`, no `pg_ctlcluster`, a different postgres home, and
pgpool under a different name in a different directory. Every one of
those is a place where something could have been hard-coded, and two
things were (findings 27 and 28).

Nothing in the suite is allowed to *guess* which layout it is in. Each
image writes `/etc/pg-agent-matrix/env`, and provisioning, the pgpool
setup, and the harness (`cluster::Facts`, read over `docker exec`) all
read that one file. `FAIL_FAST=1` stops a cell at its first failure,
which is what you want when standing up a NEW cell: the suite is
cumulative, so once provisioning is wrong every later scenario is
reporting the same finding for another ten minutes.

Cells run sequentially — they share the docker daemon and the compose
project name — and each one tears down the previous cluster first,
because a leftover container from cell N runs cell N's PostgreSQL.

It costs a full suite run per cell, which is why it is not wired into
the everyday path: its job is answering "does this still hold on the
other distro" now and then, not taxing every iteration.

`bookworm-pg15` earns its place twice over: it is the oldest supported
base, and it is the cell that catches a regression to a dynamically
linked build. Under one, the package installs there and dies at exec
with `GLIBC_2.39 not found` (finding 26). The build is static musl, so
that floor is gone — and this is where it stays gone.

## Layout

| File | Role |
|---|---|
| `compose.yaml` | 3 nodes (`db0..db2`), privileged + `cgroup: host` so systemd is PID 1 |
| `docker/Dockerfile` | Debian-family base + systemd + PostgreSQL + the `.deb`; pgpool2 installed but masked (BOOTSTRAP.md Phase 1.1) |
| `docker/Dockerfile.rhel` | the same, for Rocky 9: PostgreSQL + pgpool-II from PGDG (RHEL ships no pgpool at all), the `.rpm`, `pgpool-II.service` masked |
| *(both)* `/etc/pg-agent-matrix/env` | the cell's layout facts — unit, PGDATA, bins, config dir, postgres home, log path, pgpool unit + config dir. Written by the image, read by everything below and by the harness (`cluster::Facts`) over `docker exec`. Nothing infers the layout; one file states it |
| `docker/provision.sh` | boot-time provisioning = Ansible's Phase-1 role: node id, TLS, config.toml, pg_hba, roles/extension, archive dir. Branches on `PG_FAMILY` only where the families genuinely differ in KIND rather than in spelling — RHEL has no packaged cluster (explicit `initdb`), no `conf.d` convention (adds the `include_dir`), no `start.conf`, and logs to the journal (turns the collector on so both families have one log file to tail) |
| `docker/pgpool-setup.sh` | operator-run pgpool config + start (BOOTSTRAP Phase 1.4 + 3.1), in the target hook-contract shape. `pid_file_name`, `logdir` and `pool_passwd` are set explicitly: their compiled-in defaults differ per family, and on RHEL two of them point somewhere pgpool cannot write, which it reports by exiting 3 in a restart loop |
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
- **G8** — **holder AGENT death, PostgreSQL healthy** (mask +
  SIGKILL): the one deposal with no fence. The majority promotes while
  the deposed primary keeps serving — the suite asserts that reality
  instead of hiding it (a **declared dual-serving window** the auditor
  verifies is covered AND eventually closed), then asserts the
  quorum-commit contract that makes it safe: the acked sentinel
  survives onto the winner, a write on the deposed primary **starves**
  (5 s timeout, `timeout(1)` exit 124 is the assertion), and the
  never-acked row does not survive the rejoin. The restarted agent's
  phantom check (higher peer timeline → stop) is the fence that closes
  the window.
- **G9** — **crash-shape primary death** (SIGKILL the postgresql
  cgroup): no shutdown checkpoint, no walsender drain, and no log-file
  goodbye — the suite tails the unit journal so systemd's
  `code=killed` report is the death event, and the auditor accepts it
  as a serving-interval end (with G9's own await keeping that parser
  from going silently blind). The corpse stays down (Debian ships
  `Restart` commented out), the acked sentinel survives onto the
  winner, and the rejoin reclones the marker-less pgdata back into a
  standby — asserted by the absence of any later writable serving
  start on the crashed node.
- **G10** — **full-cluster cold restart** (SIGKILL PID 1 in all three
  containers, then power back): finding 21's scenario. The holder
  reads its persisted lease (no rival could take over while the site
  was dark — takeovers need the very quorum that was down), starts its
  primary through real crash recovery, standby-shaped nodes just
  start, and the suite asserts the blip is NOT a failover: no
  takeover, no promotion, no fence, same holder retains, the
  quorum-acked sentinel survives, replication and all three pgpool
  maps converge back.
- **G11** — **the fence-less deposal under WRITE LOAD** (gap item 4):
  a continuous ledger writer (`crate::load`) runs through the G8
  agent-death deposal, upgrading data-survival from "one at-rest
  sentinel" to the actual quorum-commit invariant: EVERY acknowledged
  row exists on the winner, checked seq by seq. The writer's naive
  discovery genuinely writes to the deposed primary during
  dual-serving (its hanging commits are counted — ack starvation
  observed, not assumed), acks legitimately keep flowing until the
  candidates detach (the freeze), and the resumption target is
  anchored at the PROMOTION so starve → discover → resume is proven.
  One scenario, three product findings (23, 24, and the freeze's
  design) before it first passed. Steady-state numbers from the first
  green run: ~750 acked writes, one hung commit, 5.2 s write outage
  across the whole deposal.
- **G12** — **the quorum-commit states nothing had ever entered**:
  stop PostgreSQL on BOTH standbys with their agents left up (raft
  keeps quorum, so this stays a quorum-commit test and not a fencing
  one) → `sync_commit=blocked`, and a write BLOCKS rather than
  silently becoming single-copy. Then the operator's escape hatch:
  `cluster allow-async --confirm` disarms (journaled, shouted), writes
  flow again, and when the standbys return the executor AUTO re-arms
  at the first attach — no second command, no lingering hatch. The
  hatch-era single-copy write is then asserted to reach the standby
  once redundancy is back.
- **G13** — **the follow-wedge tripwire** (finding 15's defense in
  depth, unreachable in the designed flows since strict flush-max
  candidacy): sever one standby's replication path only, kill its
  walreceiver, and the executor must declare `follow WEDGED` past the
  grace and expose `follow_wedged=true` in `/healthz`. The tripwire is
  a REPORT: the wedged node neither promotes nor is destructively
  rebuilt, the primary keeps its lease, and healing the path clears
  the flag with no operator action.
- **G14 / G15** — **the plane inversions**. G14 cuts the DATA plane
  (replication severed from both standbys) while every agent still
  sees a healthy holder: the answer must be no failover at all, and
  instead `sync_commit=blocked`, writes unacknowledged rather than
  quietly single-copy, and both wedge tripwires up. G15 cuts the
  CONTROL plane on the holder (9701, peer RPC and raft) while
  replication stays perfect: the holder must fence a database that is
  healthy by every data-plane measure, because it can no longer prove
  it holds the lease, and the majority promotes. Lost redundancy and
  lost authority, answered oppositely.
- **G16** — **one-way blindness**: a standby loses the ability to
  INITIATE control-plane connections while remaining fully answerable,
  so it sees a cluster in which everyone is dead while everyone else
  sees a healthy cluster including it. It can reach no quorum member,
  so it cannot act — one-sided evidence is not authority — and its
  replication is never disturbed.
- **G17** — **a blind standby must not depose a healthy holder**
  (finding 25's second-opinion gate): sever one standby's
  control-plane path to the holder alone, leaving its link to the
  third node and the whole data plane intact. It watches the holder
  "die" for a full `leader_ttl` while the holder serves happily — and
  must not take the lease, because the third node has touched the
  holder within the ttl and says so. No takeover, no promotion, no
  fence.

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

1. ~~Holder AGENT death with PostgreSQL healthy~~ — **done: G8**. Ack
   starvation is now observed, not assumed, and the auditor's
   single-primary invariant gained the declared-window mechanism
   instead of an exemption blindspot.
2. ~~Crash-shape death~~ — **done: G9**. The missing-death-event
   problem was real: the fix is a per-node unit-journal tail, making
   systemd's `code=killed` the serving-interval end. Rejoin is a
   reclone, so crash recovery of the old pgdata itself is never run —
   that only becomes reachable with a restart-in-place path.
3. ~~Full-cluster cold restart of an ESTABLISHED cluster~~ — **done:
   G10**, and it found finding 21 before ever running: nothing in the
   steady-state design started a Down instance, so an established
   cluster stayed down forever after a blip. Fixed with cold-start
   reconciliation in `Agent::serve` (standby-shaped pgdata starts
   unconditionally; primary-shaped only on a quorum-fresh lease read
   naming self; uninitialized never).
4. ~~Write load through the failover~~ — **done: G11**, and it earned
   its keep before ever passing: finding 23 (candidacy livelock under
   load → the freeze) and finding 24 (stale timeline sources crowned
   a lagging winner → the waldir TLI + stability gate) both fell out
   of it. The ledger, the acked-row audit, and the starvation
   observation are now standing assertions.
5. ~~Built-but-never-entered states~~ — **done: G12 (blocked →
   allow-async → auto re-arm) and G13 (the follow-wedge tripwire)**.
6. ~~Asymmetric / partial partitions~~ — **done: G14 (data plane cut,
   control plane intact), G15 (the inverse), G16 (one-way
   blindness)**. G14 and G15 passed first time — the two plane
   inversions are exactly the pair that separates "lost redundancy"
   from "lost authority", and the design answers them oppositely and
   correctly. G16 cost a run and produced finding 25.
7. **Double faults + soak** — G19 (raft-store deletion), G20 (primary
   death mid-rebuild of the only other standby), G21 (N-cycle failover
   soak: slot debris, open journal entries). Still open from this item:
   agent restart during basebackup, which overlaps G20's half-wiped
   node closely enough to be a variant rather than a scenario.

   **Disk-full on the WAL partition: deliberately deferred, not
   pending.** It is a break-glass condition, and by the time it fires
   the operators are already in a fire drill with human judgment in
   the loop — which is the opposite of the automatic-reaction paths
   this suite exists to police. There is also little of OUR behavior
   left to assert: PostgreSQL PANICs on its own terms, and the agent's
   state store may be on the same partition that just filled, so the
   test would mostly be re-deriving PostgreSQL's documented failure
   mode through a very expensive fixture. Revisit only if the agent
   ever grows a policy for it (pre-emptive fencing on a disk-space
   threshold, say) — a policy is something worth testing; a PANIC is
   not.
8. `detach_false_primary` storm behavior (hook-contract §5.5), which
   needs a false primary manufactured out of band.
9. ~~`.rpm` flavor on a RHEL-family image (ROADMAP distro matrix)~~ —
   **done**: `rocky9-pg16` installs the real `.rpm` on Rocky 9 and runs
   the full suite. It found one product bug (finding 28) and one
   fixture bug, and cost less than the Debian version cells did,
   because the layout is read from the image rather than assumed.
10. ~~Close finding 22's slot race in the product~~ — **done**, and
    the investigation corrected the plan: slots-at-promote alone does
    NOT close the race, because a slot cannot retroactively protect
    segments written before it existed — a standby trailing INSIDE an
    older segment needs exactly those. So the fix is three-part:
    (a) `create_slot` now passes `immediately_reserve := true`, so a
    slot retains WAL from the moment it exists rather than from the
    moment a consumer first connects (without this, creating slots
    early buys nothing at all); (b) the executor reserves EVERY
    member's slot at promotion — the winner knows the member set and
    should not wait to be asked, which closes the window for a
    survivor that reconnects late; (c) `wal_keep_size` is the only
    thing covering the pre-promotion segments, so validate-env now
    WARNs below a 512MB floor (the agent does not manage this GUC —
    it is static deployment config, unlike synchronous_standby_names —
    but it must say plainly when the gap is left open), and the
    acceptance provisioning sets it. The auditor gained a standing
    invariant that would have caught finding 22 by itself: no standby
    may ever log "WAL segment ... has already been removed".
11. ~~The second-opinion gate before deposing a holder~~ — **done**.
    `NodeStatus` grew `peer_seen_age_ms` (per-peer contact freshness,
    recorded by the HA loop's own fan-out), candidacy consults it
    before deposing a holder it cannot see, and G17 manufactures the
    shape. Three unit tests pin the behavior: defer to a fresh
    witness, proceed once no witness has seen the holder either, and
    never let an UNREACHABLE witness's stale map veto a takeover.
12. ~~The rolling agent upgrade~~ — **done: G22**, and it was the one
    routine operation the suite had never run. Every other scenario
    kills the agent; this one UPGRADES it, installing the staged
    package over the running one (`dpkg -i` / `rpm -Uvh
    --replacepkgs`) so `postinstall.sh`'s restart-if-active branch is
    what stops and starts the daemon. Standbys first, holder last.
    Asserts absences — no takeover, no promotion, no fence, no
    PostgreSQL shutdown, same holder, quorum commit still armed.

    **The hazard is arithmetic, so the scenario measures rather than
    concludes.** A holder that stops renewing is deposed at
    `leader_ttl`, and an upgrade stops renewal for exactly as long as
    the restart takes; nothing in the product relates those two
    numbers, so the margin is a property of the deployment. G22 reads
    the window off systemd's own `ActiveEnter`/`InactiveEnter` stamps
    and prints the worst against the ttl. First green run: 88ms / 73ms
    / 123ms (the holder's is the slowest, as expected — it is the one
    that also re-arms), a worst case of **1% of this cluster's 10s
    ttl**, and 0.4% of production's 30s default.

    Two harness bugs first, both of the vacuous-pass shape this list
    keeps rediscovering: `docker cp` into `/tmp` reported success
    while compose's tmpfs hid the file from everything in the
    container (now `/var/tmp`, and the node must `test -s` it itself);
    and the window measurement had no freshness test, so it reported a
    plausible 96ms belonging to a restart an EARLIER scenario caused,
    for an upgrade that never happened. Both edges must now fall after
    the caller's mark or the check fails outright. The non-vacuity
    assert that catches the whole class — `ActiveEnter` must have
    ADVANCED — is what turned "everything is green" into "the package
    never restarted anything".

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

21. **An established cluster never comes back from a full-site power
    blip.** Found statically while designing G10, confirmed by the
    code paths: PostgreSQL is agent-managed (disabled in systemd), the
    executor's demote policy ignores `Down` instances, and a holder
    whose PostgreSQL is down can only tick `WouldDemote` — so after
    every node reboots, the cluster sits with the lease intact in the
    persisted raft store and no PostgreSQL anywhere, forever. Peers
    can't even depose usefully: candidacy needs a local flush
    position, which needs a running standby. FIXED with cold-start
    reconciliation in `Agent::serve` (between the phantom check and
    the HA-loop spawn, so a primary starting through crash recovery
    never races the loop's fence): standby-shaped pgdata
    (`standby.signal`) starts unconditionally — divergence lands in
    the finding-15 wedge tripwire, never in a serving primary;
    primary-shaped pgdata starts only on a quorum-fresh lease read
    naming this node (no rival could take over while the site was
    dark — takeovers need the very quorum that was down); deposed
    ex-holders read `holder != self` and stay down, preserving the
    demote policy; uninitialized pgdata (greenfield pre-init, or a
    blip-interrupted reclone) is never touched. G10 asserts the blip
    is not a failover: same holder, same term, no fence, crash
    recovery observed, acked sentinel intact.

    The first cut earned its own sub-finding: the quorum poll ran 60 s
    ahead of `sd_notify(READY)` while the unit ships
    `TimeoutStartSec=30s` — and a VIRGIN Debian pgdata (the package's
    default cluster: `PG_VERSION`, no `standby.signal`) is
    indistinguishable from a primary's, with a pre-membership store
    that never answers. Every greenfield standby boot therefore polled
    to the budget and was killed by systemd mid-poll, failing G0/G1
    across the board. The budget is now 10 s: on a synchronized blip
    the standby-shaped peers start without consulting the store, so
    quorum exists within seconds; a holder that boots long before its
    peers exhausts it and stays down — and the site still heals,
    because the auto-started standbys give candidacy a live flush
    position and the majority promotes past the cold ex-holder.

    The second G10 run found three more, all environment/harness:
    provision.sh restarted PostgreSQL itself on every container boot —
    a crutch predating the reconcile that preempted the exact product
    path under test (the cluster "recovered" in 4 s with zero
    cold-start involvement); pgpool was unmasked+started but never
    ENABLED, so no router came back after the blip and every healthz
    went 503 (`curl -sf` then reads armed sync as ""); and the
    harness's `Pg` resolved container IPs ONCE at suite start, while
    the blip's sequential `docker start` reshuffles them — "db2"
    queries interrogated what had become db0, so node-specific checks
    (streaming count) failed against a fully-recovered cluster while
    set-shaped checks (count_primaries) kept passing. Wrong-node
    evidence is the worst kind of green: IPs now resolve per redial.

    And the pgpool-enable fix bred a fourth: `pgpool2.service` carries
    `Wants=postgresql.service`, so enabling the router made systemd
    pull Debian's postgresql meta-service at boot — whose generator
    starts every cluster whose `start.conf` says `auto` (the package
    default), regardless of `postgresql@17-main` being disabled.
    PostgreSQL was up 2 s before the agent on every post-blip boot;
    the reconcile correctly no-op'd on a running instance, silently,
    and only the missing journal lines gave it away (diagnosed via
    validate-env's DB-backed checks passing at a boot where PostgreSQL
    should have been down). An agent-managed deployment must set
    `start.conf = manual` — explicit `systemctl start` paths
    (bootstrap, recover, cold start) are unaffected. A false alarm
    from the same investigation, worth keeping: the raft store's
    9-entry log recovered IDENTICALLY on all three nodes after the
    hard kill, holder and term intact — redb's Immediate durability
    held under power loss exactly as the storage comments claim.

22. **The slot-creation race at failover: a surviving standby can lose
    its WAL window before its slot exists on the winner.** In a G9
    run, db1's FLUSH was quorum-current but its REPLAY trailed inside
    segment 0x11 when the primary died; the winner's post-promote
    checkpoint recycled 0x11 before db1's re-follow created its slot
    on the new primary (~2 s later), leaving "requested WAL segment
    has already been removed" + a timeline history file no peer had
    archived. Unrecoverable without reclone — and the finding-15
    tripwire fired exactly as designed ("follow WEDGED … until
    `cluster recover` rebuilds this node"). The HARNESS bug it
    exposed: repair_standbys' wedge scan only knew the "forked off"
    signature, and the executor's re-follow loop emits a fresh
    "now following" event each cycle, so the follow-event gate
    shielded the wedged node from the stuck-fallback for the full
    budget. The tripwire log line is now itself a wedge signature —
    the repair consumes the product's own loud diagnosis. FIXED in the
    product since (gap item 10): slots reserve WAL at creation, the
    executor reserves every member's slot at promotion, and
    validate-env warns when `wal_keep_size` leaves the pre-promotion
    gap open. The auditor now fails the run outright if any standby
    logs "WAL segment ... has already been removed".

25. **One-way blindness toward the RAFT LEADER is real quorum loss —
    and the correct response is to fence, not to shrug.** Not a
    product defect: a wrong assertion, recorded because the reasoning
    took a run to get right. G16's first cut blinded the lease
    HOLDER's outbound dials to one standby and asserted that nothing
    should move, on the theory that one node's lost view of a peer
    cannot be evidence about the cluster. The run failed the
    assertion: the holder logged "store unknown for 8.0s >
    retry_timeout 2.0s while holding the lease (quorum contact lost)",
    fenced itself, and a standby cleanly took over. The explanation is
    that the raft LEADER and the lease HOLDER are independent roles —
    the holder forwards its linearizable reads to whichever node leads
    raft, so blinding it to that node makes it unable to verify it
    still holds the lease, which is exactly the fail-closed case the
    design prices. Every safety invariant held throughout (one fence,
    one takeover, one term, no dual-primary), so the system was right
    and the test was wrong. The lesson generalizes: **any assertion
    about a partial control-plane cut must be stated relative to the
    raft leader, which the suite does not control.** G16 is now
    deterministic instead: it severs a STANDBY's outbound control
    plane, so the blind node can reach no quorum member at all and
    therefore cannot win a CAS no matter who leads raft — the
    strongest form of "one-sided evidence is not authority", provable
    without knowing the leader.

    The other half — a standby that can still reach the raft leader
    but NOT the holder, whose CAS would succeed and depose a healthy
    primary on one node's blindness — was left open here and CLOSED
    since (gap item 11): the second-opinion gate. Every node reports
    how long ago it last observed each peer SERVING as a primary
    (`NodeStatus.peer_primary_seen_age_ms`), and a candidate about to
    depose a holder it cannot see stands down when any reachable
    member has watched that holder serve within `leader_ttl`.
    Self-clearing: a dead holder ages out of every witness's map
    within one ttl, so the gate costs a bounded delay and can never
    deadlock. G17 manufactures the shape;
    docs/promotion-authority.md §"Lease semantics" carries the
    reasoning, including the disjoint-partition premise the original
    case analysis left unstated.

    **The first cut of the gate recorded REACHABILITY, and that was a
    serious mistake — G3 caught it in one run.** When a holder's
    PostgreSQL dies, its agent keeps answering `GetStatus` perfectly,
    so every witness truthfully reported "I reached the holder 1.1s
    ago" and every candidate deferred: the single most common failover
    in existence deadlocked, and the suite sat for 292 s where it
    normally takes 25. A witness must vouch for the ROLE, not the
    socket. The lesson is the same one findings 18 and 23 taught from
    other directions — evidence has to name exactly what it observed,
    because "I can talk to it" and "it is doing its job" are different
    claims about a node, and only one of them is about the lease.

26. **The packaged binary has no glibc floor: it installs on an older
    distro and dies at exec.** Found within minutes of standing up the
    OS matrix, before a single scenario ran. The `.deb` declares no
    `Depends` at all (`dpkg-deb -f` shows none — nfpm is given no
    dependency list), while the binary is dynamically linked against
    whatever glibc built it. On Debian 12 (glibc 2.36) the package
    installs cleanly and then:

        /usr/bin/pg_agentd: /lib/x86_64-linux-gnu/libc.so.6:
        version `GLIBC_2.39' not found

    A trixie-built package therefore supports glibc >= 2.39 and says so
    nowhere; the same arithmetic rules out RHEL 9 (glibc 2.34) for the
    `.rpm`, which is testing gap item 9's whole target. Ubuntu 24.04
    (2.39) passes only by coincidence of being new enough. Fix
    directions are in TODO.md: pick a baseline and build against it (an
    old container, or `cargo-zigbuild --target
    x86_64-unknown-linux-gnu.2.28`), or remove the floor with a static
    musl build — for which this project is unusually well suited, since
    its dependency set is already pure Rust where it counts. Moving to
    `cargo-deb` helps either way: it derives `Depends` from the built
    binary, turning this from a cryptic runtime crash into a clean apt
    refusal. Debian 12 stays out of the matrix until then, because a
    permanently-red cell teaches nobody anything the finding has not
    already recorded.

    **RESOLVED by switching the release build to static musl**
    (`x86_64-unknown-linux-musl`), which removes the floor rather than
    documenting it. Prerequisite was dropping an accidental
    `aws-lc-rs`: the workspace pinned rustls to `ring`, but declared
    `tokio-rustls` with default features on, and its default enables
    `rustls/aws_lc_rs` — additive features then turned it on for the
    whole graph, compiling a cmake+C crypto library nobody asked for.
    (`tonic` had it right already; we were the only source.) With that
    gone, ring's C/asm was the only native code left and the musl build
    worked first try in 98s. All three binaries are `static-pie`, and
    the resulting `.deb` installs AND runs on Debian 12, Ubuntu 24.04,
    Rocky 9 (glibc 2.34 — gap item 9's target) and Alpine. The full
    suite passes 257/257 against the static agent, which is the part
    `--version` cannot tell you: mTLS peer mesh, D-Bus, tokio-postgres,
    and peer hostname resolution through musl's resolver rather than
    glibc's all work, including across G10's full-cluster restart.
    Debian 12 is now a matrix cell, and it is the cell that will catch
    a revert to dynamic linking.

    A second, smaller lesson from the same hour: the matrix's first run
    failed everywhere with "pgpool_node_id specifies id=0 which is not
    in pool". The cause was three layers from the symptom — the image's
    `ENV PG_VERSION` does not reach a systemd UNIT (systemd hands its
    services a clean environment), so under provision.sh's `set -u` the
    script died inside the config heredoc and wrote a config with an
    EMPTY pool. It reproduced under `docker exec` not at all, because
    exec DOES inherit the image env. Build-time facts a systemd unit
    needs belong in a file, not the environment.

27. **The polkit rule was pinned to PostgreSQL 17, so the agent could
    not manage PostgreSQL on any other version.** The matrix's first
    real cell (Ubuntu 24.04 / PostgreSQL 16) failed `cluster init` with
    every peer stop returning
    `org.freedesktop.DBus.Error.InteractiveAuthorizationRequired`. The
    rule matched the unit name literally — `postgresql@17-main.service`
    — so on a 16 cluster it never matched, polkit fell through, and the
    fallback is to ask a human, which no daemon can answer. Two things
    make this worth recording beyond the one-line fix (match
    `/^postgresql@\d+-main\.service$/`):

    - `systemd.rs`'s module docs *already* described the rule as
      covering `postgresql@*.service`. The fixture had drifted from its
      own stated contract, and nobody could notice while every test ran
      on 17. Documentation that describes the general case while the
      artifact implements a special case is invisible until something
      exercises the difference — which is precisely what a matrix is
      for.
    - The file is the test fixture, but its header says it mirrors what
      Ansible installs in production. Anything copied from it inherits
      the pin, so a real PostgreSQL 15/16 deployment would hit the same
      wall — with the same unhelpful error, since "interactive
      authentication required" describes polkit's fallback rather than
      the actual cause.

    I first read this as a Debian-vs-Ubuntu polkit difference (126 vs
    124) and was wrong; the versions were a coincidence. The tell was
    that db0 could manage its OWN PostgreSQL — provisioning starts it
    as root — while only agent-issued peer operations failed.

28. **The agent read pgpool's node-id file at a Debian-only path, so
    on RHEL it silently skipped the file it exists to share.**
    `resolve_local_node_id` probes `/etc/pgpool2/pgpool_node_id` as
    step 3 of four, the whole point being that one Ansible step writes
    one file that *both* pgpool and the agent read. RHEL's pgpool keeps
    its config in `/etc/pgpool-II`, so on that family step 3 always
    missed and resolution fell through to step 4, the hostname match.

    What makes this worth a finding rather than a one-line diff is that
    it is **invisible when it fires**. There is no error: the hostname
    fallback answers correctly whenever the host is named like a pool
    entry, which is true of this harness (`db0`..`db2`) and true of
    plenty of real deployments. The agent would come up, resolve the
    right id, and log nothing — until a RHEL host whose hostname is not
    its pool name, where the agent gets `NoLocalNode` (or, worse, the
    operator's intended id is simply ignored in favour of a matching
    hostname that means something else). A green Rocky run does not
    disprove it; the fallback is what kept the run green.

    Fixed by probing both spellings in order. The contrast with the
    other family-specific paths is the lesson: `data_dir`,
    `pg_install_prefix`, and `service` are all config keys, so an
    operator on RHEL sets them and moves on. This one was never a key,
    because "it's pgpool's own file" — which is exactly why it had to
    learn both of pgpool's own spellings.

    A fixture bug of the same shape came out with it, and this one the
    suite could *not* have caught: the polkit rule matched
    `pgpool2.service` and `pgpool.service` but not RHEL's actual
    `pgpool-II.service`. The suite runs with `[supervisor] pgpool =
    false`, so it never asks polkit about pgpool at all — the rule is
    production's, not the suite's. Fixed by inspection, prompted by
    knowing the unit name at last; noted here because "the matrix went
    green" is not the same claim as "the matrix exercised it".

29. **PGDG's RHEL unit ships `Restart=on-failure` ACTIVE, so on that
    family systemd resurrects a postmaster the suite just killed —
    and, in production, one the agent just fenced.** Debian's
    `postgresql@.service` ships the same line commented out. G9's own
    comment relied on that ("Debian's unit ships Restart commented
    out, so the corpse stays down"), and the assumption held for as
    long as Debian was the only family.

    On the Rocky cell the SIGKILLed primary was back inside a second
    (`database system is ready to accept connections`), so the lease
    never expired, no standby was ever deposed, and the harness then
    tried to recover *via* a node that had never been promoted
    (`cluster recover: local node is not the primary (in recovery)`).
    Thirteen scenarios ran against a cluster no assertion expected: 32
    failures, every one downstream of this single fact, and the run
    read as "Rocky is slow" because failures are timeout budgets.

    The suite's fix is a `Restart=no` drop-in in provisioning, written
    for both families rather than branching on the RHEL one — a suite
    whose crash shape depends on which distro it booted proves less
    than it appears to.

    **The product question is the interesting half and is NOT closed
    by that drop-in.** Disabling the unit — which the deployment does
    — stops boot-time autostart, not `Restart=`. So on RHEL, a fence
    that stops PostgreSQL can be undone by systemd if the stop is
    recorded as a failure, and the agent's "a fenced node stays down"
    assumption is Debian-shaped. Tracked in TODO.md; `validate-env` is
    the natural place to catch it, since it is precisely the class of
    silent localhost misconfiguration that check exists for.

    Two meta-lessons, both familiar from finding 28. The suite went
    green on this cell yesterday with the same unit file, so the
    behaviour is timing-dependent and a single green run proved less
    than it looked like. And the difference lives in a file nobody
    writes — a packaged unit, not a config key — which is the same
    place the pgpool node-id path was hiding.

30. **The Rocky cell is timing-marginal: failures track how SLOW the
    run was, not what the code did.** After finding 29's drop-in
    removed the cascade, 8 failures remained. Four runs, and the
    correlation is the whole finding:

        cell time   failures
        1127s       9
        1223s       8
        1354s       8
         743s       0   (+1 from an over-strict new audit check)

    Slow run, failures; fast run, clean. Every failure was an
    `await_event` blowing its budget while the `wait_until` polls
    around it passed — the outcomes were all there, the awaited log
    lines just arrived late. The suite runs against `leader_ttl = 10s`
    (deliberately tight, see the provisioning comment), so the margin
    on a loaded host is thin. **A red Rocky cell is not evidence of a
    product bug until the cell time is checked.**

    Two hypotheses died on the way here, and how they died is the
    useful part.

    *Journald rate limiting.* Plausible — the agent's events ride
    `journalctl -f`, and journald drops above 10k/30s. Measured
    instead: **0 lines in 60s** at idle, 52 since boot, zero
    suppression anywhere on the box. The agent is nearly silent by
    design (`log_decision` drops to `debug` when the decision is
    unchanged), which is the opposite of the assumed failure.

    *A silently dying event tail.* `spawn_tail` respawns tail-only, so
    a died exec loses its gap for good — and two consecutive runs had
    byte-identical failure lists that all named db0, with G21 failing
    exactly the cycles db0 won (1 and 3) and passing the one db1 won
    (2). A compelling story, and wrong. The census added here says all
    18 deaths in a clean run happen in **G10**, which SIGKILLs PID 1 in
    all three containers by design — every stream on every node dies
    there, and every assertion still passed. Two identical runs are not
    determinism; the third contradicted both.

    What survives is the instrumentation, which is worth having on its
    own terms. Every run now prints a per-stream event census and
    names any tail that died, because `check_absent` — used throughout
    the suite — passes VACUOUSLY on a stream nobody is listening to.
    A dead tail would report a well-behaved cluster that simply never
    acted: silent, and in the reassuring direction, which is the worst
    way for a test to be wrong. The audit also fails outright if any
    node produced no agent events at all. Neither condition has fired
    in anger yet; both are cheap insurance against the class.

23. **Strict flush-max candidacy livelocks under write load — the
    fence-less deposal never completes.** G11 (the G8 agent-death
    deposal under a continuous ledger writer) ran its kill and then
    NOTHING happened for 90 s: both standbys entered candidacy at ttl
    and each stood down deferring to the other — db0 logged "node 1
    has more flushed WAL", db1 logged "node 0 has more flushed WAL",
    in the same second. Under load every candidate compares its own
    point-in-time flush against the peer's FRESHER status report while
    WAL advances ~70 rows/s, so everyone reads itself behind; and
    since nobody promotes, the deposed primary keeps serving and
    acking through the still-attached standbys, which keeps the WAL
    moving — a self-sustaining livelock. (The strict-selection comment
    even said it: "no livelock risk — candidacy runs against a dead
    primary, so flush positions are static." True for a dead primary;
    false for a dead agent.) FIXED with the candidacy freeze:
    detach-before-compare. A candidate still receiving gets
    `DetachingFromDeposed` (the executor rewrites `myrecovery.conf`
    conninfo-less and reloads — PostgreSQL keeps serving reads), and
    comparison waits until every counted candidate has stopped
    receiving. Frozen positions restore a total order, the deposed
    primary loses its last ack source the moment the candidates
    detach (completing §3's ack-starvation fence and closing the
    winner-promotes-while-survivor-still-acks loss window), and
    strict-max on frozen positions provably holds every ANY-1-acked
    byte. See docs/quorum-commit.md §4.

24. **A detached standby reports a stale-low timeline — and candidacy
    compares timelines FIRST, so the lagging standby won.** The
    standby timeline read COALESCEd `pg_stat_wal_receiver.received_tli`
    (gone the moment the receiver is — exactly what the finding-23
    detach produces) into the control-file checkpoint TLI, whose
    staleness was documented as "conservative" because the phantom
    check only fears HIGHER peers. Candidacy inverted that: in the
    first post-freeze run, G7's caught-up standby — standby-since-init
    with no restartpoint yet — reported TL1 after detaching, read its
    flush-lagging rival's TL3 as "a newer timeline", stood down, and
    the LAGGING node promoted past 24 MiB of missing flushed WAL: the
    g7 sentinel (an acknowledged write) was lost. The exact §2.2
    defect, resurrected by an observability bug two layers down.
    It took three layers to fix, and the middle one was a WRONG THEORY
    worth recording. Layer 1: replace the COALESCE fallback with
    GREATEST over receiver/control sources. Layer 2 (wrong): when the
    fixed run failed identically, the leading theory was a flush-report
    dip across detach (`pg_last_wal_receive_lsn()` nulling with the
    receiver) — a live experiment FALSIFIED it (the value persists in
    shared memory; the flush report never dips), but the stability
    gate built for it stays: positions enter a comparison only after
    two consecutive identical samples, and a rival that VANISHES
    between samples (one transient status Err) defers the comparison
    instead of silently leaving it — single-tick observation noise
    must never decide a takeover. Layer 3 (the truth, nailed by the
    new comparison-table log line): `min_recovery_end_timeline` is
    ALSO restartpoint-stale — a streaming standby suppresses
    min-recovery-point control-file updates until a restartpoint, so
    every control-backed timeline source on a standby-since-clone can
    read TL1 for many minutes, and GREATEST over stale sources is
    still stale. The one source that is both live and durable for a
    DETACHED standby is the pg_wal directory itself: the receiver
    wrote the segments, and the max WAL filename's first 8 hex chars
    carry the received timeline. The standby TLI is now GREATEST over
    received_tli, the waldir scan, min_recovery_end_timeline, and the
    checkpoint TLI. Also from the same runs: G11's "+200 acked past
    the kill" target was met entirely inside the pre-detach ack window
    (kill → leader_ttl, when acks legitimately still flow through the
    fence-less primary), so the writer stopped before ever writing to
    the winner — the resumption target is now anchored at the
    PROMOTION, proving starve → discover → resume; and candidacy now
    logs its full comparison table (every position, receiving flag,
    and primary claim) — findings 19/23/24 all hinged on what each
    node believed at that instant, and none of it was recorded.
