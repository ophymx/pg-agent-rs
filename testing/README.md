# Acceptance testing — dockerized 3-node cluster

Proves cluster behavior end to end against the **real artifacts**: the
packaged `.deb`/`.rpm`, the packaged systemd unit (sd_notify,
ExecStartPre `validate-env`), the polkit rule, real mTLS between agents,
and real PostgreSQL streaming replication — three systemd-booted
containers on a compose network.

**The model under test is lease-driven roles** — the only model there is,
since a daemon either joins the raft lease or refuses to start. pgpool is
present strictly as a router; it commands nothing.

Discoveries made while building and running this suite are in
[FINDINGS.md](FINDINGS.md); several are cited by number from code
comments and the design docs.

## Run

```
testing/acceptance.sh              # build package, up, suite, down
KEEP=1 testing/acceptance.sh       # keep the cluster running afterwards
SKIP_BUILD=1 testing/acceptance.sh # reuse dist/
```

On failure the cluster is kept for debugging (`docker exec -it pga-db0
bash`, `journalctl -u pg_agentd`).

### The OS / PostgreSQL matrix — on demand, not on every run

`acceptance.sh` always runs the baseline cell (Debian 13, PostgreSQL 17)
and needs no environment. The matrix is a separate, opt-in entry point
that runs the whole suite once per cell:

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
ships **natively** (no PGDG repo), so a build failure means "this pairing
does not exist" rather than "an external repo moved". The Rocky cell
cannot follow that rule: RHEL ships no pgpool-II from any of its own
repos, so PGDG is the only source — which is also what RHEL deployments
actually use.

The three Debian-family cells vary the PostgreSQL version against one
layout. `rocky9-pg16` varies the **layout**: a per-version unit instead
of a per-cluster template, `postgresql.conf` inside `PGDATA`, no packaged
`initdb`, no `pg_ctlcluster`, a different postgres home, and pgpool under
a different name in a different directory. Every one of those is a place
something could have been hard-coded, and two things were (findings 27
and 28).

Nothing in the suite is allowed to *guess* which layout it is in. Each
image writes `/etc/pg-agent-matrix/env`, and provisioning, the pgpool
setup, and the harness (`cluster::Facts`, read over `docker exec`) all
read that one file. `FAIL_FAST=1` is what you want when standing up a NEW
cell: the suite is cumulative, so once provisioning is wrong every later
scenario reports the same finding for another ten minutes.

Cells run sequentially — they share the docker daemon and the compose
project name — and each tears down the previous cluster first, because a
leftover container from cell N runs cell N's PostgreSQL.

`bookworm-pg15` earns its place twice over: it is the oldest supported
base, and it is the cell that catches a regression to a dynamically
linked build (finding 26).

## Layout

| File | Role |
|---|---|
| `compose.yaml` | 3 nodes (`db0..db2`), privileged + `cgroup: host` so systemd is PID 1 |
| `docker/Dockerfile` | Debian-family base + systemd + PostgreSQL + the `.deb`; pgpool2 installed but masked (BOOTSTRAP.md Phase 1.1) |
| `docker/Dockerfile.rhel` | the same for Rocky 9: PostgreSQL + pgpool-II from PGDG, the `.rpm`, `pgpool-II.service` masked |
| *(both)* `/etc/pg-agent-matrix/env` | the cell's layout facts — unit, PGDATA, bins, config dir, postgres home, log path, pgpool unit + config dir. Written by the image, read by everything below and by the harness over `docker exec`. Nothing infers the layout; one file states it |
| `docker/provision.sh` | boot-time provisioning = Ansible's Phase-1 role: node id, TLS, config.toml, pg_hba, roles/extension, archive dir. Branches on `PG_FAMILY` only where the families differ in KIND rather than spelling |
| `docker/pgpool-setup.sh` | operator-run pgpool config + start (BOOTSTRAP Phase 1.4 + 3.1), in the target hook-contract shape. `pid_file_name`, `logdir` and `pool_passwd` are set explicitly: their compiled-in defaults differ per family, and on RHEL two of them point somewhere pgpool cannot write |
| `docker/50-pg-agent.rules` | polkit grant (postgres user → manage PG/pgpool units), installed to `/etc/polkit-1/rules.d/` |
| `gen-certs.sh` | one CA + per-node certs, SAN = compose hostname (matches the peer SAN allowlist) |
| `acceptance.sh` | thin launcher for the Rust harness |
| `acceptance/` | the scenario driver (Rust, workspace member): tails every node's agent journal + PostgreSQL log into one ordered event log, holds live `tokio-postgres` connections to each node, and asserts on event ORDER — scenario windows are event cursors, "did X happen" awaits the event log, and absence claims cover windows bounded by awaited events rather than sampled instants |

## Scenarios

- **G0** — greenfield boot: all nodes come up through the validate-env
  gate, the pool forms itself from `[[pool]]` with no operator command,
  and raft elects. No executor acts before membership exists.
- **G1/G1b** — `cluster init` does replication + lease seeding in one
  operator command, and finds membership already formed by startup
  (idempotent either way — it still forms the pool if it genuinely got
  there first); executors converge the standbys; pgpool comes up in the
  agent-led contract and `/healthz` reports ready.
- **G2** — `check-hooks` passes clean on the deployed conf: the canonical
  `gen-pgpool` block IS what's deployed, no overrides.
- **G2b** — pgpool stays a router: a detach neither propagates between
  instances nor breaks replication (the agent refuses the slot drop for a
  streaming standby); explicit attach clears it.
- **G3** — primary death: the failover hook answers **advisory**, the
  lease promotes exactly one standby (journaled), the survivor
  re-points, pgpool discovers the new primary via `sr_check`.
- **G4/G4b** — operator rejoin: the dead ex-primary stayed stopped
  (demote policy), `cluster recover` rebuilds it with the
  recover/failover-hook slot race guarded (G4b exercises the live race on
  a running standby).
- **G5/G5b** — partition of the holder: the isolated node fences itself,
  the majority commits exactly one takeover (hysteresis asserted: no two
  takeovers within `leader_ttl`), **one primary during and after the
  partition**, rejoin via recover.
- **G6** — agent restarts are non-events: phantom check confirms, the
  lease survives, no takeover churn, replication uninterrupted.
- **G7** — real WAL lag (replay paused, ~24 MB written), primary killed:
  the caught-up standby wins, the lagging one stands down naming the gap.
- **G8** — **holder AGENT death, PostgreSQL healthy** (mask + SIGKILL):
  the one deposal with no fence. The majority promotes while the deposed
  primary keeps serving — a **declared dual-serving window** the auditor
  verifies is covered AND eventually closed — then asserts the
  quorum-commit contract that makes it safe: the acked sentinel survives
  onto the winner, a write on the deposed primary **starves**, and the
  never-acked row does not survive the rejoin.
- **G9** — **crash-shape primary death** (SIGKILL the postgresql cgroup):
  no shutdown checkpoint, no walsender drain, no log-file goodbye — the
  suite tails the unit journal so systemd's `code=killed` is the death
  event. The corpse stays down, the acked sentinel survives, and the
  rejoin reclones the marker-less pgdata back into a standby.
- **G10** — **full-cluster cold restart** (SIGKILL PID 1 in all three
  containers, then power back): the holder reads its persisted lease,
  starts its primary through real crash recovery, and the suite asserts
  the blip is NOT a failover.
- **G11** — **the fence-less deposal under WRITE LOAD**: a continuous
  ledger writer runs through the G8 deposal, upgrading data-survival from
  one at-rest sentinel to the actual quorum-commit invariant — EVERY
  acknowledged row exists on the winner, checked seq by seq.
- **G12** — **the quorum-commit states nothing had ever entered**: stop
  PostgreSQL on BOTH standbys with their agents up → `sync_commit=blocked`
  and a write BLOCKS rather than silently becoming single-copy. Then
  `cluster allow-async --confirm` disarms (journaled), writes flow, and
  the executor AUTO re-arms at the first attach.
- **G13** — **the follow-wedge tripwire**: sever one standby's
  replication path, kill its walreceiver, and the executor must declare
  `follow WEDGED` past the grace and expose `follow_wedged=true`. The
  tripwire is a REPORT — the wedged node neither promotes nor is
  destructively rebuilt.
- **G14 / G15** — **the plane inversions**. G14 cuts the DATA plane while
  every agent still sees a healthy holder: no failover at all, instead
  `sync_commit=blocked` and both wedge tripwires up. G15 cuts the CONTROL
  plane on the holder while replication stays perfect: the holder must
  fence a database healthy by every data-plane measure, because it can no
  longer prove it holds the lease. Lost redundancy and lost authority,
  answered oppositely.
- **G16** — **one-way blindness**: a standby loses the ability to
  INITIATE control-plane connections while remaining answerable. It can
  reach no quorum member, so it cannot act — one-sided evidence is not
  authority — and its replication is never disturbed.
- **G17** — **a blind standby must not depose a healthy holder**: sever
  one standby's path to the holder alone. It watches the holder "die" for
  a full `leader_ttl` while the holder serves happily, and must not take
  the lease, because a third node has touched the holder within the ttl
  and says so.
- **G18** — **maintenance mode**: `cluster pause` on one node, observed
  on ANOTHER (it is replicated, not local), then kill the primary and
  watch the cluster deliberately NOT fail over; `resume` restores it.
  Until `cluster pause` shipped, nothing could set the flag, so
  `HaDecision::Paused` had never once executed.
- **G19** — **raft-store deletion**: the recovery the design leans on to
  call the storage engine low-stakes — stop the agent, delete
  `<state_dir>/raft/`, restart, let Raft re-replicate — executed rather
  than asserted.
- **G20** — **double fault**: the primary dies mid-rebuild of the other
  standby, so one standby is half-wiped and the cluster's only viable
  candidate is the untouched one. The wiped node must not win.
- **G21** — **soak**: N-cycle failover, asserting by COMPARISON across
  cycles that nothing accumulates — orphan slots, unclosed journal
  entries, bookkeeping falling behind terms and timelines.
- **G22** — **rolling agent upgrade**: the staged package installed over
  the running one so `postinstall.sh`'s restart-if-active branch is what
  stops and starts the daemon. Standbys first, holder last. Asserts
  absences — no takeover, no promotion, no fence, no PostgreSQL shutdown.
  The hazard is arithmetic (a holder that stops renewing is deposed at
  `leader_ttl`), so the scenario **measures** the restart window off
  systemd's own `ActiveEnter`/`InactiveEnter` stamps and prints the worst
  against the ttl rather than concluding.

## Instrumentation worth knowing about

Three mechanisms exist because a test that is wrong in the reassuring
direction is the worst kind (findings 21, 30).

- **Per-stream event census.** Every run prints how many events each
  node's each stream produced and names any tail that died. `check_absent`
  passes VACUOUSLY on a stream nobody is listening to, and the audit fails
  outright if any node produced no agent events at all.
- **Tail watchdog.** A `docker exec` whose container restarts underneath
  it stops delivering *without dying* — no EOF, no exit. A per-node
  watchdog polls `docker inspect {{.State.StartedAt}}` every 3 s and
  forces a re-attach when it changes. The restart is the signal, taken
  from docker rather than inferred from silence.
- **Late-arrival sweep.** `await_event` can only report "not within the
  budget". Every timed-out await is re-run against the finished log and
  labelled **LATE** (matched, after the wait gave up — budget too tight),
  **NEVER** (nothing matched in the whole run — the only kind worth
  reading as a product failure), or **MISSED** (the log already held the
  line when the wait gave up — not a cluster fact at all, and a check
  rather than a note, because it invalidates every await in the run).

## Coverage gaps

1. `detach_false_primary` storm behavior (hook-contract §5.5), which
   needs a false primary manufactured out of band.
2. Agent restart during basebackup. Overlaps G20's half-wiped node
   closely enough to be a variant rather than a scenario.

**Disk-full on the WAL partition: deliberately deferred, not pending.**
It is a break-glass condition, and by the time it fires the operators are
already in a fire drill with human judgment in the loop — the opposite of
the automatic-reaction paths this suite polices. There is also little of
OUR behavior left to assert: PostgreSQL PANICs on its own terms, and the
agent's state store may be on the partition that just filled. Revisit only
if the agent grows a policy for it (pre-emptive fencing on a disk-space
threshold) — a policy is worth testing; a PANIC is not.
