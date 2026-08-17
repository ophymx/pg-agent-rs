# Promotion authority — relocating the failover decision

**Status:** implemented through step 7 (§10). The HA loop, embedded
Raft, executors, and the flipped pgpool contract are all landed and
exercised on the acceptance cluster; `[raft] enabled = true, shadow =
false` is the cutover switch, off by default — deployments opt in.
Step 8 (role-aware `/healthz` + the HAProxy split) remains.

Companion to [SPEC.md](../SPEC.md) (SPEC §5.1),
[ROADMAP.md](../ROADMAP.md) ("Shared cluster state"), and
[TODO.md](../TODO.md) ("pre-execution cluster-state validation pattern").

Section-reference convention: references to SPEC.md are always written
`SPEC §N`; a bare `§N` is a section of this document.

This document argues that the daemon's single most important defect is
**structural, not a bug**: pg-agent-rs derives "who should be primary"
from pgpool-II, and pgpool cannot answer that question. It proposes
moving the decision onto a quorum-backed lease held in a Raft log that
the agents replicate among themselves — no external DCS — and is explicit
about what that does and does not change.

The framing came out of a close read of Patroni. The conclusion is *not*
"use Patroni instead" — see [§8, What this preserves](#8-what-this-preserves).

---

## 1. The thesis

Compare pg-agent-rs and Patroni at the mechanism layer — promote, rewind,
basebackup, slot lifecycle, standby reconfiguration, health endpoint — and
they are near-identical. Ours is arguably richer.

They diverge on exactly one question: **who decides who's primary.**

| | Patroni | pg-agent-rs (today) |
|---|---|---|
| Decision made by | compare-and-swap on one key in a quorum store | pgpool's `failover_command`, executed by SPEC §5.1 |
| Serialization point | exactly one, linearizable | none |
| Can two nodes both win? | no, structurally | yes |

SPEC §5.1 step 4 is the whole story:

> **Primary down** (`detached.id == old_primary.id`):
> - `peers[new_main].Promote()`

`new_main` arrives as an RPC parameter from an untrusted caller. The
handler promotes whoever it is told to promote.

**pgpool's watchdog quorum is a failure detector, not a consensus
protocol.** A failure detector answers *"can I reach X?"* Consensus
answers *"is X primary?"* Those are different questions, and pgpool can
only answer the first. Treating the first answer as the second is the
root cause of everything in §2.

---

## 2. Two defects, one root cause

### 2.1 Split-brain from a false failure report (confirmed in production)

From [TODO.md](../TODO.md):

> **Confirmed in production:** on 2026-06-11, pgpool's `failover_command`
> announced `detached=db1, new_main=db0` after db1's pg_agentd briefly
> restarted. db1 was actually still primary and healthy — pgpool's quorum
> just couldn't reach the daemon during the restart window. Our handler
> trusted pgpool and promoted db0, creating split-brain.

The code did what the SPEC says. The SPEC trusts an authority that
structurally cannot know the answer.

**Reproduced on demand, 2026-08-09.** The production incident needed a
coincidence (a daemon restart inside pgpool's health-check window). The
structural claim does not: in the docker acceptance cluster
([testing/README.md](../testing/README.md) S13), isolating the primary
with `docker network disconnect` produces two primaries every time.
Measured timeline, from a clean three-node cluster:

```
t+0s    primary db0 isolated from the network
t+2s    db1 + db2 health checks fail → each pgpool fires failover_command
t+82s   db1 promoted by the majority side
        db0 still running as primary on the other side of the partition
```

Both sides behave correctly by their own lights: the majority cannot
distinguish "db0 is dead" from "db0 is unreachable" and must not stall
forever, and db0 has no reason to believe anything changed. That is
§3's dilemma with real timestamps on it, and it is the regression test
that must invert once the lease lands.

### 2.2 Candidate selection ignores WAL position

Independent of split-brain, and easy to miss. From the pgpool failover
documentation, `%m` (new main node) is selected as:

> the node being assigned the youngest (smallest) node id which is alive

**Lowest alive node ID.** Not most-advanced WAL. Not least lag. Not "is
it even caught up."

SPEC §5.1 *at the time* promoted that pick with no lag gate — so even
when pgpool was entirely correct that the primary was down, it could
hand us a candidate arbitrarily far behind, and we promoted it and
dropped the WAL delta on the floor. (Historical: the hook's promote
path is deleted; candidate selection now belongs to the lease's
candidacy, strict flush-max per docs/quorum-commit.md §4.)

Patroni picks by WAL position and refuses candidates past
`maximum_lag_on_failover` (default 1 MiB).

This is a data-loss defect hiding behind the availability defect. Both
dissolve if the decision moves; neither is fixed by validating harder.

---

## 3. Why the planned mitigation does not close it

[TODO.md](../TODO.md) proposes `validate_cluster_preconditions`, including:

> Check `peer.get_status(detached)` and refuse if
> `is_postgres_running && !is_in_recovery`

**Do this anyway** — it is cheap, it would have prevented 2026-06-11, and
preflight consistency across handlers is worth having on its own merits.

But it narrows the window rather than closing it, because it fails in
precisely the case it most needs to work. Consider a real network
partition between db0 and db1. db0 cannot reach db1's status endpoint:

- **Refuse to promote** → the cluster is unavailable during exactly the
  partition it exists to survive.
- **Promote anyway** → split-brain. The original bug.

There is no third branch. "Dead" and "unreachable" are indistinguishable
by asking around; that is not an implementation gap to engineer past, it
is the result that makes quorum-based consensus necessary. Any
check-then-act protocol without a quorum-backed serialization point has
this hole. More checks make it rarer, harder to reproduce, and no less
real.

Record `validate_cluster_preconditions` as **defense in depth**, not as
the fix, so nobody later reads it as closing the issue.

---

## 4. The gossip plane must never carry role

[ROADMAP.md](../ROADMAP.md) §"Shared cluster state" proposes
`GetClusterState` / `ProposeClusterState(version, payload)` with
last-writer-wins on a monotonic clock plus writer node id, and describes
it as:

> Conflict resolution is intentionally "last-writer-wins with humans in
> the loop" … Not as principled as Raft; far simpler than running etcd.

For its stated scope — a `paused` flag, a scheduled switchover, a
generation counter — that is a defensible call. Operator commands are
infrequent and divergence costs confusion.

LWW is **not linearizable**, so it must never be extended to carry role.
Two partitioned nodes both accept a proposal, both believe they won, and
they reconcile *after* both have been primary. That converts an
availability event into a data-loss event.

**Action:** the ROADMAP item carries a hard scope note stating role
assignment is out of scope for it — otherwise it is the natural place
someone puts role in two years. That note stands as long as the item does,
but the better outcome is that the item never ships: §5's state machine
subsumes it. See "What the state machine holds."

### Re-pricing the original tradeoff

The stated reason for avoiding a DCS was dependency cost. That was
mispriced: avoiding a serialization point did not avoid complexity, it
*relocated* it. Replay markers with TTLs, the `inflight_ops` journal, the
durable maintenance queue, best-effort cleanup contexts,
`bypass_replay_marker`, and now a shared precondition validator — a
substantial fraction of that machinery exists to compensate for not having
one.

But the original instinct was not wrong, only over-applied. It read
"we need consensus" as "we need to run a consensus *service*," and those
are separable. §5 takes the serialization point without the second daemon
by embedding Raft in the agent. The dependency being avoided was always
operational, not algorithmic — so pay the algorithmic cost, which is a
library, and skip the operational one.

Note what that does to this section's own warning: with a replicated log
in hand, the gossip plane does not need a scope note, it needs deleting.
See "What the state machine holds" in §5.

---

## 5. Target architecture

Draw the line Patroni draws — decision layer separate from mechanism
layer — but keep both inside `pg_agentd`. Patroni splits them across a
process boundary because it delegates to a DCS; the boundary that matters
is architectural, not operational.

Subsections here are deliberately unnumbered, so that `§5.1` unambiguously
means SPEC's `Failover`.

### Own the decision: embedded Raft

The serialization point moves *into* `pg_agentd` rather than beside it.
Each agent is a Raft member; the replicated state machine holds the
cluster's authoritative role assignment.

**Do not implement Raft — use [openraft](https://github.com/databendlabs/openraft).**
The protocol is not the part worth writing. What we implement is the
storage impl, the network impl, and the state machine, and those are
where our bugs will live.

Why embedded rather than an external etcd:

- **The failure domains fuse by construction.** The etcd version of this
  design leaned on "co-locate it so the store shares the database's
  failure envelope" — a deployment convention someone can violate.
  Embedded makes it definitional: the Raft cluster *is* the agent cluster,
  so "store unreachable" and "peer unreachable" are the same event. The
  argument stops depending on how it was deployed.
- **One deploy unit.** One systemd unit, one config file, one cert story,
  one thing for Ansible to manage. No second quorum to bootstrap, upgrade,
  back up, or recover.
- **The transport already exists.** The mTLS peer mesh plus `NodePool` is
  a `RaftNetwork` with the hard parts (identity, cert rotation, peer
  resolution) already solved.

What we give up, plainly: the option to move the store onto separate
nodes. Embedded means Raft's fsync traffic shares spindles with `$PGDATA`,
permanently. And **three nodes becomes a hard minimum** — a 2-node
deployment tolerates zero failures under Raft, where today it merely
degrades badly.

#### The separation that must not collapse

> **The Raft leader is not the PostgreSQL primary.** They are unrelated
> roles that happen to live in the same process.

This is the single most important invariant in the design, and embedding
Raft is exactly what makes it tempting to violate. Raft leadership churns
for reasons that have nothing to do with database health — a slow fsync, a
scheduler stall, a one-second network blip, a daemon restart. If PG primary
is defined as Raft leader, every Raft re-election is a database failover,
and we will have built a *more* eager version of the bug we are trying to
fix.

The lease is an **entry in the replicated state machine**. Raft leadership
is merely the mechanism by which entries commit. A Raft election changes
who proposes; it changes nothing about who runs PostgreSQL.

#### Lease semantics

With etcd you get TTL leases for free. With openraft the state machine is
ours, so the semantics have to be stated. The important realization:

> **Safety comes from the quorum, not from the timers.** The TTL governs
> how *eagerly* takeover happens. It is a liveness knob, not a safety one.

Work through it. Suppose a candidate takes over while the previous holder
is still healthy:

- If the old holder is in the **majority** partition, the candidate is in
  the minority and physically cannot commit the takeover. Nothing happens.
- If the old holder is in the **minority** partition, its next retain
  check cannot reach a quorum, so it demotes itself — and it does so
  whether or not it ever learns a takeover occurred.

Either way, at most one node has a committed lease *and* a reachable
quorum. That is the property SPEC §5.1 lacks, and it holds without any
assumption about synchronized clocks — only that each node's own monotonic
clock advances at roughly a real second per second.

**The premise in that case analysis, stated.** It reasons about *disjoint*
partition sides: every node is either in the majority or in the minority.
Real failures are not always cuts. Under **asymmetric** reachability — one
node's dials to the holder fail while everyone else reaches it, a firewall
rule, a one-way NIC fault — a candidate can be in the majority *and* the
holder can be in the majority, because "sides" no longer partition the
cluster. The safety invariant above survives (the CAS still admits exactly
one holder, and the deposed one fences as soon as it reads the store), but
what it buys is smaller than it looks: the takeover is *unnecessary*, and
between the CAS and the ex-holder's next read there is a window where a
healthy primary is still serving writes it can no longer have acknowledged
by a quorum. Cost paid for nothing, on the say-so of the one node that
could not see.

The store cannot arbitrate this — it has no notion of whether the incumbent
is alive, only of what the lease says. So candidacy asks the cluster
instead, and this is the **second-opinion gate**: every node reports how
long ago it last reached each peer (`NodeStatus.peer_seen_age_ms`, an age
rather than a timestamp, so no clock assumption is added), and a candidate
about to depose a holder it cannot see stands down if any *reachable*
member has touched that holder within `leader_ttl`. One node's blindness is
evidence about the observer as much as about the observed, and now the
decision says so.

The gate is self-clearing by construction: a genuinely dead holder makes
every witness's age exceed the ttl within one ttl, so failover proceeds
after a bounded delay and can never deadlock. It also does not touch the
paths that matter most — a fully isolated holder is unreachable to
*everyone* (no witness, gate opens), and a vacant lease has no incumbent to
defend. See testing/README.md finding 25, and G17 for the manufactured
case.

That reframing pays off concretely:

- **Retain is a read, not a write.** The holder confirms it still holds
  the lease via a linearizable read (openraft's `ensure_linearizable` —
  a ReadIndex quorum round-trip, no disk write). If it cannot complete one
  within `retry_timeout`, it demotes local PostgreSQL. Quorum contact is
  the thing being tested, so a read tests it exactly as well as a write.
- **The log only grows on real events.** Writes happen on holder change,
  pause/resume, and membership change — not every tick. Steady state is
  *zero* log entries per day. Log compaction and snapshotting stop being
  load-bearing, which removes an entire class of operational failure.
- **Takeover is a CAS** on `(expected_holder, expected_term)`, proposed
  after the candidate has observed the holder unhealthy for `leader_ttl`.
  Concurrent candidates are serialized by Raft; one wins, the rest see
  their expected-term precondition fail.

**Invariant:** `leader_ttl >= loop_wait + 2 * retry_timeout` — the holder
exhausts its retry budget and demotes before any candidate is eligible to
propose a takeover. This buys *hysteresis*, not correctness: violating it
causes unnecessary failovers, not split-brain.

**Second invariant:** `retry_timeout > worst-case Raft election duration`
(the election timeout's upper bound plus one round-trip). A linearizable
read cannot complete while an election is in flight — ReadIndex needs a
leader — so a retry budget shorter than an election converts every Raft
re-election into a demotion of a healthy primary. This is not a
hypothetical: it is the recurring field failure that got Patroni's
embedded-raft backend deprecated (see [prior art](#prior-art-patronis-raft-backend-pysyncobj)
below — "failed to update leader lock" during raft-plane churn, followed
by a spurious PG failover). Like the first invariant, violating it costs
availability, not correctness — but it is the availability failure this
design is most likely to actually exhibit, so it gets stated rather than
discovered. It also composes with the first invariant: raising
`retry_timeout` to clear elections raises the `leader_ttl` floor with it.

#### What the state machine holds

Small, and deliberately bounded:

```
leader     : { holder: node_id, term, since }
paused     : { bool, reason, set_by, at }
switchover : Option<{ target, not_before }>
membership : Raft's own configuration
generation : u64
```

**The log holds decisions, not progress.** `inflight_ops` stays local —
it is a journal of "what is this node in the middle of doing," and
replicating it would put every basebackup phase transition into the
consensus path. A promotion is one committed decision followed by a
locally-journaled orchestration.

Absorbing `paused` and `switchover` here has a payoff worth stating
outright: **the ROADMAP's gossip plane stops needing to exist.** §4 warns
that LWW gossip must never carry role. Once there is a replicated log,
LWW gossip is strictly worse at everything it was going to do, and the
right move is to delete the item rather than ship it with a scope note.
This direction removes planned work instead of adding it.

#### Storage engine

The state machine above is a few hundred bytes, and the honed lease
semantics mean near-zero steady-state writes. That is the whole basis for
the recommendation:

**Recommendation: `redb`, not RocksDB.**

| | redb | RocksDB (`rust-rocksdb`) |
|---|---|---|
| Implementation | pure Rust, CoW B-tree | C++ LSM, vendored + statically linked |
| Build cost | seconds, no extra toolchain | minutes cold; libclang/bindgen |
| Binary impact | hundreds of KB | tens of MB |
| Maturity | 1.0 in 2023, small maintainer base | ~decade at scale, huge deployment base |
| Tuning surface | nearly none | compaction, write amp, memtables |
| Background I/O | none | compaction threads competing with PG |
| openraft example | community only | canonical `rocksstore` |

RocksDB's advantages are real but they are all *throughput-and-scale*
advantages, and this workload has neither: a few hundred bytes of state
and, given the retain-is-a-read design above, zero steady-state writes.
Its costs are paid unconditionally — and it would be the first non-Rust
dependency in a tree that is pure Rust on purpose (`rustls` + `ring`
rather than OpenSSL; packaging ships self-contained binaries with soft
deps only).

Two observations collapse most of the remaining gap:

- **Neither engine is where the risk lives — our storage impl is.** Both
  paths must pass openraft's storage conformance suite (`openraft::testing`)
  in CI. Once that is a hard requirement, "RocksDB is better tested" stops
  transferring, because the tested part is the part we are not writing.
  **That requirement, not the engine choice, is what makes this layer
  trustworthy.**
- **A Raft node's log is recoverable from its peers.** On corruption, or a
  format change across a redb major version, recovery is: stop the agent,
  delete `<state_dir>/raft/`, restart, let Raft re-replicate. That makes
  the engine a genuinely low-stakes choice and argues for the cheaper
  dependency.

Log + snapshots live under `<state_dir>/raft/`, alongside the existing
`replay/` and `maintenance/` directories. **Flip to RocksDB if** writing
the redb storage impl turns out to fight the API badly enough that copying
`rocksstore` is worth the build cost — an implementation-time discovery,
not a design-time one, which is why it sits behind the trait. (`sled` is
openraft's third example backend; skip it — 1.0 never landed and it is
effectively unmaintained.)

#### Transport

A new `PgAgentRaft` gRPC service on the existing listener, same port, same
certs, same `NodePool` peer resolution.

One non-obvious requirement: **give Raft its own channel.** `AppendEntries`
heartbeats are small, frequent, and latency-critical; `Basebackup` streams
gigabytes. Sharing an HTTP/2 connection lets a saturated basebackup starve
heartbeats at the TCP layer and trigger a spurious election — during a
recovery, which is exactly when we least want one. Separate connection,
same endpoint.

Keep the store behind a trait (SPEC §4 already establishes the seam), but
for a sharper reason than "Consul later": it lets the HA loop be tested
against a deterministic in-memory store, so partition and failure cases
become ordinary unit tests instead of a lab exercise.

### Keep: mechanism

Everything below the decision layer stays, unchanged in intent:

- promote / rewind / basebackup / slot lifecycle
- the phased `inflight_ops` journal and `ops list|resume|abandon`
- pgpool integration: `pcp_attach_node` / `pcp_detach_node`,
  `pool_passwd`, `gen-pgpool`, the SPEC §6 hook contract
- the mTLS peer mesh and remote command dispatch (no SSH)
- the durable maintenance queue

The peer mesh survives and gains a second, strictly separated
responsibility. `PgAgentPeer` still carries **work** — create this slot,
run this basebackup, start postgres. The new `PgAgentRaft` service carries
**consensus**. Same transport, same certs, same peer identities; different
service, different connection, and no path by which a work RPC influences
role. Today's defect is precisely that a work RPC (`Failover`) *is* the
role decision.

### The HA loop (genuinely new code)

The daemon is currently *purely reactive* — it acts only when pgpool pokes
it. A lease-backed design needs its own periodic loop, roughly Patroni's
`ha.py`. Every `loop_wait`:

```
linearizable read of lease state; read local PG state
   (read failure is NOT "vacant" — it is "unknown", see below)

if I hold the lease:
    assert PG running && !in_recovery   (else demote + release)
    on read failure past retry_timeout: DEMOTE LOCAL PG

else if someone else holds it:
    if holder changed since last cycle: follow_primary(new holder)
    if holder unhealthy for leader_ttl: become a candidate (below)

else (vacant, or holder unhealthy past leader_ttl):
    if not eligible (nofailover tag, lag > max_lag_on_failover): skip
    compare WAL position against reachable peers via GetStatus
    if not most-advanced: skip, with jittered backoff
    propose CAS(expected_holder, expected_term); winner promotes
```

Notes:

- **"Cannot read" is not "vacant."** A node that has lost quorum cannot
  distinguish those by asking, and treating a failed read as an empty
  lease reintroduces §3's hole through the front door. Unknown means take
  no role-changing action — and if we currently hold the lease, unknown
  past `retry_timeout` means demote.
- Candidate comparison uses the existing `GetStatus` peer RPC — the data
  is already there; SPEC §5.1 just never consults it.
- The CAS is the **gate**; `inflight_ops` remains the **record**. A
  promotion is still a journaled orchestration.
- Per-node `nofailover` / `clonefrom` tags (already on the ROADMAP)
  become eligibility inputs rather than a separate feature.
- The "skip" branch needs a tiebreak. Two candidates reading each other's
  WAL position at slightly different instants can each conclude the other
  is ahead and both skip forever. Jittered backoff, and break ties on node
  id once positions are within a threshold.
- **The loop must run on standbys too.** It is not primary-only work: a
  standby is what detects a dead holder and becomes a candidate. This is
  the concrete sense in which the daemon stops being reactive.

### What this costs

The safety property comes from demote-on-quorum-loss, and that primitive is
also the bill. A primary that cannot reach a quorum for `retry_timeout`
**shuts down its own write path**, whether or not anything is wrong with
PostgreSQL. That failure mode does not exist today.

Stated plainly, the trade is **a rare correctness failure for a less rare
availability failure.** That is not obviously a good deal and should not be
smuggled in as an implementation detail. Three things have to hold for it
to be the right one:

- **The quorum is the cluster.** Embedding is what makes this defensible.
  "Quorum lost while PostgreSQL is fine" is not an independent new way to
  break — losing a majority of agents means losing a majority of *nodes*,
  which is the event that produces split-brain today. We are making an
  existing break fail closed rather than fail dangerous. With an external
  store this argument depended on a deployment convention; here it is
  structural.
- **The demote path is correct.** It becomes the most safety-critical code
  in the daemon: it runs rarely, under degraded conditions, and a bug is
  either an outage (demotes when it shouldn't) or the original defect
  (doesn't demote when it should). It needs fault-injection tests against
  the in-memory store, not just unit tests.
- **A minority node stays useful.** Losing quorum must degrade to
  read-only, not to "agent falls over." Standbys keep streaming; the
  local `/healthz` keeps answering; only role *changes* are blocked. A
  minority node that panics its way out of the cluster converts a
  survivable partition into an outage.

Costs specific to embedding, worth budgeting rather than discovering:

- **We own the storage and network impls.** openraft gives us the
  protocol; the components where a bug corrupts silently are ours. Hence
  the conformance-suite requirement above.
- **openraft is pre-1.0** and has had real breaking churn (the
  `RaftStorage` split into `RaftLogStorage` + `RaftStateMachine`). Pin it,
  and budget upgrade work as recurring rather than one-off.
- **Restarting `pg_agentd` gets heavier.** It is now a Raft member
  restart: log recovery, rejoin, possibly an election. Election timeouts
  must be tuned so a rolling agent restart across three nodes does not
  cascade into a PG failover — the deploy pattern Ansible already uses.
- **`panic = "abort"`** (current release profile) means a panic anywhere
  in the daemon takes down a Raft member. Keep it — a consensus
  participant with corrupt in-memory state is worse than a dead one — but
  note that an unrelated handler bug now costs a vote, and make sure
  systemd restarts fast.
- **No runbook exists** for "quorum is down but the database is healthy,"
  because today there is no quorum. Writing it is part of the work.

There is one satisfying result. The 2026-06-11 trigger — a brief
`pg_agentd` restart — becomes a **non-event by construction**. A restarting
agent loses its vote temporarily; it cannot be promoted away from unless it
is genuinely gone past `leader_ttl`, and if it were, the takeover would be
safe anyway. The incident that motivated this document is closed by the
structure rather than by a check.

### Alternatives considered

**External etcd.** The obvious answer, and the one this document originally
proposed. It buys a genuinely more battle-tested store and the option to
place the quorum on separate nodes. It costs a second distributed system to
deploy, bootstrap, upgrade, back up, cert-manage, and recover — and its
central safety argument ("co-locate it so the failure domains are shared")
is a deployment convention rather than a structural guarantee. For a
three-node Ansible-managed cluster, running a second quorum to coordinate
the first one is the larger operational burden. **Rejected**, but it is the
natural fallback if the embedded storage layer proves harder to trust than
expected — the trait keeps that door open.

**A hand-rolled quorum lease on the peer mesh.** Self-demote on losing
contact with a majority of peers; refuse to promote without a majority.
With three nodes only one partition holds 2/3, so it does deliver the
safety property with no new dependency at all. **Rejected** — openraft
dominates it on every axis that matters:

- **No durable epoch.** No fencing token, so a node that was partitioned
  and rejoins has no record proving it lost. Raft's term gives this free.
- **Asymmetric views miscount.** Each node computes the majority from its
  own reachability, so a node with one-way network breakage can believe it
  has quorum when it does not — exactly the class of bug a replicated log
  eliminates.
- **We would author the timing assumptions ourselves**, which was the
  strongest argument against it and is precisely what using a real Raft
  implementation removes.
- **It solves only role**, leaving `paused` / switchover / generation on
  LWW gossip — §4's problem, unresolved.

The framing that makes openraft the answer: this option and etcd were the
two ends of a false choice between *no new dependency but hand-rolled
correctness* and *inherited correctness but a new daemon*. An embedded Raft
library is both ends at once — no new deployment unit, no hand-rolled
consensus.

**Keep the trait regardless.** Not for "Consul later," but because a
deterministic in-memory store turns partition and failure cases into
ordinary unit tests, and lets the HA loop's shadow mode run before any real
store exists.

### Prior art: Patroni's `raft` backend (pysyncobj)

Patroni shipped exactly this shape — consensus embedded in the agent, no
external DCS — in 2.0 (September 2020), via
[pysyncobj](https://github.com/bakwc/PySyncObj) plugged in under its DCS
abstraction. It never left beta and was deprecated in
[3.0.0](https://patroni.readthedocs.io/en/latest/releases.html) (January
2023): *"we will do our best to maintain it, but take neither guarantee
nor responsibility for possible issues."* Researched 2026-08; the record
cuts both ways, and both cuts matter.

**It confirms the demand.** The issue tracker is full of users asking for
precisely this design's pitch: *"I'd like to get rid of etcd as it's an
additional layer"*
([#2112](https://github.com/patroni/patroni/issues/2112)), *"we are not
able to deploy hosts only for DCS"*
([#2147](https://github.com/patroni/patroni/issues/2147)). The
maintainer's fallback answer — run etcd co-located on the database nodes —
is the deployment-convention posture §5 rejects as non-structural. Patroni
tried to build the "no DCS to run" moat and retreated; the demand did not
go anywhere.

**Why it died — and why those reasons do not transfer.** From maintainer
comments, three causes, none architectural:

- **Never dogfooded.** *"We don't use Raft and all recent bugfixes were
  triggered by reports of existing users"*
  ([#2041](https://github.com/patroni/patroni/issues/2041), 2021). The
  backend was community-driven from its first release.
- **An opaque, unowned consensus library.** When users reported database
  failovers coinciding with raft leadership changes, the maintainer could
  not reproduce them and had no leverage on the library: *"Maybe PySyncObj
  isn't good enough for your needs. I would advise switching to Etcd"*
  ([#2147](https://github.com/patroni/patroni/issues/2147)). The stated
  deprecation reason, verbatim: *"'Occurred randomly, can not be
  reproduced' — that's the main reason we declared Raft support as
  deprecated"* ([#3051](https://github.com/patroni/patroni/issues/3051),
  2024).
- **An emulation layer.** Patroni's DCS abstraction is etcd-shaped — TTL
  keys, watches, CAS — and the raft backend had to emulate those semantics
  on top of pysyncobj. The design here owns the state machine natively;
  there is no impedance-mismatch layer for semantics to quietly diverge in.

The mitigations this document already mandates target exactly that failure
class: the `openraft::testing` conformance suite as a hard CI gate (the
answer to "can not be reproduced" is a storage layer that is exhaustively
tested before it ships), the deterministic in-memory store for
fault-injection tests, and an acceptance suite that boots the real
artifacts on a real three-node cluster and manufactures the failures —
partition, stale primary, WAL divergence — rather than waiting for a user
to report one. That last is the direct answer to "occurred randomly,
cannot be reproduced": the cases are reproduced on demand, on every run.
This is the same cluster the author operates, so it is also dogfooding
by construction, the thing Zalando never did. openraft
itself is the opposite dependency profile from pysyncobj: actively
maintained, run in production inside Databend, and pre-1.0 churn is
already budgeted in "What this costs."

**The transferable lesson.** The recurring field-failure signature —
[#1701](https://github.com/patroni/patroni/issues/1701),
[#2147](https://github.com/patroni/patroni/issues/2147),
[#3051](https://github.com/patroni/patroni/issues/3051), spanning
2021–2024 — was raft leadership churn causing *"failed to update leader
lock"* and a spurious PostgreSQL failover. That is the "Raft leader is not
the PostgreSQL primary" invariant being violated through the subtle door:
not by conflating the roles, but by the lease-refresh path failing during
elections. Hence the second invariant under "Lease semantics":
`retry_timeout` must span a worst-case election. The failure mode to fear
in this design is not split-brain — the quorum forecloses it — it is
spurious demotion via the raft plane, and Patroni's history is the
evidence of where those bodies get buried.

---

## 6. The pgpool configuration contract

pgpool conflates two concerns:

1. **Routing state** — "which backends do I send queries to, and which is
   primary?" Legitimately pgpool's job. Per-instance. Derived from
   `sr_check` plus health checks. Instances disagreeing is *tolerable*:
   the worst outcome is a query erroring against a down backend.
2. **Role assignment** — "who *should be* primary?" Must be globally
   serialized. Not pgpool's job.

`failover_command` is the bridge that lets (1) drive (2). Cutting that
bridge is the entire change.

> The hook-by-hook working of this section — exact 4.6 firing semantics
> under `use_watchdog = off`, the full hook table, and one cost this
> section had not priced (backend-status sync between pgpool instances
> is a watchdog feature; attach becomes an agent-side fan-out) — is in
> [pgpool-hook-contract.md](pgpool-hook-contract.md).

| Setting | Today | Target | Rationale |
|---|---|---|---|
| `use_watchdog` | on (quorum-only, no VIP) | **off** | see below |
| `failover_command` | promotes `%m` | **notify-only or removed** | hint, not order |
| `follow_primary_command` | reconfigures standbys | **removed (must be empty)** | agent reacts to lease change instead; a non-empty hook makes pgpool degenerate every healthy standby after a primary failover ([details](pgpool-hook-contract.md)) |
| `sr_check_period` | on | **keep** | this is how pgpool *learns* the primary |
| health checks | on | **keep** | per-instance routing, self-limiting |
| `detach_false_primary` | — | **on** | defense in depth |
| `auto_failback` | — | **off** | agent owns reattach via pcp |

**Dropping watchdog.** We already run it quorum-only with no VIP, so
there is no VIP machinery to lose. What we *do* lose is
`failover_when_quorum_exists` and `failover_require_consensus` (both
default on) — together the gate that currently stops three pgpool
instances each firing `failover_command`.

That would be a regression today. It stops mattering the moment promotion
is a CAS: N concurrent hints converge to one outcome, because only one
can win the key. **Idempotence replaces coordination.** The watchdog
becomes removable not *in spite of* the decision moving but *because* it
moved — which is a useful signal that the design is coherent.

**pgpool still learns the new primary without us.** `sr_check` polls
backends and classifies primary vs. standby every `sr_check_period`
(default 10s). `failover_command` is how pgpool *causes* a failover. We
cut the causing and keep the learning; pgpool re-points writes on its own.

**If `failover_command` is kept as a notify-only poke** (wake the HA loop
now rather than at the next tick — worth it for detection latency),
document in SPEC §6 that its arguments are **advisory**. Otherwise
someone will "fix" the handler back into trusting `%m`.

**Leave `detach_false_primary` on.** It uses `pg_stat_wal_receiver` via
sr_check to spot a backend claiming primary that shouldn't be. It is no
longer *deciding* anything — it just refuses to route to something
incoherent, which is exactly the backstop wanted if fencing ever fails.

---

## 7. SPEC deltas

| Section | Change |
|---|---|
| §1.1 | Cluster layout diagram: watchdog line goes; the agent mesh gains a Raft plane. No new box — the agents *are* the quorum. |
| §3.2 `PgAgentPeer` | Unchanged surface, restated purpose: work dispatch only. |
| **new §3.3** | `PgAgentRaft` service — openraft's `AppendEntries` / `Vote` / `InstallSnapshot`, same listener and certs, **separate connection** from work RPCs. |
| §4 | Add the consensus-store trait as an injection seam (in-memory impl for tests). |
| **§5.1 `Failover`** | **Rewrite.** Drops `new_main` from the signature entirely. Becomes: evaluate eligibility → compare WAL → propose CAS → promote iff won. |
| §5.2 `FollowPrimary` | Trigger changes from pgpool hook to observed lease change. |
| §5.5 `Escalation`/`DeEscalation` | Already no-ops; can be deleted with watchdog. |
| §5.7 `ClusterInit` | Also bootstraps Raft: commits initial membership. Becomes the one-time formation step. |
| §6 | Hook contract: `failover_command` / `follow_primary_command` reclassified as advisory notifications. |
| §8 | New `[raft]` config block: `members`, `leader_ttl`, `loop_wait`, `retry_timeout`, `election_timeout`, `max_lag_on_failover`. No endpoints or separate TLS material — it reuses the peer mesh. |
| §8.6 | Node resolution now has a second consumer; committed Raft membership and configured `NodePool` must agree. |
| §9 `/healthz` | Can now report role **authoritatively** (lease-holder or not), plus quorum reachability. |
| §10 | `<state_dir>/raft/` joins `replay/` and `maintenance/`. |
| §14 `validate-env` | Quorum reachability; config membership vs committed membership; `<state_dir>/raft/` writability. |
| new § | The HA loop, lease protocol, demote path, candidate selection, membership changes. |

### The free win in SPEC §9

With an authoritative role, `/healthz` can support the role-aware
`/primary` + `/replica` split that
`home-ansible:roles/db/README.md` currently defers with *"current lean:
dual endpoint."* That decision is blocked today precisely because **no
component authoritatively knows the role** — pgpool infers it, the agent
is told it. The lease supplies it.

---

## 8. What this preserves

This is a re-scope, not a retreat. The mechanism layer is where the
project's actual differentiation lives, and Patroni is *worse* at both of
these:

- **pgpool integration.** Patroni has no pgpool story at all — it assumes
  HAProxy plus optionally pgbouncer. `pcp_attach_node`/`detach`,
  `pool_passwd`, `gen-pgpool`, the SPEC §6 hook contract: nobody else has built
  this. If you run pgpool, Patroni does not help you.
- **Phased orchestration with operator visibility.** `inflight_ops`
  (checkpoint → create_slot → basebackup → configure_standby), resumable
  after a crash, with `ops list` / `resume` / `abandon`. Patroni's
  `reinit` is an opaque black box: it works or you run it again.

And embedding adds a third, which is the first item here that is a **moat
rather than parity**:

- **No DCS to run.** Patroni structurally requires an external
  etcd/Consul/ZooKeeper quorum; for most small deployments that store is
  the majority of the operational burden. An agent that carries its own
  consensus is not a thing Patroni can be configured into being. (Patroni
  did ship a `raft` DCS backend via `pysyncobj` for exactly this reason;
  it never left beta and was deprecated in 3.0.0. The post-mortem — see
  "Prior art" in §5 — confirms the demand and locates the failure in
  dependency quality and testing discipline, not in the architecture.)

The ROADMAP bar — *"be a more pleasant HA layer to operate than Patroni,
on top of the pgpool-II substrate we're stuck with"* — is unchanged and
better served. The pitch becomes:

> **Patroni's safety model, pgpool's ecosystem, no DCS to operate.**

And the honest answer to "why not just use Patroni" becomes *"because I run
pgpool, and I don't want to run etcd to run a database"* — rather than
*"because I didn't want an etcd dependency,"* which 2026-06-11 already
charged us for. The instinct was right; only the conclusion drawn from it
was wrong.

One property worth noting: once role lives in the replicated log and
HAProxy routes on an authoritative `/healthz`, **pgpool becomes optional
rather than load-bearing.** We keep it for pooling and read load-balancing, both real
value. But nothing in the correctness story depends on it. Given pgpool
is the component whose failure detector caused the incident, supporting
it without trusting it is a strictly better place to stand.

---

## 9. Open questions

Decisions to make before implementation, not blockers to the design:

1. **Storage engine: `redb` or RocksDB?** Recommended `redb`; see §5. The
   decision is genuinely reversible behind the trait, and the conformance
   suite matters more than the answer. Settle it by writing the `redb`
   impl and seeing whether it fights back. *(Settled: `redb` shipped and
   has not fought back.)*
2. **Membership: static or dynamic?** Committed Raft membership must agree
   with the configured `NodePool`, which is snapshotted at startup and not
   reassigned at runtime today. Simplest coherent answer: membership
   changes only via an explicit operator command
   (`pg_agentctl cluster add-node` → `change_membership`), never inferred
   from config drift, with `validate-env` flagging disagreement. Dynamic
   membership is a lot of machinery for an event that happens roughly
   never in a three-node cluster.
3. **Election timing vs. rolling restarts.** `election_timeout` has to be
   long enough that Ansible restarting three agents in sequence does not
   cascade into a PG failover, and short enough that real failure detection
   stays useful. Needs a measured answer on the live cluster, not a guess.
   Note it does not stand alone: the second lease invariant chains
   `election_timeout < retry_timeout` and the first chains `retry_timeout`
   into the `leader_ttl` floor, so these three tune together or not at all.
4. **`failover_command`: removed, or notify-only?** ~~Notify-only buys
   detection latency at the cost of a hook path that must be documented
   as non-authoritative forever.~~ **Resolved at cutover: notify-only.**
   The handler under lease-driven roles answers "advisory" and promotes
   nothing (SPEC §5.1); the non-authoritative contract is documented in
   the canonical block itself, and acceptance E3 exercises the full
   shape — hook fires, handler declines, lease promotes, `sr_check`
   discovers.
5. **Synchronous replication.** ~~Patroni's `synchronous_mode` maintains
   `synchronous_standby_names` and refuses to promote a node that was not
   in sync — trading write latency for zero-data-loss failover. Do we
   want an equivalent, and is it v1 of this work or later?~~
   **Resolved and shipped:** [quorum-commit.md](quorum-commit.md) — the
   executor arms `ANY 1` at the first-standby-attached event, candidacy
   selects by strict flush-max, and the acceptance suite asserts
   sentinel write survival across every induced failure.
6. **Migration path.** ~~Can a cluster move from hook-driven to
   lease-driven in place?~~ **Mooted by the greenfield rip:** there was
   never a trusted hook-driven deployment to migrate; the pgpool-led
   path and its staged-migration suite are deleted. The repo validates
   greenfield lease-driven deployments only.
7. **Verify under `use_watchdog=off`:** pgpool docs describe
   `failover_command` as running "once per failover event," but that is
   written for single-instance semantics. Confirm empirically how many
   times it fires across three uncoordinated instances. This does not
   change the design — CAS makes it safe either way — but it should be
   documented rather than assumed. The doc-derived expectation
   (once per instance, arguments computed from each instance's local
   view) and the test recipe are in
   [pgpool-hook-contract.md](pgpool-hook-contract.md) §5.

---

## 10. Suggested sequencing

Effort tags follow ROADMAP convention (**S** = days, **M** = weeks,
**L** = month-scale).

1. **(S)** Add a lag gate to reactive failover, reusing
   `MAX_HANDOFF_LAG_BYTES` logic. Fixes §2.2 — a live data-loss defect,
   independent of everything below and valuable even if the rest never
   ships. First because it is the only step that closes a distinct bug.
   **Landed** (post-0.7.3): `LocalServer::failover_lag_gate`, built on
   the new `cluster_view` module — a status fan-out plus a lexicographic
   `(timeline, lsn)` comparator, which is also the HA loop's future
   candidate-selection primitive. Refuses only on positive evidence that
   a strictly better candidate is reachable; missing evidence skips the
   gate (refusing there would be §3's unavailability branch).
   **Then deleted with the pgpool-led promote path itself** (greenfield
   rip): §2.2's concern lives on in the HA loop's candidacy, upgraded
   to strict flush-max comparison — see docs/quorum-commit.md §4 and
   testing/README.md findings 15/19.
2. **(S)** Land `validate_cluster_preconditions` with the
   `detached`-is-actually-down check, per TODO. Closes the known trigger
   now. Label it defense-in-depth in the code comment.
   **Landed** (post-0.7.3): `preconditions` module, wired into both
   `Failover` branches — primary-down refuses when the announced-failed
   primary is reachable and running as primary (the 2026-06-11 shape);
   standby-down refuses when the announced-failed standby is reachable
   and streaming. Unverifiable evidence logs and proceeds, per §3.
   The module docs carry the not-the-fix caveat verbatim.
3. **(S)** Mark the ROADMAP gossip-plane item as superseded rather than
   scope-noted — §5's state machine replaces it. Keep the scope note in
   force for as long as both are notionally on the board.
4. **(M)** Consensus-store trait + in-memory impl + `[raft]` config. No
   openraft yet, no behaviour change. This is what makes step 5 testable.
   **Landed** (post-0.7.3): `consensus` module — `ConsensusStore` trait
   (linearizable `read_state` where `Err` = unknown-never-vacant, lease
   CAS with generation-minted fencing terms, `release`, pause/switchover)
   plus `InMemoryConsensusStore` with fault injection (transient
   read/write failures, persistent partition switch). `[raft]` config
   block parses with both lease invariants enforced at load. Nothing
   consumes the store yet.
5. **(M)** The HA loop against the in-memory store, **shadow mode**:
   compute what it *would* decide, log it, keep obeying pgpool. Highest-value
   step — it turns the argument empirical before anything destructive
   changes, and it needs no working Raft to do so.

   > **Correction (2026-08-13):** this step was originally written as
   > "diff the two decision streams on the live cluster," with agreement
   > with pgpool as the implied success signal. That oracle is wrong.
   > pgpool's decisions are the defect under repair — §2.1 promotes on a
   > false failure report, §2.2 picks a candidate without consulting WAL
   > position. In exactly the cases that justify this work the loop
   > **must** diverge, so a diff scores the loop against a reference that
   > is wrong precisely where correctness is decided, and agreement would
   > be the alarming reading. Validation is against ground truth instead —
   > which node actually held the most WAL, whether the announced-dead
   > node was actually dead, whether exactly one node ended up promotable
   > — asserted by the dockerized acceptance suite
   > ([testing/README.md](../testing/README.md)), where that ground truth
   > is manufactured rather than inferred. Nothing in the landed loop
   > changes; only how it is judged, and there is no separate operational
   > half of this step to schedule.

   **Loop landed** (post-0.7.3): `ha` module — one `HaDecision` per
   `loop_wait` tick covering retain / follow / holder-watch / candidacy
   (originally most-advanced check with a node-id tiebreak within
   `max_lag_on_failover`; since upgraded to STRICT flush-max with id
   breaking exact ties only — the band was an acknowledged-write hole
   under quorum commit and finding 15's wedge cause), jittered backoff,
   demote-on-quorum-loss and demote-on-not-primary, with "cannot read ≠
   vacant" enforced. Shadow-safety is structural: the loop holds no
   Systemd/Pcp/StandbyOps and can only write to its process-local
   store. The shadow-only vacant-lease adoption artifact is gated off
   whenever an executor is attached (execute mode), which is the only
   deployed shape post-rip.
   The mode itself stays past this step — it is how the acceptance suite
   exercises the loop (S2, S3) without promoting anything, and how step 6
   runs the loop over a real store before step 7 hands it executors.
6. **(M)** openraft: `PgAgentRaft` service, `redb` storage impl passing
   `openraft::testing`, membership bootstrap in `ClusterInit`,
   `validate-env` checks. Swap it in behind the trait; shadow mode keeps
   running.

   **Storage landed** (post-0.7.3): `raftstore` module — `RedbLogStore`
   (log, vote, committed pointer) and `RedbStateMachine` (the applied
   `ClusterState` plus snapshots) over one redb file at
   `<state_dir>/raft/raft.redb`, against openraft 0.9.25's `storage-v2`
   traits. **`openraft::testing::Suite` passes** — the hard gate this
   section makes the basis for choosing redb over RocksDB. The gate was
   checked for teeth rather than assumed: dropping the recorded purge
   point, and an off-by-one making `truncate` exclusive, each fail the
   suite.

   Three things the design did not pin down, decided here:

   - **Time is proposed, not read.** `Utc::now()` cannot appear in
     `apply` — replicas would diverge on `Lease.since`. The timestamp is
     minted by the proposing node, carried in `ConsensusCommand::Takeover`,
     and applied verbatim everywhere. This is the only semantic
     difference from `InMemoryConsensusStore`, where a clock read at
     apply time was harmless because there was one replica.
   - **Raft node ids are `u64`, agent node ids stay `i32`.**
     `openraft::testing::Suite` requires `NodeId: From<u64>`, which
     `i32` cannot implement. They convert at the seam; the lease *inside*
     the state machine stays `i32`, so `ClusterState` is unchanged by
     which store is behind it.
   - **redb sets the workspace MSRV** (1.85 → 1.89). The alternative was
     redb 2.6.x, the last 1.85-compatible line and now maintenance-only.
     Cheap either way precisely because this document makes the file
     disposable.

   **Transport landed** (post-0.7.3): `raftnet` module + the
   `PgAgentRaft` service in `proto/pgagent_raft.proto` — `RaftGrpcService`
   inbound on `PeerServer`'s own listener (`PeerServer::with_raft`, same
   port, same certs, same SAN allowlist, so the mTLS gate that guards
   the peer plane's mutating RPCs guards this one unchanged), and
   `RaftChannelFactory`/`RaftPeerNetwork` outbound on *separate*
   channels, per this section's starvation argument. Proven by three
   real Raft nodes over three real sockets electing a leader and
   committing a lease takeover; stubbing `append_entries` fails that
   test, so it measures the transport rather than assuming it.

   Two decisions this section did not make:

   - **Frames are opaque.** Requests cross the wire as serialized
     openraft types, not protobuf-mirrored fields. Mirroring would
     re-declare a large slice of openraft's internals and re-do it every
     upgrade, buying interop that cannot arise — both ends are the same
     binary at the same version. The cost is real and named in the
     `.proto`: `grpcurl` sees a blob, so debugging the consensus plane
     goes through agent logs and openraft metrics.
   - **Every transport failure is `Unreachable`, never `NetworkError`.**
     openraft hot-retries the latter and backs off on the former, and
     hot-retrying a partitioned peer spins CPU on the node still trying
     to hold quorum together.

   **Store landed** (post-0.7.3): `raftconsensus` module —
   `RaftConsensusStore` implementing `ConsensusStore` over the Raft
   handle. The HA loop is unchanged, which is the seam paying off.

   This surfaced something the section above asserts without following
   through, and it is worth correcting rather than burying: **"retain is
   a read" is a read *only the Raft leader can perform*.**
   `ensure_linearizable` confirms leadership against a quorum and fails
   on a follower; `client_write` returns `ForwardToLeader`. Since this
   document's central invariant is that the Raft leader is *not* the
   PostgreSQL primary, the consequence is unavoidable: on most nodes,
   most of the time, both retain and takeover are an RPC to another
   node. Two `PgAgentRaft` RPCs exist for exactly this (`Propose`,
   `ReadState`), and forwarding is one hop, never two — the leader-side
   handlers refuse rather than re-forward, because a chain would make
   latency unbounded in precisely the churny conditions where the
   `retry_timeout` budget is tightest.

   The claim that survives unchanged is the one that matters: the log
   still only grows on real events, and steady-state retain still writes
   nothing. What was under-priced is latency, not write volume — one
   extra hop inside a budget the `retry_timeout > election_timeout`
   invariant already sizes.

   `Err` still means unknown, enforced at every new failure path: no
   leader known, leader unreachable, `ensure_linearizable` refused, RPC
   timed out. A test asserts that a node with no quorum errors rather
   than reporting a vacant lease — the §3 hole would otherwise walk back
   in as an `unwrap_or_default`.

   **Wiring landed** (post-0.7.3), completing this step: `RaftRuntime`
   (construction + membership bootstrap), `[raft] enabled`, and the
   `validate-env` checks.

   `enabled` is a separate switch from `shadow`, and they compose:
   `enabled = true, shadow = true` is this step's configuration — real
   consensus underneath, no executors on top. Step 7 turns `shadow`
   off. Both default off, so nothing changes for any deployment that
   does not opt in.

   Membership is formed by `ClusterInit`, not at daemon startup and not
   implicitly at first election. It is already the operator-driven
   "this is the cluster" moment; bootstrapping at startup would have
   every node racing to declare a membership, and bootstrapping on
   first election would make the member set depend on who booted first.
   It is idempotent (openraft's `NotAllowed` means "already formed",
   which is the goal of calling it) and never fatal to `ClusterInit`:
   replication has actually been configured by that point, and failing
   the command over a consensus-bootstrap problem would send the
   operator back to re-run destructive work that already succeeded. A
   *restarting* node deliberately does not bootstrap — it recovers
   membership from its own log, or a restart could redefine who the
   members are.

   `validate-env` refuses rather than warns on all four preconditions,
   because each one's failure mode only becomes visible during an
   outage: a pool smaller than three (a 2-node Raft cluster tolerates
   zero failures, where the same pool without Raft merely degraded
   badly — the one place that regression is catchable before it
   matters), an unresolved local node id, no mTLS (the consensus plane
   shares the peer listener, so this exposes lease takeover to anyone
   who can reach the port), and an unwritable state dir (a vote that
   cannot be persisted is a vote that can be cast twice after a crash).
   The checks are silent when Raft is off, which is every deployment
   before cutover — a checklist that reports on things nobody enabled
   trains operators to skim it.

   The election window is derived, not configured: `[raft]` carries one
   upper bound and the daemon randomizes half-to-full, because a single
   value has every node time out together and split the vote.

   **Acceptance coverage** (testing/ phase 4, R0–R4b): the real Raft on
   the real three-node cluster, still in shadow. S13 — the split-brain
   baseline this document opens with — is inverted at the decision
   level: the isolated lease holder decides to demote on lost quorum
   and commits nothing; the majority commits exactly one
   quorum-serialized takeover; PostgreSQL state needs zero repair
   afterwards. Getting there surfaced two partition-path bugs, each
   invisible to in-process tests (testing/README.md findings 12–13):
   leader-forwarded reads had no client-side deadline, so one HA tick
   blocked 34 s on a just-isolated Raft leader — the same header-
   deadline trap as finding 11, now fixed at three layers (leader
   client, replication RPCs, and the tick itself, which no longer
   trusts any store to fail fast); and the holder-unhealthy clock was
   not keyed to the holder it watched, letting a rival depose a
   7-second-old lease — voiding exactly the ttl window a fresh winner
   needs to finish its asynchronous promotion. Both regression-tested.
   What remains before step 7 is operational, not code: mileage — the
   `enabled = true, shadow = true` configuration accumulating decision
   history on the real cluster.
7. **(M)** Cut over: SPEC §5.1 rewrite, pgpool config contract, watchdog off.

   **Executors landed** (post-0.7.3): `pgman::instance::PostgresInstance`
   (the concern layer: `promote_and_wait`, `ensure_stopped`, `follow`,
   `rebuild_as_standby`, one authoritative `InstanceState`) and
   `roleexec::RoleExecutor`, which consumes the decision stream. The
   loop stays a pure decision function; **shadow mode is the executor's
   absence** — `[raft] shadow = false, enabled = true` is the execute
   switch, and the instance is only ever constructed alongside a real
   Raft, so executing against a process-local store is not a
   configuration that exists.

   Decisions map to convergent actions: takeover → journaled
   promotion (deadline = `leader_ttl`, the same clock rivals run
   against a fresh holder); demote → fence (`ensure_stopped`, never
   gated on journaling — a broken journal must not stand between the
   loop and stopping a lease-less primary); holder change → re-point
   the standby (slot prepped on the holder via peer RPC, then a
   conf-rewrite + reload). The executor also closes the stale-primary
   half of §2.1 from the other side: a node running as primary while
   someone else holds the lease is fenced, even though the decision
   layer only says `Following`.

   **Demote policy: stop and wait.** A fenced node stays stopped;
   rejoining (`cluster recover`) is the operator's call. The executor
   never runs a destructive rebuild — `rebuild_as_standby` exists for
   the peer-RPC handlers to converge on and for a future opt-in.

   Shadow-only vacant adoption is now gated off in execute mode, as
   this step requires: the primary claims the lease for itself through
   the shared store, and `ClusterInit` seeds it deterministically at
   bootstrap (idempotent, CAS-on-vacancy — losing means a holder
   exists, which is the goal).

   **Acceptance landed** (testing/ phase 5, E0–E2b): the executors on
   the real cluster. E1 promotes for real on lease takeover (journaled,
   survivor re-points and streams); E2 runs the S13 partition with
   executors and asserts the ending this document exists for — the
   isolated holder fences itself, the majority promotes exactly one
   standby, and **exactly one primary is on the wire during and after
   the partition**. Getting there surfaced findings 14 and 15
   (testing/README.md): the third member of the unbounded-RPC-on-
   partition class was sitting on the promotion-critical path
   (`restore_command` → `FetchWal`, now bounded and cooled down, with
   `pg_promote(false)` putting the whole wait under the caller's
   deadline), and a surviving standby can diverge past the new
   primary's fork point — repaired via the operator path per demote
   policy, with executor-side detection tracked in TODO.md.

   **Contract flipped** (post-0.7.3), completing this step. The
   canonical `pgpool.conf` block is now the §6 target: `failover_command`
   kept as a notify-only poke (open question 4, resolved),
   `follow_primary_command` empty, `wd_*` hooks gone, plus the
   decision-critical settings (`use_watchdog off`,
   `detach_false_primary on`, `auto_failback off`,
   `failover_on_backend_error on`) emitted by `gen-pgpool` and verified
   by `check-hooks`. The pre-cutover block briefly survived behind a
   `--legacy` flag; flag, block, and the pgpool-led promote path itself
   were then deleted — the repo validates greenfield deployments only,
   and there was never a trusted pgpool-led deployment to migrate. On
   the product side, `Failover`'s primary-down branch is always
   advisory: log and `ok=true`, no promotion, while standby-down slot
   hygiene — mechanism, not authority — keeps its guards and keeps
   working. SPEC §5.1 carries the hook contract and §5.15 the
   behavioral summary.
   Acceptance E3 validates the production end-state: pgpool up in the
   target contract, primary killed, hook answers advisory, the lease
   promotes exactly one standby, and pgpool discovers it through
   `sr_check` with no follow hook at all.
8. **(S)** `/healthz` role reporting + the role-aware HAProxy split in
   home-ansible.

Steps 1–3 are worth doing regardless of whether the rest is ever
scheduled. Steps 4–5 are worth doing even if openraft is never adopted —
the HA loop, eligibility rules, WAL comparison, and demote path are
identical under any store, which is what keeps the storage decision cheap
to defer and cheap to revisit.
