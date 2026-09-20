# Promotion authority — relocating the failover decision

Why "who should be primary" moved off pgpool and onto a quorum-backed
lease in a Raft log the agents replicate among themselves. Shipped; this
is the reasoning, and the constraints it leaves behind.

**There is no cutover switch.** Every daemon joins consensus and
executes, or fails to start. `[raft] enabled` and `shadow` were the
staged migration's controls and are gone — `enabled = false` is refused
at config load, `enabled = true` loads with a warning to delete the line.

Section-reference convention: references to SPEC.md are always written
`SPEC §N`; a bare `§N` is a section of this document.

---

## 1. The thesis

Compare pg-agent-rs and Patroni at the mechanism layer — promote, rewind,
basebackup, slot lifecycle, standby reconfiguration, health endpoint —
and they are near-identical. Ours is arguably richer.

They diverge on exactly one question: **who decides who's primary.**

| | Patroni | pg-agent-rs, before this |
|---|---|---|
| Decision made by | compare-and-swap on one key in a quorum store | pgpool's `failover_command` |
| Serialization point | exactly one, linearizable | none |
| Can two nodes both win? | no, structurally | yes |

The old `Failover` handler promoted whoever `new_main` named, and
`new_main` arrived as an RPC parameter from an untrusted caller.

**pgpool's watchdog quorum is a failure detector, not a consensus
protocol.** A failure detector answers *"can I reach X?"* Consensus
answers *"is X primary?"* Those are different questions, and pgpool can
only answer the first. Treating the first answer as the second was the
root cause of both defects in §2.

---

## 2. Two defects, one root cause

### 2.1 Split-brain from a false failure report (confirmed in production)

On 2026-06-11, pgpool's `failover_command` announced `detached=db1,
new_main=db0` after db1's pg_agentd briefly restarted. db1 was actually
still primary and healthy — pgpool's quorum just couldn't reach the
daemon during the restart window. The handler trusted pgpool and promoted
db0, creating split-brain. The code did what the spec said; the spec
trusted an authority that structurally cannot know the answer.

**Reproduced on demand, 2026-08-09.** The production incident needed a
coincidence. The structural claim does not: isolating the primary with
`docker network disconnect` produced two primaries every time.

```
t+0s    primary db0 isolated from the network
t+2s    db1 + db2 health checks fail → each pgpool fires failover_command
t+82s   db1 promoted by the majority side
        db0 still running as primary on the other side of the partition
```

Both sides behave correctly by their own lights: the majority cannot
distinguish "db0 is dead" from "db0 is unreachable" and must not stall
forever, and db0 has no reason to believe anything changed. That is §3's
dilemma with real timestamps on it, and it is the regression test that
had to invert once the lease landed. It did — see the acceptance suite's
partition scenarios.

### 2.2 Candidate selection ignored WAL position

Independent of split-brain, and easy to miss. From the pgpool failover
documentation, `%m` (new main node) is selected as:

> the node being assigned the youngest (smallest) node id which is alive

**Lowest alive node ID.** Not most-advanced WAL. Not least lag. Not "is
it even caught up." So even when pgpool was entirely correct that the
primary was down, it could hand us a candidate arbitrarily far behind,
and we promoted it and dropped the WAL delta on the floor. This is a
data-loss defect hiding behind an availability defect.

Candidate selection now belongs to the lease's candidacy, strict
flush-max — [quorum-commit.md](quorum-commit.md) §4.

---

## 3. Why preconditions alone do not close it

A shared `validate_cluster_preconditions` — refuse to act on a `detached`
node that is demonstrably alive — is worth having, and it would have
prevented 2026-06-11. But it narrows the window rather than closing it,
because it fails in precisely the case it most needs to work. Under a
real partition between db0 and db1, db0 cannot reach db1's status
endpoint:

- **Refuse to promote** → the cluster is unavailable during exactly the
  partition it exists to survive.
- **Promote anyway** → split-brain. The original bug.

There is no third branch. "Dead" and "unreachable" are indistinguishable
by asking around; that is not an implementation gap to engineer past, it
is the result that makes quorum-based consensus necessary. Any
check-then-act protocol without a quorum-backed serialization point has
this hole. More checks make it rarer, harder to reproduce, and no less
real.

`validate_cluster_preconditions` is therefore **defense in depth**, and
the module docs say so, so nobody later reads it as closing the issue.

---

## 4. Why not gossip, and re-pricing the DCS

An earlier plan proposed `GetClusterState` / `ProposeClusterState` with
last-writer-wins on a monotonic clock plus writer node id. For a `paused`
flag and a scheduled switchover that is defensible. **LWW is not
linearizable, so it must never carry role**: two partitioned nodes both
accept a proposal, both believe they won, and they reconcile *after* both
have been primary — converting an availability event into a data-loss
event. With a replicated log in hand, the gossip plane is strictly worse
at everything it was going to do, so it was deleted rather than shipped
with a scope note.

The stated reason for avoiding a DCS was dependency cost, and that was
mispriced: avoiding a serialization point did not avoid complexity, it
*relocated* it. Replay markers with TTLs, the `inflight_ops` journal, the
durable maintenance queue, best-effort cleanup contexts, and a shared
precondition validator — a substantial fraction of that machinery exists
to compensate for not having one.

The original instinct was not wrong, only over-applied: it read "we need
consensus" as "we need to run a consensus *service*", and those are
separable. The dependency being avoided was always operational, not
algorithmic — so pay the algorithmic cost, which is a library, and skip
the operational one.

---

## 5. Architecture

Draw the line Patroni draws — decision layer separate from mechanism
layer — but keep both inside `pg_agentd`. Patroni splits them across a
process boundary because it delegates to a DCS; the boundary that matters
is architectural, not operational.

Subsections here are deliberately unnumbered, so that `§5.1` unambiguously
means SPEC's `Failover`.

### Embedded Raft

Each agent is a Raft member ([openraft](https://github.com/databendlabs/openraft));
the replicated state machine holds the cluster's authoritative role
assignment. We do not implement Raft — we implement the storage impl, the
network impl, and the state machine, and those are where our bugs live.

Why embedded rather than an external etcd:

- **The failure domains fuse by construction.** The etcd version leaned
  on "co-locate it so the store shares the database's failure envelope" —
  a deployment convention someone can violate. Embedded makes it
  definitional: the Raft cluster *is* the agent cluster, so "store
  unreachable" and "peer unreachable" are the same event.
- **One deploy unit.** One systemd unit, one config file, one cert story.
  No second quorum to bootstrap, upgrade, back up, or recover.
- **The transport already exists.** The mTLS peer mesh plus `NodePool` is
  a `RaftNetwork` with the hard parts already solved.

What we give up, plainly: the option to move the store onto separate
nodes. Raft's fsync traffic shares spindles with `$PGDATA`, permanently.
And **three nodes is a hard minimum** — a 2-node deployment tolerates
zero failures under Raft, where before it merely degraded badly.
`validate-env` refuses a smaller pool rather than letting that be
discovered during an outage.

#### The separation that must not collapse

> **The Raft leader is not the PostgreSQL primary.** They are unrelated
> roles that happen to live in the same process.

This is the single most important invariant in the design, and embedding
Raft is exactly what makes it tempting to violate. Raft leadership churns
for reasons that have nothing to do with database health — a slow fsync,
a scheduler stall, a one-second blip, a daemon restart. If PG primary is
defined as Raft leader, every Raft re-election is a database failover,
and we will have built a *more* eager version of the bug we are fixing.

The lease is an **entry in the replicated state machine**. Raft
leadership is merely the mechanism by which entries commit. A Raft
election changes who proposes; it changes nothing about who runs
PostgreSQL.

#### Lease semantics

The important realization:

> **Safety comes from the quorum, not from the timers.** The TTL governs
> how *eagerly* takeover happens. It is a liveness knob, not a safety one.

Suppose a candidate takes over while the previous holder is still healthy:

- If the old holder is in the **majority** partition, the candidate is in
  the minority and physically cannot commit the takeover.
- If the old holder is in the **minority** partition, its next retain
  check cannot reach a quorum, so it demotes itself — whether or not it
  ever learns a takeover occurred.

Either way, at most one node has a committed lease *and* a reachable
quorum, without any assumption about synchronized clocks.

**The premise in that case analysis, stated.** It reasons about
*disjoint* partition sides. Real failures are not always cuts. Under
**asymmetric** reachability — one node's dials to the holder fail while
everyone else reaches it — a candidate can be in the majority *and* the
holder can be in the majority, because "sides" no longer partition the
cluster. The safety invariant survives (the CAS admits exactly one
holder, and the deposed one fences as soon as it reads the store), but
what it buys is smaller than it looks: the takeover is *unnecessary*, and
between the CAS and the ex-holder's next read a healthy primary is still
serving writes it can no longer have acknowledged by a quorum. Cost paid
for nothing, on the say-so of the one node that could not see.

The store cannot arbitrate this — it has no notion of whether the
incumbent is alive. So candidacy asks the cluster, and this is the
**second-opinion gate**: every node reports how long ago it last observed
each peer **serving as a primary** (`NodeStatus.peer_primary_seen_age_ms`,
an age rather than a timestamp, so no clock assumption is added), and a
candidate about to depose a holder it cannot see stands down if any
*reachable* member has watched that holder serve within `leader_ttl`.

> **It must vouch for the role, not the socket**, and the first cut got
> this wrong at real cost. When a holder's PostgreSQL dies its agent
> keeps answering `GetStatus` perfectly, so a reachability-based witness
> truthfully reports "I reached the holder 1.1 s ago" and every candidate
> defers — deadlocking the single most common failover in existence. See
> finding 25.

The gate is self-clearing by construction: a genuinely dead holder makes
every witness's age exceed the ttl within one ttl, so failover proceeds
after a bounded delay and can never deadlock. It also does not touch the
paths that matter most — a fully isolated holder is unreachable to
*everyone* (no witness, gate opens), and a vacant lease has no incumbent
to defend.

Three consequences of framing the lease this way:

- **Retain is a read, not a write.** The holder confirms it still holds
  the lease via a linearizable read (openraft's `ensure_linearizable` — a
  ReadIndex quorum round-trip, no disk write). If it cannot complete one
  within `retry_timeout`, it demotes local PostgreSQL. Quorum contact is
  the thing being tested, so a read tests it exactly as well as a write.
- **The log only grows on real events.** Writes happen on holder change,
  pause/resume, and membership change — not every tick. Steady state is
  *zero* log entries per day, so log compaction and snapshotting stop
  being load-bearing.
- **Takeover is a CAS** on `(expected_holder, expected_term)`, proposed
  after the candidate has observed the holder unhealthy for `leader_ttl`.
  Concurrent candidates are serialized by Raft; one wins, the rest see
  their expected-term precondition fail.

**Invariant:** `leader_ttl >= loop_wait + 2 * retry_timeout` — the holder
exhausts its retry budget and demotes before any candidate is eligible to
propose a takeover. This buys *hysteresis*, not correctness: violating it
causes unnecessary failovers, not split-brain.

**Second invariant:** `retry_timeout > worst-case Raft election duration`.
A linearizable read cannot complete while an election is in flight —
ReadIndex needs a leader — so a retry budget shorter than an election
converts every Raft re-election into a demotion of a healthy primary.
This is not hypothetical: it is the recurring field failure that got
Patroni's embedded-raft backend deprecated (see prior art below). Like
the first, violating it costs availability rather than correctness — but
it is the availability failure this design is most likely to actually
exhibit, so it is stated rather than discovered. Both are enforced at
config load.

#### What the state machine holds

```
leader     : { holder: node_id, term, since }
paused     : { bool, reason, set_by, at }
switchover : Option<{ target, not_before }>
membership : Raft's own configuration
generation : u64
```

**The log holds decisions, not progress.** `inflight_ops` stays local —
it is a journal of "what is this node in the middle of doing", and
replicating it would put every basebackup phase transition into the
consensus path. A promotion is one committed decision followed by a
locally-journaled orchestration.

#### Storage: redb, not RocksDB

The state machine is a few hundred bytes and steady-state writes are
zero, so RocksDB's advantages — all throughput-and-scale — buy nothing
here, while its costs (minutes of cold build, libclang/bindgen, tens of
MB of binary, compaction threads competing with PostgreSQL, and the first
non-Rust dependency in a tree that is pure Rust on purpose) are paid
unconditionally.

Two observations collapse most of the remaining gap:

- **Neither engine is where the risk lives — our storage impl is.** The
  impl passes openraft's storage conformance suite (`openraft::testing`)
  in CI. Once that is a hard requirement, "RocksDB is better tested"
  stops transferring, because the tested part is the part we are not
  writing. **That requirement, not the engine choice, is what makes this
  layer trustworthy.** It was checked for teeth rather than assumed:
  dropping the recorded purge point, and an off-by-one making `truncate`
  exclusive, each fail the suite.
- **A Raft node's log is recoverable from its peers.** On corruption, or
  a format change across a redb major version, recovery is: stop the
  agent, delete `<state_dir>/raft/`, restart, let Raft re-replicate. That
  makes the engine a genuinely low-stakes choice — and that recovery is
  executed by the acceptance suite rather than merely asserted.

#### Transport

A `PgAgentRaft` gRPC service on the existing peer listener — same port,
same certs, same SAN allowlist, so the mTLS gate that guards the peer
plane's mutating RPCs guards this one unchanged.

One non-obvious requirement: **give Raft its own channel.**
`AppendEntries` heartbeats are small, frequent, and latency-critical;
`Basebackup` streams gigabytes. Sharing an HTTP/2 connection lets a
saturated basebackup starve heartbeats at the TCP layer and trigger a
spurious election — during a recovery, which is exactly when we least
want one. Separate connection, same endpoint.

The store stays behind a trait, not for "Consul later" but because a
deterministic in-memory store turns partition and failure cases into
ordinary unit tests instead of a lab exercise.

#### Implementation decisions the design did not pin down

- **Time is proposed, not read.** `Utc::now()` cannot appear in `apply` —
  replicas would diverge on `Lease.since`. The timestamp is minted by the
  proposing node, carried in the command, and applied verbatim everywhere.
- **Raft node ids are `u64`; agent node ids stay `i32`.** They convert at
  the seam, so `ClusterState` is unchanged by which store is behind it.
- **Frames are opaque.** Requests cross the wire as serialized openraft
  types, not protobuf-mirrored fields — mirroring would re-declare a
  large slice of openraft's internals and re-do it every upgrade, buying
  interop that cannot arise (both ends are the same binary at the same
  version). The cost is real and named in the `.proto`: `grpcurl` sees a
  blob, so debugging the consensus plane goes through agent logs and
  openraft metrics.
- **Every transport failure is `Unreachable`, never `NetworkError`.**
  openraft hot-retries the latter and backs off on the former, and
  hot-retrying a partitioned peer spins CPU on the node still trying to
  hold quorum together.
- **"Retain is a read" is a read only the Raft leader can perform.**
  `ensure_linearizable` fails on a follower and `client_write` returns
  `ForwardToLeader`. Since the central invariant is that the Raft leader
  is *not* the PostgreSQL primary, on most nodes most of the time both
  retain and takeover are an RPC to another node. `Propose` and
  `ReadState` exist for exactly this, and forwarding is **one hop, never
  two** — the leader-side handlers refuse rather than re-forward, because
  a chain would make latency unbounded in precisely the churny conditions
  where the `retry_timeout` budget is tightest. What was under-priced was
  latency, not write volume.
- **`Err` means unknown, at every failure path**: no leader known, leader
  unreachable, `ensure_linearizable` refused, RPC timed out. A test
  asserts that a node with no quorum errors rather than reporting a
  vacant lease — §3's hole would otherwise walk back in as an
  `unwrap_or_default`.
- **Membership is formed at daemon startup**, from the configured
  `[[pool]]`, and also by `ClusterInit`. It was `ClusterInit` alone
  until v0.9.0, on the reasoning that forming the pool is the
  operator-driven "this is the cluster" moment and that daemons racing
  to declare one would be a hazard. Both halves were wrong, and a real
  cluster died of it.

  There is no race to lose: membership is a config file, so every node
  computes a byte-identical set and concurrent `initialize` calls
  cannot disagree about what the cluster *is* — only about which
  proposal commits, which Raft already settles. Startup staggers by
  pool position anyway, to spend no election on it.

  And gating it on `ClusterInit` made it unreachable for any cluster
  that already existed. `ClusterInit` basebackups every standby, so it
  is not a command run against a live deployment; an existing cluster
  upgraded into the Raft releases therefore came up with an empty
  store, no voters, and no leader electable *ever*. Cold start then
  reads the lease as unreadable and leaves PostgreSQL down on every
  primary-shaped node — permanently, with no operator exit documented
  anywhere. A consensus layer whose bootstrap can only be reached by a
  destructive command has no bootstrap on the path that matters.

  It remains idempotent and never fatal — to startup or to
  `ClusterInit`. A node that cannot form the pool is no worse off for
  having tried, and failing `ClusterInit` over it would send the
  operator back to re-run destructive work that already succeeded. A
  node that recovers membership from its own log does not re-form it,
  or a restart could redefine who the members are.
- **The election window is derived, not configured.** `[raft]` carries
  one upper bound and the daemon randomizes half-to-full, because a
  single value has every node time out together and split the vote.

### The HA loop

The daemon used to be *purely reactive* — it acted only when pgpool poked
it. Every `loop_wait`, each node now performs a linearizable read of the
lease and emits one decision:

```
if I hold the lease:
    assert PG running && !in_recovery   (else demote + release)
    on read failure past retry_timeout: DEMOTE LOCAL PG

else if someone else holds it:
    if holder changed since last cycle: re-point the local standby
    if holder unhealthy for leader_ttl: become a candidate

else (vacant, or holder unhealthy past leader_ttl):
    if not eligible: skip
    compare flush positions against reachable peers
    if not strictly most-advanced: skip, with jittered backoff
    propose CAS(expected_holder, expected_term); winner promotes
```

- **"Cannot read" is not "vacant."** A node that has lost quorum cannot
  distinguish those by asking, and treating a failed read as an empty
  lease reintroduces §3's hole through the front door. Unknown means take
  no role-changing action — and if we currently hold the lease, unknown
  past `retry_timeout` means demote.
- **The loop runs on standbys too.** A standby is what detects a dead
  holder and becomes a candidate. This is the concrete sense in which the
  daemon stopped being reactive.
- The CAS is the **gate**; `inflight_ops` remains the **record**.

The loop is a pure decision function; everything destructive lives in the
executor (`roleexec`), which holds the Systemd/Pcp/StandbyOps handles the
loop deliberately does not. Decisions map to convergent actions: takeover
→ journaled promotion; demote → fence, never gated on journaling (a
broken journal must not stand between the loop and stopping a lease-less
primary); holder change → re-point the standby. The executor also closes
the stale-primary half of §2.1 from the other side: a node running as
primary while someone else holds the lease is fenced, even though the
decision layer only says `Following`.

**Demote policy: stop and wait.** A fenced node stays stopped; rejoining
is `cluster recover`, the operator's call. The executor never runs a
destructive rebuild.

### What this costs

The safety property comes from demote-on-quorum-loss, and that primitive
is also the bill. A primary that cannot reach a quorum for
`retry_timeout` **shuts down its own write path**, whether or not
anything is wrong with PostgreSQL. That failure mode did not exist
before.

Stated plainly, the trade is **a rare correctness failure for a less rare
availability failure.** That is not obviously a good deal. Three things
have to hold for it to be the right one:

- **The quorum is the cluster.** Embedding is what makes this defensible.
  "Quorum lost while PostgreSQL is fine" is not an independent new way to
  break — losing a majority of agents means losing a majority of *nodes*,
  which is the event that produced split-brain before. We are making an
  existing break fail closed rather than fail dangerous.
- **The demote path is correct.** It is the most safety-critical code in
  the daemon: it runs rarely, under degraded conditions, and a bug is
  either an outage or the original defect. It needs fault injection, not
  just unit tests.
- **A minority node stays useful.** Losing quorum degrades to read-only,
  not to "agent falls over". Standbys keep streaming, `/healthz` keeps
  answering, only role *changes* are blocked.

Costs specific to embedding:

- **We own the storage and network impls** — hence the conformance-suite
  requirement.
- **openraft is pre-1.0** and has had real breaking churn. Pin it, and
  budget upgrade work as recurring rather than one-off.
- **Restarting `pg_agentd` is now a Raft member restart**: log recovery,
  rejoin, possibly an election. The acceptance suite measures the
  restart window against `leader_ttl` on every rolling-upgrade run rather
  than assuming the margin.
- **`panic = "abort"`** means a panic anywhere takes down a Raft member.
  Keep it — a consensus participant with corrupt in-memory state is worse
  than a dead one — but an unrelated handler bug now costs a vote.

One satisfying result: the 2026-06-11 trigger — a brief `pg_agentd`
restart — is a **non-event by construction**. A restarting agent loses
its vote temporarily; it cannot be promoted away from unless it is
genuinely gone past `leader_ttl`, and if it were, the takeover would be
safe anyway.

### Alternatives considered

**External etcd.** Buys a more battle-tested store and the option to
place the quorum on separate nodes. Costs a second distributed system to
deploy, bootstrap, upgrade, back up, cert-manage, and recover — and its
central safety argument ("co-locate it so the failure domains are
shared") is a deployment convention rather than a structural guarantee.
For a three-node Ansible-managed cluster, running a second quorum to
coordinate the first is the larger operational burden. **Rejected**, but
it is the natural fallback if the embedded storage layer ever proves
harder to trust than expected — the trait keeps that door open.

**A hand-rolled quorum lease on the peer mesh.** Self-demote on losing
contact with a majority; refuse to promote without one. With three nodes
only one partition holds 2/3, so it does deliver the safety property with
no new dependency. **Rejected** — openraft dominates it:

- **No durable epoch.** No fencing token, so a node that was partitioned
  and rejoins has no record proving it lost. Raft's term gives this free.
- **Asymmetric views miscount.** Each node computes the majority from its
  own reachability, so one-way network breakage lets a node believe it
  has quorum when it does not.
- **We would author the timing assumptions ourselves**, which is exactly
  what using a real Raft implementation removes.

These two were the ends of a false choice between *no new dependency but
hand-rolled correctness* and *inherited correctness but a new daemon*. An
embedded Raft library is both ends at once.

### Prior art: Patroni's `raft` backend (pysyncobj)

Patroni shipped exactly this shape — consensus embedded in the agent, no
external DCS — in 2.0 (2020), via pysyncobj under its DCS abstraction. It
never left beta and was deprecated in 3.0.0 (2023). The record cuts both
ways, and both cuts matter.

**It confirms the demand.** The issue tracker is full of users asking for
this design's pitch: *"I'd like to get rid of etcd as it's an additional
layer"* ([#2112](https://github.com/patroni/patroni/issues/2112)), *"we
are not able to deploy hosts only for DCS"*
([#2147](https://github.com/patroni/patroni/issues/2147)). The
maintainer's fallback answer — run etcd co-located on the database nodes
— is the deployment-convention posture this design rejects as
non-structural.

**Why it died, and why those reasons do not transfer.** Three causes,
none architectural:

- **Never dogfooded.** *"We don't use Raft and all recent bugfixes were
  triggered by reports of existing users"*
  ([#2041](https://github.com/patroni/patroni/issues/2041)).
- **An opaque, unowned consensus library.** The stated deprecation
  reason, verbatim: *"'Occurred randomly, can not be reproduced' — that's
  the main reason we declared Raft support as deprecated"*
  ([#3051](https://github.com/patroni/patroni/issues/3051)).
- **An emulation layer.** Patroni's DCS abstraction is etcd-shaped — TTL
  keys, watches, CAS — and the raft backend had to emulate those on top
  of pysyncobj. We own the state machine natively; there is no
  impedance-mismatch layer for semantics to diverge in.

The mitigations here target exactly that failure class: the conformance
suite as a hard CI gate (the answer to "cannot be reproduced" is a
storage layer exhaustively tested before it ships), the deterministic
in-memory store for fault injection, and an acceptance suite that boots
the real artifacts on a real three-node cluster and *manufactures* the
failures rather than waiting for a user to report one — on the same
cluster the author operates, so it is dogfooding by construction.

**The transferable lesson.** The recurring field-failure signature —
[#1701](https://github.com/patroni/patroni/issues/1701),
[#2147](https://github.com/patroni/patroni/issues/2147),
[#3051](https://github.com/patroni/patroni/issues/3051), spanning
2021–2024 — was raft leadership churn causing *"failed to update leader
lock"* and a spurious PostgreSQL failover. That is the "Raft leader is
not the PostgreSQL primary" invariant being violated through the subtle
door: not by conflating the roles, but by the lease-refresh path failing
during elections. Hence the second lease invariant. **The failure mode to
fear in this design is not split-brain — the quorum forecloses it — it is
spurious demotion via the raft plane**, and Patroni's history is the
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

`failover_command` was the bridge that let (1) drive (2). Cutting that
bridge is the entire change.

| Setting | Value | Rationale |
|---|---|---|
| `use_watchdog` | **off** | see below |
| `failover_command` | **notify-only** | hint, not order |
| `follow_primary_command` | **empty** | the agent reacts to lease change instead; a non-empty hook makes pgpool degenerate every healthy standby after a primary failover |
| `sr_check_period` | **keep** | this is how pgpool *learns* the primary |
| health checks | **keep** | per-instance routing, self-limiting |
| `detach_false_primary` | **on** | defense in depth |
| `auto_failback` | **off** | agent owns reattach via pcp |

The hook-by-hook working — exact 4.6 firing semantics under
`use_watchdog = off`, the full hook table, and the cost this section had
not priced (backend-status sync between pgpool instances is a watchdog
feature, so attach becomes an agent-side fan-out) — is in
[pgpool-hook-contract.md](pgpool-hook-contract.md).

**Dropping watchdog.** We already ran it quorum-only with no VIP, so
there was no VIP machinery to lose. What we *do* lose is
`failover_when_quorum_exists` and `failover_require_consensus` — together
the gate that stopped three pgpool instances each firing
`failover_command`. That would have been a regression before; it stops
mattering the moment promotion is a CAS, because N concurrent hints
converge to one outcome. **Idempotence replaces coordination.** The
watchdog became removable not *in spite of* the decision moving but
*because* it moved, which is a useful signal that the design is coherent.

**pgpool still learns the new primary without us.** `sr_check` polls
backends and classifies primary vs. standby every `sr_check_period`. We
cut the causing and keep the learning.

**Leave `detach_false_primary` on.** It is no longer *deciding* anything
— it just refuses to route to something incoherent, which is exactly the
backstop wanted if fencing ever fails.

---

## 7. What this preserves

This is a re-scope, not a retreat. The mechanism layer is where the
project's differentiation lives, and Patroni is *worse* at both of these:

- **pgpool integration.** Patroni has no pgpool story at all — it assumes
  HAProxy plus optionally pgbouncer. `pcp_attach_node`/`detach`,
  `pool_passwd`, `gen-pgpool`, the SPEC §6 hook contract: nobody else has
  built this. If you run pgpool, Patroni does not help you.
- **Phased orchestration with operator visibility.** `inflight_ops`,
  resumable, with `ops list` / `resume` / `abandon`. Patroni's `reinit`
  is an opaque black box: it works or you run it again.

And embedding adds a third, which is a **moat rather than parity**:

- **No DCS to run.** Patroni structurally requires an external
  etcd/Consul/ZooKeeper quorum; for most small deployments that store is
  the majority of the operational burden.

The pitch becomes:

> **Patroni's safety model, pgpool's ecosystem, no DCS to operate.**

One property worth noting: once role lives in the replicated log,
**pgpool becomes optional rather than load-bearing.** We keep it for
pooling and read load-balancing, both real value. But nothing in the
correctness story depends on it — and given pgpool is the component whose
failure detector caused the incident, supporting it without trusting it
is a strictly better place to stand.

---

## 8. Remaining work

Role-aware `/healthz` and the HAProxy split. With an authoritative role,
`/healthz` can support a `/primary` + `/replica` split; that decision was
blocked precisely because **no component authoritatively knew the role**
— pgpool inferred it, the agent was told it. The lease supplies it. See
SPEC §9.1 for why the current single-endpoint contract is deliberate, and
ROADMAP.md for where the split sits.
