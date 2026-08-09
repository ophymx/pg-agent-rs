# Promotion authority — relocating the failover decision

**Status:** design handoff, not yet scheduled. Nothing here is implemented.

Companion to [SPEC.md](../SPEC.md) (SPEC §5.1),
[ROADMAP.md](../ROADMAP.md) ("Shared cluster state"), and
[TODO.md](../TODO.md) ("pre-execution cluster-state validation pattern").

Section-reference convention: references to SPEC.md are always written
`SPEC §N`; a bare `§N` is a section of this document.

This document argues that the daemon's single most important defect is
**structural, not a bug**: pg-agent-rs derives "who should be primary"
from pgpool-II, and pgpool cannot answer that question. It proposes
moving the decision onto a quorum-backed lease, and is explicit about
what that does and does not change.

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

### 2.2 Candidate selection ignores WAL position

Independent of split-brain, and easy to miss. From the pgpool failover
documentation, `%m` (new main node) is selected as:

> the node being assigned the youngest (smallest) node id which is alive

**Lowest alive node ID.** Not most-advanced WAL. Not least lag. Not "is
it even caught up."

SPEC §5.1 promotes that pick with no lag gate. `MAX_HANDOFF_LAG_BYTES` guards
`cluster_handoff` — the *planned* path — not reactive failover. So even
in the case where pgpool is entirely correct that the primary is down, it
can hand us a candidate that is arbitrarily far behind, and we promote it
and drop the WAL delta on the floor.

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

**Action:** if the gossip plane ships, add a hard scope note to the
ROADMAP item stating that role assignment is out of scope for it.
Otherwise it is the natural place someone puts role in two years.

### Re-pricing the original tradeoff

The stated reason for avoiding a DCS was dependency cost. That was
mispriced: avoiding etcd did not avoid complexity, it *relocated* it.
Replay markers with TTLs, the `inflight_ops` journal, the durable
maintenance queue, best-effort cleanup contexts, `bypass_replay_marker`,
and now a shared precondition validator — a substantial fraction of that
machinery exists to compensate for not having a serialization point.

A co-located etcd sits inside the same 2-of-3 failure envelope the cluster
already has, so the *availability* math does not get worse in the cases we
already plan for. It is not free — see "What this costs" below — but the
cost is operational surface, not a new class of outage.

---

## 5. Target architecture

Split the daemon along the line Patroni draws, and only that line.

Subsections here are deliberately unnumbered, so that `§5.1` unambiguously
means SPEC's `Failover`.

### Delegate: consensus

- **Leader lease.** Key `/pg_agent/<scope>/leader`, value `<node_id>`,
  TTL `leader_ttl`. Holding it *is* being primary. Nothing else confers
  the role.
- **Promotion is a CAS.** Whoever wins the create-if-absent is primary.
  Not a parameter, not an announcement, not a vote.
- **Demote on DCS loss.** A leader that cannot reach the DCS for
  `retry_timeout` demotes its own PostgreSQL *before* the TTL can expire
  and someone else can win. This is the fencing primitive.
- **Invariant:** `leader_ttl >= loop_wait + 2 * retry_timeout`. It
  guarantees the old leader exhausts its retry budget before its key can
  be taken.

**Do not implement Raft.** Put the store behind a trait — SPEC §4
("Dependency-injection seams") already establishes the pattern. etcd via
the `etcd-client` crate is the reference implementation; see "Alternative"
below for why the trait matters more than the choice.

### Keep: mechanism

Everything below the decision layer stays, unchanged in intent:

- promote / rewind / basebackup / slot lifecycle
- the phased `inflight_ops` journal and `ops list|resume|abandon`
- pgpool integration: `pcp_attach_node` / `pcp_detach_node`,
  `pool_passwd`, `gen-pgpool`, the SPEC §6 hook contract
- the mTLS peer mesh and remote command dispatch (no SSH)
- the durable maintenance queue

The peer mesh survives, but its role changes: it carries **work**
(create this slot, run this basebackup, start postgres), not
**coordination**. Commands, not consensus.

### The HA loop (genuinely new code)

The daemon is currently *purely reactive* — it acts only when pgpool pokes
it. A lease-backed design needs its own periodic loop, roughly Patroni's
`ha.py`. Every `loop_wait`:

```
read leader key, read local PG state

if I hold the lease:
    assert PG running && !in_recovery   (else demote + release)
    refresh lease (CAS on holder == me)
    on refresh failure past retry_timeout: DEMOTE LOCAL PG

else if someone else holds it:
    if holder changed since last cycle: follow_primary(new holder)

else (vacant):
    if not eligible (nofailover tag, lag > max_lag_on_failover): skip
    compare WAL position against reachable peers via GetStatus
    if not most-advanced: skip one cycle
    attempt CAS create; winner promotes
```

Notes:

- Candidate comparison uses the existing `GetStatus` peer RPC — the data
  is already there; SPEC §5.1 just never consults it.
- The CAS is the **gate**; `inflight_ops` remains the **record**. A
  promotion is still a journaled orchestration.
- Per-node `nofailover` / `clonefrom` tags (already on the ROADMAP)
  become eligibility inputs rather than a separate feature.
- "skip one cycle" needs a tiebreak. Two candidates reading each other's
  WAL position at slightly different instants can each conclude the other
  is ahead and both skip forever. Jittered backoff, or break ties on node
  id once positions are within a threshold.

### What this costs

The safety property comes from demote-on-DCS-loss, and that primitive is
also the bill. A primary that cannot reach an etcd quorum for
`retry_timeout` **shuts down its own write path**, whether or not anything
is wrong with PostgreSQL. We would be introducing a failure mode that does
not exist today: *etcd unavailable → cluster unavailable.*

Stated plainly, the trade is **a rare correctness failure for a less rare
availability failure.** That is not obviously a good deal and should not be
smuggled in as an implementation detail.

Two things make it defensible, and both need to hold:

- **Shared failure domain.** With etcd co-located on the db nodes, "etcd
  quorum lost while PostgreSQL is fine" is mostly a network partition —
  the same event that produces split-brain today. We are not adding an
  independent thing that can break; we are making an existing break
  fail closed instead of fail dangerous.
- **The demote path is correct.** It becomes the most safety-critical code
  in the daemon: it runs rarely, under degraded conditions, and a bug in it
  is either an outage (demotes when it shouldn't) or the original defect
  (doesn't demote when it should). It needs fault-injection tests, not just
  unit tests.

Also real, and worth budgeting rather than discovering: etcd is operational
surface — quorum-loss recovery, disk pressure on its WAL/snapshot dir, cert
rotation, version upgrades — and on co-located nodes its fsync traffic
competes with PostgreSQL's. There is currently no runbook for "the store is
down but the database is healthy" because there is currently no store.

### Alternative: a quorum lease on the peer mesh

Worth pricing explicitly, because the jump from "check-then-act cannot
work" to "therefore etcd" skips a step. The mesh we already have can carry
a lease:

- A primary self-demotes if it cannot reach a **majority of peers** for
  `retry_timeout`.
- A candidate refuses to promote unless it reaches a majority *and* is the
  most-advanced node among those it can reach.

With three nodes only one partition can hold 2/3, so at most one node can
satisfy the promote precondition. That is the safety property, with no new
daemon and no new outage mode from a store being down.

Why it is genuinely weaker:

- **No durable epoch.** There is no fencing token, so a node that was
  partitioned and rejoins has no record proving it lost. etcd's revision
  gives that for free.
- **We would be writing the hard part.** Correctness rests on timing
  assumptions we author and test ourselves, rather than inheriting an
  implementation with a decade of adversarial testing behind it.
- **Asymmetric views miscount.** Each node computes the majority from its
  own reachability. A node whose network is broken in one direction can
  believe it has quorum when it does not — precisely the class of bug a
  replicated log exists to eliminate.
- **It solves only role.** The other shared state the ROADMAP wants
  (`paused`, scheduled switchover, generation counter) still has nowhere
  principled to live, so it falls back to LWW gossip — the problem in §4
  above, unresolved.

**Which one:** undecided, and deliberately so. The important observation is
that the HA loop, the eligibility rules, the WAL comparison, and the demote
path are **identical under both**. Only the store differs. That — not
"Consul later" — is the real argument for putting the store behind a trait:
it lets the expensive, novel work (steps 4–5 in §10) proceed while this
question stays open, and lets shadow mode run against the peer-mesh
implementation, which needs no new infrastructure to stand up.

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

| Setting | Today | Target | Rationale |
|---|---|---|---|
| `use_watchdog` | on (quorum-only, no VIP) | **off** | see below |
| `failover_command` | promotes `%m` | **notify-only or removed** | hint, not order |
| `follow_primary_command` | reconfigures standbys | **notify-only or removed** | agent reacts to lease change instead |
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
| §1.1 | Cluster layout diagram: watchdog line goes; DCS appears. |
| §3.2 `PgAgentPeer` | Unchanged surface, restated purpose: work dispatch, not coordination. |
| §4 | Add the DCS trait as a new injection seam. |
| **§5.1 `Failover`** | **Rewrite.** Drops `new_main` from the signature entirely. Becomes: evaluate eligibility → compare WAL → attempt CAS → promote iff won. |
| §5.2 `FollowPrimary` | Trigger changes from pgpool hook to observed lease change. |
| §5.5 `Escalation`/`DeEscalation` | Already no-ops; can be deleted with watchdog. |
| §6 | Hook contract: `failover_command` / `follow_primary_command` reclassified as advisory notifications. |
| §8 | New `[dcs]` config block: endpoints, TLS material, `scope`, `leader_ttl`, `loop_wait`, `retry_timeout`, `max_lag_on_failover`. |
| §9 `/healthz` | Can now report role **authoritatively** (lease-holder or not). |
| §14 `validate-env` | Add DCS reachability + lease-key permission checks. |
| new § | The HA loop, lease protocol, demote path, candidate selection. |

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

The ROADMAP bar — *"be a more pleasant HA layer to operate than Patroni,
on top of the pgpool-II substrate we're stuck with"* — is unchanged and
better served. The pitch becomes:

> **Patroni's safety model, pgpool's ecosystem, better orchestration
> ergonomics.**

And the honest answer to "why not just use Patroni" becomes *"because I
run pgpool"* — a good answer — rather than *"because I didn't want an
etcd dependency,"* which 2026-06-11 already charged us for.

One property worth noting: once role lives in the DCS and HAProxy routes
on an authoritative `/healthz`, **pgpool becomes optional rather than
load-bearing.** We keep it for pooling and read load-balancing, both real
value. But nothing in the correctness story depends on it. Given pgpool
is the component whose failure detector caused the incident, supporting
it without trusting it is a strictly better place to stand.

---

## 9. Open questions

Decisions to make before implementation, not blockers to the design:

1. **Which store — etcd, or a quorum lease on the peer mesh?** The open
   question this document deliberately does not close. See "Alternative" in
   §5. Deferrable behind the trait; not deferrable past the cutover in
   step 6 of §10.
2. **DCS placement**, if etcd wins. Co-locate on db0/1/2 (same 2-of-3
   envelope, no new containers, competes with PG for fsync latency), or
   separate nodes? Co-location is the standard small-cluster answer, and
   the shared failure domain is load-bearing for the argument in "What
   this costs."
3. **`failover_command`: removed, or notify-only?** Notify-only buys
   detection latency at the cost of a hook path that must be documented
   as non-authoritative forever.
4. **Synchronous replication.** Patroni's `synchronous_mode` maintains
   `synchronous_standby_names` and refuses to promote a node that was not
   in sync — trading write latency for zero-data-loss failover. Do we
   want an equivalent, and is it v1 of this work or later?
5. **Migration path.** Can a cluster move from hook-driven to lease-driven
   in place, or does it need a maintenance window? A cluster where some
   nodes have the HA loop and some do not has no safe semantics — this
   likely needs `cluster pause` (already on the ROADMAP) as a
   prerequisite, making pause/resume a dependency rather than a peer.
6. **Verify under `use_watchdog=off`:** pgpool docs describe
   `failover_command` as running "once per failover event," but that is
   written for single-instance semantics. Confirm empirically how many
   times it fires across three uncoordinated instances. This does not
   change the design — CAS makes it safe either way — but it should be
   documented rather than assumed.

---

## 10. Suggested sequencing

Effort tags follow ROADMAP convention (**S** = days, **M** = weeks,
**L** = month-scale).

1. **(S)** Land `validate_cluster_preconditions` with the
   `detached`-is-actually-down check, per TODO. Closes the known trigger
   now. Label it defense-in-depth in the code comment.
2. **(S)** Add the ROADMAP scope note that the gossip plane never carries
   role.
3. **(S)** Add a lag gate to reactive failover, reusing
   `MAX_HANDOFF_LAG_BYTES` logic. Fixes §2.2 independently of the DCS
   work and is valuable even if the rest slips.
4. **(M)** DCS trait + `[dcs]` config + `validate-env` checks, with at
   least one backing implementation. No behaviour change yet; the lease is
   written and refreshed but not consulted. The peer-mesh implementation is
   the cheaper one to stand up first; etcd can follow behind the same trait.
5. **(M)** The HA loop, shadow mode: compute what it *would* decide, log
   it, keep obeying pgpool. Run it on the live cluster and diff the two
   decision streams. This is the highest-value step — it turns the whole
   argument empirical before anything destructive changes.
6. **(M)** Cut over: SPEC §5.1 rewrite, pgpool config contract, watchdog off.
7. **(S)** `/healthz` role reporting + the role-aware HAProxy split in
   home-ansible.

Steps 1–3 are worth doing regardless of whether the rest is ever
scheduled.
