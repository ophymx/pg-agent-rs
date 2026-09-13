# Quorum commit: making the lease's terms bind writes

Why acknowledging a commit requires the cooperation of a node that
follows the lease, and what that buys. Shipped; this is the reasoning,
not a plan.

## 1. The gap this closes

The raft lease serializes *promotion*: terms are fencing tokens, no term
is ever won twice, and every takeover is a quorum CAS
([promotion-authority.md](promotion-authority.md) §5). And yet finding
18's incident had two nodes serving concurrently, and finding 17 measured
a 44 s fence drain — because **nothing at the write path ever checks a
term**. A deposed primary keeps accepting and locally committing writes
until its fence lands; those commits were acknowledged to clients and are
then discarded when the node is recloned onto the new timeline. Consensus
was flawless both times. The binding between "holds the lease" and "may
durably acknowledge writes" was enforced only by fence *latency*.

The fix is not to teach PostgreSQL about terms. It is to arrange that
**acknowledging a commit requires the cooperation of a node that follows
the lease** — then a primary that has lost the lease loses the ability to
acknowledge, mechanically, within one follow-convergence, regardless of
how long its own fence takes.

## 2. The mechanism

PostgreSQL synchronous replication, quorum flavor:

```
synchronous_standby_names = 'ANY 1 (node0, node1, node2)'   # minus self
synchronous_commit        = on                              # remote flush
```

A COMMIT does not return success until its WAL is flushed on at least one
listed standby. Three consequences, in increasing order of importance:

1. **Durability**: every *acknowledged* write exists on ≥ 2 of 3 nodes. A
   single node death — including the primary's — loses no acknowledged
   write.
2. **Selection soundness**: combined with flush-position candidacy (§4),
   the takeover winner always possesses every acknowledged write.
3. **Write fencing — the term binding.** Standbys stream from the node
   their executor points them at, and executors follow **the lease
   holder, only**. When the lease moves at term T+1, the executors
   re-point every standby at the new holder; the deposed primary's
   walsenders lose their consumers, and from that moment its commits
   *hang*. A standby acknowledging a primary's commit is thereby the
   delegated form of endorsing its term. The stale primary's fence stops
   mattering for acknowledged-write safety: everything in the un-acked
   window dies *unacknowledged*, which is the contract every client of
   every database already accepts.

The lease protocol itself changes not at all. The sync topology is
*derived state*: executors, which already own role convergence, also own
`synchronous_standby_names` on whichever node is primary — armed at the
first-standby-attached event, repaired on membership drift, never
auto-disarmed.

## 3. The safety argument, spelled out

3-node cluster, agent quorum = 2. Enumerate the shapes:

- **Primary dies (no partition).** Both standbys reachable; ANY 1
  guaranteed at least one of them flushed each acked commit; candidacy
  picks a winner that has all of them.
- **Primary isolated alone.** Its lease confirmation fails (quorum lost)
  → it decides demote and fences. Its commits already hang the moment
  both standbys re-point (or the partition severs the walsenders — same
  effect, faster). Majority side has both standbys; no acked write is
  missing there.
- **Primary + one standby isolated together.** With 2 of 3 agents
  isolated together they ARE the quorum: the isolated pair can still ack
  writes and retains the lease, no takeover happens elsewhere, no
  conflict. The lone node follows on heal.
- **Total partition (1/1/1).** No quorum anywhere: lease unconfirmable →
  holder fences; no standby acks → commits hang first. Unavailable,
  consistent — the chosen CAP corner.
- **Double failure (both standbys dead).** Commits hang (§5). If the
  primary then also dies, the last acked writes exist only on whichever
  standby acked them — recover the newest. ANY 1 on N=3 durably tolerates
  one failure; that is the trade.

The invariant, stated for the auditor and for operators: **every
acknowledged commit is flushed on at least two nodes, and any takeover
the lease permits selects a winner holding all of them.**

## 4. Candidacy compares flush positions, not replay

The acked guarantee attaches to *flushed* WAL, and a standby can be the
unique holder of an acked commit it has not yet replayed. Selection by
replay could crown the other standby and lose an acknowledged write — the
whole point defeated by one field. So `NodeStatus.last_flush_lsn` carries
the flush position and candidacy compares `(timeline, flush_lsn)`
**strictly**: any reachable peer with more flushed WAL outranks,
byte-for-byte, and node id breaks exact ties only.

The former ±`max_lag_on_failover` tiebreak band let a lower-id node up to
16 MiB behind win — under ANY 1 an acknowledged write can live exactly in
that delta on the higher-flush standby, so the band contradicted §3's
invariant, and it was also finding 15's wedge cause (only a behind-node
winner leaves a loser past the fork point).

**The candidacy freeze** (finding 23). "Positions are static while
candidacy decides" holds only when the primary is dead. In the fence-less
deposal — the holder's AGENT dead, its PostgreSQL still serving — the
standbys keep streaming and their flush positions keep MOVING, and a
moving stream has no stable order: each candidate compares its own
point-in-time flush against peers' fresher reports, reads itself behind,
and everyone defers forever. So candidacy freezes first: a candidate
still receiving detaches (`DetachingFromDeposed` → conninfo-less
`myrecovery.conf` + reload; PostgreSQL keeps serving reads), and
positions are compared only once every counted candidate has stopped
receiving.

The freeze also *completes* §3's fence — the moment the candidates
detach, the deposed primary has zero ack sources — and it makes
strict-max provably loss-free, because an ANY-1-acked row at LSN L was
flushed by some standby before it froze, so the frozen maximum is ≥ L.

Promotion replays everything received before exiting recovery, so a
flush-ahead/replay-behind winner promotes correctly: replay distance is
promotion *latency*, not a safety input.

## 5. Availability: managed, not configured away

The availability cliff of sync-rep is real: with no reachable listed
standby, commits hang indefinitely. Positions taken:

- **The cliff is correct.** A primary that no standby can reach is either
  partitioned (its acks would be lies about to be discarded) or the
  cluster is degraded to one node (acks would be single-copy promises
  this product exists to not make). Hanging is honest; the lease will
  usually fence such a primary shortly anyway.
- **Bootstrap and rebuild are managed states.** The requirement is armed
  only at the *first-standby-attached* event, so `cluster init` works
  unchanged — the bootstrap primary runs async-solo until its first
  basebackup child attaches. A single standby down for `cluster recover`
  costs nothing; ANY 1 is satisfied by the other.
- **The operator escape hatch is explicit and loud.** `pg_agentctl
  cluster allow-async --confirm` clears `synchronous_standby_names` on
  the current primary for genuine emergencies. It is journaled in
  `inflight_ops`, surfaced in `/healthz`, and re-armed automatically at
  the next attach. There is deliberately **no config knob** to run
  permanently async: greenfield deployments get the guarantee, period.
  Sticky-and-loud beats a timer nobody remembers.
- **Observability**: `/healthz` reports `sync_commit:
  armed|disarmed|blocked|n/a`. `blocked` means commits are hanging for
  want of a standby, and is a page.

## 6. The identity plumbing

`synchronous_standby_names` matches walsenders by `application_name`, so
`primary_conninfo` must carry one — without it a standby cannot satisfy
`ANY 1` no matter how healthy it is. Every path that writes recovery
config sets `application_name=node{id}`, the same string as the slot
name: one identity for a node everywhere, validated by the same strict
alphabet as the other conninfo fields.

`synchronous_standby_names` itself is executor-owned via `ALTER SYSTEM
SET` + reload (a SIGHUP-context GUC, no restart), written only on the
node currently primary, and always `ANY 1 (<members minus self>)`.
Residue on a demoted node is harmless — it is recloned or rewound anyway.

## 7. What this does not change

- The fence stays. Quorum commit protects *acknowledged writes*; the
  fence still stops stale reads, releases `$PGDATA` for rebuild, and
  keeps the router honest. Finding 17's immediate-mode escalation drops
  from safety-relevant to latency polish.
- Timeline divergence and rejoin (rewind/reclone, demote policy) are
  unchanged — sync-rep narrows what a deposed primary can have that the
  world cares about, not whether its timeline forked.
- The lease, terms, and the candidacy *protocol* are unchanged. Only the
  candidacy *key* and the executor's convergence duties grew.
