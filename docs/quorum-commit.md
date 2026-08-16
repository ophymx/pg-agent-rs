# Quorum commit: making the lease's terms bind writes

**Status:** implemented, all four phases. 1–2: `application_name` in
`primary_conninfo`, `last_flush_lsn` in `NodeStatus`,
`LocalDb::flush_lsn`, candidacy on flush positions, G7 reworked to
flush lag. 3: the executor arms `ANY 1 (members minus self)` at the
first-standby-attached event, repairs membership drift, never writes
the empty string; `/healthz` reports `sync_commit:
armed|disarmed|blocked|n/a`; `pg_agentctl cluster allow-async
--confirm` is the journaled escape hatch, auto-re-armed on the next
attach. 4: the acceptance suite commits a sentinel row before every
induced failure (G3, G5, G7) and asserts it exists on the
post-failover primary — the suite's first data-survival assertions —
plus armed-state checks after bootstrap and after failovers.

## 1. The gap this closes

The raft lease serializes *promotion*: terms are fencing tokens, no
term is ever won twice, and every takeover is a quorum CAS
(docs/promotion-authority.md §5). The acceptance auditor now proves
those invariants mechanically on every run. And yet finding 18's
incident had two nodes serving concurrently, and finding 17 measured a
44 s fence drain — because **nothing at the write path ever checks a
term**. A deposed primary keeps accepting and locally committing
writes until its fence lands; those commits were acknowledged to
clients and are then discarded when the node is recloned onto the new
timeline. Consensus was flawless both times. The binding between
"holds the lease" and "may durably acknowledge writes" is today
enforced only by fence *latency*.

The fix is not to teach PostgreSQL about terms. It is to arrange that
**acknowledging a commit requires the cooperation of a node that
follows the lease** — then a primary that has lost the lease loses the
ability to acknowledge, mechanically, within one follow-convergence,
regardless of how long its own fence takes.

## 2. The mechanism

PostgreSQL synchronous replication, quorum flavor:

```
synchronous_standby_names = 'ANY 1 (node0, node1, node2)'   # minus self
synchronous_commit        = on                              # remote flush
```

A COMMIT does not return success until its WAL is flushed on at least
one listed standby. Three consequences, in increasing order of
importance:

1. **Durability**: every *acknowledged* write exists on ≥ 2 of 3
   nodes. A single node death — including the primary's — loses no
   acknowledged write.
2. **Selection soundness**: combined with flush-position candidacy
   (§4), the takeover winner always possesses every acknowledged
   write. Acked writes survive any failure that still permits a
   takeover.
3. **Write fencing — the term binding.** Standbys stream from the
   node their executor points them at, and executors follow **the
   lease holder, only** (roleexec's convergence contract). When the
   lease moves at term T+1, the executors re-point every standby at
   the new holder; the deposed primary's walsenders lose their
   consumers, and from that moment its commits *hang* — clients get
   no acknowledgment, ever, for anything the majority side won't
   contain. A standby acknowledging a primary's commit is thereby the
   delegated form of endorsing its term. The stale primary's fence
   (finding 17's 44 s drain included) stops mattering for
   acknowledged-write safety: everything in the un-acked window dies
   *unacknowledged*, which is the contract every client of every
   database already accepts.

The lease protocol itself changes not at all. The sync topology is
*derived state*: executors, which already own role convergence, also
own `synchronous_standby_names` on whichever node is primary.

## 3. The safety argument, spelled out

3-node cluster, agent quorum = 2. Enumerate the shapes:

- **Primary dies (no partition).** Both standbys reachable; ANY 1
  guaranteed at least one of them flushed each acked commit; flush-
  position candidacy (§4) picks a winner that has all of them.
- **Primary isolated alone.** Its lease confirmation fails (quorum
  lost) → it decides demote and fences. Its commits already hang the
  moment both standbys re-point (or the partition itself severs the
  walsenders — same effect, faster). Majority side has both standbys;
  as above, no acked write is missing there.
- **Primary + one standby isolated together.** One agent on the
  majority side — no quorum, no takeover, no second primary. The
  isolated pair can still ack writes (ANY 1 via the co-isolated
  standby) *and retain the lease only if the raft quorum includes
  them* — with 2 of 3 agents isolated together, they ARE the quorum:
  the "majority" side is the pair, no takeover happens elsewhere, no
  conflict. The lone node just follows on heal.
- **Total partition (1/1/1).** No quorum anywhere: lease unconfirmable
  → holder fences; no standby acks → commits hang first. Unavailable,
  consistent — the chosen CAP corner.
- **Double failure (both standbys dead).** Commits hang (§5 knob
  aside). If the primary then also dies, the last acked writes exist
  only on whichever standby acked them — recover the newest one.
  ANY 1 on N=3 durably tolerates one failure; that is the trade.

The invariant this buys, stated for the auditor and for operators:
**every acknowledged commit is flushed on at least two nodes, and any
takeover the lease permits selects a winner holding all of them.**

## 4. Candidacy must compare flush positions, not replay

Today a standby's `NodeStatus.current_wal_lsn` is
`pg_last_wal_replay_lsn()`. Under quorum commit that is the wrong
key: the acked guarantee attaches to *flushed* WAL
(`pg_last_wal_receive_lsn()`), and a standby can be the unique holder
of an acked commit it has not yet replayed. Selection by replay could
crown the other standby and lose an acknowledged write — the whole
point defeated by one field.

Changes:
- `NodeStatus` grows `last_flush_lsn` (standby: receive/flush
  position; primary: current LSN). Additive proto field.
- The HA loop's candidacy (`WalPosition`) compares
  `(timeline, flush_lsn)` **strictly**: any reachable peer with more
  flushed WAL outranks, byte-for-byte, and node id breaks exact ties
  only. The former ±`max_lag_on_failover` tiebreak band let a lower-id
  node up to 16 MiB behind win — under ANY 1 an acknowledged write can
  live exactly in that delta on the higher-flush standby, so the band
  contradicted §3's invariant (and was finding 15's wedge cause: only
  a behind-node winner leaves a loser past the fork point). Strict-max
  is livelock-free because candidacy runs against a dead primary —
  flush positions are static while it decides. The config knob remains
  accepted but is vestigial in candidacy.
- Promotion already replays everything received before exiting
  recovery, so a flush-ahead/replay-behind winner promotes correctly —
  the replay distance is promotion *latency*, not a safety input.
  (Candidacy MAY still subtract replay debt when choosing between
  flush-equal candidates; latency tiebreak only.)
- Acceptance G7 changes meaning: replay-paused-but-receiving is no
  longer a "lagging" candidate — it is flush-equal and eligible. The
  scenario must arrange *receive* lag instead (partition the standby,
  generate WAL, heal after the kill — or use two separate scenarios:
  flush-lag refusal, and replay-debt-is-not-disqualifying).

## 5. Availability: managed, not configured away

The availability cliff of sync-rep is real: with no reachable listed
standby, commits hang indefinitely. Positions taken:

- **The cliff is correct.** Post-cutover, a primary that no standby
  can reach is either partitioned (its acks would be lies about to be
  discarded) or the cluster is degraded to one node (acks would be
  single-copy promises the product exists to not make). Hanging is
  the honest behavior; the lease will usually fence such a primary
  shortly anyway.
- **Bootstrap and rebuild are the managed states.** The executor
  enables the sync requirement on the primary only at the
  *first-standby-attached* event (observed in `pg_stat_replication` —
  the same event vocabulary as the cross-op discharge), and never
  auto-disables it afterward. `cluster init` therefore works
  unchanged: the bootstrap primary runs async-solo until its first
  basebackup child attaches. A single standby down for `cluster
  recover` costs nothing — ANY 1 is satisfied by the other.
- **The operator escape hatch is explicit and loud.**
  `pg_agentctl cluster allow-async --confirm` (name TBD) clears
  `synchronous_standby_names` on the current primary for genuine
  emergencies (both standbys destroyed, business says write anyway).
  It is a state the operator enters, journaled in `inflight_ops`, and
  surfaced in `/healthz` until a standby attaches and the executor
  re-arms the requirement. There is deliberately no config file knob
  to run permanently async: greenfield deployments get the guarantee,
  period (same posture as the legacy rip).
- **Observability**: `/healthz` gains `sync_commit: armed|disarmed|
  blocked` — `blocked` meaning commits are currently hanging for want
  of a standby. `blocked` is a page.

## 6. Plumbing inventory

- `application_name`: `primary_conninfo` today carries none, so
  walsenders cannot be matched by `synchronous_standby_names`. The
  follow path (`UpstreamSpec` → conf rewrite) and
  `render_recovery_conf` add `application_name=node{id}` (the slot
  name — one identity everywhere). Validated by the same strict
  alphabet as the other conninfo fields.
- `synchronous_standby_names` management: executor-owned via
  `ALTER SYSTEM SET` + reload (SIGHUP-context GUC — no restart). Set
  on promotion and on membership change; content is always
  `ANY 1 (<members minus self>)`. Written only on the node currently
  primary; harmless residue on demoted nodes (recloned/rewound
  anyway).
- `LocalDb` gains `set_synchronous_standby_names(Option<&str>)` and
  the flush-LSN accessor; `NodeStatus` the new field; healthz the
  `sync_commit` tri-state (derived from the GUC + `pg_stat_replication
  sync_state` + a hanging-commit probe is NOT attempted — blocked is
  inferred from armed ∧ zero sync-eligible standbys).
- Config: `[sync]` section reserved but empty in v1 — the mode is not
  operator-tunable (see §5).

## 7. What this does NOT change

- The fence stays. Quorum commit protects *acknowledged writes*; the
  fence still stops stale reads, releases `$PGDATA` for rebuild, and
  keeps the router honest. Finding 17's immediate-mode escalation
  drops from safety-relevant to latency polish.
- Timeline divergence and rejoin (rewind/reclone, demote policy)
  unchanged — sync-rep narrows what a deposed primary can have that
  the world cares about, not whether its timeline forked.
- The lease, terms, candidacy *protocol* — unchanged. Only the
  candidacy *key* (§4) and the executor's convergence duties grow.

## 8. Phasing

1. **Plumbing** (no behavior change): `application_name` in conninfo;
   `last_flush_lsn` in `NodeStatus`; `LocalDb` accessors. Everything
   additive, shippable alone.
2. **Candidacy key** switch to flush position + G7 rework. Shippable
   alone (improves selection even before sync-rep — a receive-ahead
   standby is *already* the better candidate today).
3. **Executor arms quorum commit**: first-standby-attached event →
   `ANY 1`; promotion path sets it before first client write is
   acked; healthz tri-state; `allow-async` escape hatch.
4. **Acceptance**: sentinel write-survival asserts — a committed row
   written immediately before every induced failure (G3, G5, G7) must
   exist after convergence. This is the suite's first *data*
   assertion; today nothing asserts writes survive failover at all.
   Plus an audit note: `blocked`/`disarmed` windows are events,
   auditable against the scenarios that legitimately cause them.

## 9. Open questions

- **`ANY 1` vs `FIRST 1`**: `ANY` is order-free and the right quorum
  semantics; no known reason to prefer `FIRST`. Decided unless
  implementation surprises.
- **remote_apply?** No — flush suffices for the safety argument;
  apply adds latency for read-your-writes on standbys, which nothing
  here needs.
- **Does the escape hatch need a timer?** (auto-re-arm after N
  minutes vs. sticky until standby attach). Current position: sticky
  + loud beats a timer nobody remembers — revisit with operator
  feedback.
- **Sentinel probe writes from the harness vs. real workload**: start
  with single-row sentinels; a background write load generator is a
  separate harness investment.
