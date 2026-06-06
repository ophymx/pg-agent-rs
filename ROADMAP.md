# pg-agent-rs — ROADMAP

Companion to [SPEC.md](SPEC.md). The SPEC describes **what we're building
first**; this document describes **where we're going after that**.

The bar is: be a more pleasant HA layer to operate than Patroni, on top of
the pgpool-II substrate we're stuck with. The path is three tiers — close
Patroni's parity gap, build the observability + ergonomics moat, then push
into territory no one else has covered yet.

Each item is tagged with a rough effort estimate (**S** = days,
**M** = weeks, **L** = month-scale) and a "why now" so the ordering is
arguable rather than dogmatic.

---

## v1 — Baseline (SPEC.md scope, recap)

The starting line. Everything listed here is already specified; this
section exists so the rest of the roadmap has a clear "from" to its "to".

- 1:1 replacement of the five pgpool shell hooks via a typed RPC surface.
- Persistent mTLS peer mesh, hot cert reload via SIGHUP.
- Local Unix-socket RPC for `pg_agentc` (filesystem permission == auth).
- Durable maintenance queue for failed slot cleanups (file-backed, atomic
  writes, capped retries with exponential backoff).
- Hook idempotency via on-disk replay markers, swept on a cadence.
- `pg_agentctl preflight` (TLS, polkit, .pcppass, PostgreSQL tuning, roles,
  extensions, pg_hba — every silent-failure mode we know about).
- `/healthz` HTTPS listener with snapshot-based readiness for HAProxy.
- `pg_agentctl cluster init` one-shot bootstrap from a chosen primary.
- Strict input validation (regex on every value reaching libpq / subprocess
  / SQL identifier).

What this gets you: a cluster that operates correctly without bash, ssh, or
hand-rolled hook scripts. What it does **not** get you: any operator-facing
ergonomics beyond `pg_agentc status` and `pg_agentctl maintenance list`.

---

## v1.x — Patroni parity (close the operator UX gap)

These are the table-stakes features anyone coming from Patroni will
immediately miss. Tackling them early prevents "this is great, but I can't
schedule a switchover" from blocking adoption.

### Cluster control plane

- **`pg_agentctl cluster status`** *(S)* — fan-out `GetStatus` to every peer,
  render a topology table (id, hostname, role, lag, slot, pg state, pgpool
  state, last-seen, maintenance-queue depth). One command, full cluster.
  *Why now:* this is the single biggest day-1 ergonomics win and is purely
  local plumbing — no new state, no new RPC.

- **`pg_agentctl cluster pause [--reason …]` / `cluster resume`** *(M)* —
  maintenance mode. A boolean in shared cluster state (see "Shared state"
  below) that every agent honors: `Failover`, `FollowPrimary`, and the
  maintenance worker all short-circuit while paused, with a clear reason
  surfaced in `cluster status`. *Why now:* without this, every kernel
  upgrade or major DDL is a fight with the cluster.

- **`pg_agentctl cluster switchover --to <id> [--at <RFC3339>]`** *(M)* —
  planned promotion, separate from emergency failover. Drains the HAProxy
  backend for the current primary (see Drain hook below), waits for the
  candidate's lag to fall under a threshold, calls a (new) `PgAgentPeer.Demote`
  on the old primary, `Promote` on the new one, updates pgpool via PCP.
  *Why now:* operations need a planned, reversible-up-to-the-cutover path —
  emergency failover is a different code path with different invariants.

- **Per-node tags in `config.toml`** *(S)* — surface and respect
  `nofailover = true`, `noloadbalance = true`, `clonefrom = true`.
  Failover skips nofailover nodes when picking a candidate (returning to
  the `%m = -1` sentinel if none qualify); `gen-pgpool` emits the right
  `backend_flag` for noloadbalance.
  *Why now:* universal Patroni feature; users have muscle memory for it.

### Shared cluster state (the foundation switchover and pause need)

Patroni gets free shared state from the DCS. We chose to avoid that
dependency, so we need a lightweight equivalent:

- **Cluster-state RPC + gossip** *(M)* — extend `PgAgentPeer` with
  `GetClusterState` / `ProposeClusterState(version, payload)`. State is a
  small JSON document (paused flag, scheduled switchover, current
  generation), versioned with a monotonic clock + writer node id. On any
  mutation, the writer fans out `ProposeClusterState` to every peer; an
  agent only accepts a proposal whose version > local version. On startup
  / SIGHUP an agent reconciles by pulling from every peer and taking the
  highest-version document. *Why now:* this is the substrate every
  subsequent v1.x item needs.

  Conflict resolution is intentionally "last-writer-wins with humans in
  the loop" — operator commands are infrequent and the gen counter makes
  the divergence visible in `cluster status`. Not as principled as Raft;
  far simpler than running etcd.

### Observability and ergonomics

- **Structured JSON logging by default** *(S)* — `tracing-subscriber` with
  a JSON layer, configurable via `--log-format=json|text`. Includes
  `trace_id` (per-hook-RPC) so cross-node correlation works in Loki /
  OpenSearch without parsing slog. *Why now:* tiny change, immediate gain.

- **In-memory event ring buffer + `pg_agentctl events`** *(S)* — last N
  hook firings + state transitions, exposed over the local socket. `events
  --cluster` fans out and merges by timestamp. *Why now:* post-mortem
  today means tailing journalctl on three boxes; this collapses it to one
  command, no persistence cost.

- **REST surface on the `/healthz` listener** *(M)* — extend to `/cluster`
  (current state), `/events?since=…`, `/config` (active runtime config),
  read-only. Same TLS, optional bearer token for write endpoints in a
  future iteration. *Why now:* every monitoring stack speaks HTTP; gRPC
  client lib in dashboards is friction.

---

## v2 — Differentiation moats (where Patroni stops, we keep going)

These are the features that make pg-agent-rs the better choice rather than
a different choice.

### First-class observability

- **Prometheus / OpenMetrics endpoint on `/healthz`** *(S–M)* — per-node
  lag bytes, replication slot LSN gap, hook latency histograms, maintenance
  queue depth by status, cert days-to-expiry, peer connection age,
  basebackup / rewind in-progress bytes. Lift directly from the
  existing healthsnap + maintenance + certreload surfaces. *Why now:*
  Patroni offloads this to a third-party exporter that lags upstream; we
  ship it natively and it never drifts.

- **Append-only on-disk event log** *(M)* — promote the in-memory ring
  (v1.x) to a durable log under `<agent_dir>/events/`. Daily rotation,
  configurable retention. `pg_agentctl events --since 1h --cluster`
  reconstructs the global timeline. Closest analog: `kubectl events`;
  no HA tool currently does this well. *Why now:* this is the "what
  happened to my cluster last night" feature operators repeatedly ask
  Patroni for.

- **`pg_agentctl top` — TUI dashboard** *(M–L)* — ratatui-backed live view.
  Topology, lag, slot states, event tail, paused/switchover banners,
  cert expiry warnings. `patronictl list` is tabular; the gap is real
  and the lift is contained. *Why now:* this is the demo-day feature that
  changes "yet another HA tool" into "oh, *that's* nice".

### Operational safety

- **Fence hook (`stonith_command`)** *(M)* — before any promotion,
  optionally invoke an operator-supplied command (PDU API, hypervisor
  STOP, IPMI power-off) to verify the old primary is dead. Patroni's
  `/dev/watchdog` works only for self-fencing; STONITH covers the case
  where the primary is alive but partitioned. Default off; on by config.
  *Why now:* split-brain insurance — the one failure mode no amount of
  good design eliminates.

- **Audit log of admin actions** *(M)* — every mutation initiated via
  `pg_agentctl` is timestamped, signed by the issuing cert's CN, and
  appended to a tamper-evident chain under `<agent_dir>/audit/`.
  `pg_agentctl audit verify` walks the chain. *Why now:* compliance-grade
  for regulated environments; trivial to add early, costly to retrofit.

- **HAProxy drain integration** *(S)* — `pg_agentctl cluster drain <id>`
  marks a backend as `MAINT` in HAProxy's runtime API (or via a configurable
  drain hook) so connections drift off before maintenance. Used internally
  by switchover. *Why now:* the missing piece between "I planned a switch"
  and "no clients saw an error".

---

## Exploratory — bigger bets

Speculative, but worth keeping in view because the design choices we make
in v1.x and v2 should not foreclose them.

- **Config drift detection** — periodic SHA-256 of pgpool.conf,
  postgresql.conf, pg_hba.conf, pool_passwd, .pcppass per node; fan out
  via a new `Peer.ConfigDigest` RPC and surface mismatches in
  `cluster status`. Catches "someone hand-edited node3 last Tuesday"
  before it bites during the next failover. Extends naturally from the
  `gen-pgpool` machinery.

- **Backup rotation orchestration** — track which standby took the last
  `pg_basebackup` and rotate so the primary isn't always loaded. Pair
  with WAL archive integrity sweep: enumerate each peer's archive_dir
  via a `Peer.ListWal` RPC, identify holes, surface to operators
  before they bite during a recovery.

- **DR / standby cluster mode** — a second cluster following the first via
  cascading replication. Requires extending topology to express "this
  pool is a follower of pool X"; the wire format already supports
  cross-pool addressing if we wanted it. Patroni has standby clusters;
  this is the bar.

- **Pluggable backup providers** — pgbackrest / wal-g / barman as
  first-class alternatives to `pg_basebackup` for cluster_init and
  recovery_1st_stage. The current `PgStandby` trait already abstracts
  basebackup; the lift is mostly adding adapters and a config knob.

- **Pure-Rust `pg_basebackup` replacement** over a temporary HTTPS
  listener using the same mTLS material (the SPEC already reserves this
  in §17/§18 as future work). Eliminates the last external subprocess
  dependency for the data path and enables progress/cancel semantics the
  CLI tools don't expose.

- **A web UI** that consumes the v1.x REST surface + v2 metrics. Out of
  scope to build in-tree, but the REST surface should be designed
  assuming someone will eventually ship one.

---

## Non-goals (explicit)

To keep this list honest, the following are **not** on the roadmap, even
as speculation:

- Replacing pgpool itself. If we ever wanted to escape pgpool, we'd switch
  to Patroni + PgBouncer + HAProxy, not reinvent the routing/pooling
  layer. The whole point of pg-agent-rs is to make pgpool tolerable.
- Multi-region active-active. Out of scope for the substrate (pgpool is
  not designed for this; PostgreSQL streaming replication is not designed
  for this).
- A DCS dependency. The lightweight gossip approach in v1.x is a deliberate
  choice; if it stops working we redesign rather than bolt on etcd.
- Connection pooling. pgpool already does this; doing it again is a different
  product.

---

## Ordering principle

Within each tier, ship the items that **unblock other items** first:

- v1.x shared cluster state unlocks pause + switchover + tags.
- v1.x event ring buffer is the in-memory prototype of the v2 on-disk log.
- v1.x REST surface is the substrate for v2 metrics and the eventual web UI.
- v2 audit log is cheap if added during the REST surface build; expensive
  to retrofit.

The cheapest immediate win is `cluster status` — one fan-out of an
existing RPC. Start there.
