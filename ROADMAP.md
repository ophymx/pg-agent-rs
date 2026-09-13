# pg-agent-rs — ROADMAP

Where we're going. [SPEC.md](SPEC.md) describes what exists.

The bar is: be a more pleasant HA layer to operate than Patroni, on top
of the pgpool-II substrate we're stuck with.

Items are tagged with a rough effort estimate (**S** = days, **M** =
weeks, **L** = month-scale) and a "why now" so the ordering is arguable
rather than dogmatic. Open engineering defects live in
[TODO.md](TODO.md); this is the feature roadmap.

---

## Next — Patroni parity

Table-stakes features anyone coming from Patroni will miss.

- **Role-aware `/healthz` + the HAProxy split** *(S)* — `/primary` and
  `/replica` endpoints backed by the lease. Blocked for a long time
  because nothing authoritatively knew the role; the lease supplies it
  now. *Why now:* it is the last piece of the promotion-authority work
  and the cheapest item on this list. See SPEC §9.1 for why the current
  single-endpoint contract is what it is.

- **`pg_agentctl cluster switchover --to <id> [--at <RFC3339>]`** *(M)* —
  planned promotion, separate from emergency failover and from
  `cluster handoff` (which is immediate and operator-blocking). The
  replicated state already carries a `switchover` field; what is missing
  is the operator verb and the executor's scheduled path. *Why now:*
  operations need a planned, reversible-up-to-the-cutover path.

- **`pg_agentctl cluster attach <id>`** *(S)* — wrap `pcp_attach_node`
  with the pgpool reload and `gen-pgpool` refresh so "add a new standby
  to a running cluster" is one command. Today the flow ends in a manual
  `pcp_attach_node` plus a manual `gen-pgpool --write` and reload. *Why
  now:* the only remaining manual steps in the add-standby workflow;
  pure ergonomics, no new state.

- **Per-node tags in `config.toml`** *(S)* — surface and respect
  `nofailover = true`, `noloadbalance = true`, `clonefrom = true`.
  Candidacy skips `nofailover` nodes; `gen-pgpool` emits the right
  `backend_flag` for `noloadbalance`. *Why now:* universal Patroni
  feature; users have muscle memory for it.

- **Phantom-primary detection is startup-only** *(M)* — a returning node
  is checked against peer timelines before it comes up, but a node that
  drifts into a wrong role while running is caught only by the executor's
  fence. Extending the timeline comparison into the steady-state loop
  would close the gap the fence covers by side effect rather than by
  design. *Why now:* it is the one reconciliation path that still relies
  on a restart to run.

- **`restore_wal` follows the current primary** *(S)* — `walstore.rs`
  fetches archive WAL from whichever peers the configuration named at
  startup. After a promotion the *new* primary is the source of truth;
  the old one may be offline or stale. The fetch should consult live
  cluster state rather than a static peer list. *Why now:* any standby
  catching up across a promotion hammers the old primary first.

---

## Then — observability and ergonomics

- **Structured JSON logging** *(S)* — `--log-format=json|text`, with a
  `trace_id` per hook RPC so cross-node correlation works in Loki /
  OpenSearch. *Why now:* tiny change, immediate gain.

- **In-memory event ring + `pg_agentctl events`** *(S)* — last N hook
  firings and state transitions over the local socket; `events --cluster`
  fans out and merges by timestamp. *Why now:* post-mortem today means
  tailing journalctl on three boxes.

- **`cluster status` warns on multi-timeline primaries** *(S)* — when the
  fan-out shows two nodes reporting `primary` on different timelines,
  print a `WARN: stale primary on node <id>` line. *Why now:* `cluster
  status` already fans out `GetStatus` and has everything it needs.

- **Prometheus / OpenMetrics endpoint** *(S–M)* — per-node lag, slot LSN
  gap, hook latency histograms, maintenance queue depth, cert
  days-to-expiry, peer connection age, basebackup bytes in flight. Lift
  from the existing healthsnap / maintenance / certreload surfaces. *Why
  now:* Patroni offloads this to a third-party exporter that lags
  upstream; shipping it natively means it never drifts.

- **Append-only on-disk event log** *(M)* — promote the ring buffer to a
  durable log under `<state_dir>/events/` with rotation and retention.
  Closest analog is `kubectl events`. *Why now:* "what happened to my
  cluster last night" is the feature operators repeatedly ask Patroni
  for.

- **REST surface on the healthz listener** *(M)* — `/cluster`,
  `/events?since=…`, `/config`, read-only. Plain HTTP like the rest of
  that listener (SPEC §9.2); operators who need TLS front it with a
  reverse proxy. *Why now:* every monitoring stack speaks HTTP.

- **`pg_agentctl top`** *(M–L)* — ratatui live view: topology, lag, slot
  states, event tail, paused/switchover banners, cert expiry.
  `patronictl list` is tabular; the gap is real and the lift contained.

---

## Operational safety

- **Distro profiles** *(S)* — a `distro = "debian" | "rhel"` knob, or
  auto-detection from `/etc/os-release`, selecting defaults for
  `pg_install_prefix`, `user_home`, `data_dir`, `service`, and
  `pcp.pgpool_service`. Operator overrides still win per field. **RHEL
  already works** — the `rocky9-pg16` matrix cell installs the `.rpm` and
  runs the full suite green — so this is about deleting a five-field
  chore, not about portability. The discipline that made RHEL cheap, and
  which these defaults must not break: no Debian path outside the
  `DEFAULT_*` consts, and no `postgresql@*-main` instance naming assumed
  in subprocess args.

- **A BOOTSTRAP distro matrix appendix** *(S)* — the path-pair
  differences between supported distros plus the "this is what Ansible
  writes differently per OS family" inventory snippet. The table exists
  in three places (testing/README.md, `Dockerfile.rhel`,
  `cluster::Facts`); BOOTSTRAP is where an operator would look for it.

- **Fence hook (`stonith_command`)** *(M)* — before any promotion,
  optionally invoke an operator-supplied command (PDU API, hypervisor
  STOP, IPMI power-off) to verify the old primary is dead. Patroni's
  `/dev/watchdog` covers only self-fencing; STONITH covers the primary
  that is alive but partitioned. Default off. *Why now:* split-brain
  insurance for the one failure mode no amount of good design eliminates.

- **HAProxy drain integration** *(S)* — `pg_agentctl cluster drain <id>`
  marks a backend `MAINT` via HAProxy's runtime API so connections drift
  off before maintenance; used internally by switchover. *Why now:* the
  missing piece between "I planned a switch" and "no clients saw an
  error".

- **Audit log of admin actions** *(M)* — every `pg_agentctl` mutation
  timestamped, attributed to the issuing cert's CN, appended to a
  tamper-evident chain under `<state_dir>/audit/`. *Why now:* trivial to
  add early, costly to retrofit.

---

## Exploratory

Speculative, kept in view because current design choices should not
foreclose them.

- **Config drift detection** — periodic SHA-256 of pgpool.conf,
  postgresql.conf, pg_hba.conf, pool_passwd per node, fanned out via a
  `Peer.ConfigDigest` RPC and surfaced in `cluster status`. Catches
  "someone hand-edited node3 last Tuesday" before the next failover does.

- **WAL archive integrity sweep** — enumerate each peer's `archive_dir`
  via a `Peer.ListWal` RPC, identify holes, surface them before they bite
  during a recovery. Pairs with rotating which standby takes the next
  `pg_basebackup` so the primary isn't always the source.

- **DR / standby cluster mode** — a second cluster following the first
  via cascading replication. Requires topology to express "this pool
  follows pool X". Patroni has standby clusters; that's the bar.

- **Pluggable backup providers** — pgbackrest / wal-g / barman as
  first-class alternatives to `pg_basebackup`. `StandbyOps` already
  abstracts the seam; the lift is adapters and a config knob.

- **Pure-Rust `pg_basebackup` replacement** over a temporary listener
  using the same mTLS material. Eliminates the last external subprocess
  on the data path and enables progress/cancel semantics the CLI tools
  don't expose.

- **VIP failover via pgpool watchdog `delegate_IP`** — for deployments
  that would rather lean on pgpool's watchdog VIP than provision HAProxy.
  Needs `Escalation`/`DeEscalation` wired to `ip addr add`/`del` plus a
  gratuitous-ARP burst, a `[watchdog]` config block, a `CAP_NET_ADMIN`
  preflight, and VIP ownership in `cluster status`. Not now because
  HAProxy is the supported entry point and watchdog leader election is a
  separate failure surface to debug — and because turning the watchdog
  back on reintroduces a second thing with opinions about failover.

- **Not locked to systemd** — the agent drives PostgreSQL through systemd
  over D-Bus with a polkit rule, waiting on `JobRemoved` for job
  completion. A second `ServiceManager` impl would buy OpenRC, Gentoo,
  Devuan and Alpine at once. See TODO.md for the scoping; the hard part
  is rebuilding job-completion semantics on a weaker primitive, and the
  honest fix there would strengthen the systemd path too.

- **A web UI** consuming the REST surface and metrics. Out of scope to
  build in-tree, but the REST surface should be designed assuming someone
  eventually ships one.

---

## Non-goals (explicit)

- **Replacing pgpool itself.** If we wanted to escape pgpool we'd switch
  to Patroni + PgBouncer + HAProxy, not reinvent the routing/pooling
  layer. The whole point is to make pgpool tolerable.
- **Multi-region active-active.** Neither pgpool nor PostgreSQL streaming
  replication is designed for it.
- **An external DCS dependency.** Consensus is embedded
  ([docs/promotion-authority.md](docs/promotion-authority.md) §5); if
  that stops working we redesign rather than bolt on etcd.
- **Connection pooling.** pgpool already does this; doing it again is a
  different product.
