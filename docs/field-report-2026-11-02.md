# Field report: the two-week quiet outage (2026-10-19 → 2026-11-02)

Operator-side handoff from the home fleet. Written from the ansible side
of the fence after a day spent getting the db cluster back; the point is
to record what the agent did, what it should have done, and the exact
evidence, so the fixes can be scoped from the agent's side without
re-deriving the incident.

Short version: PostgreSQL replication was healthy the entire time.
Everything the operator could see was green. Routing through pgpool was
degraded on every node for two weeks, and every client that verified
TLS was refused, and nothing the agent reports made either visible.

## Timeline

| When (UTC) | What |
|---|---|
| 2026-09 | LE cert for `db.home.ophymx.com` renewed on disk by vault-cert-agent; `try-reload-or-restart` SIGHUPs pgpool; pgpool keeps serving the old cert (loads TLS at start only). |
| 2026-10-19 20:06 | Old cert expires. Every `sslmode=verify-full` client (the SeaweedFS filers among them) starts failing. Health checks keep passing: `/healthz` is plain HTTP on 9702 and does not look at the served cert. |
| 2026-10-19 → 11-01 | Various transient failures (restarts, the cert, a stretch of broken DNS on the fleet) cause pgpool health checks to fail and detach backends. Postgres itself never stops streaming. |
| 2026-11-02 01:3x | Filers found crash-looping on `x509: certificate has expired`. pgpool restarted on all three nodes → cert fixed. |
| 2026-11-02 14:0x | `pg_agentctl cluster status`: all green. `/healthz` on each node: `ready: true` on all three, but three *different* pgpool backend maps: db0 held db0+db1 down, db1 and db2 held db1 down. `replication_state: none` for every backend on every node. |
| 14:26 | `pcp_attach_node` run by hand per instance. db0's pgpool immediately re-detaches its own backend: its health check to db0 fails. |
| 14:26 | pgpool log on db0: `health check failed on node 0`, `sr_check_worker: No persistent db connection for the node 0`, and the agent's failover hook: *"refusing to drop slot node0: announced-failed standby node 0 (db0.home.ophymx.com) is reachable, running, and streaming — the failure report is wrong"*. |
| 14:3x | Cause: db0 resolves its own FQDN to its IPv6 ULA first (nss myhostname); pg_hba only admitted 10.0.0.0/24. Peers arrive over v4 and are fine. Fixed in pg_hba. |
| 14:4x | `replication_state: none` traced to `backend_application_nameN = <FQDN>` in pgpool.conf while the agent writes `application_name=node<id>` into `primary_conninfo`. Fixed in pgpool.conf. Cluster verified healthy. |

## What the agent got right

- The slot-drop refusal. The failover hook recognised that pgpool's
  failure report was false, said so in one clear sentence, and did not
  touch the slot. That log line is what cracked the diagnosis.
- Quorum commit stayed `armed` on the primary throughout.
- `cluster status` is a good table: role, PG, pgpool, ready, lag,
  replication state, timeline, LSN, in one screen.

## What it got wrong, ranked

### 1. It knew pgpool's failure report was false and left pgpool's map wrong

The hook that refused to drop `node0`'s slot had, in hand, the exact
fact needed to repair the situation: "this backend is reachable, running
and streaming; pgpool is wrong". It also holds the PCP credential. The
one-line follow-up was `AttachNode` on the local instance (or at least a
retry loop until the health check passes again). Instead the refusal was
the end of the story, and pgpool on that node routed around a healthy
standby until a human noticed.

This is the sharpest item because the information and the capability
were both already in the same code path.

### 2. Detach never heals, by design, and there is no non-destructive attach for the operator

`auto_failback` is off on purpose (slots), and attach is agent-owned.
But the agent only attaches in two places: the `cluster recover`
fan-out, and the finding-16 self-attach on promotion. A transient
outage that detaches backends on an otherwise healthy cluster therefore
leaves routing degraded forever.

The operator-facing gap compounds it. `pg_agentctl cluster recover` is
the only attach entry point and it *reclones* the target (and refuses
while PG is running). There was no `pg_agentctl` verb for "pgpool's map
is stale, make it match reality". The fallback was raw
`pcp_attach_node -w -h localhost -p 9898 -U pgpool -n N` on each node,
which the README explicitly says the agent owns.

Wanted:
- A periodic reconcile on the executor tick: for each backend the
  local pgpool map holds down, if the cluster's view (peer `GetStatus`,
  or `pg_stat_replication` on the lease holder for standbys) says it is
  up and streaming, `AttachNode` locally. Only-if-down, same semantics
  as the existing RPC. This is the attach half of the watchdog's
  backend-status sync that `pgpool-hook-contract.md` §3 says was priced
  and paid; it was paid only for the recover path.
- `pg_agentctl cluster attach [--node N]` that fans the same thing out
  on demand, never recloning.

### 3. `/healthz` said `ready: true` with two of three backends down in the local map

Readiness is what HAProxy routes on. A pgpool that can only reach one
backend is serving, so `ready: true` is defensible, but it is also
exactly the state nobody looked at for two weeks because the only
signal was green. The map was in the body the whole time
(`pgpool.backends[].status`); nothing summarised it.

Wanted: a top-level `pgpool_map_degraded: true` (any backend down or
waiting that the cluster believes is up), and a matching line in
`cluster status`, which today prints each node's PG/pgpool as `running`
and READY as `yes` without consulting the map. A `cluster status` that
shows the same map disagreement `/healthz` already carries would have
been enough.

### 4. `replication_state: none` for two weeks was a config contract the agent depends on but does not check

The agent writes `primary_conninfo … application_name=<slot>` and names
slots `node<id>`; `localdb.rs` even selects members from
`pg_stat_replication` with `application_name ~ '^node[0-9]+$'`. pgpool
fills `replication_state` / `sync_state` by matching
`backend_application_nameN` against the same column. Our pgpool.conf had
the FQDN there, so pgpool matched nothing and reported `none`, and
`/healthz` relayed it.

Two things follow:
- `check-hooks` / `gen-pgpool` should assert
  `backend_application_nameN == node<N>`. The agent already parses
  pgpool.conf for the hook stanzas; this is one more line per backend
  and it is a contract the agent itself created.
- The agent can see the contradiction at runtime: its own
  `pg_stat_replication` query says `streaming`, pgpool says `none` for
  the same backend. That is a config error worth one log line at
  warn, not a value to pass through.

Worth verifying from the agent side: whether `detach_false_primary`
also keys on `application_name`. If it does, this mismatch is not
cosmetic; it is a latent false-primary detach of the real primary.

### 5. Preflight does not try the one connection pgpool will actually make

`validate-env` and `check-hooks` inspect a great deal of config. Neither
dials `backend_hostnameN:5432` for the local node the way pgpool's
health check will. On db0 that connection goes to the node's own ULA
address, because that is what the host resolves its own name to, and
pg_hba only knew the v4 range. A preflight that resolves each backend
hostname, attempts a TLS connection as the health-check user over every
returned address family, and reports which `pg_hba` line (if any) would
admit it, would have failed loudly on day one. The fleet is moving to
IPv6 generally, so this will recur on every host that gets a ULA before
its hba rules do.

### 6. The pgpool supervisor keeps pgpool running but not pgpool *current*

`pgpool_supervisor.rs` exists because pgpool dies from transient causes
and systemd's restart policy does not always cover it. The same module
is the natural owner of "pgpool is running with TLS material that is
older than the file on disk". pgpool loads its cert at start; the
renewer rewrote the file; no one restarted pgpool. Two cheap signals:
- `/healthz` exposes `pgpool_tls_not_after` by connecting to the local
  pgpool port and reading the served certificate. Prometheus alerts on
  it; the operator sees it in `cluster status`.
- The supervisor compares the served cert's fingerprint to the file's
  and restarts pgpool (it already has `StartUnit`) when they differ,
  or at least flags it.

The ansible side has switched the renewer to `try-restart`, so the
immediate hole is closed, but the agent is the only thing on the host
that can tell the two apart at runtime.

## Evidence

pgpool on db0, 2026-11-02 14:26 UTC, after a successful `pcp_attach_node -n 0`:

```
health_check0 pid 764557: LOG:  health check retrying on DB node: 0 (round:5)
sr_check_worker pid 764576: ERROR:  Failed to check replication time lag
sr_check_worker pid 764576: DETAIL:  No persistent db connection for the node 0
health_check0 pid 764557: LOG:  health check failed on node 0 (timeout:0)
failover: failover: refusing to drop slot node0: announced-failed standby node 0 (db0.home.ophymx.com) is reachable, running, and streaming — the failure report is wrong, and dropping its slot would break healthy replication. No action taken.
```

`getent ahosts db0.home.ophymx.com` on db0 (ULA first, v4 second,
link-local third); Postgres listening on `[::]:5432`; pg_hba admitting
`10.0.0.0/24` only.

`/healthz` on the three nodes before repair (all `ready: true`):

```
db0: [(db0, standby, down, none), (db1, standby, down, none), (db2, primary, up, none)]
db1: [(db0, standby, up,   none), (db1, standby, down, none), (db2, primary, up, none)]
db2: [(db0, standby, up,   none), (db1, standby, down, none), (db2, primary, up, none)]
```

`pg_agentctl cluster status` at the same moment: every row `running /
running / yes / lag 0 / streaming`, lease on node 2, raft leader node 0.

After the pg_hba and `backend_application_name` fixes:

```
db2: True primary armed [(db0, up, streaming), (db1, up, streaming), (db2, up, none)]
```

## Suggested order

1. (#1) Attach locally when the failover hook has already decided the
   report is false. Smallest change, highest value.
2. (#2) `cluster attach` verb plus the tick-time reconcile.
3. (#3) `pgpool_map_degraded` in `/healthz` and in `cluster status`.
4. (#4) `check-hooks` asserts `backend_application_nameN`; runtime
   warn on the streaming/none contradiction; confirm
   `detach_false_primary`'s matching rule.
5. (#5) Preflight dials each backend over every resolved address.
6. (#6) Served-cert observability and the supervisor restart.

Ansible-side changes already made for the same incident, for
cross-reference: `books/db/deploy.yml` (`reload_method: try-restart` on
the LE cert, `alt_names` declared), `roles/db/templates/pg_hba.conf.j2`
and `pool_hba.conf.j2` (loop over `lan_cidrs`, v4 + v6 ULA),
`roles/db/templates/pgpool.conf.j2` (`backend_application_nameN =
'node<N>'`), and `roles/db/TODO.md` (both gaps recorded).

## Agent-side status (2026-11-02)

Every item above has a change in the agent; the ordering followed the
"Suggested order". Two corrections to the report from verifying it
against the code and pgpool's documentation:

- `detach_false_primary` judges connectivity through
  `pg_stat_wal_receiver`, not `application_name`, so the
  `backend_application_name` mismatch was cosmetic for that path. It
  was not cosmetic for `delay_threshold_by_time` or for the
  `replication_state` / `sync_state` columns — which is enough.
- Item 1's "attach after the refusal" would have flapped in this very
  incident: pgpool's health check to db0 kept failing, so each attach
  would have been undone, re-firing the hook. The attach exists, with
  backoff, and the warning it logs on a revert says what the
  disagreement means — that sentence is the fix for a misconfigured
  host, the attach is the fix for a transient.

| # | Where |
|---|---|
| 1 | The failover hook's refusal kicks the map reconciler (`localserver.rs`), which attaches the detached backend after the hook returns. |
| 2 | `pgpool_reconcile.rs`: tick every 30 s, attach only-if-down on cluster evidence, holder first, backoff on reverts. `pg_agentctl cluster attach [--node N]` runs it now and fans `AttachNode` out. |
| 3 | `/healthz`: `pgpool_map_degraded`, `pgpool_backends_down`. `GetStatus` carries each node's map; `cluster status` shows a `POOL_MAP` column and a `!` line per instance whose map contradicts the cluster. |
| 4 | `check-hooks` and `gen-pgpool` enforce `backend_application_nameN = nodeN`; `/healthz` `sr_check_mismatch` plus a warn once per transition. |
| 5 | `validate-env` "pgpool backend admission": every backend, every resolved address, as `health_check_user` and `sr_check_user`, libpq's first-answering-address rule, the server's own refusal text. WARN, never ERR: on this very cluster an ERR would have made the next rolling upgrade a chain of agents refusing to start. |
| 6 | `/healthz` `pgpool_tls` (served and on-disk expiry, `stale`); the supervisor restarts pgpool once per newer file (`[supervisor] restart_pgpool_on_stale_tls`). |
