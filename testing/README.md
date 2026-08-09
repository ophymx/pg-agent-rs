# Acceptance testing — dockerized 3-node cluster

Proves cluster behavior end to end against the **real artifacts**: the
nfpm-built `.deb`, the packaged systemd unit (sd_notify, ExecStartPre
`validate-env`), the polkit rule, real mTLS between agents, and real
PostgreSQL 17 streaming replication — three systemd-booted Debian 13
containers on a compose network.

This harness is the substitute for live-cluster shadow validation
(promotion-authority §10 step 5): the live cluster's existing behavior
is not a useful oracle, so robustness is demonstrated here instead —
and this same harness is where step 6 (openraft) gets its
partition/restart acceptance scenarios.

## Run

```
testing/acceptance.sh              # build .deb, up, all scenarios, down
KEEP=1 testing/acceptance.sh       # keep the cluster running afterwards
SKIP_BUILD=1 testing/acceptance.sh # reuse dist/ .deb
```

On failure the cluster is kept for debugging (`docker exec -it pga-db0
bash`, `journalctl -u pg_agentd`).

## Layout

| File | Role |
|---|---|
| `compose.yaml` | 3 nodes (`db0..db2`), privileged + `cgroup: host` so systemd is PID 1 |
| `docker/Dockerfile` | debian:trixie + systemd + postgresql-17 + the `.deb`; pgpool2 installed but masked (BOOTSTRAP.md Phase 1.1) |
| `docker/provision.sh` | boot-time provisioning = Ansible's Phase-1 role: node id, TLS, config.toml, pg_hba, roles/extension, archive dir |
| `docker/pgpool-setup.sh` | operator-run pgpool config + start (BOOTSTRAP Phase 1.4 + 3.1), in the target hook-contract shape |
| `docker/50-pg-agent.rules` | polkit grant (postgres user → manage PG/pgpool units) |
| `gen-certs.sh` | one CA + per-node certs, SAN = compose hostname (matches the peer SAN allowlist) |
| `acceptance.sh` | scenario driver + assertions |

## Scenarios (phase 1)

- **S0** — all three daemons pass `validate-env` and come up; standby
  PostgreSQLs deliberately down pre-init.
- **S1** — `pg_agentctl cluster init` from db0: slots, basebackups,
  standbys streaming; `cluster status` renders the topology.
- **S2** — shadow HA loop steady state: db0 retains a self-claimed
  lease; db1/db2 adopt the observed primary and follow.
- **S3** — primary death: both standbys watch the holder for
  `leader_ttl`, then **exactly one** (db1, node-id tiebreak at equal
  WAL positions) shadow-takes the lease; db2 stands down. Primary
  returns → shadow converges back (nothing was actually promoted).
- **S3c** — lag gate with *real* WAL lag: replay paused on db2, ~80 MB
  of WAL generated, primary stopped, then the handler is handed the
  lagging node as `new_main`. Refused, naming db1 as the better
  candidate. Runs before pgpool exists so nothing else reacts to the
  primary going down.
- **S4** — the 2026-06-11 incident replayed: `pg_agentc failover`
  announcing the healthy primary as dead → refused
  ("running as primary"), nothing promoted.
- **S5** — agent restart on the primary: phantom check confirms, no
  conservative stop, PG untouched.

## Phase 2 — pgpool in the loop

pgpool-II 4.6 on all three nodes, configured per
[docs/pgpool-hook-contract.md](../docs/pgpool-hook-contract.md) §4:
watchdog **off**, `failover_command` as an advisory poke,
`follow_primary_command` **empty**, `detach_false_primary` on,
`auto_failback` off. `docker/pgpool-setup.sh` plays BOOTSTRAP Phase 1.4
+ 3.1 and is run by the driver after `cluster init`.

- **S6** — pgpool starts on all three; every instance shows all three
  backends up; `/healthz` reports ready.
- **S7** — `check-hooks` detects drift between `gen-pgpool`'s canonical
  block and the target contract (see finding 8).
- **S8** — detach does **not** propagate between instances, and the
  detach-fired hook is refused by the precondition check because the
  "failed" standby is still streaming (hook-contract §5, item 2).
- **S9** — real primary failover driven by pgpool, with every
  `failover_command` invocation recorded per instance (hook-contract
  §5, item 1). Asserts a single primary afterwards.

Run `PHASE=1 testing/acceptance.sh` to stop before pgpool comes up.

## Phase 3 (planned)

- Network partitions (`docker network disconnect`) once the real
  consensus store exists — partition scenarios against a process-local
  shadow store prove nothing. This is step 6's acceptance criteria.
- The three unmeasured hook-contract items: `pgpool_status` staleness
  after an instance restart, `follow_primary_command` non-empty
  mass-degeneration, `detach_false_primary` storms.
- `.rpm` flavor on a RHEL-family image (ROADMAP distro matrix).

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