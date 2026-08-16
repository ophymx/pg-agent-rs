# pgpool hooks under agent-led failover

**Status:** implemented — §4's block is the canonical contract
(`pg_agentctl gen-pgpool` emits it, `check-hooks` verifies it; the
pre-cutover block and its `--legacy` flag are deleted along with the
pgpool-led promote path), and the one open choice in it is resolved:
`failover_command` is kept as a notify-only poke, with the handler
always answering "advisory" on primary-down (SPEC §5.1).
Researched against the
[pgpool-II 4.6 documentation](https://www.pgpool.net/docs/46/en/html/index.html)
(the version SPEC §"PROXY protocol" is already verified against),
2026-08; quotes verified against a local mirror of the 4.6 docs
(`~/src/pgpool-4.6-docs`).

The question this answers: **when `use_watchdog = off`, which hooks does
pgpool actually fire, what do they mean, and what must the agent do about
each?** Plus one finding §6 had not priced: backend-status
synchronization between pgpool instances is a watchdog feature, and
losing it creates a new fan-out obligation for the agent.

---

## 1. What fires the hooks today (watchdog on, quorum-only)

Per [pgpool 4.6 §"Watchdog"](https://www.pgpool.net/docs/46/en/html/tutorial-watchdog-intro.html):

> "Watchdog also coordinates with all connected Pgpool-II nodes to ensure
> that failback, failover and follow_primary commands must be executed
> only on one pgpool-II node."

and:

> "When a backend node status changes by failover etc.., watchdog
> notifies the information to other Pgpool-II nodes and synchronizes
> them."

So today, three properties come bundled with `use_watchdog = on`:

1. **Single execution** — `failover_command` / `follow_primary_command` /
   `failback_command` run once cluster-wide, on the watchdog leader.
2. **Quorum gating** — `failover_when_quorum_exists` and
   `failover_require_consensus` (both default on) suppress failover
   unless a majority of pgpool instances agree the backend is gone.
3. **Backend-status sync** — an attach/detach performed on one instance
   propagates to all instances; a starting instance syncs status from
   the leader.

All three disappear together when watchdog goes off. Property 2 is the
one the promotion-authority design deliberately replaces (the CAS makes
N uncoordinated hints converge to one outcome). Properties 1 and 3 have
consequences of their own, priced below.

---

## 2. The hook table

Trigger conditions are from
[pgpool 4.6 §"Failover and Failback"](https://www.pgpool.net/docs/46/en/html/runtime-config-failover.html)
and
[§"Online Recovery"](https://www.pgpool.net/docs/46/en/html/runtime-online-recovery.html).
"Today" = current SPEC behavior; "Target" = under agent-led failover
with `use_watchdog = off`.

| Hook | Fired by (pgpool 4.6) | With watchdog off | Target meaning for the agent |
|---|---|---|---|
| `failover_command` | Backend degeneration, from any of: health-check failure exhausting retries; backend connection error (`failover_on_backend_error`, default on); backend shutdown codes 57P01/57P02 (`failover_on_backend_shutdown`); `pcp_detach_node`; a `detach_false_primary` detach; `pcp_promote_node --switchover`. | **Fires once per pgpool instance** — no leader, no single-execution guarantee, no quorum gate. Each instance computes `%m` (lowest alive node id) from its own local view, so N invocations may carry **different arguments**. | **Advisory wake-up only.** Never an order. The HA loop may use it to run an immediate tick instead of waiting for `loop_wait`, improving detection latency. All arguments (`%m`, `%d`, …) are hints; the lease CAS is the sole authority. Duplicate/conflicting invocations are expected and harmless by design. |
| `follow_primary_command` | After a failover in which the *primary* was degenerated (not for standby failovers), once per remaining non-primary backend; also on `pcp_promote_node`. Streaming-replication mode only. **Side effect when non-empty:** after a primary failover, pgpool first *"degenerates all nodes except the new primary"*, then runs the command per degenerated node — i.e. configuring this hook at all makes pgpool detach every healthy standby as part of primary failover. | Fires per instance × per standby — the multiplication of the previous row. Each instance also performs its own mass-degeneration. | **Remove (set empty), don't make it notify-only.** Leaving it empty skips the mass-degeneration of healthy standbys, which under agent-led failover is pure damage: the agent reconfigures standbys on lease change and would then have to re-attach everything pgpool detached. This tilts promotion-authority's "notify-only vs. removed" question decisively for this hook (the question stays open for `failover_command` only). |
| `failback_command` | A backend node gets attached (`pcp_attach_node`, `auto_failback`). | Fires per instance, only on the instance where the attach happened (no sync — see §3). | **Not wired today, stays unwired.** The agent initiates attaches itself and needs no callback. Listed for completeness. |
| `wd_escalation_command` | Watchdog leader acquisition (before VIP-up, but fires even with no VIP). | **Never fires.** Watchdog-only. | Delete. Already a no-op in SPEC §5.5; the RPC can be retired with the watchdog. |
| `wd_de_escalation_command` | Watchdog leader resignation (shutdown, network blackout, lost quorum). | **Never fires.** | Delete, as above. |
| `recovery_1st_stage` | `pcp_recovery_node` → `pgpool_recovery` extension exec's `$PGDATA/recovery_1st_stage` **on the current primary**. Not event-driven; operator/agent-initiated. Watchdog plays no role in triggering. | Still fires and still works — the documented multi-pgpool-without-watchdog restriction is scoped to native replication / snapshot isolation mode (2nd-stage client blocking), which we don't run. What *is* lost is attach propagation: the final node-attach lands only on the instance that received the `pcp_recovery_node` (see §3). | Unchanged as a mechanism, but the agent should keep driving recovery through its own orchestration (`cluster recover` already does) and own the attach fan-out at the end. |
| `pgpool_remote_start` | Final step of `pcp_recovery_node`: exec'd on the primary to start the recovered node's postmaster. | Same as above. | Same as above. |

Two hook-adjacent settings that are not commands but shape the contract:

| Setting | Behavior (4.6) | Target |
|---|---|---|
| `detach_false_primary` | Detaches a backend claiming primary that is not connected to its standbys (needs sr_check user with `pg_monitor`). Doc note: *"if watchdog is enabled, detaching false primary is only done by leader watchdog node."* | **On.** With watchdog off, each instance judges and detaches independently — acceptable, because a detach is per-instance *routing* state, and refusing to route to an incoherent primary is exactly the backstop wanted. Each detach fires that instance's `failover_command` (an advisory poke, per the table). |
| `auto_failback` | Re-attaches a down standby whose streaming replication is observed healthy. Doc caveat: *"auto_failback may not work, when replication slot is used."* | **Off.** We use slots, the caveat applies, and the agent owns reattach. |

---

## 3. The unpriced cost: backend-status sync is a watchdog feature

> **Priced and paid:** the attach half of this obligation is closed —
> `cluster recover` fans the attach out via the `AttachNode` peer RPC
> (each agent attaches on its OWN instance, only-if-down, primary's
> backend first when its map lacks one), and the executor's finding-16
> self-attach covers the promotion case. The analysis below is the
> original pricing.

From [pgpool 4.6 §"Watchdog"](https://www.pgpool.net/docs/46/en/html/tutorial-watchdog-intro.html):

> "At the startup, if the watchdog is enabled, Pgpool-II node sync the
> status of all configured backend nodes from the leader watchdog node.
> … When a backend node status changes by failover etc.., watchdog
> notifies the information to other Pgpool-II nodes and synchronizes
> them."

With watchdog off, **nothing propagates backend status between pgpool
instances** — and the loss is asymmetric:

- **Detach converges on its own.** Each instance runs its own health
  checks against the same backends, so a genuinely dead backend gets
  detached everywhere within a health-check period, independently.
- **Attach does not.** Down status is sticky: a backend marked down is
  no longer health-checked back to life (that is what `auto_failback`
  would be for, and it is off — see §2). A `pcp_attach_node` against
  one instance attaches the backend on that instance only. The other
  instances keep routing without it until each receives its own attach.
- **Restart resumes stale state.** A restarting instance trusts its own
  `pgpool_status` file rather than syncing from a leader; nothing
  corrects it except explicit PCP or discarding the file
  (`pgpool -D`).

(For completeness: the online-recovery docs carry a hard restriction —
*"If Pgpool-II itself is installed on multiple hosts without enabling
watchdog, online recovery does not work correctly"* — but it is scoped
to **native replication and snapshot isolation modes**, whose 2nd stage
must block clients on every instance. Streaming replication mode has no
2nd stage; our recovery mechanism keeps working. The attach-propagation
gap above is the part that survives into our mode.)

Consequences for the agent, which today can issue a single PCP call and
rely on watchdog to spread it:

- **Every attach becomes a fan-out.** `pcp_attach_node` (and any
  deliberate `pcp_detach_node`) must be issued against *every* pgpool
  instance — each agent hitting its local pgpool over the existing peer
  mesh is the natural shape. Partial success must be surfaced, not
  hidden: two instances routing to a backend the third lacks is a
  legitimate degraded state for `cluster status` to report.
- **`pgpool_status` staleness is now an operator-visible failure mode.**
  `cluster status` (which already fans out) is the natural place to
  diff per-instance backend maps and warn on divergence; worth
  considering whether the agent should reconcile automatically
  (fan-out attach of any backend the lease says is healthy).

None of this is an argument against dropping watchdog — routing-state
divergence is self-limiting (worst case: queries error against a down
backend, promotion-authority §6's "routing state" concern) where role
divergence is fatal. But it converts an implicit pgpool guarantee into
explicit agent work, and it belongs in the implementation estimate for
the cutover step.

---

## 4. Intended pgpool configuration (expanded from promotion-authority §6)

```
# --- consensus/coordination: none. The agent mesh owns role. ---
use_watchdog               = off      # escalation/de-escalation hooks retire with it

# --- hooks: advisory pokes at most ---
failover_command           = 'pg_agentc failover %d %h %p %D %m %H %M %P %r %R %N %S'
                                      # KEEP (notify-only): wakes the HA loop for
                                      # detection latency; args documented advisory.
                                      # Alternative: remove entirely — open question.
follow_primary_command     = ''       # agent reacts to lease change instead.
                                      # MUST be empty, not notify-only: non-empty
                                      # makes pgpool degenerate every healthy
                                      # standby after a primary failover (§2)
failback_command           = ''       # unused today, stays unused
wd_escalation_command      =          # (n/a — watchdog off)
wd_de_escalation_command   =          # (n/a — watchdog off)

# --- detection: keep, this is how pgpool learns and routes ---
sr_check_period            = 10       # pgpool discovers the primary by itself
health_check_period        = (keep current)
failover_on_backend_error  = on       # per-instance routing reaction, self-limiting
detach_false_primary       = on       # backstop: refuse to route to incoherent primary
auto_failback              = off      # slots in use (doc caveat); agent owns reattach

# --- recovery: mechanism retained, entry point moves to the agent ---
recovery_1st_stage_command = 'recovery_1st_stage'   # still exec'd via extension
                                      # but driven by agent orchestration, not
                                      # pcp_recovery_node (see §3)
```

The one live choice in that block is `failover_command`: keep as a
notify-only poke (buys detection latency, costs a forever-documented
"these args are advisory" contract) or remove (HA loop polls at
`loop_wait`). That is promotion-authority open question 4, unchanged.

---

## 5. Empirical results

Measured on the dockerized 3-node acceptance cluster
([testing/README.md](../testing/README.md)), pgpool-II 4.6 with
`use_watchdog = off`, this config, and real PostgreSQL 17 replication.
Items 1–2 are **confirmed**; 3–5 remain open.

1. **Per-instance firing count — CONFIRMED** (promotion-authority open
   question 7). Stopping the primary's PostgreSQL produced **exactly
   one `failover_command` invocation per pgpool instance, three across
   the cluster**, all within the same second, all carrying *identical*
   arguments (`%d = 0`, `%m = 1`, `%P = 0`). So without watchdog the
   hook is not deduplicated, but neither did the instances disagree:
   each computed `%m` = lowest alive node id from its own view and
   reached the same answer.

   Two consequences for the design. The N-invocations prediction holds,
   so the cutover must keep the hook idempotent or advisory — that part
   is confirmed. But the *reason* they agreed is worth naming: `%m` is
   a pure function of the backend-status map, and the instances agreed
   because their maps agreed. Under the asymmetric-partition case where
   the maps differ, the invocations will differ too, and today only
   idempotence + the "already primary" short-circuit stand between
   that and a double promotion. That is precisely the gap the lease
   CAS closes.

   Today's outcome without a lease: **exactly one primary, no split
   brain** — three agents each received the same hint, the first
   promoted, the rest short-circuited on `get_status` reporting the
   target already primary. Idempotence carried it. Worth having as the
   pre-consensus baseline.

2. **Attach/detach propagation — CONFIRMED ABSENT, and asymmetric as
   predicted (§3).** `pcp_detach_node -n 2` on db0's instance marked
   node 2 down *there only*; db1's instance still routed to it. The
   detach was sticky (no `auto_failback`), and an explicit
   `pcp_attach_node` per instance was required to converge. This
   confirms the fan-out obligation §3 describes.

   **New finding, not predicted:** a `pcp_detach_node` against a
   *healthy* standby fires that instance's `failover_command` with the
   standby as `%d`, which routes into the agent's standby-down branch —
   whose job is to drop the detached node's replication slot. Without
   the precondition check (SPEC §5.1 step 3), an operator detaching a
   backend for maintenance would have silently destroyed a live
   standby's slot and broken its replication. With the check, the agent
   refuses because the "failed" standby is reachable and streaming.
   The precondition work therefore protects against routine operator
   actions, not only against false health reports — a stronger
   justification than the one it was written for.

3. **`pgpool_status` after restart.** Not yet measured. Restart one
   instance with a stale status file while the cluster shape has
   changed; confirm the down-is-sticky behavior and that only explicit
   attach or `pgpool -D` corrects it.
4. **`follow_primary_command` empty vs. mass-degeneration.** Not yet
   measured — the acceptance cluster runs it empty (the target
   contract) and the standbys were not degenerated on failover, which
   is consistent with the claim but does not isolate it. Isolating it
   needs a run with the hook non-empty, comparing backend states after
   a primary failover.
5. **`detach_false_primary` storm behavior.** Not yet measured.
   Requires manufacturing a false primary (promote a standby out of
   band) with three independent instances watching.
