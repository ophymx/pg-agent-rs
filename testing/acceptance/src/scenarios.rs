//! The G-scenarios, ported at parity from the bash suite (same checks,
//! same order, same wording where it still fits) — but with the
//! assertions event-aware: scenario windows are event cursors instead
//! of `journalctl --since` wall-clock bounds, "did X happen" awaits or
//! scans the shared event log, and absence claims cover the whole
//! window bounded by awaited events rather than a sampled instant.

use std::collections::HashMap;
use std::time::Duration;

use crate::checks::Ctx;
use crate::cluster::{self, exec, exec_ok, exec_pg, node_id, unit_active, NODES};
use crate::events::{Cursor, Event, Source};

fn agent(ev: &Event, node: &str, needle: &str) -> bool {
    ev.source == Source::Agent && ev.node == node && ev.line.contains(needle)
}

fn agent_any(ev: &Event, needle: &str) -> bool {
    ev.source == Source::Agent && ev.line.contains(needle)
}

/// First member that isn't `not` (bash's "first non-X" picks).
fn other_node(not: &str) -> &'static str {
    NODES.iter().copied().find(|n| *n != not).unwrap()
}

async fn pcp_node_info(node: &str, id: &str) -> String {
    exec_pg(
        node,
        &format!("pcp_node_info -h localhost -p 9898 -U pgpool -w -n {id}"),
    )
    .await
    .unwrap_or_default()
}

async fn pcp_backends_up(node: &str) -> usize {
    exec_pg(node, "pcp_node_info -h localhost -p 9898 -U pgpool -w -a")
        .await
        .unwrap_or_default()
        .lines()
        .filter(|l| l.contains(" up "))
        .count()
}

/// Commit a sentinel row on `node` — the write whose survival the
/// scenario asserts after the induced failure. With quorum commit
/// armed, the acknowledgment itself proves the row is on ≥ 2 nodes
/// (docs/quorum-commit.md §3); this returning at all is part of the
/// test.
async fn write_sentinel(cx: &mut Ctx, node: &'static str, label: &str) {
    let res = cx
        .pg
        .execute(
            node,
            &format!(
                "create table if not exists sentinel(label text primary key, at timestamptz); \
                 insert into sentinel(label, at) values ('{label}', now()) \
                 on conflict (label) do update set at = now()"
            ),
        )
        .await;
    cx.check(
        &format!("sentinel '{label}' committed on {node} (quorum-acked)"),
        res.is_ok(),
    );
}

/// The suite's data-survival assertion: an ACKNOWLEDGED write from
/// before the failure must exist on the post-failover primary. Before
/// quorum commit, nothing asserted this — a lost sentinel was exactly
/// the acknowledged-write loss findings 17/18 priced.
async fn check_sentinel(cx: &mut Ctx, node: &'static str, label: &str) {
    let found = cx
        .pg
        .scalar(
            node,
            &format!("select count(*)::text from sentinel where label = '{label}'"),
        )
        .await;
    cx.check(
        &format!("sentinel '{label}' survived onto {node} (acked write not lost)"),
        found.as_deref().map(|v| v == "1").unwrap_or(false),
    );
}

/// The node's `/healthz` body, parsed. `curl -s`, never `-sf`: an
/// UNHEALTHY node answers 503 with the same JSON, and those are exactly
/// the nodes whose health the suite most wants to read (a `-f` here
/// once discarded the very body a wedge flag lives in).
async fn healthz(node: &str) -> serde_json::Value {
    exec(node, "curl -s localhost:9702/healthz")
        .await
        .ok()
        .and_then(|body| serde_json::from_str(&body).ok())
        .unwrap_or(serde_json::Value::Null)
}

/// Healthz-reported quorum-commit posture on `node`.
async fn sync_commit_state(node: &str) -> String {
    healthz(node).await["sync_commit"]
        .as_str()
        .unwrap_or("")
        .into()
}

async fn cluster_recover(cx: &Ctx, via: &str, target: &str) {
    // One retry after a pause, like the operator it models: the first
    // RPC after a partition heals can ride a cached-but-broken peer
    // channel (finding 20 — the pool evicts by age, not on error) and
    // fail with a transport error; tonic redials underneath and the
    // retry succeeds.
    for attempt in 0..2 {
        match exec_pg(
            via,
            &format!(
                "pg_agentctl cluster recover --target {} --stop-target-pg",
                node_id(target)
            ),
        )
        .await
        {
            Ok(_) => return,
            Err(e) => {
                let text = e.to_string();
                let tail: Vec<&str> = text.lines().filter(|l| !l.trim().is_empty()).collect();
                let tail = tail[tail.len().saturating_sub(3)..].join(" | ");
                cx.note(&format!(
                    "cluster recover of {target} via {via} failed (attempt {}): {tail}",
                    attempt + 1
                ));
            }
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}

/// Run one scenario, unless `FAIL_FAST` and something already failed.
///
/// Bailing out returns from `run_all` entirely, which also skips the
/// audit: the auditor reasons over the whole run's event order, and on
/// a truncated run its complaints are artifacts of the truncation
/// rather than findings.
macro_rules! stage {
    ($cx:expr, $call:expr) => {{
        if $cx.stop_early() {
            $cx.note("FAIL_FAST: stopping at the first failure");
            return;
        }
        $call
    }};
}

pub async fn run_all(cx: &mut Ctx) {
    let mut marks: HashMap<&'static str, Cursor> = NODES.iter().map(|n| (*n, Cursor(0))).collect();

    stage!(cx, g0(cx).await);
    stage!(cx, g1(cx).await);
    stage!(cx, g1b(cx).await);
    stage!(cx, g2(cx).await);
    stage!(cx, g2b(cx).await);
    let w1 = stage!(cx, g3(cx).await);
    stage!(cx, g4(cx, w1, "db0", &mut marks).await);
    stage!(cx, g4b(cx, w1, &mut marks).await);
    let w2 = stage!(cx, g5(cx, w1).await);
    stage!(cx, g5b(cx, w1, w2, &mut marks).await);
    stage!(cx, g6(cx, w2).await);
    let w3 = stage!(cx, g7(cx, w2, &mut marks).await);
    let w4 = stage!(cx, g8(cx, w3, &mut marks).await);
    let w5 = stage!(cx, g9(cx, w4, &mut marks).await);
    stage!(cx, g10(cx, w5).await);
    let w6 = stage!(cx, g11(cx, w5, &mut marks).await);
    let w7 = stage!(cx, g12(cx, w6, &mut marks).await);
    stage!(cx, g13(cx, w7, &mut marks).await);
    stage!(cx, g14(cx, w7, &mut marks).await);
    let w8 = stage!(cx, g15(cx, w7, &mut marks).await);
    stage!(cx, g16(cx, w8).await);
    stage!(cx, g17(cx, w8).await);
    stage!(cx, g18(cx, w8, &mut marks).await);
    let w9 = cx.pg.current_primary().await.unwrap_or(w8);
    stage!(cx, g19(cx, w9).await);
    let w10 = stage!(cx, g20(cx, w9, &mut marks).await);
    stage!(cx, g21(cx, w10, &mut marks).await);
    // Last on purpose: the one scenario whose claim is that nothing
    // happens reads best against the most-abused cluster the suite can
    // hand it, and it needs a primary the soak settled on rather than
    // one this list predicted.
    let w11 = cx.pg.current_primary().await.unwrap_or(w10);
    stage!(cx, g22(cx, w11).await);
    stage!(cx, crate::audit::run(cx).await);
}

async fn g0(cx: &mut Ctx) {
    cx.say("G0: greenfield boot — the loop acts from the first start");
    // Consensus is not configurable: every daemon joins it or fails to
    // start. validate-env (the systemd ExecStartPre gate) runs the raft
    // prerequisite checks on every start; the daemons coming up IS that
    // assertion.
    //
    // This scenario used to await a `StoreUnknown` tick here, on the
    // premise that membership did not exist until `ClusterInit` and the
    // loop therefore had a pre-membership window to be careful in.
    // v0.9.0 removed the window: a daemon forms the pool from its own
    // `[[pool]]` at startup. That premise had also cost a live cluster
    // every one of its PostgreSQL instances — gating membership on a
    // command that basebackups every standby left an upgraded cluster
    // with no voters and no reachable bootstrap at all — so what this
    // asserts now is the guarantee that replaced it: the pool forms
    // unattended, before any operator command.
    //
    // The "never acts on an unformed pool" half is kept below. It is
    // the half that was always load-bearing, and it survives the
    // window it used to be observed in.
    for n in NODES {
        cx.wait_until(
            60,
            &format!("{n}: pg_agentd active (validate-env gate incl. raft checks)"),
            || async move { unit_active(n, "pg_agentd").await },
        )
        .await;
    }
    cx.wait_until(30, "db0: postgres active (bootstrap primary)", || async {
        unit_active("db0", &cluster::pg_unit()).await
    })
    .await;
    for n in ["db1", "db2"] {
        cx.check(
            &format!("{n}: postgres intentionally down pre-init"),
            !unit_active(n, &cluster::pg_unit()).await,
        );
    }
    for n in NODES {
        // The loop is running, which means consensus opened and the
        // executor is attached — the daemon cannot reach this line
        // otherwise (`agent::HaWiring` binds all three together, and
        // there is no config that spells any subset). The line used to
        // say "EXECUTE mode", back when a loop could also be running
        // in the mode that acts on nothing.
        cx.await_event(
            30,
            &format!("{n}: raft started, ha loop acting"),
            Cursor(0),
            |ev| agent(ev, n, "ha loop: starting"),
        )
        .await;
    }
    // Any node, not db0: the stagger makes the lowest pool position the
    // usual bootstrapper, but "usual" is not an invariant worth
    // encoding — whichever node gets there first is a correct outcome,
    // and pinning the assertion to one of them would make container
    // start order a test failure.
    cx.await_event(
        60,
        "membership forms at startup, with no operator command",
        Cursor(0),
        |ev| agent_any(ev, "raft: membership ensured at startup"),
    )
    .await;
    // The pool is not merely configured, it elected — the property
    // cold start depends on, and the one whose absence left a real
    // cluster unable to ever read its own lease.
    //
    // One event, not one per node: exactly one node becomes leader and
    // openraft logs nothing at INFO for the followers, so a per-node
    // assertion would be asserting something that cannot be true.
    cx.await_event(60, "raft elected a leader", Cursor(0), |ev| {
        agent_any(ev, "become leader")
    })
    .await;
    cx.check_absent(
        "no executor action before membership exists",
        Cursor(0),
        |ev| {
            ev.source == Source::Agent
                && ev.node == "db0"
                && (ev.line.contains("FENCING") || ev.line.contains("roleexec: promoting"))
        },
    );
}

async fn g1(cx: &mut Ctx) {
    cx.say("G1: cluster init — replication + membership + lease, one command");
    let init1 = exec_pg("db0", "pg_agentctl cluster init")
        .await
        .unwrap_or_else(|e| e.to_string());
    cx.check(
        "cluster init: standbys initialised",
        init1.contains("cluster_init complete"),
    );
    // Either wording is a pass, and which one appears is itself the
    // point. Since v0.9.0 the daemons form the pool at startup, so on a
    // greenfield cluster whose agents are already up — G0 just watched
    // them come up — `cluster init` arrives to find membership done and
    // says "already formed". It still says "initialized" if it genuinely
    // got there first, which is the ordering this suite does not
    // legislate. What must not happen is init FAILING over a pool that
    // someone else formed: idempotence is the contract, and the second
    // init below asserts the same thing from the other direction.
    cx.check(
        "raft membership present after init (formed by it, or already by startup)",
        init1.contains("raft membership initialized (3 nodes)")
            || init1.contains("raft membership already formed"),
    );
    // Same story as membership above, one layer up. Now that the pool
    // forms at startup, raft elects before `cluster init` is typed, and
    // the HA loop reaches the vacant lease first: db0 is the only node
    // observably running as a primary, which is precisely the case the
    // vacant branch claims for itself. So init finds the lease already
    // held rather than seeding it — by the node it would have seeded it
    // for, at the term it would have minted.
    //
    // Both wordings pass because which of the two got there first is a
    // race this suite has no business legislating. The OUTCOME is
    // asserted right below ("db0 retains the lease"), and that is the
    // claim that was ever worth making.
    cx.check(
        "bootstrap primary holds the lease after init (seeded by it, or claimed by the loop)",
        init1.contains("lease seeded") || init1.contains("lease already held"),
    );
    if cx.failures.iter().any(|f| f.contains("cluster init")) {
        cx.note(&format!("init output: {}", init1.trim()));
    }
    let pg = cx.pg.clone();
    cx.wait_until(60, "db0 has 2 streaming standbys", || {
        let pg = pg.clone();
        async move { pg.streaming_count("db0").await == Some(2) }
    })
    .await;
    cx.await_event(
        30,
        "db0 retains the lease (execute-mode steady state)",
        Cursor(0),
        |ev| agent(ev, "db0", "RetainedLease"),
    )
    .await;
    for n in ["db1", "db2"] {
        cx.await_event(
            60,
            &format!("{n} executor converged onto the holder"),
            Cursor(0),
            |ev| agent(ev, n, "now following lease holder"),
        )
        .await;
    }
    let init2 = exec_pg("db0", "pg_agentctl cluster init --only-node 99")
        .await
        .unwrap_or_else(|e| e.to_string());
    cx.check(
        "re-running init is idempotent (membership already formed)",
        init2.contains("already formed"),
    );
}

async fn g1b(cx: &mut Ctx) {
    cx.say("G1b: pgpool up as the router (agent-led contract)");
    for n in NODES {
        cx.check(
            &format!("{n}: pgpool configured + started"),
            exec_ok(n, "/usr/local/sbin/pg-agent-pgpool-setup").await,
        );
    }
    for n in NODES {
        cx.wait_until(
            60,
            &format!("{n}: pgpool shows 3 backends up"),
            || async move { pcp_backends_up(n).await == 3 },
        )
        .await;
    }
    cx.wait_until(30, "db0: /healthz reports ready", || async {
        exec("db0", "curl -sf localhost:9702/healthz")
            .await
            .map(|o| o.contains("\"ready\":true"))
            .unwrap_or(false)
    })
    .await;
    // Quorum commit arms at the first-standby-attached event
    // (docs/quorum-commit.md §5) — by now both standbys stream, so the
    // executor's next probe must have armed ANY 1.
    cx.wait_until(
        30,
        "db0: quorum commit armed (healthz sync_commit)",
        || async { sync_commit_state("db0").await == "armed" },
    )
    .await;
}

async fn g2(cx: &mut Ctx) {
    cx.say("G2: the hook contract holds (check-hooks fully clean)");
    // The deployed conf ends with the canonical gen-pgpool block, no
    // overrides — check-hooks must pass verbatim: hooks AND settings.
    let conf = format!("{}/pgpool.conf", cluster::pgpool_conf_dir());
    match exec_pg("db0", &format!("pg_agentctl check-hooks {conf}")).await {
        Ok(_) => cx.pass("check-hooks passes on the deployed conf (exit 0)"),
        Err(e) => {
            let text = e.to_string();
            let drift: Vec<&str> = text.lines().filter(|l| l.contains("ERR")).collect();
            cx.fail(&format!("check-hooks found drift: {}", drift.join("; ")));
        }
    }
}

async fn g2b(cx: &mut Ctx) {
    cx.say("G2b: pgpool stays a router — detach neither propagates nor breaks replication");
    // Steady state, pristine maps: ROUTING semantics before any
    // failover rearranges the per-instance maps.
    let since = cx.log.cursor();
    let _ = exec_pg(
        "db0",
        "pcp_detach_node -h localhost -p 9898 -U pgpool -w -n 2",
    )
    .await;
    cx.wait_until(15, "db0's pgpool shows node 2 down", || async {
        pcp_node_info("db0", "2").await.contains("down")
    })
    .await;
    cx.check(
        "db1's pgpool still shows node 2 up (no propagation)",
        pcp_node_info("db1", "2").await.contains(" up "),
    );
    cx.await_event(
        30,
        "agent refused the slot drop (detached standby is still streaming)",
        since,
        |ev| agent(ev, "db0", "refusing to drop slot"),
    )
    .await;
    let streaming = cx.pg.streaming_count("db0").await;
    cx.check("replication intact (2 streaming)", streaming == Some(2));
    let _ = exec_pg(
        "db0",
        "pcp_attach_node -h localhost -p 9898 -U pgpool -w -n 2",
    )
    .await;
    cx.wait_until(30, "explicit attach clears the detach", || async {
        pcp_node_info("db0", "2").await.contains(" up ")
    })
    .await;
}

async fn g3(cx: &mut Ctx) -> &'static str {
    cx.say("G3: primary death — hook advises, the lease decides, pgpool discovers");
    write_sentinel(cx, "db0", "g3").await;
    let since = cx.log.cursor();
    let _ = exec("db0", &format!("systemctl stop {}", cluster::pg_unit())).await;
    cx.await_event(
        30,
        "failover hook answered advisory (notify-only, as contracted)",
        since,
        |ev| agent_any(ev, "failover: advisory"),
    )
    .await;
    let winner_ev = cx
        .await_event(60, "the lease promoted a standby", since, |ev| {
            ev.node != "db0" && agent_any(ev, "roleexec: promotion complete")
        })
        .await;
    let w1: &'static str = match winner_ev {
        Some(ev) => {
            cx.pass(&format!("winner: {}", ev.node));
            ev.node
        }
        None => {
            cx.fail("no promotion winner found");
            "db1"
        }
    };
    let primaries = cx.pg.count_primaries().await;
    cx.check(
        "exactly one PostgreSQL primary after failover",
        primaries == 1,
    );
    let ops = exec_pg(w1, "pg_agentctl ops list")
        .await
        .unwrap_or_default();
    cx.check(
        "promotion is journaled (ops list shows a done promote op)",
        ops.lines()
            .any(|l| l.contains("promote") && l.to_lowercase().contains("done")),
    );
    let pg = cx.pg.clone();
    cx.wait_until(
        90,
        &format!("the surviving standby re-pointed and streams from {w1}"),
        || {
            let pg = pg.clone();
            async move { pg.streaming_count(w1).await == Some(1) }
        },
    )
    .await;
    let wid = node_id(w1);
    cx.wait_until(
        60,
        "pgpool discovered the new primary via sr_check (no follow hook)",
        || async move {
            pcp_node_info(w1, wid)
                .await
                .to_lowercase()
                .contains("primary")
        },
    )
    .await;
    // Finding 16: failover_on_backend_error can degenerate the
    // winner's own backend on its own instance (~20 s post-promote) —
    // permanent under auto_failback off unless the executor's
    // self-attach probe converges it. Role alone doesn't prove
    // routability; status must be up.
    cx.wait_until(
        60,
        "the winner's own pgpool routes to it (backend up — finding 16)",
        || async move { pcp_node_info(w1, wid).await.contains(" up ") },
    )
    .await;
    check_sentinel(cx, w1, "g3").await;
    cx.wait_until(
        60,
        &format!("{w1}: quorum commit re-armed after the failover"),
        || async move { sync_commit_state(w1).await == "armed" },
    )
    .await;
    w1
}

async fn g4(
    cx: &mut Ctx,
    w1: &'static str,
    dead: &'static str,
    marks: &mut HashMap<&'static str, Cursor>,
) {
    cx.say("G4: operator rejoin — demote policy, the slot-race guard, repair");
    cx.check(
        &format!("dead ex-primary {dead} stayed stopped (rejoin is never automatic)"),
        !unit_active(dead, &cluster::pg_unit()).await,
    );
    cluster_recover(cx, w1, dead).await;
    repair_standbys(cx, w1, "g4", marks).await;
    let primaries = cx.pg.count_primaries().await;
    cx.check("exactly one primary after the rejoin", primaries == 1);
    // Hook-contract §3: recover fans the attach out, so EVERY
    // instance's map converges on the recovered node — previously only
    // the recovering primary's did, and the others routed around it
    // until an operator attached per instance.
    for n in NODES {
        cx.wait_until(
            60,
            &format!("{n}: pgpool routes to recovered {dead} (attach fan-out)"),
            || async move { pcp_node_info(n, node_id(dead)).await.contains(" up ") },
        )
        .await;
    }
}

async fn g4b(cx: &mut Ctx, w1: &'static str, marks: &mut HashMap<&'static str, Cursor>) {
    cx.say("G4b: reclone a LIVE standby — the slot-race guard (finding 9)");
    // The recover/failover-hook race: the standby-down hook's job is
    // to drop the very slot the in-flight recovery just created — the
    // cross-op consult must defer. The hook used to fire NATURALLY
    // (pgpool's health check detached the recovery-stopped standby in
    // ~4 s); with blip-tolerant health checking (~22 s, G10) natural
    // detection no longer lands inside a small test reclone — but a
    // production reclone takes minutes and the race is as real as
    // ever. Manufacture it deterministically: an explicit detach on
    // the primary's instance mid-reclone fires the same hook.
    pcp_attach_everywhere(cx).await;
    let target = other_node(w1);
    let since = cx.log.cursor();
    let recover = cluster_recover(cx, w1, target);
    let detach_mid_op = async {
        tokio::time::sleep(Duration::from_secs(2)).await;
        let _ = exec_pg(
            w1,
            &format!(
                "pcp_detach_node -h localhost -p 9898 -U pgpool -w -n {}",
                node_id(target)
            ),
        )
        .await;
    };
    tokio::join!(recover, detach_mid_op);
    cx.await_event(
        30,
        "failover hook deferred to the in-flight recovery (slot survived)",
        since,
        |ev| agent(ev, w1, "in-flight op owns this node; skipping slot drop"),
    )
    .await;
    // The recover's attach fan-out (only-if-down, runs at the tail of
    // the reclone — after the +2 s detach) restores the backend this
    // detach downed; G4 asserted exactly that convergence shape.
    let tid = node_id(target);
    cx.wait_until(
        60,
        &format!("{w1}: pgpool re-attached recovered {target} (fan-out after mid-op detach)"),
        || async move { pcp_node_info(w1, tid).await.contains(" up ") },
    )
    .await;
    repair_standbys(cx, w1, "g4b", marks).await;
}

async fn g5(cx: &mut Ctx, w1: &'static str) -> &'static str {
    cx.say("G5: partition of the holder — fence + one takeover + one primary");
    write_sentinel(cx, w1, "g5").await;
    let since = cx.log.cursor();
    cluster::network_disconnect(w1).await;
    cx.say(&format!(
        "     (isolated {w1} — current primary and lease holder)"
    ));
    cx.await_event(
        30,
        "isolated holder fenced itself (quorum loss → stop)",
        since,
        |ev| agent(ev, w1, "FENCING"),
    )
    .await;
    // Await the shutdown EVENT, not a systemd sample: `is-active`
    // reads `deactivating` as stopped while the postmaster still owns
    // $PGDATA, and proceeding into G5b's recover on that sample raced
    // basebackup's pgdata clear against the dying postmaster —
    // observed live. Budget 90 s: a PARTITIONED primary's fast
    // shutdown drains walsenders toward wal_sender_timeout (60 s
    // default; 44 s observed — finding 17). Write service ends at the
    // shutdown request, so this drain is fence *latency*, not a
    // dual-primary window; the audit's serving intervals encode that.
    cx.await_event(
        90,
        &format!("isolated {w1} PostgreSQL fully shut down"),
        since,
        move |ev| {
            ev.node == w1
                && ev.source == Source::Postgres
                && ev.line.contains("database system is shut down")
        },
    )
    .await;
    let winner_ev = cx
        .await_event(
            60,
            "majority completed a real promotion",
            since,
            move |ev| ev.node != w1 && agent_any(ev, "roleexec: promotion complete"),
        )
        .await;
    let w2: &'static str = match winner_ev {
        Some(ev) => {
            cx.pass(&format!("majority promoted {}", ev.node));
            ev.node
        }
        None => {
            cx.fail("no majority winner");
            other_node(w1)
        }
    };
    cx.check_absent(
        "isolated node committed nothing (no quorum, no writes)",
        since,
        |ev| agent(ev, w1, "TookOver"),
    );
    // Hysteresis: every takeover ≥ leader_ttl after the previous — a
    // fresh holder's protection window (finding 13) held under real
    // execution. Event receipt times are host-side; millisecond tail
    // latency against a 10 s bound.
    let takeovers = cx
        .log
        .find_all(since, |ev| ev.node != w1 && agent_any(ev, "TookOver"));
    let mut violation = None;
    for pair in takeovers.windows(2) {
        let gap = pair[1].at.duration_since(pair[0].at);
        if gap < Duration::from_secs(10) {
            violation = Some(gap);
        }
    }
    match violation {
        None => cx.pass("every takeover ≥ leader_ttl after the previous (hysteresis held)"),
        Some(gap) => cx.fail(&format!(
            "takeovers {}s apart — a fresh holder was deposed inside its ttl",
            gap.as_secs()
        )),
    }
    w2
}

async fn g5b(
    cx: &mut Ctx,
    w1: &'static str,
    w2: &'static str,
    marks: &mut HashMap<&'static str, Cursor>,
) {
    cx.say("G5b: partition heals — one primary throughout, operator rejoins");
    cluster::network_connect(w1).await;
    // Short settle for the veth before recover dials the healed node's
    // agent; the primary-count check itself needs none (the fenced
    // node's PostgreSQL is verified stopped right below).
    tokio::time::sleep(Duration::from_secs(2)).await;
    let primaries = cx.pg.count_primaries().await;
    cx.check(
        "exactly one PostgreSQL primary during and after the partition",
        primaries == 1,
    );
    cx.check(
        "fenced ex-holder stays down after reconnect (demote policy)",
        !unit_active(w1, &cluster::pg_unit()).await,
    );
    cluster_recover(cx, w2, w1).await;
    repair_standbys(cx, w2, "g5", marks).await;
    let primaries = cx.pg.count_primaries().await;
    cx.check("exactly one primary after the rejoin", primaries == 1);
    check_sentinel(cx, w2, "g5").await;
}

async fn g6(cx: &mut Ctx, w2: &'static str) {
    cx.say("G6: agent restarts are non-events");
    let since = cx.log.cursor();
    let _ = exec(w2, "systemctl restart pg_agentd").await;
    cx.wait_until(
        30,
        &format!("{w2}: pg_agentd active after restart"),
        || async move { unit_active(w2, "pg_agentd").await },
    )
    .await;
    cx.await_event(
        30,
        "phantom-primary check confirmed (no conservative stop)",
        since,
        |ev| agent(ev, w2, "phantom-primary check: confirmed"),
    )
    .await;
    let in_rec = cx.pg.is_in_recovery(w2).await;
    cx.check(
        &format!("{w2} postgres untouched by the agent restart"),
        in_rec == Some(false),
    );
    cx.await_event(
        30,
        &format!("{w2} retains the lease after its agent restart"),
        since,
        |ev| agent(ev, w2, "RetainedLease"),
    )
    .await;
    let standby = other_node(w2);
    let _ = exec(standby, "systemctl restart pg_agentd").await;
    cx.wait_until(
        30,
        &format!("{standby}: pg_agentd active after restart"),
        || async move { unit_active(standby, "pg_agentd").await },
    )
    .await;
    // Event-bounded absence window: wait for the restarted standby to
    // reach its post-restart steady state (the positive event that
    // closes the danger window), THEN assert no takeover happened in
    // it — instead of sleeping a guessed number of seconds.
    cx.await_event(
        30,
        &format!("{standby} resumed following after its restart"),
        since,
        |ev| agent(ev, standby, "now following lease holder"),
    )
    .await;
    cx.check_absent("no takeover churn from the restarts", since, |ev| {
        agent_any(ev, "TookOver")
    });
    let streaming = cx.pg.streaming_count(w2).await;
    cx.check("replication intact (2 streaming)", streaming == Some(2));
}

async fn g7(
    cx: &mut Ctx,
    w2: &'static str,
    marks: &mut HashMap<&'static str, Cursor>,
) -> &'static str {
    cx.say("G7: candidate selection refuses a FLUSH-lagging standby (§2.2 under the lease)");
    // The §2.2 defect was pgpool picking a candidate by lowest node id
    // with no WAL comparison. Under the lease, candidacy runs the
    // most-advanced check itself — comparing FLUSH positions
    // (docs/quorum-commit.md §4, finding 19: replay lag is not data
    // lag; a replay-paused standby holds every flushed byte and is a
    // legitimate winner). Arrange real flush lag: sever one standby's
    // walreceiver path only (PG port — its agent stays reachable, so
    // it PARTICIPATES in candidacy and the lag gate is what refuses
    // it; a full partition would just exclude it from quorum), write
    // ~24 MB on the primary, kill the primary.
    let mut rest = NODES.iter().copied().filter(|n| *n != w2);
    let s_lag = rest.next().unwrap();
    let s_ok = rest.next().unwrap();
    let prim_ip = cluster::container_ip(w2).await.unwrap_or_default();
    cluster::sever_peer_port(s_lag, &prim_ip, 5432).await;
    // Kill the established walreceiver connection; reconnects hit the
    // DROP rules and hang in connect, so receive (flush) goes static.
    let _ = cx
        .pg
        .execute(
            s_lag,
            "select pg_terminate_backend(pid) from pg_stat_wal_receiver",
        )
        .await;
    let _ = cx
        .pg
        .execute(
            w2,
            "create table if not exists bulk(id int, pad text); \
             insert into bulk select g, repeat(chr(97+(g%26)),200) from generate_series(1,120000) g; \
             checkpoint; select pg_switch_wal()",
        )
        .await;
    let pg = cx.pg.clone();
    cx.wait_until(
        30,
        &format!("{s_lag} trails by > 16 MiB of FLUSHED WAL (walreceiver severed)"),
        || {
            let pg = pg.clone();
            async move {
                let Ok(lag_flush) = pg
                    .scalar(
                        s_lag,
                        "select greatest(coalesce(pg_last_wal_receive_lsn(),'0/0'::pg_lsn), \
                         coalesce(pg_last_wal_replay_lsn(),'0/0'::pg_lsn))::text",
                    )
                    .await
                else {
                    return false;
                };
                pg.scalar(
                    w2,
                    &format!(
                        "select (pg_wal_lsn_diff(pg_current_wal_lsn(), '{lag_flush}'::pg_lsn) \
                         > 16*1024*1024)::text"
                    ),
                )
                .await
                .is_ok_and(|v| v == "t" || v == "true")
            }
        },
    )
    .await;
    write_sentinel(cx, w2, "g7").await;
    let since = cx.log.cursor();
    let _ = exec(w2, &format!("systemctl stop {}", cluster::pg_unit())).await;
    cx.await_event(
        60,
        &format!("the caught-up standby {s_ok} won the takeover"),
        since,
        |ev| agent(ev, s_ok, "roleexec: promotion complete"),
    )
    .await;
    cx.check_absent("the lagging standby was not promoted", since, |ev| {
        agent(ev, s_lag, "roleexec: promotion complete")
    });
    // The stand-down log is timing-dependent: it only appears if the
    // lagging node's candidacy tick fires before the winner's CAS
    // lands. The enforced property is the asserts above; the log, when
    // present, is corroboration.
    if cx
        .log
        .find(since, |ev| agent(ev, s_lag, "is ahead by"))
        .is_some()
    {
        cx.pass(&format!(
            "{s_lag} stood down naming the WAL gap (corroboration)"
        ));
    } else {
        cx.note(&format!(
            "{s_lag} never reached candidacy (winner's CAS landed first)"
        ));
    }
    cluster::heal_firewall(s_lag).await;
    // Repair via the ACTUAL primary, not the predicted winner: if the
    // enforced assert above failed (finding 19's candidacy race), the
    // repairs must still converge the cluster instead of cascading
    // "not the primary" refusals through the remaining checks.
    let actual = cx.pg.current_primary().await.unwrap_or(s_ok);
    if actual != s_ok {
        cx.note(&format!(
            "repairing via actual primary {actual} (predicted winner was {s_ok})"
        ));
    }
    cluster_recover(cx, actual, w2).await;
    repair_standbys(cx, actual, "g7", marks).await;
    let primaries = cx.pg.count_primaries().await;
    cx.check(
        "exactly one primary after the lag-gated failover + rejoin",
        primaries == 1,
    );
    check_sentinel(cx, actual, "g7").await;
    actual
}

async fn g8(
    cx: &mut Ctx,
    prim: &'static str,
    marks: &mut HashMap<&'static str, Cursor>,
) -> &'static str {
    cx.say("G8: holder agent death — the one deposal with no fence");
    // Kill the AGENT on the lease holder while its PostgreSQL stays
    // healthy. The lease expires, the majority promotes — but nothing
    // can fence the old primary: its executor is the thing that died.
    // Dual-serving at the PostgreSQL level is REAL here, and the
    // scenario's claims are exactly the quorum-commit contract
    // (docs/quorum-commit.md §3): acked writes survive onto the winner,
    // and the deposed primary can no longer get anything ACKED —
    // topological ack starvation is the write fence when process
    // fencing is impossible.
    cx.wait_until(
        60,
        &format!("{prim}: quorum commit armed before the kill"),
        || async move { sync_commit_state(prim).await == "armed" },
    )
    .await;
    write_sentinel(cx, prim, "g8").await;
    let since = cx.log.cursor();
    // Mask FIRST: the unit carries Restart=on-failure/RestartSec=5s, so
    // a bare SIGKILL is a 5-second blip, not a death. Masked + killed,
    // the agent stays dead until the operator brings it back.
    cx.check(
        &format!("{prim}: agent masked and SIGKILLed (PostgreSQL left running)"),
        exec_ok(
            prim,
            // `--kill-whom=main`: systemd 255 (Ubuntu 24.04) REFUSES a
            // plain `systemctl kill` on a masked unit — "Failed to send
            // signal SIGKILL to auxiliary processes: Invalid argument"
            // — while 257 (Debian 13) allows it. The agent is a single
            // process, so naming the main one is both portable and
            // exactly what this scenario means.
            "systemctl mask --runtime pg_agentd && \
             systemctl kill -s SIGKILL --kill-whom=main pg_agentd",
        )
        .await,
    );
    // Declare the expected dual-serving window to the auditor: the
    // no-concurrent-primaries invariant must see this overlap covered
    // AND verify it eventually closed (the phantom-check fence below).
    cx.expect_dual_serving(prim, since);
    let winner_ev = cx
        .await_event(
            90,
            "majority deposed the dead-agent holder and promoted",
            since,
            move |ev| ev.node != prim && agent_any(ev, "roleexec: promotion complete"),
        )
        .await;
    let w: &'static str = match winner_ev {
        Some(ev) => {
            cx.pass(&format!("winner: {}", ev.node));
            ev.node
        }
        None => {
            cx.fail("no winner despite the holder's agent being dead");
            other_node(prim)
        }
    };
    // The fence-less reality, asserted honestly: the deposed node's
    // PostgreSQL is still up and still believes it is a primary.
    cx.check(
        &format!("{prim} PostgreSQL still running (nothing could fence it)"),
        unit_active(prim, &cluster::pg_unit()).await,
    );
    cx.check(
        &format!("{prim} still believes it is a primary (expected dual-serving)"),
        cx.pg.is_in_recovery(prim).await == Some(false),
    );
    // Both ack sources must LEAVE the deposed primary before starvation
    // holds: the winner's walreceiver died at its promotion; the
    // survivor leaves when its executor re-points it at the winner.
    let survivor = NODES
        .iter()
        .copied()
        .find(|n| *n != prim && *n != w)
        .unwrap();
    cx.await_event(
        60,
        &format!("{survivor} re-pointed at {w} (last ack source leaves the deposed primary)"),
        since,
        |ev| agent(ev, survivor, "now following lease holder"),
    )
    .await;
    // Ack starvation: a write on the deposed primary must hang in the
    // sync-rep wait — connection accepted, commit never acknowledged.
    // Via a throwaway in-container psql (not the harness connection
    // cache: the hung commit would wedge a pipelined cached
    // connection); `timeout` exiting 124 IS the assertion.
    let starved = exec_pg(
        prim,
        "timeout 5 psql -qAt -d postgres -c \
         \"insert into sentinel(label, at) values ('g8-unacked', now())\"; echo rc=$?",
    )
    .await
    .unwrap_or_default();
    cx.check(
        "write on the deposed primary starves (no ack within 5s)",
        starved.contains("rc=124"),
    );
    check_sentinel(cx, w, "g8").await;
    // Operator path: bring the agent back. The phantom-primary check is
    // the mechanism that fences a returned stale primary — it sees the
    // peer's higher timeline and stops PostgreSQL, which is the event
    // that CLOSES the declared dual-serving window.
    let _ = exec(
        prim,
        "systemctl unmask --runtime pg_agentd && systemctl start pg_agentd",
    )
    .await;
    cx.await_event(
        60,
        &format!("{prim}: restarted agent's phantom check fenced the stale primary"),
        since,
        |ev| agent(ev, prim, "phantom-primary check") && ev.line.contains("stopping postgres"),
    )
    .await;
    cx.await_event(
        90,
        &format!("{prim} PostgreSQL fully shut down (dual-serving window closed)"),
        since,
        move |ev| {
            ev.node == prim
                && ev.source == Source::Postgres
                && ev.line.contains("database system is shut down")
        },
    )
    .await;
    cluster_recover(cx, w, prim).await;
    // Between the phantom stop and the recover just above, the deposed
    // node's executor may have tried to follow the new timeline and
    // logged the "forked off" wedge signature — those events predate
    // the reclone that fixed them. Consume them so repair_standbys
    // doesn't read them as a live wedge and reclone a second time.
    marks.insert(prim, cx.log.cursor());
    repair_standbys(cx, w, "g8", marks).await;
    let primaries = cx.pg.count_primaries().await;
    cx.check(
        "exactly one primary after the operator rejoin",
        primaries == 1,
    );
    // The starved write must NOT have survived anywhere: it was never
    // acknowledged, and the reclone discarded the deposed primary's
    // divergent tail. (If this row exists, the "starvation" above was
    // an ack that merely arrived late — a real contract violation.)
    let unacked = cx
        .pg
        .scalar(
            w,
            "select count(*)::text from sentinel where label = 'g8-unacked'",
        )
        .await;
    cx.check(
        "the never-acked write did not survive (starvation was real, tail discarded)",
        unacked.ok().as_deref() == Some("0"),
    );
    cx.wait_until(
        60,
        &format!("{w}: quorum commit re-armed after the rejoin"),
        || async move { sync_commit_state(w).await == "armed" },
    )
    .await;
    w
}

async fn g9(
    cx: &mut Ctx,
    prim: &'static str,
    marks: &mut HashMap<&'static str, Cursor>,
) -> &'static str {
    cx.say("G9: crash-shape primary death — no checkpoint, no goodbye");
    // SIGKILL the whole postgresql cgroup: no shutdown checkpoint, no
    // walsender drain, and none of the log-file events the suite keys
    // on — a killed postmaster writes nothing. systemd's code=killed
    // report in the unit journal is the ONE event the death leaves,
    // and it is what the auditor now accepts as this serving
    // interval's end; the await below keeps that parser honest.
    // Debian's unit ships Restart commented out, so the corpse stays
    // down, and rejoin is the operator path onto a pgdata with no
    // clean-shutdown marker.
    cx.wait_until(
        60,
        &format!("{prim}: quorum commit armed before the crash"),
        || async move { sync_commit_state(prim).await == "armed" },
    )
    .await;
    write_sentinel(cx, prim, "g9").await;
    let since = cx.log.cursor();
    cx.check(
        &format!("{prim}: postmaster SIGKILLed (crash shape, whole cgroup)"),
        exec_ok(
            prim,
            &format!("systemctl kill -s SIGKILL {}", cluster::pg_unit()),
        )
        .await,
    );
    // How systemd RECORDS a killed postmaster is not portable. On
    // Debian 13 the main process death is logged directly ("Main
    // process exited, code=killed, status=9/KILL"). On Ubuntu 24.04 the
    // SIGKILL leaves pg_ctlcluster to run, which reports "Cluster is
    // not running" and exits 2, so the unit records "Control process
    // exited, code=exited" and "Failed with result 'exit-code'" —
    // `code=killed` never appears. Both distros always emit a
    // `Failed with result` line, so the portable claim is "systemd
    // recorded the unit dying badly", not the exact cause string.
    cx.await_event(
        30,
        &format!("{prim}: systemd recorded the crash (the only death event)"),
        since,
        move |ev| {
            ev.node == prim
                && ev.source == Source::Postgres
                && (ev.line.contains("code=killed") || ev.line.contains("Failed with result"))
        },
    )
    .await;
    let winner_ev = cx
        .await_event(
            60,
            "the lease promoted past the crashed primary",
            since,
            move |ev| ev.node != prim && agent_any(ev, "roleexec: promotion complete"),
        )
        .await;
    let w: &'static str = match winner_ev {
        Some(ev) => {
            cx.pass(&format!("winner: {}", ev.node));
            ev.node
        }
        None => {
            cx.fail("no winner after the crash");
            other_node(prim)
        }
    };
    cx.check(
        &format!("{prim} stayed dead (no auto-restart of a crashed postmaster)"),
        !unit_active(prim, &cluster::pg_unit()).await,
    );
    check_sentinel(cx, w, "g9").await;
    cluster_recover(cx, w, prim).await;
    repair_standbys(cx, w, "g9", marks).await;
    // The crashed node's pgdata carried no clean-shutdown marker; the
    // reclone must have brought it back as a STANDBY — it never logs a
    // writable serving start again in this window.
    cx.check_absent(
        &format!("{prim} never came back writable (rejoin is a reclone into standby)"),
        since,
        |ev| {
            ev.node == prim
                && ev.source == Source::Postgres
                && ev.line.contains("is ready to accept connections")
        },
    );
    let primaries = cx.pg.count_primaries().await;
    cx.check("exactly one primary after the crash rejoin", primaries == 1);
    cx.wait_until(
        60,
        &format!("{w}: quorum commit re-armed after the crash failover"),
        || async move { sync_commit_state(w).await == "armed" },
    )
    .await;
    w
}

async fn g10(cx: &mut Ctx, prim: &'static str) {
    cx.say("G10: full-cluster cold restart — the site power blip");
    // SIGKILL PID 1 in all three containers at once, then power back
    // on. Nothing shut down cleanly, PostgreSQL is disabled in systemd
    // (agent-managed), and before finding 21's cold-start
    // reconciliation NOTHING would ever start it again: followers
    // honor the demote policy on a Down instance and a holder with
    // PostgreSQL down can only fence — an established cluster stayed
    // down forever, lease intact, waiting for an operator. The
    // contract now: the holder reads its persisted lease (no rival
    // could take over while the site was dark — takeovers need the
    // very quorum that was down) and starts its primary through crash
    // recovery; standby-shaped nodes just start; nobody promotes,
    // nobody fences, the term survives the blip.
    cx.wait_until(
        60,
        &format!("{prim}: quorum commit armed before the blip"),
        || async move { sync_commit_state(prim).await == "armed" },
    )
    .await;
    write_sentinel(cx, prim, "g10").await;
    let since = cx.log.cursor();
    cluster::power_blip().await;
    for n in NODES {
        cx.wait_until(
            90,
            &format!("{n}: pg_agentd active after the blip"),
            || async move { unit_active(n, "pg_agentd").await },
        )
        .await;
    }
    // Boot-window lines race the tail respawn: the exec-based tails
    // die with the container and reattach seconds into the new boot,
    // while these lines are written 2-10 s in — G10 is the only
    // scenario that restarts containers, so it asserts the MECHANISM
    // against the journals/logs directly (the state checks below pin
    // the outcome). Each needle is unique to the post-blip boot:
    // "this node holds"/"standby-shaped" never occur on the greenfield
    // or agent-restart paths, and this is prim's first hard kill.
    cx.wait_until(
        60,
        &format!("{prim}: cold start read the persisted lease and started as primary"),
        || async move {
            exec(
                prim,
                "journalctl -u pg_agentd --no-pager | \
                 grep -q 'cold start: this node holds the persisted lease'",
            )
            .await
            .is_ok()
        },
    )
    .await;
    cx.wait_until(
        60,
        &format!("{prim}: crash recovery ran (hard kill left no clean shutdown)"),
        || async move {
            exec(
                prim,
                &format!(
                    "grep -q 'database system was not properly shut down' {}",
                    cluster::pg_log()
                ),
            )
            .await
            .is_ok()
        },
    )
    .await;
    for n in NODES {
        if n != prim {
            cx.wait_until(
                60,
                &format!("{n}: cold start brought the standby back"),
                || async move {
                    exec(
                        n,
                        "journalctl -u pg_agentd --no-pager | \
                         grep -q 'cold start: standby-shaped pgdata'",
                    )
                    .await
                    .is_ok()
                },
            )
            .await;
        }
    }
    cx.await_event(
        60,
        &format!("{prim} retains the lease across the blip"),
        since,
        |ev| agent(ev, prim, "RetainedLease"),
    )
    .await;
    let pg = cx.pg.clone();
    cx.wait_until(
        120,
        &format!("{prim} has 2 streaming standbys after the blip"),
        || {
            let pg = pg.clone();
            async move { pg.streaming_count(prim).await == Some(2) }
        },
    )
    .await;
    // The blip is not a failover: same holder, same term, no fence.
    cx.check_absent("no takeover across the blip", since, |ev| {
        agent_any(ev, "TookOver")
    });
    cx.check_absent("no promotion across the blip", since, |ev| {
        agent_any(ev, "roleexec: promotion complete")
    });
    cx.check_absent("no fence across the blip", since, |ev| {
        agent_any(ev, "FENCING")
    });
    let primaries = cx.pg.count_primaries().await;
    cx.check("exactly one primary after the blip", primaries == 1);
    check_sentinel(cx, prim, "g10").await;
    cx.wait_until(
        60,
        &format!("{prim}: quorum commit re-armed after the blip"),
        || async move { sync_commit_state(prim).await == "armed" },
    )
    .await;
    // Full routing recovery: the agents' pgpool supervisors bring the
    // routers back and the self-attach/attach convergence restores all
    // three backends on every instance.
    for n in NODES {
        cx.wait_until(
            120,
            &format!("{n}: pgpool back with 3 backends up after the blip"),
            || async move { pcp_backends_up(n).await == 3 },
        )
        .await;
    }
}

async fn g11(
    cx: &mut Ctx,
    prim: &'static str,
    marks: &mut HashMap<&'static str, Cursor>,
) -> &'static str {
    cx.say("G11: fence-less deposal under WRITE LOAD — every acked row must survive");
    // G8 proved one probe starves; a continuous ledger (crate::load)
    // upgrades the claim to the actual quorum-commit invariant: EVERY
    // acknowledged row survives the deposal, and the boundary between
    // "acked before the ack sources left" and "hung after" is walked
    // by real concurrent traffic instead of a single at-rest sentinel.
    // The writer's naive discovery also means it genuinely writes to
    // the deposed primary during dual-serving — those commits hanging
    // (timeouts) is the observation the gap list asked for.
    cx.wait_until(
        60,
        &format!("{prim}: quorum commit armed before the load"),
        || async move { sync_commit_state(prim).await == "armed" },
    )
    .await;
    let _ = cx
        .pg
        .execute(
            prim,
            "create table if not exists ledger(seq bigint primary key, at timestamptz default now())",
        )
        .await;
    let load = crate::load::Load::start();
    {
        let l = &load;
        cx.wait_until(60, "load: ≥200 writes acked before the kill", || async {
            l.acked_count() >= 200
        })
        .await;
    }
    let acked_at_kill = load.acked_count();
    let since = cx.log.cursor();
    cx.check(
        &format!("{prim}: agent masked and SIGKILLed under load"),
        exec_ok(
            prim,
            // `--kill-whom=main`: systemd 255 (Ubuntu 24.04) REFUSES a
            // plain `systemctl kill` on a masked unit — "Failed to send
            // signal SIGKILL to auxiliary processes: Invalid argument"
            // — while 257 (Debian 13) allows it. The agent is a single
            // process, so naming the main one is both portable and
            // exactly what this scenario means.
            "systemctl mask --runtime pg_agentd && \
             systemctl kill -s SIGKILL --kill-whom=main pg_agentd",
        )
        .await,
    );
    cx.expect_dual_serving(prim, since);
    let winner_ev = cx
        .await_event(
            90,
            "majority deposed the loaded holder and promoted",
            since,
            move |ev| ev.node != prim && agent_any(ev, "roleexec: promotion complete"),
        )
        .await;
    let w: &'static str = match winner_ev {
        Some(ev) => {
            cx.pass(&format!("winner: {}", ev.node));
            ev.node
        }
        None => {
            cx.fail("no winner under load");
            other_node(prim)
        }
    };
    let survivor = NODES
        .iter()
        .copied()
        .find(|n| *n != prim && *n != w)
        .unwrap();
    cx.await_event(
        60,
        &format!("{survivor} re-pointed at {w} (ack sources leave the deposed primary)"),
        since,
        |ev| agent(ev, survivor, "now following lease holder"),
    )
    .await;
    // Threshold from NOW — after the promotion — not from the kill:
    // between the kill and the candidates' detach (leader_ttl) acks
    // legitimately keep flowing through the fence-less primary, and a
    // kill-anchored target was met entirely by that window on the
    // first run, stopping the writer before it ever wrote to the
    // winner. Requiring acks past this point proves the post-failover
    // write path: the writer must starve on the deposed primary,
    // discover the winner, and resume.
    let acked_at_promotion = load.acked_count();
    cx.note(&format!(
        "load: {acked_at_kill} acked at the kill, {acked_at_promotion} by the promotion \
         (the delta rode the pre-detach ack window)"
    ));
    {
        let l = &load;
        cx.wait_until(
            120,
            &format!("load: writes RESUMED on {w} (+100 acked past its promotion)"),
            || async { l.acked_count() >= acked_at_promotion + 100 },
        )
        .await;
    }
    let stats = load.stop().await;
    cx.note(&format!(
        "load: {} acked, {} landed-unacked (indeterminate), {} hung writes (timeouts), \
         {} connection errors, longest ack gap {} ms",
        stats.acked.len(),
        stats.landed,
        stats.timeouts,
        stats.conn_errors,
        stats.max_ack_gap_ms
    ));
    cx.check(
        "load observed hanging commits during the deposal (ack starvation under load)",
        stats.timeouts >= 1,
    );
    // Operator path back to redundancy (G8's shape).
    let _ = exec(
        prim,
        "systemctl unmask --runtime pg_agentd && systemctl start pg_agentd",
    )
    .await;
    cx.await_event(
        60,
        &format!("{prim}: phantom check fenced the stale loaded primary"),
        since,
        |ev| agent(ev, prim, "phantom-primary check") && ev.line.contains("stopping postgres"),
    )
    .await;
    cx.await_event(
        90,
        &format!("{prim} PostgreSQL fully shut down (dual-serving window closed)"),
        since,
        move |ev| {
            ev.node == prim
                && ev.source == Source::Postgres
                && ev.line.contains("database system is shut down")
        },
    )
    .await;
    cluster_recover(cx, w, prim).await;
    marks.insert(prim, cx.log.cursor());
    repair_standbys(cx, w, "g11", marks).await;
    // THE audit: every acknowledged seq exists on the winner. This is
    // the line finding 17/18 priced and quorum commit exists to hold.
    let rows = cx
        .pg
        .rows_i64(w, "select seq from ledger")
        .await
        .unwrap_or_default();
    let present: std::collections::HashSet<i64> = rows.into_iter().collect();
    let missing: Vec<i64> = stats
        .acked
        .iter()
        .copied()
        .filter(|s| !present.contains(s))
        .collect();
    cx.check(
        &format!(
            "every acknowledged write survived onto {w} ({} acked{})",
            stats.acked.len(),
            if missing.is_empty() {
                String::new()
            } else {
                format!(
                    " — {} MISSING, first: {:?}",
                    missing.len(),
                    &missing[..missing.len().min(8)]
                )
            }
        ),
        missing.is_empty(),
    );
    let primaries = cx.pg.count_primaries().await;
    cx.check(
        "exactly one primary after the loaded rejoin",
        primaries == 1,
    );
    cx.wait_until(
        60,
        &format!("{w}: quorum commit re-armed after the loaded deposal"),
        || async move { sync_commit_state(w).await == "armed" },
    )
    .await;
    w
}

/// Healthz-reported follow-wedge tripwire on `node`.
async fn follow_wedged(node: &str) -> bool {
    healthz(node).await["follow_wedged"]
        .as_bool()
        .unwrap_or(false)
}

/// A write attempt with a hard timeout, run INSIDE the container so a
/// commit hung in the sync-rep wait cannot wedge the harness's pooled
/// connections (G8's lesson). Returns true iff it committed; a
/// `timeout(1)` kill (rc 124) is the hang.
async fn timed_write(node: &str, label: &str, budget_s: u32) -> bool {
    exec_pg(
        node,
        &format!(
            "timeout {budget_s} psql -qAt -d postgres -c \
             \"insert into sentinel(label, at) values ('{label}', now()) \
              on conflict (label) do update set at = now()\"; echo rc=$?"
        ),
    )
    .await
    .map(|out| out.contains("rc=0"))
    .unwrap_or(false)
}

async fn g12(
    cx: &mut Ctx,
    prim: &'static str,
    marks: &mut HashMap<&'static str, Cursor>,
) -> &'static str {
    cx.say("G12: quorum-commit states — blocked, the allow-async hatch, auto re-arm");
    // The three states the design builds and the suite never entered.
    // Stop PostgreSQL on BOTH standbys while their agents stay up:
    // raft keeps quorum (so the lease holds and this stays a
    // quorum-commit test, not a fencing test), the primary keeps
    // serving reads, and `ANY 1` has no ack source left — commits
    // hang. That is `blocked`, and it is the state the design says is
    // a page: the cluster is UP and refusing to acknowledge writes,
    // on purpose, rather than acknowledging single-copy ones.
    cx.wait_until(
        60,
        &format!("{prim}: quorum commit armed before the blackout"),
        || async move { sync_commit_state(prim).await == "armed" },
    )
    .await;
    write_sentinel(cx, prim, "g12-pre").await;
    let since = cx.log.cursor();
    for n in NODES {
        if n != prim {
            let _ = exec(n, &format!("systemctl stop {}", cluster::pg_unit())).await;
        }
    }
    cx.wait_until(
        60,
        &format!("{prim}: /healthz reports sync_commit=blocked (no ack source)"),
        || async move { sync_commit_state(prim).await == "blocked" },
    )
    .await;
    cx.check(
        "a write BLOCKS while quorum commit has no ack source (not silently single-copy)",
        !timed_write(prim, "g12-blocked", 5).await,
    );
    cx.check(
        &format!("{prim} still serves reads while blocked (the cluster is up, not down)"),
        cx.pg.is_in_recovery(prim).await == Some(false),
    );
    // The escape hatch: an operator who accepts single-copy writes
    // trades the guarantee for availability, on the record.
    let out = exec_pg(prim, "pg_agentctl cluster allow-async --confirm")
        .await
        .unwrap_or_else(|e| e.to_string());
    cx.check(
        "allow-async disarmed quorum commit (operator escape hatch)",
        out.contains("disarmed"),
    );
    cx.await_event(
        30,
        "the disarm is journaled and shouted (QUORUM COMMIT DISARMED)",
        since,
        |ev| agent(ev, prim, "QUORUM COMMIT DISARMED"),
    )
    .await;
    cx.wait_until(
        30,
        &format!("{prim}: /healthz reports sync_commit=disarmed"),
        || async move { sync_commit_state(prim).await == "disarmed" },
    )
    .await;
    cx.check(
        "writes flow again after the hatch (single-copy, as advertised)",
        timed_write(prim, "g12-async", 10).await,
    );
    let ops = exec_pg(prim, "pg_agentctl ops list")
        .await
        .unwrap_or_default();
    cx.check(
        "the disarm shows in the op journal (incident review)",
        ops.to_lowercase().contains("allowasync") || ops.to_lowercase().contains("allow_async"),
    );
    // Operator brings the standbys back; the executor re-arms at the
    // first standby attach — no second command, no lingering hatch.
    repair_standbys(cx, prim, "g12", marks).await;
    cx.wait_until(
        90,
        &format!("{prim}: quorum commit AUTO re-armed at the first standby attach"),
        || async move { sync_commit_state(prim).await == "armed" },
    )
    .await;
    check_sentinel(cx, prim, "g12-pre").await;
    // The single-copy write taken during the hatch is now replicated:
    // the hatch's window closed without losing what it let through.
    let standby = other_node(prim);
    let pg = cx.pg.clone();
    cx.wait_until(
        60,
        &format!("the hatch-era write reached {standby} once redundancy returned"),
        || {
            let pg = pg.clone();
            async move {
                pg.scalar(
                    standby,
                    "select count(*)::text from sentinel where label = 'g12-async'",
                )
                .await
                .is_ok_and(|v| v == "1")
            }
        },
    )
    .await;
    prim
}

async fn g13(cx: &mut Ctx, prim: &'static str, marks: &mut HashMap<&'static str, Cursor>) {
    cx.say("G13: the follow-wedge tripwire fires (finding 15's defense in depth)");
    // Strict flush-max candidacy made the finding-15 wedge structurally
    // unreachable in the designed flows — which left its DETECTION
    // untested. Provoke the state directly: sever one standby's
    // replication path only (PG port, G7's technique — its agent stays
    // reachable, so its executor keeps deciding Following and keeps
    // confirming the follow), then kill the walreceiver. The executor
    // sees confirmed-onto-the-holder-but-not-streaming past the grace
    // (= leader_ttl) and must say so LOUDLY rather than sitting on a
    // silently degraded standby.
    let target = other_node(prim);
    let prim_ip = cluster::container_ip(prim).await.unwrap_or_default();
    let since = cx.log.cursor();
    cluster::sever_peer_port(target, &prim_ip, 5432).await;
    let _ = cx
        .pg
        .execute(
            target,
            "select pg_terminate_backend(pid) from pg_stat_wal_receiver",
        )
        .await;
    cx.await_event(
        90,
        &format!("{target}: executor declares the follow WEDGED past the grace"),
        since,
        |ev| agent(ev, target, "follow WEDGED"),
    )
    .await;
    cx.wait_until(
        30,
        &format!("{target}: /healthz exposes follow_wedged=true (operator-visible)"),
        || async move { follow_wedged(target).await },
    )
    .await;
    // The tripwire is a REPORT, not an action: a degraded standby must
    // never be destructively rebuilt by the executor, and must never
    // depose the healthy primary it cannot reach on 5432.
    cx.check_absent(
        "the wedged standby neither promoted nor was auto-rebuilt",
        since,
        |ev| {
            agent(ev, target, "roleexec: promotion complete")
                || agent(ev, target, "rebuild_as_standby")
        },
    );
    cx.check(
        &format!("{prim} kept the lease throughout (a wedged follower is not a failover)"),
        cx.pg.is_in_recovery(prim).await == Some(false),
    );
    // Heal the path: the executor's re-follow finds the stream again
    // and the tripwire clears itself — no operator action for a
    // transient cause.
    cluster::heal_firewall(target).await;
    cx.wait_until(
        120,
        &format!("{target}: tripwire self-cleared once streaming resumed"),
        || async move { !follow_wedged(target).await },
    )
    .await;
    repair_standbys(cx, prim, "g13", marks).await;
    let primaries = cx.pg.count_primaries().await;
    cx.check(
        "exactly one primary after the wedge cleared",
        primaries == 1,
    );
}

/// One daemon's own view of whether it can reach every peer. `|| true`
/// because the CLI exits non-zero precisely when the answer is "no",
/// which is the interesting case.
async fn all_reachable(node: &str) -> Option<bool> {
    let out = exec_pg(node, "pg_agentctl cluster status --json || true")
        .await
        .ok()?;
    serde_json::from_str::<serde_json::Value>(&out).ok()?["all_reachable"].as_bool()
}

async fn g14(cx: &mut Ctx, prim: &'static str, marks: &mut HashMap<&'static str, Cursor>) {
    cx.say("G14: DATA-plane partition — replication severed, control plane intact");
    // Cut 5432 between the primary and BOTH standbys while leaving the
    // agent mesh untouched. Every agent still sees a healthy holder, so
    // the correct answer is emphatically NOT to fail over — the lease
    // is doing its job. What must happen instead is that the silent
    // failure becomes loud: quorum commit has no ack source left, so
    // writes stop being acknowledged rather than quietly becoming
    // single-copy, and both followers raise the wedge tripwire. This is
    // the shape where a health-check-driven design (pgpool's §2.1)
    // fails over into a split brain and the lease design refuses to.
    let pg = cx.pg.clone();
    cx.wait_until(
        60,
        &format!("{prim}: armed with both standbys streaming before the cut"),
        || {
            let pg = pg.clone();
            async move { pg.streaming_count(prim).await == Some(2) }
        },
    )
    .await;
    let prim_ip = cluster::container_ip(prim).await.unwrap_or_default();
    let since = cx.log.cursor();
    for n in NODES {
        if n != prim {
            cluster::sever_peer_port(n, &prim_ip, 5432).await;
            let _ = cx
                .pg
                .execute(
                    n,
                    "select pg_terminate_backend(pid) from pg_stat_wal_receiver",
                )
                .await;
        }
    }
    cx.wait_until(
        90,
        &format!("{prim}: sync_commit=blocked (both ack sources gone)"),
        || async move { sync_commit_state(prim).await == "blocked" },
    )
    .await;
    cx.check(
        "writes stop being acknowledged (redundancy loss is loud, not silent)",
        !timed_write(prim, "g14-blocked", 5).await,
    );
    for n in NODES {
        if n != prim {
            cx.wait_until(
                120,
                &format!("{n}: follow_wedged=true (its stream is gone)"),
                || async move { follow_wedged(n).await },
            )
            .await;
        }
    }
    // The whole point: a broken DATA plane must not move the lease.
    cx.check_absent(
        "no takeover while only replication was broken",
        since,
        |ev| agent_any(ev, "TookOver"),
    );
    cx.check_absent(
        "no promotion while only replication was broken",
        since,
        |ev| agent_any(ev, "roleexec: promotion complete"),
    );
    cx.check_absent("no fence while only replication was broken", since, |ev| {
        agent_any(ev, "FENCING")
    });
    cx.check(
        &format!("{prim} kept the lease and its role throughout"),
        cx.pg.is_in_recovery(prim).await == Some(false),
    );
    for n in NODES {
        if n != prim {
            cluster::heal_firewall(n).await;
        }
    }
    cx.wait_until(
        180,
        &format!("{prim}: quorum commit back to armed once streams returned"),
        || async move { sync_commit_state(prim).await == "armed" },
    )
    .await;
    for n in NODES {
        if n != prim {
            cx.wait_until(
                120,
                &format!("{n}: wedge tripwire cleared after the heal"),
                || async move { !follow_wedged(n).await },
            )
            .await;
        }
    }
    repair_standbys(cx, prim, "g14", marks).await;
}

async fn g15(
    cx: &mut Ctx,
    prim: &'static str,
    marks: &mut HashMap<&'static str, Cursor>,
) -> &'static str {
    cx.say("G15: CONTROL-plane partition — agent mesh cut, replication healthy");
    // The exact inverse of G14: sever 9701 (peer RPC *and* raft) on the
    // holder while 5432 keeps streaming perfectly. The holder loses
    // quorum and must fence a PostgreSQL that is, by every data-plane
    // measure, in perfect health — the fail-closed bill the design
    // prices explicitly (a holder that cannot prove it still holds the
    // lease must not keep serving writes). The majority, which can
    // still see each other, promotes.
    let since = cx.log.cursor();
    cluster::sever_port_everywhere(prim, 9701).await;
    cx.await_event(
        60,
        &format!("{prim}: fenced itself on quorum loss (control plane, not data)"),
        since,
        |ev| agent(ev, prim, "FENCING"),
    )
    .await;
    let winner_ev = cx
        .await_event(90, "the majority promoted a standby", since, move |ev| {
            ev.node != prim && agent_any(ev, "roleexec: promotion complete")
        })
        .await;
    let w: &'static str = match winner_ev {
        Some(ev) => {
            cx.pass(&format!("winner: {}", ev.node));
            ev.node
        }
        None => {
            cx.fail("no winner after the control-plane partition");
            other_node(prim)
        }
    };
    cx.check_absent(
        "the isolated holder never committed anything (no quorum, no writes)",
        since,
        |ev| agent(ev, prim, "TookOver"),
    );
    cluster::heal_firewall(prim).await;
    cx.check(
        &format!("fenced ex-holder {prim} stays down after the heal (demote policy)"),
        !unit_active(prim, &cluster::pg_unit()).await,
    );
    cluster_recover(cx, w, prim).await;
    marks.insert(prim, cx.log.cursor());
    repair_standbys(cx, w, "g15", marks).await;
    let primaries = cx.pg.count_primaries().await;
    cx.check(
        "exactly one primary after the control-plane partition healed",
        primaries == 1,
    );
    cx.wait_until(90, &format!("{w}: quorum commit re-armed"), || async move {
        sync_commit_state(w).await == "armed"
    })
    .await;
    w
}

async fn g16(cx: &mut Ctx, prim: &'static str) {
    cx.say("G16: ASYMMETRIC visibility — a node that can ask nothing must do nothing");
    // Finding 18's lesson in its strongest form: one node loses the
    // ability to INITIATE control-plane connections while remaining
    // fully answerable. It therefore sees a cluster in which everyone
    // is dead — including the lease holder — while everyone else sees
    // a completely healthy cluster including it. One-sided evidence is
    // not authority: the blind node must not depose anyone, and since
    // it cannot reach any quorum member it cannot, which is the point.
    //
    // The blind node is a STANDBY on purpose. The first cut of this
    // scenario blinded the HOLDER to one peer and asserted nothing
    // would move — wrong, and the run said so: if the unreachable peer
    // happens to be the raft leader, the holder cannot verify it still
    // holds the lease, and fencing is then the only correct answer
    // (finding 25). Whose-leader-is-it makes that shape
    // nondeterministic; severing a standby's outbound control plane is
    // deterministic, because no reachable quorum member means no CAS
    // regardless of which node leads raft.
    let blind = other_node(prim);
    let since = cx.log.cursor();
    cluster::sever_outbound_port(blind, 9701).await;
    // Evidence the asymmetry is live, from each side's own fan-out.
    cx.wait_until(
        90,
        &format!("{blind} can reach nobody (its own status fan-out says so)"),
        || async move { all_reachable(blind).await == Some(false) },
    )
    .await;
    cx.check(
        &format!("{prim} still reaches everyone including {blind} (the cut is one-way)"),
        all_reachable(prim).await == Some(true),
    );
    // The blind node believes the holder is dead. Hold that belief
    // well past leader_ttl — the window is the whole point.
    cx.await_event(
        60,
        &format!("{blind} concluded the store is unreadable (it sees a dead cluster)"),
        since,
        |ev| agent(ev, blind, "StoreUnknown"),
    )
    .await;
    // 1.5x leader_ttl of held belief. The absence claims below cover
    // the whole window by event order; this sleep is only the liveness
    // bound that gives the bug time to manifest.
    tokio::time::sleep(Duration::from_secs(15)).await;
    cx.check_absent("no takeover from one-sided blindness", since, |ev| {
        agent_any(ev, "TookOver")
    });
    cx.check_absent("no fence from one-sided blindness", since, |ev| {
        agent_any(ev, "FENCING")
    });
    cx.check_absent("no promotion from one-sided blindness", since, |ev| {
        agent_any(ev, "roleexec: promotion complete")
    });
    cx.check(
        &format!("{prim} still holds the lease and serves as primary"),
        cx.pg.is_in_recovery(prim).await == Some(false),
    );
    // A blinded CONTROL plane must not disturb the data plane: the
    // node keeps streaming throughout, so its redundancy value is
    // untouched by its inability to participate in decisions.
    let streaming = cx.pg.streaming_count(prim).await;
    cx.check(
        "replication was never disturbed (only the control plane was cut)",
        streaming == Some(2),
    );
    cluster::heal_firewall(blind).await;
    cx.wait_until(
        90,
        &format!("{blind}: full peer visibility restored"),
        || async move { all_reachable(blind).await == Some(true) },
    )
    .await;
    cx.check_absent(
        "the heal itself caused no churn (no takeover on reconnect)",
        since,
        |ev| agent_any(ev, "TookOver"),
    );
}

async fn g17(cx: &mut Ctx, prim: &'static str) {
    cx.say("G17: a blind standby must not depose a healthy holder (the second-opinion gate)");
    // Finding 25's open half, closed: sever ONE standby's control-plane
    // path to the holder alone, leaving that standby's link to the
    // third node — and the whole data plane — intact. The blind standby
    // now watches the holder "die" for a full leader_ttl while the
    // holder serves happily and the third node sees everyone.
    //
    // Before the gate, whether this deposed a healthy primary came down
    // to which node happened to lead raft: if the blind standby could
    // still reach the raft leader, its CAS succeeded and a healthy
    // primary lost its lease to one node's blindness. Now the candidate
    // asks the other members first — the third node has touched the
    // holder within the ttl, so the blindness is diagnosed as local.
    // The assertion is deterministic either way, which is the point:
    // no takeover, no promotion, no fence.
    let blind = other_node(prim);
    let prim_ip = cluster::container_ip(prim).await.unwrap_or_default();
    let since = cx.log.cursor();
    cluster::sever_peer_port(blind, &prim_ip, 9701).await;
    // Verify the CUT, not the cluster's reaction to it. Waiting for a
    // decision event here is a trap: the decision log dedups by
    // variant, and G16 leaves this same node in StoreUnknown — so when
    // the cut lands before its next tick, the node is already in the
    // state the await is watching for and no new line is ever emitted.
    // The manufactured condition is directly observable, so observe it.
    let ip = prim_ip.clone();
    cx.wait_until(
        30,
        &format!("{blind} cannot reach the holder's agent port (cut verified)"),
        || {
            let ip = ip.clone();
            async move { !cluster::can_reach(blind, &ip, 9701).await }
        },
    )
    .await;
    // Past leader_ttl (1.5x): the window in which the unguarded code
    // would have taken the lease.
    tokio::time::sleep(Duration::from_secs(15)).await;
    cx.check_absent(
        "the healthy holder was not deposed by a blind standby",
        since,
        |ev| agent_any(ev, "TookOver"),
    );
    cx.check_absent("no promotion from one node's blindness", since, |ev| {
        agent_any(ev, "roleexec: promotion complete")
    });
    cx.check_absent("no fence from one node's blindness", since, |ev| {
        agent_any(ev, "FENCING")
    });
    cx.check(
        &format!("{prim} still serves as primary throughout"),
        cx.pg.is_in_recovery(prim).await == Some(false),
    );
    // The gate's own voice, when the candidate got far enough to ask.
    // Whether it does depends on which node leads raft (a candidate
    // that cannot reach the raft leader never reaches the gate at all
    // — finding 25), so this corroborates rather than enforces.
    if cx
        .log
        .find(since, |ev| agent(ev, blind, "blindness is local"))
        .is_some()
    {
        cx.pass(&format!(
            "{blind} named its own blindness and deferred (the gate fired)"
        ));
    } else {
        cx.note(&format!(
            "{blind} never reached the gate (it could not reach the raft leader either)"
        ));
    }
    cluster::heal_firewall(blind).await;
    let pg = cx.pg.clone();
    cx.wait_until(
        90,
        &format!("{prim}: replication intact after the heal"),
        || {
            let pg = pg.clone();
            async move { pg.streaming_count(prim).await == Some(2) }
        },
    )
    .await;
}

/// Whether `node`'s own daemon answers `cluster status` AND got an
/// answer out of consensus.
///
/// [`pause_status`] cannot say this on its own: it renders the empty
/// string both for "not paused" and for "the CLI never got a document
/// back at all", so waiting for it to go empty is satisfied by a
/// daemon that is simply down. The daemon serving the fan-out is the
/// local one, so a parseable document proves liveness, and a
/// `pause_status` that does not start with "unknown" proves the
/// linearizable read behind it completed.
async fn consensus_readable(node: &str) -> bool {
    let out = exec_pg(node, "pg_agentctl cluster status --json")
        .await
        .unwrap_or_default();
    let Ok(v) = serde_json::from_str::<serde_json::Value>(&out) else {
        return false;
    };
    let answered = v["pause_status"]
        .as_str()
        .map(|s| !s.starts_with("unknown"))
        .unwrap_or(false);
    answered
        && v["nodes"]
            .as_array()
            .map(|n| !n.is_empty())
            .unwrap_or(false)
}

/// One of systemd's own monotonic timestamps for `pg_agentd`, in
/// microseconds since boot. Asking systemd rather than polling
/// `is-active` is what makes a sub-second restart measurable at all.
async fn agent_stamp_us(node: &str, property: &str) -> Option<u64> {
    exec(
        node,
        &format!("systemctl show pg_agentd -p {property} --value"),
    )
    .await
    .ok()?
    .trim()
    .parse()
    .ok()
}

/// How long `pg_agentd` was down over the restart caused since
/// `after_us`, in milliseconds: the span between systemd recording the
/// old process gone and the new one answering `sd_notify READY`.
/// `Type=notify` makes that second edge the moment the agent is
/// functional again, which is the edge the lease cares about.
///
/// `None` unless BOTH edges belong to that restart — the unit went
/// inactive after the caller's mark, and came back after that. Without
/// the freshness test a node whose agent was restarted by an earlier
/// scenario reports THAT window instead: a plausible-looking number
/// measuring the wrong event, which is worse than no number at all.
async fn agent_restart_gap_ms(node: &str, after_us: Option<u64>) -> Option<u64> {
    let active = agent_stamp_us(node, "ActiveEnterTimestampMonotonic").await?;
    let inactive = agent_stamp_us(node, "InactiveEnterTimestampMonotonic").await?;
    let floor = after_us.unwrap_or(0);
    (inactive > floor && active > inactive).then_some((active - inactive) / 1000)
}

/// This cluster's configured `leader_ttl`, read from the config the
/// nodes actually booted with rather than assumed. The acceptance
/// cluster runs a deliberately tight 10s against production's 30s
/// default, so a margin proven here is a margin with room to spare.
async fn leader_ttl_secs(node: &str) -> u64 {
    exec(
        node,
        "awk -F= '/leader_ttl_secs/ {gsub(/[^0-9]/, \"\", $2); print $2}' \
         /etc/pg_agent/config.toml",
    )
    .await
    .ok()
    .and_then(|v| v.trim().parse().ok())
    .unwrap_or(10)
}

/// The package this cell staged for the image build, and where the
/// scenario drops it inside a container. Same artifact the nodes were
/// installed from — an upgrade to the identical version, because what
/// is under test is the scriptlet path, not a version bump.
///
/// `/var/tmp`, NOT `/tmp`: compose mounts a tmpfs over `/tmp` in every
/// node, and `docker cp` writes into the image layer UNDERNEATH that
/// mount. The copy reports success, the file is invisible to everything
/// running in the container, and the install fails with "cannot access
/// archive".
fn staged_package() -> (&'static str, &'static str) {
    match cluster::facts().family.as_str() {
        "rhel" => (
            "testing/docker/pg-agent.rpm",
            "/var/tmp/pg-agent-upgrade.rpm",
        ),
        _ => (
            "testing/docker/pg-agent.deb",
            "/var/tmp/pg-agent-upgrade.deb",
        ),
    }
}

/// Install the staged package over the running one, the way an
/// operator's package manager does. `--replacepkgs` / plain `dpkg -i`
/// because the version is identical; `postinstall.sh` does not branch
/// on install-vs-upgrade anyway — its restart-if-active check is the
/// whole mechanism under test.
fn package_upgrade_cmd(dest: &str) -> String {
    match cluster::facts().family.as_str() {
        "rhel" => format!("rpm -Uvh --replacepkgs {dest}"),
        _ => format!("dpkg -i {dest}"),
    }
}

/// The `pause_status` line from a node's own `cluster status`.
async fn pause_status(node: &str) -> String {
    let out = exec_pg(node, "pg_agentctl cluster status --json || true")
        .await
        .unwrap_or_default();
    serde_json::from_str::<serde_json::Value>(&out)
        .ok()
        .and_then(|v| v["pause_status"].as_str().map(str::to_string))
        .unwrap_or_default()
}

async fn g18(cx: &mut Ctx, prim: &'static str, marks: &mut HashMap<&'static str, Cursor>) {
    cx.say("G18: maintenance mode — pause suspends failover, resume restores it");
    // The pause flag has existed in the consensus state machine since
    // the lease landed, and the loop has always honored it — but until
    // `cluster pause` shipped, nothing could set it, so HaDecision
    // ::Paused had never once executed. This scenario is the proof that
    // the feature is real: pause on one node, observe it on ANOTHER
    // (it is replicated, not local), kill the primary, and watch the
    // cluster deliberately NOT fail over — which is the hazard the
    // command's own help text warns about, asserted rather than
    // described. Then resume, and the failover it was holding back
    // happens.
    let observer = other_node(prim);
    let since = cx.log.cursor();
    let out = exec_pg(
        prim,
        "pg_agentctl cluster pause --reason 'acceptance G18 maintenance window'",
    )
    .await
    .unwrap_or_else(|e| e.to_string());
    cx.check(
        "cluster pause accepted",
        out.contains("Automatic failover is OFF"),
    );
    // Replicated, not local: the node that did NOT issue it must see it.
    cx.wait_until(
        30,
        &format!("{observer} sees the pause (it is cluster state, not a local flag)"),
        || async move { pause_status(observer).await.contains("PAUSED") },
    )
    .await;
    cx.check(
        "the pause records its reason for whoever finds it later",
        pause_status(observer).await.contains("acceptance G18"),
    );
    cx.await_event(
        30,
        "the HA loop reports Paused (a decision that had never run before)",
        since,
        |ev| agent_any(ev, "decision=Paused"),
    )
    .await;
    // The hazard, made real: kill the primary while paused.
    let killed = cx.log.cursor();
    let _ = exec(prim, &format!("systemctl stop {}", cluster::pg_unit())).await;
    tokio::time::sleep(Duration::from_secs(15)).await; // 1.5x leader_ttl
    cx.check_absent(
        "paused: no takeover while the primary is down",
        killed,
        |ev| agent_any(ev, "TookOver"),
    );
    cx.check_absent(
        "paused: no promotion while the primary is down",
        killed,
        |ev| agent_any(ev, "roleexec: promotion complete"),
    );
    cx.check(
        "paused: the cluster really is without a primary (the documented cost)",
        cx.pg.count_primaries().await == 0,
    );
    // Resume: the held-back failover proceeds on its own.
    let resumed = cx.log.cursor();
    let out = exec_pg(observer, "pg_agentctl cluster resume")
        .await
        .unwrap_or_else(|e| e.to_string());
    cx.check("cluster resume accepted", out.contains("resumed"));
    let winner_ev = cx
        .await_event(
            90,
            "the failover pause was holding back completes on resume",
            resumed,
            move |ev| ev.node != prim && agent_any(ev, "roleexec: promotion complete"),
        )
        .await;
    let w: &'static str = match winner_ev {
        Some(ev) => {
            cx.pass(&format!("winner: {}", ev.node));
            ev.node
        }
        None => {
            cx.fail("no winner after resume");
            other_node(prim)
        }
    };
    cx.wait_until(
        30,
        &format!("{w}: pause cleared everywhere"),
        || async move { pause_status(w).await.is_empty() },
    )
    .await;
    cluster_recover(cx, w, prim).await;
    marks.insert(prim, cx.log.cursor());
    repair_standbys(cx, w, "g18", marks).await;
    cx.check(
        "exactly one primary after the maintenance window",
        cx.pg.count_primaries().await == 1,
    );
}

async fn g19(cx: &mut Ctx, prim: &'static str) {
    cx.say("G19: raft-store deletion — the documented recovery, finally run");
    // docs/promotion-authority.md argues the storage engine is a
    // low-stakes choice BECAUSE "a Raft node's log is recoverable from
    // its peers: stop the agent, delete <state_dir>/raft/, restart, let
    // Raft re-replicate". That claim carries real design weight and had
    // never been executed — the same shape as finding 21, where a
    // documented recovery path turned out not to exist.
    //
    // The wiped node is a NON-holder on purpose: it makes the test
    // deterministic, and it is the operationally common case (a node
    // with a corrupt store gets rebuilt while the cluster serves).
    let victim = other_node(prim);
    let since = cx.log.cursor();
    cx.check(
        &format!("{victim}: agent stopped and raft store deleted"),
        exec_ok(
            victim,
            &format!(
                "systemctl stop pg_agentd && rm -rf {raft} && test ! -d {raft}",
                raft = format!("{}/raft", cluster::agent_state_dir())
            ),
        )
        .await,
    );
    let _ = exec(victim, "systemctl start pg_agentd").await;
    cx.wait_until(
        60,
        &format!("{victim}: agent back up on an empty store"),
        || async move { unit_active(victim, "pg_agentd").await },
    )
    .await;
    // Re-replication proof, in the node's own words: it starts from
    // nothing (no vote, no log) and is fed by the leader.
    cx.await_event(
        90,
        &format!("{victim}: raft restarted from an empty log"),
        since,
        |ev| agent(ev, victim, "get_initial_state vote=T0-N0:uncommitted"),
    )
    .await;
    // The real proof is not a log line: it is that the node can serve a
    // LINEARIZABLE read again. `cluster status` reports the pause state
    // from the store, and reports "unknown (consensus read failed…)"
    // when it cannot — so an empty pause line here means this node is
    // reading committed cluster state through Raft once more.
    cx.wait_until(
        120,
        &format!("{victim}: linearizable reads work again (store re-replicated)"),
        || async move { pause_status(victim).await.is_empty() },
    )
    .await;
    cx.await_event(
        60,
        &format!("{victim}: participating in decisions again"),
        since,
        |ev| agent(ev, victim, "ha decision"),
    )
    .await;
    // The cluster must not have noticed. A node rebuilding its own
    // consensus state is not a failover trigger.
    cx.check_absent("no takeover while a peer rebuilt its store", since, |ev| {
        agent_any(ev, "TookOver")
    });
    cx.check_absent("no fence while a peer rebuilt its store", since, |ev| {
        agent_any(ev, "FENCING")
    });
    cx.check(
        &format!("{prim} still holds the lease and serves"),
        cx.pg.is_in_recovery(prim).await == Some(false),
    );
    let pg = cx.pg.clone();
    cx.wait_until(
        60,
        "replication untouched by the store rebuild (2 streaming)",
        || {
            let pg = pg.clone();
            async move { pg.streaming_count(prim).await == Some(2) }
        },
    )
    .await;
}

async fn g20(
    cx: &mut Ctx,
    prim: &'static str,
    marks: &mut HashMap<&'static str, Cursor>,
) -> &'static str {
    cx.say("G20: DOUBLE FAULT — the primary dies mid-rebuild of the other standby");
    // Every scenario so far has induced one fault at a time. This is
    // the shape an on-call engineer actually meets: you are already
    // rebuilding a standby when the primary dies under you. At the
    // moment of death one standby is HALF-WIPED (its pgdata is being
    // overwritten by a basebackup whose source just vanished), so the
    // cluster's only viable candidate is the untouched one — and the
    // wiped node must not win, must not be crowned by a candidacy that
    // reads its absent state as "no objection", and must not be left
    // believing it is anything.
    let rebuilding = other_node(prim);
    let survivor = NODES
        .iter()
        .copied()
        .find(|n| *n != prim && *n != rebuilding)
        .unwrap();
    write_sentinel(cx, prim, "g20").await;
    let since = cx.log.cursor();
    // Start the rebuild and kill the source while it runs. The recover
    // RPC is synchronous, so it goes to the background; the kill lands
    // while pg_basebackup is streaming from `prim`.
    let recover_target = node_id(rebuilding).to_string();
    let bg = tokio::spawn(async move {
        let _ = exec_pg(
            prim,
            &format!("pg_agentctl cluster recover --target {recover_target} --stop-target-pg"),
        )
        .await;
    });
    cx.await_event(
        60,
        &format!("{rebuilding}: rebuild started (pgdata is being overwritten)"),
        since,
        |ev| agent(ev, rebuilding, "basebackup") || agent(ev, prim, "recovery_1st_stage"),
    )
    .await;
    let _ = exec(prim, &format!("systemctl stop {}", cluster::pg_unit())).await;
    cx.note("primary killed mid-rebuild — the basebackup's source is now gone");
    let winner_ev = cx
        .await_event(
            120,
            "the UNTOUCHED standby won (a half-wiped node cannot be the candidate)",
            since,
            |ev| agent(ev, survivor, "roleexec: promotion complete"),
        )
        .await;
    let w = winner_ev.map(|e| e.node).unwrap_or(survivor);
    cx.check_absent(
        &format!("the half-wiped {rebuilding} was never promoted"),
        since,
        |ev| agent(ev, rebuilding, "roleexec: promotion complete"),
    );
    let _ = bg.await;
    cx.check(
        "exactly one primary after the double fault",
        cx.pg.count_primaries().await == 1,
    );
    check_sentinel(cx, w, "g20").await;
    // Both broken nodes come back by the operator path: the interrupted
    // rebuild has to be redone against the NEW primary, and the dead
    // ex-primary rejoins as a standby.
    for target in [rebuilding, prim] {
        cluster_recover(cx, w, target).await;
        marks.insert(target, cx.log.cursor());
    }
    repair_standbys(cx, w, "g20", marks).await;
    cx.check(
        "exactly one primary after both faults were repaired",
        cx.pg.count_primaries().await == 1,
    );
    cx.wait_until(
        90,
        &format!("{w}: quorum commit re-armed after the double fault"),
        || async move { sync_commit_state(w).await == "armed" },
    )
    .await;
    w
}

/// Replication slots present on `node`, as `name=active` pairs.
async fn slots(cx: &Ctx, node: &'static str) -> Vec<String> {
    cx.pg
        .scalar(
            node,
            "select coalesce(string_agg(slot_name || '=' || active::text, ' ' order by slot_name), '')
             from pg_replication_slots",
        )
        .await
        .map(|s| {
            s.split_whitespace()
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

async fn g21(cx: &mut Ctx, start: &'static str, marks: &mut HashMap<&'static str, Cursor>) {
    cx.say("G21: SOAK — repeated failover must not accumulate anything");
    // Individual failovers have been proven many times over. What no
    // single scenario can show is whether the cluster is quietly
    // getting worse each time: replication slots left behind by nodes
    // that moved on, in-flight journal entries nobody closed, terms
    // and timelines climbing without their bookkeeping keeping up.
    // Debris is invisible per-cycle and fatal by cycle fifty, so the
    // assertion is a COMPARISON across cycles, not a snapshot.
    const CYCLES: usize = 3;
    let mut prim = start;
    let mut slot_counts: Vec<usize> = Vec::new();
    for cycle in 1..=CYCLES {
        let since = cx.log.cursor();
        let _ = exec(prim, &format!("systemctl stop {}", cluster::pg_unit())).await;
        let winner_ev = cx
            .await_event(
                90,
                &format!("soak cycle {cycle}: a standby took over"),
                since,
                move |ev| ev.node != prim && agent_any(ev, "roleexec: promotion complete"),
            )
            .await;
        let dead = prim;
        prim = winner_ev.map(|e| e.node).unwrap_or(other_node(prim));
        cluster_recover(cx, prim, dead).await;
        marks.insert(dead, cx.log.cursor());
        repair_standbys(cx, prim, &format!("g21c{cycle}"), marks).await;
        // The primary should hold exactly one slot per OTHER member —
        // no more, cycle after cycle.
        let s = slots(cx, prim).await;
        cx.check(
            &format!(
                "soak cycle {cycle}: {prim} holds exactly the member slots ({})",
                if s.is_empty() {
                    "none".to_string()
                } else {
                    s.join(" ")
                }
            ),
            s.len() == NODES.len() - 1,
        );
        slot_counts.push(s.len());
    }
    // The comparison that a snapshot cannot make.
    cx.check(
        &format!("slot count never grew across {CYCLES} failovers ({slot_counts:?})"),
        slot_counts.windows(2).all(|w| w[1] <= w[0]),
    );
    // Standbys must not hoard slots of their own: a node that was
    // primary two cycles ago should not still be holding slots for
    // peers that now stream from someone else.
    for n in NODES {
        if n != prim {
            let s = slots(cx, n).await;
            cx.check(
                &format!("soak: ex-primary {n} left no slots behind ({})", s.len()),
                s.is_empty(),
            );
        }
    }
    // Nothing may be left mid-flight: every op the soak opened either
    // completed or was abandoned with a reason.
    let ops = exec_pg(prim, "pg_agentctl ops list")
        .await
        .unwrap_or_default();
    let in_progress = ops
        .lines()
        .filter(|l| l.to_lowercase().contains("inprogress"))
        .count();
    cx.check(
        &format!("soak: no in-flight ops left open after {CYCLES} cycles ({in_progress})"),
        in_progress == 0,
    );
    cx.check(
        "exactly one primary at the end of the soak",
        cx.pg.count_primaries().await == 1,
    );
    cx.wait_until(
        90,
        &format!("{prim}: quorum commit armed at the end of the soak"),
        || async move { sync_commit_state(prim).await == "armed" },
    )
    .await;
}

async fn g22(cx: &mut Ctx, prim: &'static str) {
    cx.say("G22: rolling agent upgrade — the routine operation nothing asserted");
    // Every other scenario kills the agent to see what breaks. This one
    // upgrades it the way an operator does — the real package, through
    // the real package manager, letting `postinstall.sh`'s
    // restart-if-active branch be the thing that stops and starts the
    // daemon — and asserts that NOTHING happens. No takeover, no
    // promotion, no fence, no PostgreSQL touched, same primary at the
    // end, writes acknowledged throughout.
    //
    // The hazard is arithmetic, which is why this is a scenario rather
    // than a unit test. A holder that stops renewing its lease is
    // deposed once `leader_ttl` expires, and an upgrade stops renewal
    // for exactly as long as the restart takes. NOTHING IN THE PRODUCT
    // RELATES THOSE TWO NUMBERS — the margin is a property of the
    // deployment, not an invariant the code maintains — so the
    // scenario measures the gap on every node and reports it against
    // the ttl instead of merely observing that things worked out. On
    // this cluster the ttl is 10s against production's 30s default,
    // so a restart that fits here fits there three times over.
    //
    // Ordering is the operator's: standbys first, the lease holder
    // last. Only the holder's restart can cost anything, and by then
    // its two witnesses are already running the new binary.
    let ttl_s = leader_ttl_secs(prim).await;
    cx.note(&format!(
        "leader_ttl on this cluster is {ttl_s}s (production default: 30s)"
    ));
    cx.wait_until(
        60,
        &format!("{prim}: quorum commit armed before the roll"),
        || async move { sync_commit_state(prim).await == "armed" },
    )
    .await;
    write_sentinel(cx, prim, "g22").await;
    let since = cx.log.cursor();

    let mut order: Vec<&'static str> = NODES.iter().copied().filter(|n| *n != prim).collect();
    order.push(prim);
    let (staged, dest) = staged_package();
    let mut gaps: Vec<(&'static str, u64)> = Vec::new();

    for node in order {
        let role = if node == prim { "holder" } else { "standby" };
        let before = agent_stamp_us(node, "ActiveEnterTimestampMonotonic").await;
        // `docker cp` exiting 0 is not evidence the file is readable
        // inside the container (see `staged_package`), so the node has
        // to see it too.
        let staged_ok = cluster::host(&["docker", "cp", staged, &format!("pga-{node}:{dest}")])
            .await
            .is_ok()
            && exec_ok(node, &format!("test -s {dest}")).await;
        cx.check(
            &format!("{node} ({role}): package staged where the container can read it"),
            staged_ok,
        );
        match exec(node, &package_upgrade_cmd(dest)).await {
            Ok(_) => cx.pass(&format!("{node}: package installed over the running one")),
            Err(e) => {
                cx.fail(&format!("{node}: package installed over the running one"));
                // The package manager's own last word, so a failure is
                // diagnosable from the transcript instead of a re-run.
                let text = e.to_string();
                let tail = text.lines().rfind(|l| !l.trim().is_empty());
                cx.note(&format!("{node}: {}", tail.unwrap_or("(no output)").trim()));
            }
        }
        // The upgrade must have RESTARTED the daemon. A postinst that
        // quietly skipped the restart would leave every assertion
        // below green while testing nothing at all — finding 28's
        // lesson, where a fallback kept a run green and hid the bug.
        // systemd's own ActiveEnter stamp moving forward is the proof.
        cx.wait_until(
            60,
            &format!("{node}: the package restarted pg_agentd (ActiveEnter advanced)"),
            || async move {
                unit_active(node, "pg_agentd").await
                    && agent_stamp_us(node, "ActiveEnterTimestampMonotonic").await > before
            },
        )
        .await;
        match agent_restart_gap_ms(node, before).await {
            Some(ms) => {
                gaps.push((node, ms));
                cx.check(
                    &format!("{node}: agent down {ms}ms over the upgrade, inside the {ttl_s}s ttl"),
                    ms < ttl_s * 1000,
                );
            }
            None => cx.fail(&format!(
                "{node}: systemd records no restart window belonging to this upgrade"
            )),
        }
        // Back in the mesh, not merely back as a process: the node
        // answers its own status fan-out and the linearizable read
        // behind it completed.
        cx.wait_until(
            60,
            &format!("{node}: rejoined consensus after the upgrade"),
            || async move { consensus_readable(node).await },
        )
        .await;
        // The cluster kept serving through it — asserted per node, so
        // a stall is attributed to the restart that caused it.
        cx.check(
            &format!("writes still acknowledged after {node}'s upgrade"),
            timed_write(prim, "g22", 10).await,
        );
    }

    // The whole point, stated as absences over the entire roll.
    cx.check_absent("no takeover across the rolling upgrade", since, |ev| {
        agent_any(ev, "TookOver")
    });
    cx.check_absent("no promotion across the rolling upgrade", since, |ev| {
        agent_any(ev, "roleexec: promotion complete")
    });
    cx.check_absent("no fence across the rolling upgrade", since, |ev| {
        agent_any(ev, "FENCING")
    });
    // An agent upgrade is not a PostgreSQL event. The agent owns the
    // postmaster's lifecycle, so "restarting the agent leaves the
    // database alone" is a claim worth holding it to.
    cx.check_absent(
        "no PostgreSQL shutdown across the rolling upgrade",
        since,
        |ev| {
            ev.source == Source::Postgres
                && (ev.line.contains("shutdown request")
                    || ev.line.contains("database system is shut down"))
        },
    );
    for n in NODES {
        cx.check(
            &format!("{n}: PostgreSQL still running after its agent was upgraded"),
            unit_active(n, &cluster::pg_unit()).await,
        );
    }
    cx.check(
        &format!("{prim} still holds the lease after all three agents were upgraded"),
        cx.pg.current_primary().await == Some(prim),
    );
    cx.check(
        "exactly one primary after the roll",
        cx.pg.count_primaries().await == 1,
    );
    let pg = cx.pg.clone();
    cx.wait_until(
        60,
        &format!("{prim} still has 2 streaming standbys after the roll"),
        || {
            let pg = pg.clone();
            async move { pg.streaming_count(prim).await == Some(2) }
        },
    )
    .await;
    check_sentinel(cx, prim, "g22").await;
    cx.wait_until(
        60,
        &format!("{prim}: quorum commit still armed after the roll"),
        || async move { sync_commit_state(prim).await == "armed" },
    )
    .await;
    // The number an operator actually needs, printed whether or not
    // anything failed: how much of the lease the slowest restart ate.
    if let Some((worst, ms)) = gaps.iter().copied().max_by_key(|(_, ms)| *ms) {
        cx.note(&format!(
            "worst restart window: {worst} at {ms}ms — {}% of this cluster's {ttl_s}s ttl",
            ms * 100 / (ttl_s * 1000)
        ));
    }
}

/// Operator path: rebuild broken standbys via `cluster recover` —
/// criteria-driven, no clock decides anything:
/// - the finding-15 wedge signature (a fresh "forked off" PostgreSQL
///   log event since the last PROVEN convergence) → reclone now: a
///   standby past the new primary's fork point loops that error
///   forever while its walreceiver flaps through 'streaming', so a
///   bare streaming count can declare victory on a flap (observed);
/// - streaming AND a clean scan → converged (marks consumed);
/// - not streaming but replay LSN advancing → converging, leave it;
/// - neither → genuinely stuck, reclone.
///
/// The wedge marks are event-log cursors (per node), which replace the
/// bash suite's PG-log line counts: same ordering idea, one log.
async fn repair_standbys(
    cx: &mut Ctx,
    prim: &'static str,
    ctxname: &str,
    marks: &mut HashMap<&'static str, Cursor>,
) {
    let t0 = std::time::Instant::now();
    let mut recovered: Vec<&'static str> = Vec::new();
    let budget = Duration::from_secs(30);
    loop {
        for n in NODES {
            if n == prim || recovered.contains(&n) {
                continue;
            }
            let mark = *marks.get(n).unwrap_or(&Cursor(0));
            // Two wedge signatures, either one → operator recover now:
            // - PostgreSQL's timeline fork ("forked off", finding 15);
            // - the executor's own tripwire ("follow WEDGED"), which
            //   covers every confirmed-but-never-streaming shape —
            //   first seen for WAL-removed-under-slot-race (finding
            //   22), where the re-follow loop emits a FRESH follow
            //   event each cycle and the follow-event gate below would
            //   otherwise shield the node from repair forever.
            let wedged = cx.log.find(mark, |ev| {
                (ev.source == Source::Postgres && ev.node == n && ev.line.contains("forked off"))
                    || agent(ev, n, "follow WEDGED")
            });
            if wedged.is_some() {
                cx.note(&format!(
                    "{n} shows the timeline-fork wedge ({}s) — recovering it now",
                    t0.elapsed().as_secs()
                ));
                cluster_recover(cx, prim, n).await;
                marks.insert(n, cx.log.cursor());
                recovered.push(n);
            }
        }
        if cx.pg.streaming_count(prim).await == Some(2) {
            let consumed = cx.log.cursor();
            for n in NODES {
                if n != prim {
                    marks.insert(n, consumed);
                }
            }
            let tail = if recovered.is_empty() {
                String::new()
            } else {
                format!(", recovered: {}", recovered.join(" "))
            };
            cx.pass(&format!(
                "{prim} has 2 streaming standbys ({ctxname}, {}s{tail})",
                t0.elapsed().as_secs()
            ));
            return;
        }
        if t0.elapsed() >= budget {
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    // Out of budget: decide per node by EVIDENCE, not by patience —
    // reclone only a node that is neither streaming, nor replaying
    // forward, nor visibly being driven by its executor.
    for n in NODES {
        if n == prim || recovered.contains(&n) {
            continue;
        }
        let receiver = cx
            .pg
            .scalar(
                n,
                "select coalesce((select status from pg_stat_wal_receiver), '')",
            )
            .await
            .unwrap_or_default();
        if receiver == "streaming" {
            continue;
        }
        // A node whose PostgreSQL is not even answering is the
        // operator-path case, full stop — no gate applies. (A fenced
        // node with a STALE follow event once slipped past the
        // follow-gate below and was never rebuilt.)
        if cx.pg.is_in_recovery(n).await.is_none() {
            cx.note(&format!(
                "{n} PostgreSQL unreachable — operator recover ({ctxname})"
            ));
            cluster_recover(cx, prim, n).await;
            marks.insert(n, cx.log.cursor());
            continue;
        }
        // Executor re-pointed this node since its last proven
        // convergence (and no wedge signature — the scan above would
        // have recovered it): mid-convergence, e.g. between finishing
        // replay and joining the new timeline. Let the long wait
        // decide; a follow that never streams then fails loudly there
        // instead of being papered over by a reclone.
        let mark = *marks.get(n).unwrap_or(&Cursor(0));
        if cx
            .log
            .find(mark, |ev| agent(ev, n, "now following lease holder"))
            .is_some()
        {
            cx.note(&format!(
                "{n} not streaming but its executor re-pointed it — letting it converge ({ctxname})"
            ));
            continue;
        }
        let l1 = cx.pg.replay_lsn(n).await;
        tokio::time::sleep(Duration::from_secs(3)).await;
        let l2 = cx.pg.replay_lsn(n).await;
        match (l1, l2) {
            (Some(a), Some(b)) if a != b => {
                cx.note(&format!(
                    "{n} not streaming but replay is advancing ({a} -> {b}) — letting it converge ({ctxname})"
                ));
            }
            _ => {
                cx.note(&format!(
                    "{n} not streaming, not advancing, not followed — operator recover ({ctxname})"
                ));
                cluster_recover(cx, prim, n).await;
                marks.insert(n, cx.log.cursor());
            }
        }
    }
    let pg = cx.pg.clone();
    if cx
        .wait_until(
            180,
            &format!("{prim} has 2 streaming standbys ({ctxname}, after repair)"),
            || {
                let pg = pg.clone();
                async move { pg.streaming_count(prim).await == Some(2) }
            },
        )
        .await
    {
        let consumed = cx.log.cursor();
        for n in NODES {
            if n != prim {
                marks.insert(n, consumed);
            }
        }
    }
}

/// Attach every down backend on every instance, primary's backend
/// first (attaching any other node while an instance's map has no up
/// primary blocks in find_primary_node_repeatedly — finding 16), and
/// only when actually down (blindly attaching an up backend makes
/// pgpool re-run failover processing and transiently degenerate
/// healthy backends).
async fn pcp_attach_everywhere(cx: &mut Ctx) {
    let prim = cx.pg.current_primary().await.unwrap_or("db0");
    let prim_id = node_id(prim).to_string();
    let mut order: Vec<String> = vec![prim_id.clone()];
    for i in ["0", "1", "2"] {
        if i != prim_id {
            order.push(i.to_string());
        }
    }
    for n in NODES {
        for i in &order {
            if pcp_node_info(n, i).await.contains("down") {
                let _ = exec_pg(
                    n,
                    &format!("pcp_attach_node -h localhost -p 9898 -U pgpool -w -n {i}"),
                )
                .await;
            }
        }
    }
    for n in NODES {
        cx.wait_until(
            60,
            &format!("{n}: pgpool map normalized (3 backends up)"),
            || async move { pcp_backends_up(n).await == 3 },
        )
        .await;
    }
}
