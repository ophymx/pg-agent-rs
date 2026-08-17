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

/// Healthz-reported quorum-commit posture on `node`.
async fn sync_commit_state(node: &str) -> String {
    exec(node, "curl -sf localhost:9702/healthz")
        .await
        .ok()
        .and_then(|body| {
            body.split("\"sync_commit\":\"")
                .nth(1)
                .and_then(|s| s.split('"').next())
                .map(str::to_string)
        })
        .unwrap_or_default()
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

pub async fn run_all(cx: &mut Ctx) {
    let mut marks: HashMap<&'static str, Cursor> = NODES.iter().map(|n| (*n, Cursor(0))).collect();

    g0(cx).await;
    g1(cx).await;
    g1b(cx).await;
    g2(cx).await;
    g2b(cx).await;
    let w1 = g3(cx).await;
    g4(cx, w1, "db0", &mut marks).await;
    g4b(cx, w1, &mut marks).await;
    let w2 = g5(cx, w1).await;
    g5b(cx, w1, w2, &mut marks).await;
    g6(cx, w2).await;
    let w3 = g7(cx, w2, &mut marks).await;
    let w4 = g8(cx, w3, &mut marks).await;
    let w5 = g9(cx, w4, &mut marks).await;
    g10(cx, w5).await;
    g11(cx, w5, &mut marks).await;
    crate::audit::run(cx);
}

async fn g0(cx: &mut Ctx) {
    cx.say("G0: greenfield boot — execute mode from the first start");
    // Nodes are provisioned with [raft] enabled = true, shadow = false.
    // validate-env (the systemd ExecStartPre gate) runs the raft
    // prerequisite checks on every start; the daemons coming up IS that
    // assertion. Before ClusterInit there is no membership and hence no
    // quorum: the loop must tick StoreUnknown — never vacant, never act.
    for n in NODES {
        cx.wait_until(
            60,
            &format!("{n}: pg_agentd active (validate-env gate incl. raft checks)"),
            || async move { unit_active(n, "pg_agentd").await },
        )
        .await;
    }
    cx.wait_until(30, "db0: postgres active (bootstrap primary)", || async {
        unit_active("db0", "postgresql@17-main").await
    })
    .await;
    for n in ["db1", "db2"] {
        cx.check(
            &format!("{n}: postgres intentionally down pre-init"),
            !unit_active(n, "postgresql@17-main").await,
        );
    }
    for n in NODES {
        cx.await_event(
            30,
            &format!("{n}: raft started, EXECUTE mode"),
            Cursor(0),
            |ev| agent(ev, n, "EXECUTE mode"),
        )
        .await;
    }
    cx.await_event(
        30,
        "pre-membership: loop reports store unknown (not vacant, no action)",
        Cursor(0),
        |ev| agent(ev, "db0", "StoreUnknown"),
    )
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
    cx.check(
        "raft membership formed by init",
        init1.contains("raft membership initialized (3 nodes)"),
    );
    cx.check(
        "lease seeded for the bootstrap primary",
        init1.contains("lease seeded"),
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
    match exec_pg("db0", "pg_agentctl check-hooks /etc/pgpool2/pgpool.conf").await {
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
    let _ = exec("db0", "systemctl stop postgresql@17-main").await;
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
        !unit_active(dead, "postgresql@17-main").await,
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
        |ev| {
            ev.node == w1
                && ev.source == Source::Postgres
                && ev.line.contains("database system is shut down")
        },
    )
    .await;
    let winner_ev = cx
        .await_event(60, "majority completed a real promotion", since, |ev| {
            ev.node != w1 && agent_any(ev, "roleexec: promotion complete")
        })
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
        !unit_active(w1, "postgresql@17-main").await,
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
    let _ = exec(
        s_lag,
        &format!(
            "iptables -A OUTPUT -d {prim_ip} -p tcp --dport 5432 -j DROP && \
             iptables -A INPUT -s {prim_ip} -p tcp --sport 5432 -j DROP"
        ),
    )
    .await;
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
    let _ = exec(w2, "systemctl stop postgresql@17-main").await;
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
    // Heal the severed walreceiver path (rules are the only ones in
    // these chains — the containers run no other firewalling).
    let _ = exec(s_lag, "iptables -F OUTPUT && iptables -F INPUT").await;
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
            "systemctl mask --runtime pg_agentd && systemctl kill -s SIGKILL pg_agentd",
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
            |ev| ev.node != prim && agent_any(ev, "roleexec: promotion complete"),
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
        unit_active(prim, "postgresql@17-main").await,
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
        |ev| {
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
        exec_ok(prim, "systemctl kill -s SIGKILL postgresql@17-main").await,
    );
    cx.await_event(
        30,
        &format!("{prim}: systemd recorded the crash (code=killed — the only death event)"),
        since,
        |ev| ev.node == prim && ev.source == Source::Postgres && ev.line.contains("code=killed"),
    )
    .await;
    let winner_ev = cx
        .await_event(
            60,
            "the lease promoted past the crashed primary",
            since,
            |ev| ev.node != prim && agent_any(ev, "roleexec: promotion complete"),
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
        !unit_active(prim, "postgresql@17-main").await,
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
                "grep -q 'database system was not properly shut down' \
                 /var/log/postgresql/postgresql-17-main.log",
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

async fn g11(cx: &mut Ctx, prim: &'static str, marks: &mut HashMap<&'static str, Cursor>) {
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
            "systemctl mask --runtime pg_agentd && systemctl kill -s SIGKILL pg_agentd",
        )
        .await,
    );
    cx.expect_dual_serving(prim, since);
    let winner_ev = cx
        .await_event(
            90,
            "majority deposed the loaded holder and promoted",
            since,
            |ev| ev.node != prim && agent_any(ev, "roleexec: promotion complete"),
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
        |ev| {
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
