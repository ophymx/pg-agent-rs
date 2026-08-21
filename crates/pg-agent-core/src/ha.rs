//! The HA loop (docs/promotion-authority.md §"The HA loop").
//!
//! Every `loop_wait` the loop performs one *tick*: read the lease from
//! the [`ConsensusStore`], observe local PostgreSQL and every peer, and
//! produce exactly one [`HaDecision`], which the executor then acts on.
//! It runs on standbys too — a standby is what detects a dead holder
//! and becomes a candidate.
//!
//! # A decision function, with the acting kept out of it
//!
//! The loop itself computes; it never touches PostgreSQL. The struct
//! holds no `Systemd`, no `Pcp`, no `StandbyOps`, and never dials a
//! peer mutation RPC. Everything destructive goes through
//! [`crate::roleexec::RoleExecutor`], which the daemon always attaches
//! ([`HaLoop::with_executor`]).
//!
//! That split is a testing seam, not a mode. It used to be one: a loop
//! with no executor was "shadow mode", the staged migration's way of
//! watching decisions against a live pgpool-led cluster without letting
//! them act. There is no pgpool-led cluster to shadow any more — the
//! lease is the only promotion authority — so an executor-less loop
//! would be a daemon that watches a cluster nobody is running. The
//! daemon builds the loop, its executor and the store as one value
//! (`agent::HaWiring`); only this module's own unit tests construct a
//! loop without one, to assert the decision and not its consequences.
//!
//! # How the decisions are judged
//!
//! **Not against pgpool.** The logged stream (target `ha`) is asserted
//! against ground truth — which node actually held the most WAL,
//! whether the announced-dead node was actually dead, whether exactly
//! one node became promotable — in the dockerized acceptance suite
//! (`testing/`), where those facts are manufactured rather than
//! inferred. Diffing against pgpool's live behavior was the design
//! doc's original plan and is explicitly abandoned: pgpool's decisions
//! are the defect this loop exists to replace (promotion-authority
//! §2.1, §2.2), so in the cases that matter agreement would be the bad
//! outcome, not the good one.
//!
//! # Decision rules carried over from the design doc
//!
//! - **"Cannot read" is not "vacant."** A failed store read yields
//!   [`HaDecision::StoreUnknown`]; no role-changing decision is made.
//!   If we *hold* the lease and the store stays unknown past
//!   `retry_timeout`, the decision is [`HaDecision::WouldDemote`] —
//!   the fail-closed bill the design prices explicitly.
//! - **The CAS is the gate.** Candidacy proposes
//!   `try_takeover(expected)`; concurrent candidates serialize in the
//!   store and losers back off.
//! - **Candidates must be the most-advanced reachable node**, by
//!   lexicographic `(timeline, lsn)`. Within `max_lag_on_failover`
//!   of the best position, node id breaks the tie (lower id proceeds)
//!   — otherwise two candidates sampling each other at different
//!   instants can each see the other ahead and both skip forever.
//!   Stand-downs carry a jittered backoff for the same reason.
//! - **A vacant lease is never filled in on another node's behalf.**
//!   A standby that sees no lease and a live primary elsewhere reports
//!   the bootstrap gap and stands down. The primary claims for itself,
//!   or `ClusterInit` seeds; inferring a holder from what is observed
//!   is how a shared store learns something nobody committed. (The
//!   loop did once adopt an observed primary, because each node's
//!   store was private and would otherwise never leave the vacant
//!   state. With one replicated store there is nothing to seed and
//!   nobody to seed it for.)

use crate::cluster_view::{collect_statuses, WalPosition, STATUS_FANOUT_BUDGET};
use crate::config::{NodeConfig, NodePool};
use crate::consensus::{ConsensusStore, TakeoverOutcome};
use crate::localdb::LocalDb;
use crate::peers::PeerRegistry;
use rand::Rng;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

/// Timing knobs, projected from `[raft]`
/// (`crate::config::RaftConfig::effective_*`). Invariants between them
/// are enforced at config load.
#[derive(Debug, Clone)]
pub struct HaTiming {
    pub loop_wait: Duration,
    pub retry_timeout: Duration,
    pub leader_ttl: Duration,
}

/// One tick's outcome. Exactly one per tick; the `run` loop logs them
/// (deduplicated by variant) on the `ha` target.
#[derive(Debug, Clone, PartialEq)]
pub enum HaDecision {
    /// Cluster is paused — no automatic role decisions.
    Paused,
    /// Store read failed; state unknown (NOT vacant). Includes how long
    /// it has been unknown. No role-changing action.
    StoreUnknown { unknown_for: Duration },
    /// We hold (or held) the lease and the loop would demote local
    /// PostgreSQL. The reason names which rule fired.
    WouldDemote { reason: String },
    /// We hold the lease, local PG is running as primary, and the
    /// linearizable read confirmed it — the healthy steady state.
    RetainedLease { term: u64 },
    /// We hold the lease but local PostgreSQL is still in recovery,
    /// within the post-takeover grace window. Expected: `pg_promote()`
    /// is asynchronous, so a freshly-won lease legitimately precedes
    /// the role change. No action — releasing here would thrash the
    /// lease (see the grace-window note on `HaLoop::promotion_grace`).
    AwaitingPromotion { term: u64, waiting: Duration },
    /// Someone else holds the lease and looks healthy; we follow.
    Following { holder: i32 },
    /// The holder changed since the last tick — the loop would
    /// reconfigure local PostgreSQL onto the new holder
    /// (`follow_primary`).
    WouldFollowNewHolder { prev: i32, holder: i32 },
    /// The holder is unreachable or not running as primary, but hasn't
    /// been for `leader_ttl` yet — watching, not acting.
    HolderUnhealthy {
        holder: i32,
        unhealthy_for: Duration,
    },
    /// Entering candidacy while still streaming from the deposed
    /// holder: the executor detaches the walreceiver so the local
    /// flush position freezes (finding 23). Positions are compared
    /// only once frozen — a moving stream has no stable order, and
    /// comparing a point-in-time local read against peers' fresher
    /// reports made every candidate defer to every other forever
    /// while the fence-less deposed primary kept serving.
    DetachingFromDeposed { holder: i32 },
    /// Candidacy considered and declined; the reason says why
    /// (ineligible, not most-advanced, tiebreak, backoff, …).
    StoodDown { reason: String },
    /// Our takeover CAS won. `already_primary` distinguishes "claiming
    /// the lease for the primary we already are" (adoption of self,
    /// no PG action implied) from "would promote local PostgreSQL".
    TookOver { term: u64, already_primary: bool },
    /// Our takeover CAS lost — someone else moved first. Back off.
    LostTakeover { current_holder: Option<i32> },
}

/// What the loop knows about local PostgreSQL this tick.
struct LocalView {
    /// `Some(true)` = running as primary, `Some(false)` = in recovery
    /// (standby), `None` = unknown (query failed / PG down).
    is_primary: Option<bool>,
    pos: Option<WalPosition>,
    /// The walreceiver is active — this standby's flush position is
    /// still MOVING. A moving position must never enter a candidacy
    /// comparison (finding 23).
    receiving: bool,
}

/// What the loop knows about one peer this tick.
struct PeerView {
    node: NodeConfig,
    /// Reachable and reported running && !in_recovery.
    running_as_primary: bool,
    pos: Option<WalPosition>,
    /// Peer reports an active walreceiver (`replication_state` is
    /// "streaming"/"catchup") — its position is still moving.
    receiving: bool,
    /// This peer answered us at all this tick. A peer we could not
    /// reach has no opinion to offer about anyone else.
    reachable: bool,
    /// The peer's OWN per-node "last seen SERVING as primary" ages, in
    /// milliseconds (`NodeStatus.peer_primary_seen_age_ms`) — the
    /// second opinion candidacy consults before deposing a holder this
    /// node cannot see.
    seen_ages: std::collections::HashMap<i32, u64>,
}

struct TickState {
    last_holder: Option<i32>,
    /// `(holder, since)` — the clock is keyed to the holder it watched.
    /// A lease that changes hands must NOT inherit the previous
    /// holder's unhealthy time: the ttl is each holder's protection
    /// window, and the acceptance suite's R4 caught a rival deposing a
    /// 7-second-old lease because its clock had been running against
    /// the *previous* holder (finding 13).
    holder_unhealthy_since: Option<(i32, Instant)>,
    store_unknown_since: Option<Instant>,
    backoff_until: Option<Instant>,
    /// The lease term we currently believe we hold, and when we first
    /// observed ourselves holding it — the clock for the promotion
    /// grace window.
    held_term: Option<u64>,
    held_since: Option<Instant>,
    last_logged: Option<std::mem::Discriminant<HaDecision>>,
    /// Candidacy-era position samples from the PREVIOUS tick, keyed by
    /// node id (local node included, under its own id). A position
    /// enters a comparison only when it matches the previous sample,
    /// and a rival sampled last tick that VANISHES this tick (status
    /// fetch failure) defers the comparison one tick instead of
    /// silently leaving it — one transient Err must not hand the CAS
    /// to whoever raced past the only rival that outranked it.
    /// "Frozen" therefore means STABLE-AND-PRESENT, not merely
    /// receiver-less: belt-and-braces under finding 23's moving
    /// streams, and the hysteresis that finding 24's investigation
    /// showed comparisons need against single-tick observation noise.
    settled_pos: std::collections::HashMap<i32, WalPosition>,
}

pub struct HaLoop {
    store: Arc<dyn ConsensusStore>,
    db: Arc<dyn LocalDb>,
    peers: Arc<dyn PeerRegistry>,
    pool: NodePool,
    timing: HaTiming,
    state: Mutex<TickState>,
    /// Every tick's decision is handed to the executor after logging.
    /// `None` only in this module's unit tests, which assert the
    /// decision rather than its consequences — see the module docs on
    /// why that is a test seam and not a mode.
    executor: Option<Arc<crate::roleexec::RoleExecutor>>,
    /// Records which peers this loop reached, per tick. `None` in
    /// tests that do not exercise the second-opinion path.
    peer_seen: Option<Arc<crate::cluster_view::PeerSeen>>,
}

impl HaLoop {
    pub fn new(
        store: Arc<dyn ConsensusStore>,
        db: Arc<dyn LocalDb>,
        peers: Arc<dyn PeerRegistry>,
        pool: NodePool,
        timing: HaTiming,
    ) -> Self {
        Self {
            store,
            db,
            peers,
            pool,
            timing,
            state: Mutex::new(TickState {
                last_holder: None,
                holder_unhealthy_since: None,
                store_unknown_since: None,
                backoff_until: None,
                held_term: None,
                held_since: None,
                last_logged: None,
                settled_pos: std::collections::HashMap::new(),
            }),
            executor: None,
            peer_seen: None,
        }
    }

    /// Share the per-tick contact record with `Agent::get_status`, so
    /// this node can serve as a witness for its peers' candidacy
    /// decisions (finding 25's second-opinion gate).
    pub fn with_peer_seen(mut self, seen: Arc<crate::cluster_view::PeerSeen>) -> Self {
        self.peer_seen = Some(seen);
        self
    }

    /// Attach the executor: every decision is acted on. `pg_agentd`
    /// always calls this — a loop without it writes only to its store
    /// and its log, which is what the unit tests want and what no
    /// deployment does.
    pub fn with_executor(mut self, executor: Arc<crate::roleexec::RoleExecutor>) -> Self {
        self.executor = Some(executor);
        self
    }

    /// Continuous loop: tick every `loop_wait` until shutdown. Decisions
    /// are logged on the `ha` target — info on variant change, debug on
    /// repeats, warn for the destructive decisions.
    pub async fn run(self: Arc<Self>, shutdown: CancellationToken) {
        // The one line that says this node is participating: it has a
        // lease to read and an executor to act with. There is no other
        // way for the daemon to reach here, which is why the message no
        // longer distinguishes an "execute mode" from anything else.
        info!(
            loop_wait_secs = self.timing.loop_wait.as_secs(),
            acts_on_postgres = self.executor.is_some(),
            "ha loop: starting"
        );
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    info!("ha loop: stopping");
                    return;
                }
                _ = tokio::time::sleep(self.timing.loop_wait) => {}
            }
            let decision = self.tick_once().await;
            self.log_decision(&decision);
            if let Some(executor) = &self.executor {
                // Inline, not spawned: a promotion blocking the loop for
                // up to its deadline is by design — the deadline equals
                // leader_ttl, the same clock rivals run against us, so
                // there is nothing useful for this node's loop to decide
                // while the promotion is in flight.
                executor.apply(&decision).await;
            }
        }
    }

    /// One tick. Public so tests (and a future `pg_agentctl` debug
    /// command) can drive the loop deterministically.
    pub async fn tick_once(&self) -> HaDecision {
        let now = Instant::now();
        let local_id = self.pool.local_node_id;

        // The read is bounded by the loop's own retry budget, whatever
        // the store behind the trait does. The raft-backed store learned
        // this the hard way in the acceptance suite's R4: a forwarded
        // read to a just-isolated leader blocked one tick for 34 s — the
        // entire partition window — where "evidence we cannot get within
        // the budget is evidence we do not get" would have produced a
        // StoreUnknown tick and kept the loop's clock running. The store
        // now bounds itself too (LEADER_RPC_TIMEOUT); this is the loop
        // refusing to depend on that.
        let state =
            match tokio::time::timeout(self.timing.retry_timeout, self.store.read_state()).await {
                Ok(result) => result,
                Err(_) => Err(anyhow::anyhow!(
                    "store read exceeded retry_timeout {:?}",
                    self.timing.retry_timeout
                )),
            };
        let local = self.observe_local().await;
        let peers = self.observe_peers().await;

        let state = match state {
            Ok(s) => {
                self.state.lock().unwrap().store_unknown_since = None;
                s
            }
            Err(_) => {
                let unknown_for = {
                    let mut ts = self.state.lock().unwrap();
                    let since = *ts.store_unknown_since.get_or_insert(now);
                    now.duration_since(since)
                };
                // Only a lease *holder* escalates unknown to demote —
                // and only past the retry budget. Everyone else takes
                // no role-changing action on an unknown store.
                let held = self.state.lock().unwrap().last_holder == Some(local_id);
                if held && unknown_for > self.timing.retry_timeout {
                    return HaDecision::WouldDemote {
                        reason: format!(
                            "store unknown for {:.1?} > retry_timeout {:.1?} while holding \
                             the lease (quorum contact lost)",
                            unknown_for, self.timing.retry_timeout
                        ),
                    };
                }
                return HaDecision::StoreUnknown { unknown_for };
            }
        };

        if state.paused.is_some() {
            return HaDecision::Paused;
        }

        let decision = match &state.lease {
            Some(lease) if lease.holder == local_id => {
                let held_for = {
                    let mut ts = self.state.lock().unwrap();
                    ts.holder_unhealthy_since = None;
                    ts.settled_pos.clear();
                    if ts.held_term != Some(lease.term) {
                        ts.held_term = Some(lease.term);
                        ts.held_since = Some(now);
                    }
                    now.duration_since(ts.held_since.unwrap_or(now))
                };
                match local.is_primary {
                    Some(true) => HaDecision::RetainedLease { term: lease.term },
                    Some(false) => {
                        // Holding the lease while in recovery. Do NOT
                        // release immediately: pg_promote() is
                        // asynchronous, so this is the expected state
                        // for a short window after winning a takeover.
                        // Releasing on sight thrashes the lease — the
                        // docker acceptance suite caught exactly that
                        // (take → release → retake every two ticks).
                        if held_for < self.promotion_grace() {
                            HaDecision::AwaitingPromotion {
                                term: lease.term,
                                waiting: held_for,
                            }
                        } else {
                            // Promotion is not coming. Release so a real
                            // primary can claim the lease, and back off
                            // so we don't immediately re-take it.
                            let _ = self.store.release(local_id, lease.term).await;
                            self.arm_backoff(now);
                            HaDecision::WouldDemote {
                                reason: format!(
                                    "held lease for {held_for:.1?} without local PostgreSQL \
                                     leaving recovery (promotion did not take effect); \
                                     released lease"
                                ),
                            }
                        }
                    }
                    None => HaDecision::WouldDemote {
                        reason: "holding lease but local PostgreSQL state is unknown \
                                 (query failed)"
                            .into(),
                    },
                }
            }
            Some(lease) => {
                // Someone else holds it. Healthy?
                let healthy = peers
                    .iter()
                    .find(|p| p.node.id == lease.holder)
                    .is_some_and(|p| p.running_as_primary);
                if healthy {
                    let prev = {
                        let mut ts = self.state.lock().unwrap();
                        ts.holder_unhealthy_since = None;
                        ts.settled_pos.clear();
                        ts.last_holder
                    };
                    match prev {
                        Some(p) if p != lease.holder => HaDecision::WouldFollowNewHolder {
                            prev: p,
                            holder: lease.holder,
                        },
                        _ => HaDecision::Following {
                            holder: lease.holder,
                        },
                    }
                } else {
                    let unhealthy_for = {
                        let mut ts = self.state.lock().unwrap();
                        let since = match ts.holder_unhealthy_since {
                            Some((h, t)) if h == lease.holder => t,
                            // First unhealthy observation of THIS
                            // holder — restart the clock, whatever it
                            // said about a predecessor.
                            _ => {
                                ts.holder_unhealthy_since = Some((lease.holder, now));
                                now
                            }
                        };
                        now.duration_since(since)
                    };
                    if unhealthy_for >= self.timing.leader_ttl {
                        self.consider_candidacy(
                            Some((lease.holder, lease.term)),
                            &local,
                            &peers,
                            now,
                        )
                        .await
                    } else {
                        HaDecision::HolderUnhealthy {
                            holder: lease.holder,
                            unhealthy_for,
                        }
                    }
                }
            }
            None => {
                // Vacant. Who, if anyone, is observably running as a
                // primary decides whether this is a leaderless cluster
                // or a cluster whose lease has not been seeded yet.
                let mut observed: Vec<i32> = peers
                    .iter()
                    .filter(|p| p.running_as_primary)
                    .map(|p| p.node.id)
                    .collect();
                if local.is_primary == Some(true) {
                    observed.push(local_id);
                }
                match observed.as_slice() {
                    [only] if *only != local_id => {
                        // Never write a lease on another node's behalf.
                        // The primary claims for itself (or ClusterInit
                        // seeds), and until one of those happens a
                        // vacant lease with a live primary is a
                        // bootstrap gap to report, not to paper over.
                        HaDecision::StoodDown {
                            reason: format!(
                                "lease vacant but node {only} runs as primary; \
                                 waiting for it to claim (or ClusterInit to seed)"
                            ),
                        }
                    }
                    [] | [_] => {
                        // Vacant and either no primary anywhere (true
                        // leaderless cluster) or the only primary is us
                        // (claim the lease for ourselves).
                        self.consider_candidacy(None, &local, &peers, now).await
                    }
                    many => HaDecision::StoodDown {
                        reason: format!(
                            "multiple nodes observed running as primary ({many:?}); \
                             refusing any role decision (phantom-check territory)"
                        ),
                    },
                }
            }
        };

        // Remember the committed holder for next tick's comparisons.
        {
            let mut ts = self.state.lock().unwrap();
            ts.last_holder = match &decision {
                HaDecision::TookOver { .. } => Some(local_id),
                _ => state.lease.as_ref().map(|l| l.holder),
            };
        }
        decision
    }

    /// The candidacy ladder: eligibility → most-advanced check →
    /// tiebreak → CAS. `expected` carries the observed holder for the
    /// takeover-from-dead-holder case, `None` for observed vacancy.
    async fn consider_candidacy(
        &self,
        expected: Option<(i32, u64)>,
        local: &LocalView,
        peers: &[PeerView],
        now: Instant,
    ) -> HaDecision {
        let local_id = self.pool.local_node_id;

        if let Some(until) = self.state.lock().unwrap().backoff_until {
            if now < until {
                return HaDecision::StoodDown {
                    reason: format!(
                        "in backoff for {:.1?} after a previous stand-down",
                        until.duration_since(now)
                    ),
                };
            }
        }
        if local.is_primary.is_none() {
            return HaDecision::StoodDown {
                reason: "local PostgreSQL state unknown; not a candidate".into(),
            };
        }

        // The SECOND-OPINION gate (finding 25). Reaching here with
        // `expected = Some(holder)` means: this node has not seen the
        // holder healthy for a full leader_ttl and is about to take
        // its lease. But "the holder looks dead to me" is evidence
        // about the observer exactly as much as about the holder — and
        // the CAS cannot tell them apart, because the store has no
        // notion of whether the incumbent is still alive. So ask the
        // other members: every node reports how long ago it last
        // observed each peer SERVING as a primary
        // (`NodeStatus.peer_primary_seen_age_ms`), and if any
        // REACHABLE member has watched the holder serve within the
        // ttl, this node's blindness is local and the holder keeps its
        // lease.
        //
        // Serving, not answering. The first cut of this gate recorded
        // mere reachability and deadlocked the most ordinary failover
        // there is: when a holder's PostgreSQL dies its agent keeps
        // answering GetStatus perfectly, so every witness truthfully
        // reported "I reached it 1s ago" and no standby would ever
        // take the lease (caught by G3 on the first run).
        //
        // Self-clearing by construction: a genuinely dead holder makes
        // every witness's age grow past the ttl within one ttl, so the
        // gate opens on its own. It costs a bounded delay, never a
        // deadlock — and it never blocks the paths that matter most
        // (a fully isolated holder is unreachable to everyone, and a
        // vacant lease has no incumbent to defend).
        if let Some((holder, _)) = expected {
            let ttl_ms = self.timing.leader_ttl.as_millis() as u64;
            let witness = peers
                .iter()
                .filter(|p| p.reachable && p.node.id != holder)
                .find_map(|p| {
                    p.seen_ages
                        .get(&holder)
                        .filter(|age| **age <= ttl_ms)
                        .map(|age| (p.node.id, *age))
                });
            if let Some((witness_id, age_ms)) = witness {
                return HaDecision::StoodDown {
                    reason: format!(
                        "node {witness_id} saw holder {holder} SERVING {age_ms}ms ago \
                         (within leader_ttl {ttl_ms}ms) — my blindness is local, \
                         not the holder's death; deferring"
                    ),
                };
            }
        }
        // The candidacy freeze (finding 23). A fence-less deposal —
        // the holder's AGENT dead, its PostgreSQL serving — leaves the
        // standbys streaming and their flush positions MOVING. A
        // moving stream has no stable order: each candidate compares
        // its own point-in-time flush against peers' fresher reports,
        // reads itself behind, and everyone defers forever, while the
        // continuing acks are exactly what keeps the WAL moving. So:
        // detach first (the executor stops the walreceiver; position
        // freezes), and compare only against peers that have also
        // stopped receiving. Frozen positions are also what makes the
        // strict-max choice loss-free: an ANY-1-acked row at LSN L was
        // flushed by some standby before it froze, so the frozen
        // maximum is ≥ L and the winner holds every acked byte.
        if local.is_primary == Some(false) && local.receiving {
            return match expected {
                Some((holder, _)) => HaDecision::DetachingFromDeposed { holder },
                None => HaDecision::StoodDown {
                    reason: "still streaming with a vacant lease; not a candidate this tick".into(),
                },
            };
        }
        if let Some(mover) = peers
            .iter()
            .find(|p| !p.running_as_primary && p.pos.is_some() && p.receiving)
        {
            // A rival candidate is still absorbing (and possibly
            // ACKING) writes from the deposed primary — promoting past
            // it could discard acknowledged rows that exist only in
            // its unfrozen tail. Defer, without backoff: it detaches
            // on its own ttl clock within a tick or two.
            return HaDecision::StoodDown {
                reason: format!(
                    "node {} is still receiving from the deposed holder; \
                     positions not frozen — deferring",
                    mover.node.id
                ),
            };
        }
        let Some(my_pos) = local.pos else {
            return HaDecision::StoodDown {
                reason: "local WAL position unknown; not a candidate".into(),
            };
        };

        // Stability gate — the freeze's second half. A position —
        // local or peer — enters the comparison only after it matched
        // the PREVIOUS tick's sample, and a rival that vanishes
        // between samples defers the comparison a tick rather than
        // silently leaving it. Positions that move mid-candidacy
        // (finding 23's load) and single-tick observation noise
        // (finding 24's investigation) both defer instead of deciding.
        // Costs one confirming tick; arms no backoff.
        let (unsettled, vanished): (Vec<i32>, Vec<i32>) = {
            let mut ts = self.state.lock().unwrap();
            let mut current: std::collections::HashMap<i32, WalPosition> =
                std::collections::HashMap::new();
            current.insert(local_id, my_pos);
            for p in peers {
                if let Some(pos) = p.pos {
                    current.insert(p.node.id, pos);
                }
            }
            let unsettled = current
                .iter()
                .filter(|(id, pos)| ts.settled_pos.get(id) != Some(pos))
                .map(|(id, _)| *id)
                .collect();
            // A rival whose position was sampled LAST tick but is
            // absent THIS tick (status fetch failed) must not silently
            // vanish from the comparison — one transient Err would
            // otherwise hand the CAS to whoever raced past it. Defer
            // exactly one tick: the stale entry is replaced below, so
            // a genuinely dead rival costs one tick of hysteresis,
            // never a livelock.
            let vanished = ts
                .settled_pos
                .keys()
                .filter(|id| **id != local_id && !current.contains_key(id))
                .copied()
                .collect();
            ts.settled_pos = current;
            (unsettled, vanished)
        };
        if !unsettled.is_empty() || !vanished.is_empty() {
            return HaDecision::StoodDown {
                reason: format!(
                    "positions not frozen-and-stable yet (settling: {unsettled:?}, \
                     vanished this tick: {vanished:?}); deferring"
                ),
            };
        }
        // The comparison table, logged in full: every wrong-winner
        // candidacy defect so far (findings 19, 23, 24) hinged on WHAT
        // each node believed at this exact moment, and none of it was
        // recorded.
        info!(
            my = %my_pos,
            peers = ?peers
                .iter()
                .map(|p| {
                    (
                        p.node.id,
                        p.pos.map(|x| x.to_string()),
                        p.receiving,
                        p.running_as_primary,
                    )
                })
                .collect::<Vec<_>>(),
            "candidacy: comparing frozen positions"
        );

        // Most-advanced check against every reachable position.
        let best_other = peers
            .iter()
            .filter_map(|p| p.pos.map(|pos| (p.node.id, pos)))
            .max_by_key(|(id, pos)| (*pos, std::cmp::Reverse(*id)));
        if let Some((best_id, best_pos)) = best_other {
            // >= not >: equal positions are the COMMON case after a
            // clean primary death (all standbys replayed to the same
            // LSN), and they must still funnel into the node-id
            // tiebreak below — otherwise every equal candidate
            // proceeds and only the store CAS separates them.
            // STRICT selection: any reachable peer with strictly more
            // flushed WAL outranks us, byte-for-byte — node id breaks
            // EXACT ties only. The old rule tiebroke within a
            // `max_lag_on_failover` band, which let a lower-id node up
            // to 16 MiB of flush BEHIND win — and under quorum commit
            // an ANY-1-acked write can live exactly in that delta on
            // the higher-flush standby (docs/quorum-commit.md §3-4).
            // The band was also finding 15's structural cause: a loser
            // flushed past the winner's fork point wedges its light
            // follow. Strict-max makes both impossible: loser replay ≤
            // loser flush ≤ winner flush = the fork point. The
            // livelock this trades for — comparing positions that are
            // still MOVING (a fence-less deposed primary keeps feeding
            // the standbys; finding 23) — is closed by the freeze
            // above: by this point every compared position is static.
            // (`max_lag_on_failover` is vestigial here; kept in config
            // for compatibility.)
            if best_pos > my_pos {
                self.arm_backoff(now);
                let reason = match my_pos.lag_behind(&best_pos) {
                    None => format!(
                        "node {best_id} is on a newer timeline ({best_pos} vs {my_pos}); \
                         not a candidate"
                    ),
                    Some(lag) => format!(
                        "node {best_id} has more flushed WAL (ahead by {lag} bytes, \
                         {best_pos} vs {my_pos}); deferring — an acknowledged write may \
                         exist only in that delta"
                    ),
                };
                return HaDecision::StoodDown { reason };
            }
            if best_pos == my_pos && best_id < local_id {
                self.arm_backoff(now);
                return HaDecision::StoodDown {
                    reason: format!(
                        "node {best_id} is flush-equal and has the lower node id; \
                         deferring (tiebreak)"
                    ),
                };
            }
        }

        match self.store.try_takeover(local_id, expected).await {
            Ok(TakeoverOutcome::Won { lease }) => HaDecision::TookOver {
                term: lease.term,
                already_primary: local.is_primary == Some(true),
            },
            Ok(TakeoverOutcome::Lost { current }) => {
                self.arm_backoff(now);
                HaDecision::LostTakeover {
                    current_holder: current.map(|l| l.holder),
                }
            }
            Err(e) => HaDecision::StoodDown {
                reason: format!("takeover proposal failed: {e}"),
            },
        }
    }

    /// How long a fresh lease holder may sit in recovery before the
    /// loop concludes promotion failed and releases.
    ///
    /// Deliberately equal to `leader_ttl` rather than a separate knob:
    /// `leader_ttl` is already the window the *rest* of the cluster
    /// gives a holder before treating it as dead, so matching it means
    /// self-release and others' takeover eligibility mature together.
    /// A longer grace would leave a window where we hold a lease we
    /// cannot use and nobody else may claim; a shorter one risks
    /// releasing during a legitimately slow promotion.
    fn promotion_grace(&self) -> Duration {
        self.timing.leader_ttl
    }

    /// Jittered backoff: `loop_wait + rand(0..loop_wait)`. Enough to
    /// desynchronize candidates that stood down for symmetric reasons.
    fn arm_backoff(&self, now: Instant) {
        let base = self.timing.loop_wait;
        let jitter_ms = if base.as_millis() == 0 {
            0
        } else {
            rand::thread_rng().gen_range(0..base.as_millis() as u64)
        };
        self.state.lock().unwrap().backoff_until =
            Some(now + base + Duration::from_millis(jitter_ms));
    }

    async fn observe_local(&self) -> LocalView {
        let is_primary = match self.db.is_in_recovery().await {
            Ok(in_recovery) => Some(!in_recovery),
            Err(_) => None,
        };
        let pos = if is_primary.is_some() {
            let tl = self.db.timeline_id().await.ok().filter(|t| *t > 0);
            // FLUSH position, matching what peers report in
            // last_flush_lsn: candidacy compares what each node has
            // durably flushed and will replay before promoting
            // (docs/quorum-commit.md §4, finding 19).
            let lsn = self.db.flush_lsn().await.ok().filter(|l| *l > 0);
            match (tl, lsn) {
                (Some(timeline), Some(lsn)) => Some(WalPosition { timeline, lsn }),
                _ => None,
            }
        } else {
            None
        };
        // Standby with an active walreceiver → its flush is moving.
        let receiving = if is_primary == Some(false) {
            self.db
                .replication_lag()
                .await
                .map(|l| !l.state.is_empty())
                .unwrap_or(false)
        } else {
            false
        };
        LocalView {
            is_primary,
            pos,
            receiving,
        }
    }

    async fn observe_peers(&self) -> Vec<PeerView> {
        let local_id = self.pool.local_node_id;
        let others: Vec<NodeConfig> = self
            .pool
            .members
            .iter()
            .filter(|n| n.id != local_id)
            .cloned()
            .collect();
        if others.is_empty() {
            return Vec::new();
        }
        // Per-peer degradation, never collective: an unreachable peer
        // yields ONE PeerView with running_as_primary=false (dead and
        // unreachable are indistinguishable by design — that per-peer
        // equivalence is what lets a real takeover happen), while the
        // peers that answered keep their evidence. The old collective
        // Err → empty-view path made one partitioned peer blind the
        // loop to a healthy holder, and the deposal clock ran on that
        // blindness — see collect_statuses' docs.
        // The fan-out records our own sightings (every node is a
        // potential witness for its peers' candidacy decisions); the
        // rule for what counts lives in `collect_statuses`.
        collect_statuses(
            self.peers.clone(),
            &others,
            STATUS_FANOUT_BUDGET,
            self.peer_seen.as_deref(),
        )
        .await
        .into_iter()
        .map(|v| match v.status {
            Ok(s) => PeerView {
                running_as_primary: s.is_postgres_running && !s.is_in_recovery,
                pos: WalPosition::from_status(&s),
                receiving: s.is_in_recovery && !s.replication_state.is_empty(),
                reachable: true,
                seen_ages: s.peer_primary_seen_age_ms,
                node: v.node,
            },
            Err(_) => PeerView {
                node: v.node,
                running_as_primary: false,
                pos: None,
                receiving: false,
                reachable: false,
                seen_ages: std::collections::HashMap::new(),
            },
        })
        .collect()
    }

    fn log_decision(&self, decision: &HaDecision) {
        let disc = std::mem::discriminant(decision);
        let changed = {
            let mut ts = self.state.lock().unwrap();
            let changed = ts.last_logged != Some(disc);
            ts.last_logged = Some(disc);
            changed
        };
        match decision {
            HaDecision::WouldDemote { .. }
            | HaDecision::TookOver { .. }
            | HaDecision::LostTakeover { .. } => {
                warn!(target: "ha", ?decision, "ha decision");
            }
            _ if changed => {
                info!(target: "ha", ?decision, "ha decision");
            }
            _ => {
                debug!(target: "ha", ?decision, "ha decision");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consensus::InMemoryConsensusStore;
    use crate::peers::PeerClient;
    use async_trait::async_trait;
    use pg_agent_proto::pgagentpb as pb;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, Ordering};
    use std::sync::Mutex as StdMutex;

    // ----- stubs ------------------------------------------------------------

    #[derive(Default)]
    struct StubDb {
        /// None = is_in_recovery errors (PG down / unreachable).
        in_recovery: StdMutex<Option<bool>>,
        timeline: AtomicI32,
        lsn: AtomicU64,
        /// Local walreceiver active — the flush position is moving
        /// (finding 23's candidacy freeze gates on this).
        receiving: AtomicBool,
    }

    impl StubDb {
        fn primary(tl: i32, lsn: u64) -> Self {
            let s = Self::default();
            *s.in_recovery.lock().unwrap() = Some(false);
            s.timeline.store(tl, Ordering::SeqCst);
            s.lsn.store(lsn, Ordering::SeqCst);
            s
        }
        fn standby(tl: i32, lsn: u64) -> Self {
            let s = Self::default();
            *s.in_recovery.lock().unwrap() = Some(true);
            s.timeline.store(tl, Ordering::SeqCst);
            s.lsn.store(lsn, Ordering::SeqCst);
            s
        }
        fn streaming_standby(tl: i32, lsn: u64) -> Self {
            let s = Self::standby(tl, lsn);
            s.receiving.store(true, Ordering::SeqCst);
            s
        }
        fn down() -> Self {
            Self::default()
        }
    }

    #[async_trait]
    impl crate::localdb::LocalDb for StubDb {
        async fn promote(&self) -> anyhow::Result<()> {
            unreachable!("the loop must never touch PG — that is the executor's job")
        }
        async fn slot_active(&self, _: &str) -> anyhow::Result<bool> {
            Ok(false)
        }
        async fn set_synchronous_standby_names(&self, _: &str) -> anyhow::Result<()> {
            Ok(())
        }
        async fn reload_conf(&self) -> anyhow::Result<()> {
            Ok(())
        }
        async fn connected_standby_names(&self) -> anyhow::Result<Vec<String>> {
            Ok(Vec::new())
        }
        async fn checkpoint(&self) -> anyhow::Result<()> {
            unreachable!()
        }
        async fn create_slot(&self, _: &str) -> anyhow::Result<()> {
            unreachable!()
        }
        async fn drop_slot(&self, _: &str) -> anyhow::Result<()> {
            unreachable!()
        }
        async fn is_in_recovery(&self) -> anyhow::Result<bool> {
            self.in_recovery
                .lock()
                .unwrap()
                .ok_or_else(|| anyhow::anyhow!("stub: pg down"))
        }
        async fn timeline_id(&self) -> anyhow::Result<i32> {
            Ok(self.timeline.load(Ordering::SeqCst))
        }
        async fn current_wal_lsn(&self) -> anyhow::Result<u64> {
            Ok(self.lsn.load(Ordering::SeqCst))
        }
        async fn flush_lsn(&self) -> anyhow::Result<u64> {
            Ok(self.lsn.load(Ordering::SeqCst))
        }
        async fn replication_lag(&self) -> anyhow::Result<crate::localdb::ReplicationLag> {
            Ok(crate::localdb::ReplicationLag {
                bytes: 0,
                state: if self.receiving.load(Ordering::SeqCst) {
                    "streaming".into()
                } else {
                    String::new()
                },
            })
        }
        async fn setting(&self, _: &str) -> anyhow::Result<String> {
            unreachable!()
        }
        async fn extension_exists(&self, _: &str) -> anyhow::Result<bool> {
            unreachable!()
        }
        async fn role_exists(&self, _: &str) -> anyhow::Result<bool> {
            unreachable!()
        }
        async fn create_replication_role(&self, _: &str) -> anyhow::Result<()> {
            unreachable!()
        }
    }

    /// Peer answering only get_status from canned per-node state.
    struct CannedPeer {
        status: pb::NodeStatus,
    }

    #[async_trait]
    impl PeerClient for CannedPeer {
        async fn get_status(&self) -> anyhow::Result<pb::NodeStatus> {
            Ok(self.status.clone())
        }
        // Everything below is unreachable: the loop only calls
        // get_status — everything destructive belongs to the executor,
        // which these tests do not attach. A test that strays into a
        // mutation is asserting against a loop no deployment runs, so
        // it fails loudly instead.
        async fn drop_slot(&self, _: &str) -> anyhow::Result<()> {
            unreachable!("the loop only calls get_status")
        }
        async fn create_slot(&self, _: &str) -> anyhow::Result<()> {
            unreachable!("the loop only calls get_status")
        }
        async fn get_node_config(&self) -> anyhow::Result<pb::NodeConfigResponse> {
            unreachable!("the loop only calls get_status")
        }
        async fn start(&self) -> anyhow::Result<()> {
            unreachable!("the loop only calls get_status")
        }
        async fn start_pgpool(&self) -> anyhow::Result<()> {
            unreachable!("the loop only calls get_status")
        }
        async fn attach_node(&self, _: i32, _: i32) -> anyhow::Result<()> {
            unreachable!("the loop only calls get_status")
        }
        async fn stop(&self) -> anyhow::Result<()> {
            unreachable!("the loop only calls get_status")
        }
        async fn rewind(&self, _: crate::pgstandby::RewindOpts) -> anyhow::Result<()> {
            unreachable!("the loop only calls get_status")
        }
        async fn basebackup(&self, _: crate::pgstandby::BasebackupOpts) -> anyhow::Result<()> {
            unreachable!("the loop only calls get_status")
        }
        async fn configure_standby(
            &self,
            _: crate::pgstandby::WriteRecoveryConfOpts,
        ) -> anyhow::Result<()> {
            unreachable!("the loop only calls get_status")
        }
        async fn promote(&self) -> anyhow::Result<()> {
            unreachable!("the loop only calls get_status")
        }
        async fn fetch_wal(
            &self,
            _: &str,
        ) -> anyhow::Result<Option<Box<dyn tokio::io::AsyncRead + Send + Unpin>>> {
            unreachable!("the loop only calls get_status")
        }
    }

    #[derive(Default)]
    struct StubPeers {
        canned: StdMutex<HashMap<i32, pb::NodeStatus>>,
        unreachable: StdMutex<std::collections::HashSet<i32>>,
        all_unreachable: AtomicBool,
    }

    impl StubPeers {
        fn set(&self, id: i32, status: pb::NodeStatus) {
            self.canned.lock().unwrap().insert(id, status);
        }
        fn mark_unreachable(&self, id: i32) {
            self.unreachable.lock().unwrap().insert(id);
        }
    }

    #[async_trait]
    impl crate::peers::PeerRegistry for StubPeers {
        async fn client(&self, node: &NodeConfig) -> anyhow::Result<Arc<dyn PeerClient>> {
            if self.all_unreachable.load(Ordering::SeqCst)
                || self.unreachable.lock().unwrap().contains(&node.id)
            {
                anyhow::bail!("stub: node {} unreachable", node.id);
            }
            let status = self
                .canned
                .lock()
                .unwrap()
                .get(&node.id)
                .cloned()
                .unwrap_or_default();
            Ok(Arc::new(CannedPeer { status }))
        }
        async fn close(&self) -> anyhow::Result<()> {
            Ok(())
        }
    }

    fn primary_status(tl: i32, lsn: u64) -> pb::NodeStatus {
        pb::NodeStatus {
            is_postgres_running: true,
            is_in_recovery: false,
            timeline_id: tl,
            current_wal_lsn: lsn,
            last_flush_lsn: lsn,
            ..Default::default()
        }
    }

    fn standby_status(tl: i32, lsn: u64) -> pb::NodeStatus {
        pb::NodeStatus {
            is_postgres_running: true,
            is_in_recovery: true,
            timeline_id: tl,
            current_wal_lsn: lsn,
            last_flush_lsn: lsn,
            ..Default::default()
        }
    }

    /// A standby whose walreceiver is ACTIVE — its position is moving,
    /// so candidacy must not compare against it (finding 23).
    fn streaming_standby_status(tl: i32, lsn: u64) -> pb::NodeStatus {
        pb::NodeStatus {
            replication_state: "streaming".into(),
            ..standby_status(tl, lsn)
        }
    }

    /// A standby that reports having seen `seen_id` SERVING as primary
    /// `age_ms` ago — a witness for the second-opinion gate
    /// (finding 25).
    fn standby_seeing(tl: i32, lsn: u64, seen_id: i32, age_ms: u64) -> pb::NodeStatus {
        let mut s = standby_status(tl, lsn);
        s.peer_primary_seen_age_ms.insert(seen_id, age_ms);
        s
    }

    fn pool3(local: i32) -> NodePool {
        NodePool {
            members: vec![
                NodeConfig {
                    id: 0,
                    hostname: "db0".into(),
                },
                NodeConfig {
                    id: 1,
                    hostname: "db1".into(),
                },
                NodeConfig {
                    id: 2,
                    hostname: "db2".into(),
                },
            ],
            local_node_id: local,
        }
    }

    fn timing() -> HaTiming {
        HaTiming {
            loop_wait: Duration::from_millis(10),
            retry_timeout: Duration::from_millis(20),
            leader_ttl: Duration::from_millis(50),
        }
    }

    struct Fixture {
        ha: HaLoop,
        store: Arc<InMemoryConsensusStore>,
        peers: Arc<StubPeers>,
        db: Arc<StubDb>,
    }

    fn fixture(local: i32, db: StubDb) -> Fixture {
        let store = Arc::new(InMemoryConsensusStore::new());
        let peers = Arc::new(StubPeers::default());
        let db = Arc::new(db);
        let ha = HaLoop::new(
            store.clone(),
            db.clone(),
            peers.clone(),
            pool3(local),
            timing(),
        );
        Fixture {
            ha,
            store,
            peers,
            db,
        }
    }

    /// Seed the lease the way `ClusterInit` does — one CAS against a
    /// vacant store, committed before the loop ever ticks.
    ///
    /// These tests used to reach this state by letting the loop's first
    /// tick ADOPT the observed primary. Adoption is gone (a node never
    /// writes a lease on another node's behalf), and seeding is what
    /// the cluster actually does, so the fixtures now say so out loud
    /// instead of depending on a code path no deployment ran.
    async fn seed_lease(f: &Fixture, holder: i32) {
        match f.store.try_takeover(holder, None).await {
            Ok(TakeoverOutcome::Won { .. }) => {}
            other => panic!("seeding the lease for node {holder} failed: {other:?}"),
        }
        assert_eq!(f.store.snapshot().lease.unwrap().holder, holder);
    }

    const BASE: u64 = 1 << 32;

    /// Drive a tick through the stability gate: candidacy compares
    /// only positions confirmed by two consecutive samples (finding
    /// 24), so the first candidacy tick after any position change is a
    /// "settling" stand-down. Tests that assert the candidacy OUTCOME
    /// go through this; tests asserting the settling behavior itself
    /// use `tick_once` directly.
    async fn tick_candidacy(ha: &HaLoop) -> HaDecision {
        match ha.tick_once().await {
            HaDecision::StoodDown { reason } if reason.contains("settling") => ha.tick_once().await,
            other => other,
        }
    }

    // ----- scenarios --------------------------------------------------------

    /// A standby seeing a vacant lease with a live primary elsewhere
    /// must never write a lease on that primary's behalf — the primary
    /// claims for itself through the shared store (or ClusterInit
    /// seeds), and until then the honest decision is standing down, not
    /// papering over the bootstrap gap.
    #[tokio::test]
    async fn reports_the_bootstrap_gap_instead_of_adopting_a_primary() {
        let f = fixture(1, StubDb::standby(2, BASE));
        f.peers.set(0, primary_status(2, BASE + 100));
        f.peers.set(2, standby_status(2, BASE));

        match f.ha.tick_once().await {
            HaDecision::StoodDown { reason } => {
                assert!(reason.contains("vacant"), "{reason}");
                assert!(reason.contains("ClusterInit"), "{reason}");
            }
            other => panic!("expected StoodDown, got {other:?}"),
        }
        assert!(
            f.store.snapshot().lease.is_none(),
            "no lease may be written on another node's behalf"
        );

        // And once the primary's own claim lands (as the shared store
        // delivers it), the standby follows normally.
        seed_lease(&f, 0).await;
        assert_eq!(f.ha.tick_once().await, HaDecision::Following { holder: 0 });
    }

    #[tokio::test]
    async fn vacant_leaderless_cluster_most_advanced_standby_takes_over() {
        // No primary anywhere; local (node 1) is the most-advanced standby.
        let f = fixture(1, StubDb::standby(2, BASE + 10_000));
        f.peers.set(2, standby_status(2, BASE));
        f.peers.mark_unreachable(0); // the dead ex-primary

        match tick_candidacy(&f.ha).await {
            HaDecision::TookOver {
                term,
                already_primary,
            } => {
                assert!(term > 0);
                assert!(!already_primary, "standby takeover would promote");
            }
            other => panic!("expected TookOver, got {other:?}"),
        }
        assert_eq!(f.store.snapshot().lease.unwrap().holder, 1);
    }

    #[tokio::test]
    async fn vacant_stands_down_when_peer_far_ahead_then_backs_off() {
        let f = fixture(1, StubDb::standby(2, BASE));
        f.peers.set(2, standby_status(2, BASE + 1_000_000)); // way past max_lag 1024
        f.peers.mark_unreachable(0);

        match tick_candidacy(&f.ha).await {
            HaDecision::StoodDown { reason } => {
                assert!(reason.contains("ahead by"), "{reason}");
            }
            other => panic!("expected StoodDown, got {other:?}"),
        }
        // Immediately after: still inside jittered backoff.
        match f.ha.tick_once().await {
            HaDecision::StoodDown { reason } => assert!(reason.contains("backoff"), "{reason}"),
            other => panic!("expected backoff StoodDown, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn any_strictly_ahead_peer_outranks_regardless_of_id() {
        // Node 1 is a single COMMIT ahead of local node 2. Under
        // quorum commit that byte may be an acknowledged write that
        // exists nowhere else — strict selection defers to it, with no
        // "close enough" band (the old ±max_lag tiebreak let a
        // behind node win and was finding 15's wedge cause).
        let f = fixture(2, StubDb::standby(2, BASE));
        f.peers.set(1, standby_status(2, BASE + 100));
        f.peers.mark_unreachable(0);

        match tick_candidacy(&f.ha).await {
            HaDecision::StoodDown { reason } => {
                assert!(reason.contains("more flushed WAL"), "{reason}")
            }
            other => panic!("expected StoodDown, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn candidacy_compares_flush_not_replay() {
        // Finding 19's shape (docs/quorum-commit.md §4): the peer has
        // every byte FLUSHED (last_flush_lsn current) while its replay
        // position trails far past max_lag. Under the flush key it is
        // position-equal — the node-id tiebreak decides, not the lag
        // gate. Replay-key selection called exactly this node
        // "lagging", and it lost nothing when it won anyway.
        let f = fixture(2, StubDb::standby(2, BASE));
        let mut peer = standby_status(2, BASE); // flush-equal…
        peer.current_wal_lsn = BASE - 900_000; // …replay far behind: irrelevant
        f.peers.set(1, peer);
        f.peers.mark_unreachable(0);

        match tick_candidacy(&f.ha).await {
            HaDecision::StoodDown { reason } => assert!(
                reason.contains("tiebreak"),
                "flush-equal peer must funnel into the tiebreak, not the lag gate: {reason}"
            ),
            other => panic!("expected tiebreak StoodDown, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn strictly_ahead_local_proceeds_regardless_of_id() {
        // Local node 2 has more flushed WAL than lower-id node 1:
        // strict-max wins outright — id matters only at exact
        // equality. (The inverse of this shape — behind-but-lower-id
        // winning — is exactly what the old band allowed.)
        let f = fixture(2, StubDb::standby(2, BASE + 100));
        f.peers.set(1, standby_status(2, BASE));
        f.peers.mark_unreachable(0);

        assert!(matches!(
            tick_candidacy(&f.ha).await,
            HaDecision::TookOver { .. }
        ));
    }

    #[tokio::test]
    async fn vacant_equal_positions_still_tiebreak_by_node_id() {
        // Clean primary death: both standbys replayed to the SAME LSN.
        // Exactly one may proceed — node id decides, even at equality.
        let ahead = fixture(1, StubDb::standby(2, BASE));
        ahead.peers.set(2, standby_status(2, BASE));
        ahead.peers.mark_unreachable(0);
        assert!(matches!(
            tick_candidacy(&ahead.ha).await,
            HaDecision::TookOver { .. }
        ));

        let behind = fixture(2, StubDb::standby(2, BASE));
        behind.peers.set(1, standby_status(2, BASE));
        behind.peers.mark_unreachable(0);
        match tick_candidacy(&behind.ha).await {
            HaDecision::StoodDown { reason } => {
                assert!(reason.contains("tiebreak"), "{reason}")
            }
            other => panic!("expected tiebreak StoodDown, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn local_primary_claims_vacant_lease_for_itself() {
        let f = fixture(0, StubDb::primary(2, BASE + 500));
        f.peers.set(1, standby_status(2, BASE));
        f.peers.set(2, standby_status(2, BASE));

        match tick_candidacy(&f.ha).await {
            HaDecision::TookOver {
                already_primary, ..
            } => assert!(already_primary, "claiming for the primary we already are"),
            other => panic!("expected TookOver, got {other:?}"),
        }
        // Steady state thereafter.
        match f.ha.tick_once().await {
            HaDecision::RetainedLease { .. } => {}
            other => panic!("expected RetainedLease, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn fresh_holder_in_recovery_awaits_promotion_before_releasing() {
        // Seed: we (node 0) hold the lease, but local PG is a standby —
        // the state right after a takeover, since pg_promote() is
        // asynchronous. The loop must WAIT, not thrash the lease.
        let f = fixture(0, StubDb::standby(2, BASE));
        f.store.try_takeover(0, None).await.unwrap();
        f.peers.set(1, primary_status(2, BASE + 10));

        match f.ha.tick_once().await {
            HaDecision::AwaitingPromotion { .. } => {}
            other => panic!("expected AwaitingPromotion, got {other:?}"),
        }
        assert!(
            f.store.snapshot().lease.is_some(),
            "lease must be held through the promotion grace window"
        );

        // Past the grace window (leader_ttl = 50ms), promotion clearly
        // did not take effect: release so a real primary can claim it.
        tokio::time::sleep(Duration::from_millis(60)).await;
        match f.ha.tick_once().await {
            HaDecision::WouldDemote { reason } => {
                assert!(reason.contains("promotion did not take effect"), "{reason}");
            }
            other => panic!("expected WouldDemote, got {other:?}"),
        }
        assert!(
            f.store.snapshot().lease.is_none(),
            "lease must be released once promotion is deemed failed"
        );
    }

    #[tokio::test]
    async fn takeover_does_not_thrash_the_lease() {
        // Regression for the flapping the docker acceptance suite found:
        // take over → (no promotion happens) → the next ticks must NOT
        // release-and-retake on every cycle.
        let f = fixture(1, StubDb::standby(2, BASE + 10_000));
        f.peers.set(2, standby_status(2, BASE));
        f.peers.mark_unreachable(0);

        assert!(matches!(
            tick_candidacy(&f.ha).await,
            HaDecision::TookOver { .. }
        ));
        let term = f.store.snapshot().lease.unwrap().term;
        for _ in 0..3 {
            match f.ha.tick_once().await {
                HaDecision::AwaitingPromotion { .. } => {}
                other => panic!("expected AwaitingPromotion, got {other:?}"),
            }
        }
        assert_eq!(
            f.store.snapshot().lease.unwrap().term,
            term,
            "term must not churn while awaiting promotion"
        );
    }

    #[tokio::test]
    async fn store_unknown_is_not_vacant_and_escalates_only_for_holder() {
        // Non-holder: unknown store → StoreUnknown forever, never demote.
        let f = fixture(1, StubDb::standby(2, BASE));
        f.store.set_unavailable(true);
        assert!(matches!(
            f.ha.tick_once().await,
            HaDecision::StoreUnknown { .. }
        ));
        tokio::time::sleep(Duration::from_millis(30)).await; // > retry_timeout
        assert!(matches!(
            f.ha.tick_once().await,
            HaDecision::StoreUnknown { .. }
        ));

        // Holder: unknown past retry_timeout → WouldDemote.
        let f = fixture(0, StubDb::primary(2, BASE));
        f.peers.set(1, standby_status(2, BASE));
        f.peers.set(2, standby_status(2, BASE));
        assert!(matches!(
            tick_candidacy(&f.ha).await,
            HaDecision::TookOver { .. }
        ));
        f.store.set_unavailable(true);
        assert!(matches!(
            f.ha.tick_once().await,
            HaDecision::StoreUnknown { .. } // within grace
        ));
        tokio::time::sleep(Duration::from_millis(30)).await; // > retry_timeout 20ms
        match f.ha.tick_once().await {
            HaDecision::WouldDemote { reason } => {
                assert!(reason.contains("quorum"), "{reason}");
            }
            other => panic!("expected WouldDemote, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn dead_holder_watched_until_ttl_then_taken_over() {
        // Node 0 holds the seeded lease, then dies. Local node 1 is
        // the best surviving standby.
        let f = fixture(1, StubDb::standby(2, BASE + 100));
        f.peers.set(0, primary_status(2, BASE + 200));
        f.peers.set(2, standby_status(2, BASE));
        seed_lease(&f, 0).await;

        // Holder dies.
        f.peers.mark_unreachable(0);
        match f.ha.tick_once().await {
            HaDecision::HolderUnhealthy { holder: 0, .. } => {}
            other => panic!("expected HolderUnhealthy, got {other:?}"),
        }
        // Past leader_ttl (50ms): candidacy fires and wins.
        tokio::time::sleep(Duration::from_millis(60)).await;
        match tick_candidacy(&f.ha).await {
            HaDecision::TookOver {
                already_primary, ..
            } => assert!(!already_primary),
            other => panic!("expected TookOver, got {other:?}"),
        }
        assert_eq!(f.store.snapshot().lease.unwrap().holder, 1);
    }

    /// Finding 23, the fence-less deposal's livelock: the holder's
    /// AGENT is dead but its PostgreSQL keeps serving, so the standbys
    /// keep STREAMING and their flush positions keep moving — and a
    /// moving position must never enter a candidacy comparison. The
    /// candidate first detaches (DetachingFromDeposed → the executor
    /// stops the walreceiver), and only a frozen local position
    /// proceeds.
    #[tokio::test]
    async fn candidacy_detaches_to_freeze_before_comparing() {
        let f = fixture(1, StubDb::streaming_standby(2, BASE + 100));
        f.peers.set(0, primary_status(2, BASE + 200));
        f.peers.set(2, standby_status(2, BASE));
        seed_lease(&f, 0).await;

        // The holder's agent dies (its PostgreSQL may well still be
        // serving — that is exactly why the receiver still streams).
        f.peers.mark_unreachable(0);
        match f.ha.tick_once().await {
            HaDecision::HolderUnhealthy { holder: 0, .. } => {}
            other => panic!("expected HolderUnhealthy, got {other:?}"),
        }
        tokio::time::sleep(Duration::from_millis(60)).await;
        // Still receiving → detach, do NOT compare.
        assert_eq!(
            f.ha.tick_once().await,
            HaDecision::DetachingFromDeposed { holder: 0 }
        );
        // The executor's detach lands; the position is frozen now.
        f.db.receiving.store(false, Ordering::SeqCst);
        match tick_candidacy(&f.ha).await {
            HaDecision::TookOver {
                already_primary, ..
            } => assert!(!already_primary),
            other => panic!("expected TookOver after freeze, got {other:?}"),
        }
        assert_eq!(f.store.snapshot().lease.unwrap().holder, 1);
    }

    /// The other half of the freeze: a frozen candidate must not
    /// compare against a RIVAL that is still receiving — the rival's
    /// unfrozen tail may hold ANY-1-acked rows that would be lost by
    /// promoting past it. Defer (no backoff — the rival detaches on
    /// its own ttl clock), then proceed once every position is frozen.
    #[tokio::test]
    async fn candidacy_defers_while_a_rival_is_still_receiving() {
        let f = fixture(1, StubDb::standby(2, BASE + 100));
        f.peers.set(0, primary_status(2, BASE + 200));
        f.peers.set(2, streaming_standby_status(2, BASE));
        seed_lease(&f, 0).await;
        f.peers.mark_unreachable(0);
        let _ = f.ha.tick_once().await; // start the unhealthy clock
        tokio::time::sleep(Duration::from_millis(60)).await;
        match f.ha.tick_once().await {
            HaDecision::StoodDown { reason } => {
                assert!(reason.contains("still receiving"), "{reason}");
            }
            other => panic!("expected StoodDown, got {other:?}"),
        }
        // The rival detached (its report freezes) — now compare and win.
        f.peers.set(2, standby_status(2, BASE));
        match tick_candidacy(&f.ha).await {
            HaDecision::TookOver {
                already_primary, ..
            } => assert!(!already_primary),
            other => panic!("expected TookOver, got {other:?}"),
        }
    }

    /// The second-opinion gate (finding 25). Local node 1 cannot see
    /// holder 0 and has watched it "die" for a full ttl — but node 2
    /// answers and reports having reached node 0 well inside the ttl
    /// (10ms against this fixture's 50ms). One node's blindness is not
    /// the cluster's verdict: the holder keeps its lease, and the
    /// healthy primary is never deposed.
    #[tokio::test]
    async fn a_blind_candidate_defers_to_a_witness_that_still_sees_the_holder() {
        let f = fixture(1, StubDb::standby(2, BASE + 500));
        f.peers.set(0, primary_status(2, BASE));
        f.peers.set(2, standby_status(2, BASE));
        seed_lease(&f, 0).await;

        // The holder becomes unreachable TO US only; node 2 still sees
        // it (and says so, freshly).
        f.peers.mark_unreachable(0);
        f.peers.set(2, standby_seeing(2, BASE, 0, 10));
        let _ = f.ha.tick_once().await; // unhealthy clock starts
        tokio::time::sleep(Duration::from_millis(60)).await; // past ttl (50ms)
        match f.ha.tick_once().await {
            HaDecision::StoodDown { reason } => {
                assert!(reason.contains("blindness is local"), "{reason}");
                assert!(reason.contains("node 2 saw holder 0 SERVING"), "{reason}");
            }
            other => panic!("expected the second-opinion stand-down, got {other:?}"),
        }
        assert_eq!(
            f.store.snapshot().lease.unwrap().holder,
            0,
            "a healthy holder must keep its lease"
        );
    }

    /// The gate is self-clearing: once the witness's own contact with
    /// the holder ages past the ttl, nobody can still see it and the
    /// takeover proceeds. A genuinely dead holder costs at most one
    /// extra ttl, never a deadlock.
    #[tokio::test]
    async fn the_gate_opens_once_no_witness_has_seen_the_holder_either() {
        let f = fixture(1, StubDb::standby(2, BASE + 500));
        f.peers.set(0, primary_status(2, BASE));
        f.peers.set(2, standby_status(2, BASE));
        seed_lease(&f, 0).await;
        f.peers.mark_unreachable(0);
        // The witness last reached the holder LONGER ago than the ttl:
        // its evidence has expired too, so the cluster agrees.
        f.peers.set(2, standby_seeing(2, BASE, 0, 10_000));
        let _ = f.ha.tick_once().await;
        tokio::time::sleep(Duration::from_millis(60)).await;
        match tick_candidacy(&f.ha).await {
            HaDecision::TookOver {
                already_primary, ..
            } => assert!(!already_primary),
            other => panic!("expected TookOver once no witness sees the holder, got {other:?}"),
        }
        assert_eq!(f.store.snapshot().lease.unwrap().holder, 1);
    }

    /// The gate must not confuse ANSWERING with SERVING. When a
    /// holder's PostgreSQL dies, its agent keeps answering GetStatus
    /// perfectly and every peer keeps reaching it — so a witness whose
    /// last *primary* sighting has aged out must not vouch for it,
    /// however recently it was contacted. Regression for the first cut
    /// of this gate, which recorded reachability and thereby blocked
    /// the single most common failover in the suite (G3) forever.
    #[tokio::test]
    async fn a_reachable_but_dead_primary_gets_no_witness() {
        let f = fixture(1, StubDb::standby(2, BASE + 500));
        f.peers.set(0, primary_status(2, BASE));
        f.peers.set(2, standby_status(2, BASE));
        seed_lease(&f, 0).await;
        // The holder's PostgreSQL dies; its AGENT stays up and
        // reachable to everyone, including us.
        f.peers.set(
            0,
            pb::NodeStatus {
                is_postgres_running: false,
                ..primary_status(2, BASE)
            },
        );
        // The witness reaches the holder constantly, but its last
        // sighting of it SERVING is older than the ttl.
        f.peers.set(2, standby_seeing(2, BASE, 0, 10_000));
        let _ = f.ha.tick_once().await;
        tokio::time::sleep(Duration::from_millis(60)).await;
        match tick_candidacy(&f.ha).await {
            HaDecision::TookOver {
                already_primary, ..
            } => assert!(!already_primary),
            other => panic!("a dead-but-reachable holder must be deposable, got {other:?}"),
        }
        assert_eq!(f.store.snapshot().lease.unwrap().holder, 1);
    }

    /// A witness that cannot be reached offers no opinion — its stale
    /// map must not veto a takeover, or one unreachable bystander
    /// would freeze every failover.
    #[tokio::test]
    async fn an_unreachable_witness_cannot_veto_a_takeover() {
        let f = fixture(1, StubDb::standby(2, BASE + 500));
        f.peers.set(0, primary_status(2, BASE));
        f.peers.set(2, standby_seeing(2, BASE, 0, 10)); // fresh, but…
        seed_lease(&f, 0).await;
        f.peers.mark_unreachable(0);
        f.peers.mark_unreachable(2); // …we cannot ask it
        let _ = f.ha.tick_once().await;
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert!(matches!(
            tick_candidacy(&f.ha).await,
            HaDecision::TookOver { .. }
        ));
    }

    /// Finding 24's dip: a just-detached rival's flush REPORT drops to
    /// its replay position (pg_last_wal_receive_lsn() nulls with the
    /// walreceiver) and climbs back while replay drains the local WAL
    /// tail. Sampling the dip crowned a flush-lagging candidate over
    /// the true maximum — G7 lost an acknowledged write to it. The
    /// stability gate defers while ANY compared position moved since
    /// the previous tick, then the true maximum wins.
    #[tokio::test]
    async fn candidacy_defers_while_a_position_is_still_moving() {
        let f = fixture(1, StubDb::standby(3, BASE));
        f.peers.set(0, primary_status(3, BASE + 200));
        f.peers.set(2, standby_status(3, BASE + 50));
        seed_lease(&f, 0).await;
        f.peers.mark_unreachable(0);
        let _ = f.ha.tick_once().await; // unhealthy clock starts
        tokio::time::sleep(Duration::from_millis(60)).await;
        match f.ha.tick_once().await {
            HaDecision::StoodDown { reason } => assert!(reason.contains("settling"), "{reason}"),
            other => panic!("expected settling StoodDown, got {other:?}"),
        }
        // The rival's report climbs (replay draining its tail): the
        // gate must keep deferring — the sampled value is not truth.
        f.peers.set(2, standby_status(3, BASE + 150));
        match f.ha.tick_once().await {
            HaDecision::StoodDown { reason } => assert!(reason.contains("settling"), "{reason}"),
            other => panic!("expected settling StoodDown, got {other:?}"),
        }
        // Stable across two ticks — and the true maximum outranks us.
        match f.ha.tick_once().await {
            HaDecision::StoodDown { reason } => {
                assert!(reason.contains("more flushed WAL"), "{reason}")
            }
            other => panic!("expected flush-defer StoodDown, got {other:?}"),
        }
    }

    /// The unhealthy clock is each holder's, not the lease's. Watching
    /// a dead holder past ttl earns candidacy against THAT holder; if
    /// someone else wins the race, the new holder gets a fresh ttl —
    /// the clock must not carry over. Regression for acceptance
    /// finding 13: a rival deposed a 7-second-old lease during the R4
    /// partition because its clock had been running against the
    /// previous holder, voiding exactly the hysteresis window a fresh
    /// winner needs to finish its (asynchronous) promotion.
    #[tokio::test]
    async fn a_new_holder_does_not_inherit_its_predecessors_unhealthy_clock() {
        let f = fixture(1, StubDb::standby(2, BASE + 100));
        f.peers.set(0, primary_status(2, BASE + 200));
        f.peers.set(2, standby_status(2, BASE));
        seed_lease(&f, 0).await;

        // Holder 0 dies; local watches it well past leader_ttl (50ms).
        f.peers.mark_unreachable(0);
        f.ha.tick_once().await;
        tokio::time::sleep(Duration::from_millis(60)).await;

        // Before local's next tick, node 2 wins the takeover race (as
        // the real store allows). Node 2 is a standby mid-promotion:
        // not yet running_as_primary, i.e. "unhealthy" to the watch.
        let cur = f.store.snapshot().lease.unwrap();
        match f
            .store
            .try_takeover(2, Some((cur.holder, cur.term)))
            .await
            .unwrap()
        {
            TakeoverOutcome::Won { .. } => {}
            other => panic!("arrangement takeover lost: {other:?}"),
        }

        // Local's tick sees the NEW holder. With an inherited clock it
        // would consider candidacy immediately (>50ms already elapsed —
        // against the wrong holder). It must instead start watching
        // node 2 from zero.
        match f.ha.tick_once().await {
            HaDecision::HolderUnhealthy {
                holder: 2,
                unhealthy_for,
            } => {
                assert!(
                    unhealthy_for < Duration::from_millis(50),
                    "clock carried over from holder 0: {unhealthy_for:?}"
                );
            }
            other => panic!("expected a fresh HolderUnhealthy watch on node 2, got {other:?}"),
        }
        assert_eq!(
            f.store.snapshot().lease.unwrap().holder,
            2,
            "the 7-second-old lease must survive local's tick"
        );

        // Once node 2 has been unhealthy for ITS OWN ttl, candidacy is
        // legitimate again.
        tokio::time::sleep(Duration::from_millis(60)).await;
        match tick_candidacy(&f.ha).await {
            HaDecision::TookOver { .. } => {}
            other => panic!("expected TookOver after a full ttl on the new holder, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn holder_change_yields_would_follow_new_holder() {
        let f = fixture(2, StubDb::standby(2, BASE));
        f.peers.set(0, primary_status(2, BASE + 10));
        f.peers.set(1, standby_status(2, BASE));
        seed_lease(&f, 0).await;
        assert_eq!(f.ha.tick_once().await, HaDecision::Following { holder: 0 });

        // Simulate the lease moving to node 1 (as the real store would
        // after a takeover elsewhere).
        let cur = f.store.snapshot().lease.unwrap();
        f.store
            .try_takeover(1, Some((cur.holder, cur.term)))
            .await
            .unwrap();
        f.peers.set(0, standby_status(3, BASE + 10));
        f.peers.set(1, primary_status(3, BASE + 20));

        assert_eq!(
            f.ha.tick_once().await,
            HaDecision::WouldFollowNewHolder { prev: 0, holder: 1 }
        );
        assert_eq!(f.ha.tick_once().await, HaDecision::Following { holder: 1 });
    }

    #[tokio::test]
    async fn paused_short_circuits_everything() {
        let f = fixture(1, StubDb::standby(2, BASE));
        f.store
            .set_paused(Some(crate::consensus::Paused {
                reason: "maintenance".into(),
                set_by: "op".into(),
                at: chrono::Utc::now(),
            }))
            .await
            .unwrap();
        assert_eq!(f.ha.tick_once().await, HaDecision::Paused);
    }

    #[tokio::test]
    async fn split_brain_observation_stands_down() {
        // Two nodes both claim primary; the loop refuses any decision.
        let f = fixture(2, StubDb::standby(2, BASE));
        f.peers.set(0, primary_status(2, BASE + 10));
        f.peers.set(1, primary_status(2, BASE + 20));

        match f.ha.tick_once().await {
            HaDecision::StoodDown { reason } => {
                assert!(reason.contains("multiple nodes"), "{reason}");
            }
            other => panic!("expected StoodDown, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn pg_down_locally_is_never_a_candidate() {
        let f = fixture(1, StubDb::down());
        f.peers.mark_unreachable(0);
        f.peers.set(2, standby_status(2, BASE));

        match f.ha.tick_once().await {
            HaDecision::StoodDown { reason } => {
                assert!(reason.contains("state unknown"), "{reason}");
            }
            other => panic!("expected StoodDown, got {other:?}"),
        }
    }
}
