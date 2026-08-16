//! The HA loop — shadow mode (docs/promotion-authority.md §"The HA
//! loop", sequencing step 5).
//!
//! Every `loop_wait` the loop performs one *tick*: read the lease from
//! the [`ConsensusStore`], observe local PostgreSQL and every peer, and
//! produce exactly one [`HaDecision`]. This is the daemon's first
//! non-reactive role logic — it runs on standbys too, because a standby
//! is what detects a dead holder and becomes a candidate.
//!
//! # Shadow mode, enforced by construction
//!
//! The current implementation computes decisions and logs them; it
//! **cannot** act on PostgreSQL. This is not a runtime flag — the
//! struct holds no `Systemd`, no `Pcp`, no `StandbyOps`, and
//! never dials a peer mutation RPC — unless an executor is attached
//! ([`HaLoop::with_executor`], the step-7 cutover switch), in which
//! case every decision is handed to [`crate::roleexec::RoleExecutor`]
//! after logging. Without one, the loop's only writes go to the
//! [`ConsensusStore`], which today is the process-local
//! [`InMemoryConsensusStore`](crate::consensus::InMemoryConsensusStore)
//! — private bookkeeping, authoritative for
//! nothing. At cutover (step 7) the decisions gain executors and this
//! module's docs change; until then a bug here can mislead a log reader
//! and nothing else.
//!
//! # How the decisions are judged
//!
//! **Not against pgpool.** The logged stream (target `ha_shadow`) is
//! asserted against ground truth — which node actually held the most
//! WAL, whether the announced-dead node was actually dead, whether
//! exactly one node became promotable — in the dockerized acceptance
//! suite (`testing/`), where those facts are manufactured rather than
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
//!
//! # Shadow-only adoption
//!
//! With a process-local store the lease starts vacant even though the
//! cluster has a working primary. To make the decision stream
//! meaningful, a vacant tick that observes **exactly one** node running
//! as primary adopts it into the local store
//! ([`HaDecision::AdoptedObservedPrimary`]) — thereafter the loop
//! exercises the real branches (follow / holder-unhealthy / candidacy)
//! against observed reality. Adoption is a shadow artifact: at cutover
//! the lease is seeded once by `ClusterInit`, not inferred, and this
//! branch is removed with the mode.

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
    pub max_lag_on_failover: u64,
}

/// One tick's outcome. Exactly one per tick; the `run` loop logs them
/// (deduplicated by variant) on the `ha_shadow` target.
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
    /// Candidacy considered and declined; the reason says why
    /// (ineligible, not most-advanced, tiebreak, backoff, …).
    StoodDown { reason: String },
    /// Our takeover CAS won. `already_primary` distinguishes "claiming
    /// the lease for the primary we already are" (adoption of self,
    /// no PG action implied) from "would promote local PostgreSQL".
    TookOver { term: u64, already_primary: bool },
    /// Our takeover CAS lost — someone else moved first. Back off.
    LostTakeover { current_holder: Option<i32> },
    /// Shadow-only: vacant lease + exactly one observed primary
    /// elsewhere → recorded it as holder in the local store.
    AdoptedObservedPrimary { node: i32 },
}

/// What the loop knows about local PostgreSQL this tick.
struct LocalView {
    /// `Some(true)` = running as primary, `Some(false)` = in recovery
    /// (standby), `None` = unknown (query failed / PG down).
    is_primary: Option<bool>,
    pos: Option<WalPosition>,
}

/// What the loop knows about one peer this tick.
struct PeerView {
    node: NodeConfig,
    /// Reachable and reported running && !in_recovery.
    running_as_primary: bool,
    pos: Option<WalPosition>,
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
}

pub struct HaLoop {
    store: Arc<dyn ConsensusStore>,
    db: Arc<dyn LocalDb>,
    peers: Arc<dyn PeerRegistry>,
    pool: NodePool,
    timing: HaTiming,
    state: Mutex<TickState>,
    /// `Some` = execute mode: every tick's decision is handed to the
    /// executor after logging. `None` = shadow — the decision stream
    /// is the entire output, which is the structural guarantee shadow
    /// mode has always rested on, now expressed as this field's
    /// absence.
    executor: Option<Arc<crate::roleexec::RoleExecutor>>,
    /// Shadow-only vacant-lease adoption (see the module docs). Off in
    /// execute mode: with a shared store the primary claims the lease
    /// for itself (`TookOver { already_primary: true }`) and everyone
    /// else reads it — adoption existed for per-node stores where each
    /// standby had to seed its own private view, and promotion-authority
    /// step 7 removes it from the real path. `ClusterInit` seeding is
    /// the deterministic bootstrap.
    vacant_adoption: bool,
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
            }),
            executor: None,
            vacant_adoption: true,
        }
    }

    /// Attach the executor: every decision is now acted on, and
    /// shadow-only vacant adoption turns off. This is the cutover
    /// switch — a loop without this call can only ever write to its
    /// store and its log.
    pub fn with_executor(mut self, executor: Arc<crate::roleexec::RoleExecutor>) -> Self {
        self.executor = Some(executor);
        self.vacant_adoption = false;
        self
    }

    /// Test-only: execute mode's adoption gating without an executor.
    #[cfg(test)]
    fn without_vacant_adoption(mut self) -> Self {
        self.vacant_adoption = false;
        self
    }

    /// Continuous loop: tick every `loop_wait` until shutdown. Decisions
    /// are logged on the `ha_shadow` target — info on variant change,
    /// debug on repeats, warn for the destructive-would-be decisions.
    pub async fn run(self: Arc<Self>, shutdown: CancellationToken) {
        info!(
            loop_wait_secs = self.timing.loop_wait.as_secs(),
            "ha loop (shadow): starting"
        );
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    info!("ha loop (shadow): stopping");
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
                // Vacant. Shadow-only adoption first: if the cluster
                // observably has exactly one primary, record it rather
                // than treating a working cluster as leaderless.
                let mut observed: Vec<i32> = peers
                    .iter()
                    .filter(|p| p.running_as_primary)
                    .map(|p| p.node.id)
                    .collect();
                if local.is_primary == Some(true) {
                    observed.push(local_id);
                }
                match observed.as_slice() {
                    [only] if *only != local_id && !self.vacant_adoption => {
                        // Execute mode: never write a lease on another
                        // node's behalf. The primary claims for itself
                        // (or ClusterInit seeds), and until one of those
                        // happens a vacant lease with a live primary is
                        // a bootstrap gap to report, not to paper over.
                        HaDecision::StoodDown {
                            reason: format!(
                                "lease vacant but node {only} runs as primary; \
                                 waiting for it to claim (or ClusterInit to seed)"
                            ),
                        }
                    }
                    [only] if *only != local_id => {
                        let node = *only;
                        match self.store.try_takeover(node, None).await {
                            Ok(TakeoverOutcome::Won { .. }) => {
                                HaDecision::AdoptedObservedPrimary { node }
                            }
                            Ok(TakeoverOutcome::Lost { current }) => HaDecision::LostTakeover {
                                current_holder: current.map(|l| l.holder),
                            },
                            Err(e) => HaDecision::StoodDown {
                                reason: format!("adoption write failed: {e}"),
                            },
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
                HaDecision::AdoptedObservedPrimary { node } => Some(*node),
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
        let Some(my_pos) = local.pos else {
            return HaDecision::StoodDown {
                reason: "local WAL position unknown; not a candidate".into(),
            };
        };

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
            // loser flush ≤ winner flush = the fork point. No livelock
            // risk in exchange — candidacy runs against a dead
            // primary, so flush positions are static while it decides.
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
        LocalView { is_primary, pos }
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
        collect_statuses(self.peers.clone(), &others, STATUS_FANOUT_BUDGET)
            .await
            .into_iter()
            .map(|v| match v.status {
                Ok(s) => PeerView {
                    running_as_primary: s.is_postgres_running && !s.is_in_recovery,
                    pos: WalPosition::from_status(&s),
                    node: v.node,
                },
                Err(_) => PeerView {
                    node: v.node,
                    running_as_primary: false,
                    pos: None,
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
                warn!(target: "ha_shadow", ?decision, "ha shadow decision");
            }
            _ if changed => {
                info!(target: "ha_shadow", ?decision, "ha shadow decision");
            }
            _ => {
                debug!(target: "ha_shadow", ?decision, "ha shadow decision");
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
        fn down() -> Self {
            Self::default()
        }
    }

    #[async_trait]
    impl crate::localdb::LocalDb for StubDb {
        async fn promote(&self) -> anyhow::Result<()> {
            unreachable!("shadow loop must never touch PG")
        }
        async fn slot_active(&self, _: &str) -> anyhow::Result<bool> {
            Ok(false)
        }
        async fn set_synchronous_standby_names(&self, _: &str) -> anyhow::Result<()> {
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
            unreachable!()
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
        // Everything below is unreachable: the shadow loop only calls
        // get_status, and a test that strays fails loudly.
        async fn drop_slot(&self, _: &str) -> anyhow::Result<()> {
            unreachable!("shadow loop only calls get_status")
        }
        async fn create_slot(&self, _: &str) -> anyhow::Result<()> {
            unreachable!("shadow loop only calls get_status")
        }
        async fn get_node_config(&self) -> anyhow::Result<pb::NodeConfigResponse> {
            unreachable!("shadow loop only calls get_status")
        }
        async fn start(&self) -> anyhow::Result<()> {
            unreachable!("shadow loop only calls get_status")
        }
        async fn start_pgpool(&self) -> anyhow::Result<()> {
            unreachable!("shadow loop only calls get_status")
        }
        async fn attach_node(&self, _: i32, _: i32) -> anyhow::Result<()> {
            unreachable!("shadow loop only calls get_status")
        }
        async fn stop(&self) -> anyhow::Result<()> {
            unreachable!("shadow loop only calls get_status")
        }
        async fn rewind(&self, _: crate::pgstandby::RewindOpts) -> anyhow::Result<()> {
            unreachable!("shadow loop only calls get_status")
        }
        async fn basebackup(&self, _: crate::pgstandby::BasebackupOpts) -> anyhow::Result<()> {
            unreachable!("shadow loop only calls get_status")
        }
        async fn configure_standby(
            &self,
            _: crate::pgstandby::WriteRecoveryConfOpts,
        ) -> anyhow::Result<()> {
            unreachable!("shadow loop only calls get_status")
        }
        async fn promote(&self) -> anyhow::Result<()> {
            unreachable!("shadow loop only calls get_status")
        }
        async fn fetch_wal(
            &self,
            _: &str,
        ) -> anyhow::Result<Option<Box<dyn tokio::io::AsyncRead + Send + Unpin>>> {
            unreachable!("shadow loop only calls get_status")
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
            max_lag_on_failover: 1024,
        }
    }

    struct Fixture {
        ha: HaLoop,
        store: Arc<InMemoryConsensusStore>,
        peers: Arc<StubPeers>,
    }

    fn fixture(local: i32, db: StubDb) -> Fixture {
        let store = Arc::new(InMemoryConsensusStore::new());
        let peers = Arc::new(StubPeers::default());
        let ha = HaLoop::new(
            store.clone(),
            Arc::new(db),
            peers.clone(),
            pool3(local),
            timing(),
        );
        Fixture { ha, store, peers }
    }

    const BASE: u64 = 1 << 32;

    // ----- scenarios --------------------------------------------------------

    #[tokio::test]
    async fn adopts_single_observed_primary_then_follows() {
        // Local (node 1) is a standby; node 0 is the observed primary.
        let f = fixture(1, StubDb::standby(2, BASE));
        f.peers.set(0, primary_status(2, BASE + 100));
        f.peers.set(2, standby_status(2, BASE));

        assert_eq!(
            f.ha.tick_once().await,
            HaDecision::AdoptedObservedPrimary { node: 0 }
        );
        assert_eq!(f.store.snapshot().lease.unwrap().holder, 0);
        assert_eq!(f.ha.tick_once().await, HaDecision::Following { holder: 0 });
    }

    /// Execute mode: adoption is off. A standby seeing a vacant lease
    /// with a live primary elsewhere must never write a lease on that
    /// primary's behalf — the primary claims for itself through the
    /// shared store (or ClusterInit seeds), and until then the honest
    /// decision is standing down, not papering over the bootstrap gap.
    #[tokio::test]
    async fn execute_mode_reports_the_bootstrap_gap_instead_of_adopting() {
        let f = fixture(1, StubDb::standby(2, BASE));
        let ha = HaLoop::new(
            f.store.clone(),
            Arc::new(StubDb::standby(2, BASE)),
            f.peers.clone(),
            pool3(1),
            timing(),
        )
        .without_vacant_adoption();
        f.peers.set(0, primary_status(2, BASE + 100));
        f.peers.set(2, standby_status(2, BASE));

        match ha.tick_once().await {
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
        f.store.try_takeover(0, None).await.unwrap();
        assert_eq!(ha.tick_once().await, HaDecision::Following { holder: 0 });
    }

    #[tokio::test]
    async fn vacant_leaderless_cluster_most_advanced_standby_takes_over() {
        // No primary anywhere; local (node 1) is the most-advanced standby.
        let f = fixture(1, StubDb::standby(2, BASE + 10_000));
        f.peers.set(2, standby_status(2, BASE));
        f.peers.mark_unreachable(0); // the dead ex-primary

        match f.ha.tick_once().await {
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

        match f.ha.tick_once().await {
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

        match f.ha.tick_once().await {
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

        match f.ha.tick_once().await {
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
            f.ha.tick_once().await,
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
            ahead.ha.tick_once().await,
            HaDecision::TookOver { .. }
        ));

        let behind = fixture(2, StubDb::standby(2, BASE));
        behind.peers.set(1, standby_status(2, BASE));
        behind.peers.mark_unreachable(0);
        match behind.ha.tick_once().await {
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

        match f.ha.tick_once().await {
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
            f.ha.tick_once().await,
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
            f.ha.tick_once().await,
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
        // Node 0 held the lease (adopted), then dies. Local node 1 is
        // the best surviving standby.
        let f = fixture(1, StubDb::standby(2, BASE + 100));
        f.peers.set(0, primary_status(2, BASE + 200));
        f.peers.set(2, standby_status(2, BASE));
        assert_eq!(
            f.ha.tick_once().await,
            HaDecision::AdoptedObservedPrimary { node: 0 }
        );

        // Holder dies.
        f.peers.mark_unreachable(0);
        match f.ha.tick_once().await {
            HaDecision::HolderUnhealthy { holder: 0, .. } => {}
            other => panic!("expected HolderUnhealthy, got {other:?}"),
        }
        // Past leader_ttl (50ms): candidacy fires and wins.
        tokio::time::sleep(Duration::from_millis(60)).await;
        match f.ha.tick_once().await {
            HaDecision::TookOver {
                already_primary, ..
            } => assert!(!already_primary),
            other => panic!("expected TookOver, got {other:?}"),
        }
        assert_eq!(f.store.snapshot().lease.unwrap().holder, 1);
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
        assert_eq!(
            f.ha.tick_once().await,
            HaDecision::AdoptedObservedPrimary { node: 0 }
        );

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
        match f.ha.tick_once().await {
            HaDecision::TookOver { .. } => {}
            other => panic!("expected TookOver after a full ttl on the new holder, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn holder_change_yields_would_follow_new_holder() {
        let f = fixture(2, StubDb::standby(2, BASE));
        f.peers.set(0, primary_status(2, BASE + 10));
        f.peers.set(1, standby_status(2, BASE));
        assert_eq!(
            f.ha.tick_once().await,
            HaDecision::AdoptedObservedPrimary { node: 0 }
        );
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
