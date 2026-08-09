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
//! struct simply holds no `Systemd`, no `Pcp`, no `StandbyOps`, and
//! never dials a peer mutation RPC. Its only writes go to the
//! [`ConsensusStore`], which today is the process-local
//! [`InMemoryConsensusStore`] — private bookkeeping, authoritative for
//! nothing. Diffing the logged decision stream (target `ha_shadow`)
//! against pgpool's actual behavior on the live cluster is the step-5
//! validation the design doc calls for. At cutover (step 7) the
//! decisions gain executors and this module's docs change; until then
//! a bug here can mislead a log reader and nothing else.
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
    /// lease (see the grace-window note on [`HaLoop::promotion_grace`]).
    AwaitingPromotion { term: u64, waiting: Duration },
    /// Someone else holds the lease and looks healthy; we follow.
    Following { holder: i32 },
    /// The holder changed since the last tick — the loop would
    /// reconfigure local PostgreSQL onto the new holder
    /// (`follow_primary`).
    WouldFollowNewHolder { prev: i32, holder: i32 },
    /// The holder is unreachable or not running as primary, but hasn't
    /// been for `leader_ttl` yet — watching, not acting.
    HolderUnhealthy { holder: i32, unhealthy_for: Duration },
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
    holder_unhealthy_since: Option<Instant>,
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
        }
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
        }
    }

    /// One tick. Public so tests (and a future `pg_agentctl` debug
    /// command) can drive the loop deterministically.
    pub async fn tick_once(&self) -> HaDecision {
        let now = Instant::now();
        let local_id = self.pool.local_node_id;

        let state = self.store.read_state().await;
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
                        let since = *ts.holder_unhealthy_since.get_or_insert(now);
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
            if best_pos >= my_pos {
                match my_pos.lag_behind(&best_pos) {
                    None => {
                        self.arm_backoff(now);
                        return HaDecision::StoodDown {
                            reason: format!(
                                "node {best_id} is on a newer timeline ({best_pos} vs \
                                 {my_pos}); not a candidate"
                            ),
                        };
                    }
                    Some(lag) if lag > self.timing.max_lag_on_failover => {
                        self.arm_backoff(now);
                        return HaDecision::StoodDown {
                            reason: format!(
                                "node {best_id} is ahead by {lag} bytes ({best_pos} vs \
                                 {my_pos}) > max_lag_on_failover {}; not a candidate",
                                self.timing.max_lag_on_failover
                            ),
                        };
                    }
                    Some(_) => {
                        // Within threshold: positions effectively equal.
                        // Node id breaks the tie so two near-equal
                        // candidates can't both defer forever.
                        if best_id < local_id {
                            self.arm_backoff(now);
                            return HaDecision::StoodDown {
                                reason: format!(
                                    "node {best_id} is within max_lag_on_failover and has \
                                     the lower node id; deferring (tiebreak)"
                                ),
                            };
                        }
                    }
                }
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
            let lsn = self.db.current_wal_lsn().await.ok().filter(|l| *l > 0);
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
        match collect_statuses(self.peers.clone(), &others, STATUS_FANOUT_BUDGET).await {
            Ok(views) => views
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
                .collect(),
            Err(_) => Vec::new(),
        }
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
            ..Default::default()
        }
    }

    fn standby_status(tl: i32, lsn: u64) -> pb::NodeStatus {
        pb::NodeStatus {
            is_postgres_running: true,
            is_in_recovery: true,
            timeline_id: tl,
            current_wal_lsn: lsn,
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
    async fn vacant_tiebreak_defers_to_lower_node_id_within_threshold() {
        // Node 2 (local) and node 1 within max_lag of each other, node 1
        // slightly ahead. Lower id proceeds; we defer.
        let f = fixture(2, StubDb::standby(2, BASE));
        f.peers.set(1, standby_status(2, BASE + 100)); // within 1024
        f.peers.mark_unreachable(0);

        match f.ha.tick_once().await {
            HaDecision::StoodDown { reason } => assert!(reason.contains("tiebreak"), "{reason}"),
            other => panic!("expected tiebreak StoodDown, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn vacant_tiebreak_proceeds_when_local_has_lower_id() {
        // Same shape, but local is node 1 and node 2 is slightly ahead:
        // within threshold + higher id → we proceed.
        let f = fixture(1, StubDb::standby(2, BASE));
        f.peers.set(2, standby_status(2, BASE + 100));
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
