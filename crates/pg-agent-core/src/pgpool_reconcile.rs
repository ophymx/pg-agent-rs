//! Converge the LOCAL pgpool's backend map onto what the cluster says.
//!
//! # Why this exists
//!
//! With the watchdog off nothing propagates backend status between
//! pgpool instances (docs/pgpool-hook-contract.md §3), `auto_failback`
//! is off because slots are in use, and pgpool never health-checks a
//! backend it holds `down`. So a transient failure — a restart, a
//! stretch of broken DNS, an expired certificate — that makes an
//! instance detach a backend leaves that instance routing around a
//! healthy node **forever**. The field report of 2026-11-02 found three
//! nodes with three different maps, every one of them `ready: true`,
//! two weeks after the last transient cleared.
//!
//! The agent attached backends in two places before this module: the
//! `cluster recover` fan-out (which reclones first) and the executor's
//! self-attach of a new primary's own backend. Neither covers a map
//! that merely went stale. This module does: on a slow tick, and on a
//! kick from the failover hook, it reads the local map and re-attaches
//! every backend the map holds down that the CLUSTER holds up.
//!
//! # What counts as evidence
//!
//! The same evidence the failover precondition already trusts to call
//! a failure report wrong (`preconditions.rs`): the node's own agent
//! answers `GetStatus`, PostgreSQL is running there, and either it is
//! a standby that is `streaming` or it is the lease holder running as
//! primary. "The agent can reach it" is not enough on its own — a
//! stopped standby is reachable — and "it is a primary" is not enough
//! either, because a primary that is not the holder is a phantom that
//! must stay unrouted. Evidence that cannot be gathered (peer
//! unreachable, lease unreadable) leaves the backend down: unlike the
//! precondition, there is no availability cost to waiting a tick.
//!
//! Only-if-down, primary first. Attaching a backend that is already up
//! makes pgpool re-run its failover processing on healthy backends, and
//! a standby attach into a primary-less map blocks in
//! `find_primary_node_repeatedly` for 300 s, wedging every later pcp
//! request behind it (`peerserver::attach_node` carries the same two
//! rules for the fan-out path).
//!
//! # When pgpool keeps undoing the attach
//!
//! The report's own case: pgpool's health check to db0 failed because
//! the host resolved its own name to an address `pg_hba.conf` did not
//! admit, while the agent reached db0 over the address it did. Attach,
//! health check, detach, hook, attach… would have run every tick. So
//! every attach is remembered, a backend found down again after one is
//! counted as reverted, and the next attempt waits `BACKOFF_BASE × 2ⁿ`
//! up to `BACKOFF_MAX`. The warning on each revert says what the
//! disagreement means — the agent's connection to the node works and
//! pgpool's does not — because that sentence, not the attach, is what
//! fixes a misconfigured host.

use crate::config::{NodeConfig, NodePool};
use crate::consensus::ConsensusStore;
use crate::localdb::LocalDb;
use crate::pcp::{NodeInfo, Pcp};
use crate::peers::PeerRegistry;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

/// Cadence of the periodic reconcile. Slow: a stale map is a
/// degradation, not an outage, and every tick that finds a backend
/// down costs a `GetStatus` fan-out. The failover hook kicks an
/// immediate run for the case that matters most.
pub const RECONCILE_INTERVAL: Duration = Duration::from_secs(30);

/// First wait after pgpool reverts an attach; doubles per revert.
pub const BACKOFF_BASE: Duration = Duration::from_secs(60);

/// Ceiling on the revert backoff. A host whose health check is
/// misconfigured gets one attach attempt — and one diagnostic warning
/// — every ten minutes until someone reads it.
pub const BACKOFF_MAX: Duration = Duration::from_secs(600);

/// Budget for gathering evidence from the peers holding down backends.
/// Same number as the status fan-out everywhere else.
pub const EVIDENCE_BUDGET: Duration = crate::cluster_view::STATUS_FANOUT_BUDGET;

/// What the cluster says about one pool member.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Evidence {
    /// Running as the lease holder.
    Primary,
    /// Running as a standby with an active walreceiver.
    StreamingStandby,
    /// Reachable, and positively not serving: stopped, or a standby
    /// that is not streaming, or a primary that does not hold the
    /// lease.
    NotServing { reason: String },
    /// Could not find out.
    Unknown { reason: String },
}

impl Evidence {
    /// The attach predicate.
    pub fn says_up(&self) -> bool {
        matches!(self, Evidence::Primary | Evidence::StreamingStandby)
    }
}

/// What one reconcile pass did (or declined to do) about one backend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    Attached,
    AttachFailed {
        error: String,
    },
    /// Down here, and the cluster does not say it is up.
    LeftDown {
        reason: String,
    },
    /// Down here and up in the cluster, but pgpool reverted an earlier
    /// attach and the backoff has not elapsed.
    Deferred {
        until_secs: u64,
        reverts: u32,
    },
    /// Down here, up in the cluster, and the lease holder's backend is
    /// also down here and could not be attached first.
    BlockedOnPrimary {
        error: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendAction {
    pub node_id: i32,
    pub hostname: String,
    pub action: Action,
}

/// One pass over the local map.
#[derive(Debug, Clone, Default)]
pub struct ReconcileReport {
    /// A pass was already running; nothing was read or done. Callers
    /// that need an answer (`cluster attach`) retry; the loop just
    /// waits for its next tick.
    pub skipped: bool,
    /// `None` when the local map could not be read (pgpool down).
    pub map_error: Option<String>,
    /// Backends the map held down, in attach order.
    pub actions: Vec<BackendAction>,
    /// Evidence gathered for every member, for callers that fan out.
    pub evidence: HashMap<i32, Evidence>,
}

impl ReconcileReport {
    pub fn attached(&self) -> Vec<i32> {
        self.actions
            .iter()
            .filter(|a| a.action == Action::Attached)
            .map(|a| a.node_id)
            .collect()
    }
}

#[derive(Debug, Default)]
struct Revert {
    /// Attaches this reconciler issued that pgpool later undid.
    reverts: u32,
    /// Set after every attach; cleared when the backend is seen up.
    attached_at: Option<Instant>,
    /// Earliest next attempt, once reverted.
    not_before: Option<Instant>,
}

pub struct PgpoolReconciler {
    pcp: Arc<dyn Pcp>,
    db: Arc<dyn LocalDb>,
    peers: Arc<dyn PeerRegistry>,
    pool: NodePool,
    /// `None` only in unit tests of handlers that need no lease; a
    /// running daemon always has one. Without it a primary backend
    /// never attaches here — the fan-out caller must know the holder.
    store: Option<Arc<dyn ConsensusStore>>,
    interval: Duration,
    backoff_base: Duration,
    backoff_max: Duration,
    kick: Notify,
    in_flight: AtomicBool,
    reverts: Mutex<HashMap<i32, Revert>>,
}

impl PgpoolReconciler {
    pub fn new(
        pcp: Arc<dyn Pcp>,
        db: Arc<dyn LocalDb>,
        peers: Arc<dyn PeerRegistry>,
        pool: NodePool,
        store: Option<Arc<dyn ConsensusStore>>,
    ) -> Self {
        Self::with_params(
            pcp,
            db,
            peers,
            pool,
            store,
            RECONCILE_INTERVAL,
            BACKOFF_BASE,
            BACKOFF_MAX,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn with_params(
        pcp: Arc<dyn Pcp>,
        db: Arc<dyn LocalDb>,
        peers: Arc<dyn PeerRegistry>,
        pool: NodePool,
        store: Option<Arc<dyn ConsensusStore>>,
        interval: Duration,
        backoff_base: Duration,
        backoff_max: Duration,
    ) -> Self {
        Self {
            pcp,
            db,
            peers,
            pool,
            store,
            interval,
            backoff_base,
            backoff_max,
            kick: Notify::new(),
            in_flight: AtomicBool::new(false),
            reverts: Mutex::new(HashMap::new()),
        }
    }

    /// Ask for an immediate pass. Never blocks, never fails; a pass
    /// already running absorbs the request. The failover hook calls
    /// this after refusing a slot drop on the evidence that pgpool's
    /// failure report was wrong — the hook has to return to pgpool
    /// promptly, and the attach has to happen after pgpool finishes
    /// the failover processing that fired the hook.
    pub fn kick(&self) {
        self.kick.notify_one();
    }

    /// Tick on `interval` or on a kick until shutdown.
    pub async fn run(self: Arc<Self>, shutdown: CancellationToken) {
        info!(
            interval_secs = self.interval.as_secs(),
            "pgpool_reconcile: running"
        );
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return,
                _ = tokio::time::sleep(self.interval) => {}
                _ = self.kick.notified() => {
                    debug!("pgpool_reconcile: kicked");
                }
            }
            let _ = self.reconcile_once(false, None).await;
        }
    }

    /// One pass. `force` ignores the revert backoff (an operator asked);
    /// `only` restricts the pass to one backend. Single-flight: a pass
    /// that overlaps a running one returns an empty report.
    pub async fn reconcile_once(&self, force: bool, only: Option<i32>) -> ReconcileReport {
        if self.in_flight.swap(true, Ordering::SeqCst) {
            debug!("pgpool_reconcile: pass already running");
            return ReconcileReport {
                skipped: true,
                ..Default::default()
            };
        }
        let report = self.reconcile_inner(force, only).await;
        self.in_flight.store(false, Ordering::SeqCst);
        report
    }

    async fn reconcile_inner(&self, force: bool, only: Option<i32>) -> ReconcileReport {
        let mut report = ReconcileReport::default();
        let map = match self.pcp.node_info_all().await {
            Ok(m) => m,
            Err(e) => {
                // pgpool down is a normal state (pre-bootstrap, operator
                // maintenance); the supervisor owns it. Nothing to
                // reconcile against.
                debug!(err = %e, "pgpool_reconcile: local map unavailable");
                report.map_error = Some(e.to_string());
                return report;
            }
        };
        self.note_recoveries(&map);
        // `only` narrows what the pass acts on, but a holder held down
        // here stays in scope regardless: it is attached first or
        // blocks the rest (below), and an operator asking for node 1
        // is asking for node 1 to be routed, which needs the primary
        // in the map just as much.
        let down_all: Vec<&NodeInfo> = map.iter().filter(|n| !n.is_up()).collect();
        if down_all.iter().all(|n| only.is_some_and(|id| id != n.id)) {
            return report;
        }

        // Evidence for every member: the fan-out caller (`cluster
        // attach`) wants it for the peers' maps too, and the holder's
        // identity is needed to order the attaches.
        let holder = self.lease_holder().await;
        report.evidence = self.gather_evidence(holder).await;

        // Primary first (module docs). A holder whose backend is down
        // here and whose evidence is anything but Primary blocks every
        // standby attach on this instance — attaching those would wedge
        // pcp for five minutes, and routing to them without a primary
        // in the map is not something pgpool does anyway.
        let primary_down = holder.filter(|h| down_all.iter().any(|n| n.id == *h));
        let mut primary_blocked: Option<String> = None;
        let mut ordered: Vec<&NodeInfo> = Vec::with_capacity(down_all.len());
        if let Some(h) = primary_down {
            if let Some(n) = down_all.iter().find(|n| n.id == h) {
                ordered.push(n);
            }
        }
        ordered.extend(
            down_all
                .iter()
                .filter(|n| Some(n.id) != primary_down)
                .filter(|n| only.is_none_or(|id| id == n.id)),
        );

        for n in ordered {
            let hostname = self
                .pool
                .node_by_id(n.id)
                .map(|c| c.hostname.clone())
                .unwrap_or_else(|_| n.hostname.clone());
            let evidence = report
                .evidence
                .get(&n.id)
                .cloned()
                .unwrap_or(Evidence::Unknown {
                    reason: "not a pool member".into(),
                });
            let action = if !evidence.says_up() {
                let reason = match &evidence {
                    Evidence::NotServing { reason } | Evidence::Unknown { reason } => {
                        reason.clone()
                    }
                    _ => unreachable!(),
                };
                if Some(n.id) == primary_down {
                    primary_blocked = Some(format!(
                        "lease holder node {} is down here and {reason}",
                        n.id
                    ));
                }
                Action::LeftDown { reason }
            } else if let Some(err) = &primary_blocked {
                Action::BlockedOnPrimary { error: err.clone() }
            } else if let Some((until, reverts)) = self.deferred(n.id, force) {
                Action::Deferred {
                    until_secs: until,
                    reverts,
                }
            } else {
                match self.pcp.attach_node(n.id).await {
                    Ok(()) => {
                        self.note_attached(n.id);
                        info!(
                            node_id = n.id,
                            hostname = %hostname,
                            evidence = ?evidence,
                            "pgpool_reconcile: attached — local pgpool held a backend down \
                             that the cluster reports up"
                        );
                        Action::Attached
                    }
                    Err(e) => {
                        if Some(n.id) == primary_down {
                            primary_blocked =
                                Some(format!("attaching lease holder node {} failed: {e}", n.id));
                        }
                        warn!(node_id = n.id, err = %e, "pgpool_reconcile: pcp_attach_node failed");
                        Action::AttachFailed {
                            error: e.to_string(),
                        }
                    }
                }
            };
            report.actions.push(BackendAction {
                node_id: n.id,
                hostname,
                action,
            });
        }
        report
    }

    /// The lease holder, or `None` when there is no store or it cannot
    /// be read. Unknown is not vacant: nothing primary-shaped attaches
    /// on an unreadable lease.
    pub async fn lease_holder(&self) -> Option<i32> {
        let store = self.store.as_ref()?;
        match store.read_state().await {
            Ok(state) => state.lease.map(|l| l.holder),
            Err(e) => {
                debug!(err = %e, "pgpool_reconcile: lease unreadable; no primary evidence this pass");
                None
            }
        }
    }

    /// Evidence for every pool member: the local node from its own
    /// database, peers via the budgeted status fan-out.
    pub async fn gather_evidence(&self, holder: Option<i32>) -> HashMap<i32, Evidence> {
        let mut out = HashMap::with_capacity(self.pool.members.len());
        let local_id = self.pool.local_node_id;
        let remotes: Vec<NodeConfig> = self
            .pool
            .members
            .iter()
            .filter(|n| n.id != local_id)
            .cloned()
            .collect();
        let (local, views) = tokio::join!(
            self.local_evidence(holder),
            crate::cluster_view::collect_statuses(
                self.peers.clone(),
                &remotes,
                EVIDENCE_BUDGET,
                None,
            )
        );
        if self.pool.members.iter().any(|n| n.id == local_id) {
            out.insert(local_id, local);
        }
        for v in views {
            let ev = match v.status {
                Ok(s) => classify(
                    s.is_postgres_running,
                    s.is_in_recovery,
                    &s.replication_state,
                    holder == Some(v.node.id),
                ),
                Err(e) => Evidence::Unknown {
                    reason: format!("get_status({}): {e}", v.node.hostname),
                },
            };
            out.insert(v.node.id, ev);
        }
        out
    }

    async fn local_evidence(&self, holder: Option<i32>) -> Evidence {
        let in_recovery = match self.db.is_in_recovery().await {
            Ok(r) => r,
            Err(e) => {
                return Evidence::NotServing {
                    reason: format!("local PostgreSQL not answering: {e}"),
                }
            }
        };
        let state = if in_recovery {
            match self.db.replication_lag().await {
                Ok(l) => l.state,
                Err(e) => {
                    return Evidence::Unknown {
                        reason: format!("local replication state: {e}"),
                    }
                }
            }
        } else {
            String::new()
        };
        classify(
            true,
            in_recovery,
            &state,
            holder == Some(self.pool.local_node_id),
        )
    }

    /// `Some((seconds_until_allowed, reverts))` when a reverted attach
    /// is still in backoff.
    fn deferred(&self, node_id: i32, force: bool) -> Option<(u64, u32)> {
        if force {
            return None;
        }
        let map = self.reverts.lock().unwrap();
        let r = map.get(&node_id)?;
        let until = r.not_before?;
        let now = Instant::now();
        if until <= now {
            return None;
        }
        Some(((until - now).as_secs(), r.reverts))
    }

    fn note_attached(&self, node_id: i32) {
        let mut map = self.reverts.lock().unwrap();
        map.entry(node_id).or_default().attached_at = Some(Instant::now());
    }

    /// Reconcile the revert bookkeeping against a fresh map: a backend
    /// seen up forgets its history; a backend seen down after an attach
    /// we issued was reverted by pgpool, and the next attempt waits.
    fn note_recoveries(&self, map: &[NodeInfo]) {
        let mut reverts = self.reverts.lock().unwrap();
        for n in map {
            if n.is_up() {
                if reverts.remove(&n.id).is_some_and(|r| r.reverts > 0) {
                    info!(
                        node_id = n.id,
                        "pgpool_reconcile: backend stays up; revert backoff cleared"
                    );
                }
                continue;
            }
            let Some(r) = reverts.get_mut(&n.id) else {
                continue;
            };
            if r.attached_at.take().is_none() {
                continue;
            }
            r.reverts += 1;
            let wait = self
                .backoff_base
                .saturating_mul(2u32.saturating_pow(r.reverts.saturating_sub(1)))
                .min(self.backoff_max);
            r.not_before = Some(Instant::now() + wait);
            let host = self
                .pool
                .node_by_id(n.id)
                .map(|c| c.hostname.clone())
                .unwrap_or_else(|_| n.hostname.clone());
            warn!(
                node_id = n.id,
                hostname = %host,
                reverts = r.reverts,
                next_attempt_secs = wait.as_secs(),
                "pgpool_reconcile: pgpool detached backend {} again after the agent attached it. \
                 The agent reaches {host} and finds it serving; pgpool's own health check to \
                 {}:{} does not. That is a connectivity or pg_hba.conf difference between the \
                 two paths — typically an address family this host resolves {host} to that \
                 pg_hba.conf does not admit for the health-check user. `pg_agentd validate-env` \
                 dials every resolved address and reports which are refused.",
                n.id,
                n.hostname,
                n.port
            );
        }
    }
}

/// The evidence predicate, as a pure function of a `NodeStatus`.
pub fn classify(
    running: bool,
    in_recovery: bool,
    replication_state: &str,
    is_holder: bool,
) -> Evidence {
    if !running {
        return Evidence::NotServing {
            reason: "PostgreSQL is not running".into(),
        };
    }
    if in_recovery {
        if replication_state == "streaming" {
            Evidence::StreamingStandby
        } else {
            Evidence::NotServing {
                reason: format!(
                    "standby is not streaming (replication_state={replication_state:?})"
                ),
            }
        }
    } else if is_holder {
        Evidence::Primary
    } else {
        Evidence::NotServing {
            reason: "running as primary without holding the lease".into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consensus::{ClusterState, InMemoryConsensusStore, Lease};
    use crate::localdb::ReplicationLag;
    use crate::peers::{PeerClient, PeerRegistry};
    use async_trait::async_trait;
    use pg_agent_proto::pgagentpb as pb;
    use std::sync::Mutex as StdMutex;

    // ----- stubs ------------------------------------------------------------

    #[derive(Default)]
    struct StubPcp {
        map: StdMutex<Vec<NodeInfo>>,
        attach_calls: StdMutex<Vec<i32>>,
        attach_fails: AtomicBool,
        /// Model pgpool's health check: an attach lands only for ids
        /// NOT in this set; the others stay down (reverted).
        revert_ids: StdMutex<Vec<i32>>,
    }

    impl StubPcp {
        fn with_map(map: Vec<NodeInfo>) -> Arc<Self> {
            Arc::new(Self {
                map: StdMutex::new(map),
                ..Default::default()
            })
        }
        fn attaches(&self) -> Vec<i32> {
            self.attach_calls.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl Pcp for StubPcp {
        async fn attach_node(&self, node_id: i32) -> anyhow::Result<()> {
            self.attach_calls.lock().unwrap().push(node_id);
            if self.attach_fails.load(Ordering::SeqCst) {
                anyhow::bail!("stub: attach boom");
            }
            if !self.revert_ids.lock().unwrap().contains(&node_id) {
                for n in self.map.lock().unwrap().iter_mut() {
                    if n.id == node_id {
                        n.status_code = 2;
                        n.status_name = "up".into();
                    }
                }
            }
            Ok(())
        }
        async fn detach_node(&self, _: i32) -> anyhow::Result<()> {
            unreachable!()
        }
        async fn node_count(&self) -> anyhow::Result<i32> {
            Ok(self.map.lock().unwrap().len() as i32)
        }
        async fn node_info_all(&self) -> anyhow::Result<Vec<NodeInfo>> {
            Ok(self.map.lock().unwrap().clone())
        }
    }

    struct StubDb {
        in_recovery: bool,
        state: &'static str,
    }

    #[async_trait]
    impl LocalDb for StubDb {
        async fn is_in_recovery(&self) -> anyhow::Result<bool> {
            Ok(self.in_recovery)
        }
        async fn replication_lag(&self) -> anyhow::Result<ReplicationLag> {
            Ok(ReplicationLag {
                bytes: 0,
                state: self.state.into(),
            })
        }
        async fn slot_active(&self, _: &str) -> anyhow::Result<bool> {
            unreachable!()
        }
        async fn set_synchronous_standby_names(&self, _: &str) -> anyhow::Result<()> {
            unreachable!()
        }
        async fn reload_conf(&self) -> anyhow::Result<()> {
            unreachable!()
        }
        async fn connected_standby_names(&self) -> anyhow::Result<Vec<String>> {
            unreachable!()
        }
        async fn timeline_id(&self) -> anyhow::Result<i32> {
            unreachable!()
        }
        async fn current_wal_lsn(&self) -> anyhow::Result<u64> {
            unreachable!()
        }
        async fn flush_lsn(&self) -> anyhow::Result<u64> {
            unreachable!()
        }
        async fn promote(&self) -> anyhow::Result<()> {
            unreachable!()
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

    struct StubPeer(pb::NodeStatus);

    #[async_trait]
    impl PeerClient for StubPeer {
        async fn get_status(&self) -> anyhow::Result<pb::NodeStatus> {
            Ok(self.0.clone())
        }
        async fn drop_slot(&self, _: &str) -> anyhow::Result<()> {
            unreachable!()
        }
        async fn create_slot(&self, _: &str) -> anyhow::Result<()> {
            unreachable!()
        }
        async fn get_node_config(&self) -> anyhow::Result<pb::NodeConfigResponse> {
            unreachable!()
        }
        async fn start(&self) -> anyhow::Result<()> {
            unreachable!()
        }
        async fn start_pgpool(&self) -> anyhow::Result<()> {
            unreachable!()
        }
        async fn attach_node(&self, _: i32, _: i32) -> anyhow::Result<()> {
            unreachable!()
        }
        async fn stop(&self) -> anyhow::Result<()> {
            unreachable!()
        }
        async fn rewind(&self, _: crate::pgstandby::RewindOpts) -> anyhow::Result<()> {
            unreachable!()
        }
        async fn basebackup(&self, _: crate::pgstandby::BasebackupOpts) -> anyhow::Result<()> {
            unreachable!()
        }
        async fn configure_standby(
            &self,
            _: crate::pgstandby::WriteRecoveryConfOpts,
        ) -> anyhow::Result<()> {
            unreachable!()
        }
        async fn promote(&self) -> anyhow::Result<()> {
            unreachable!()
        }
        async fn fetch_wal(
            &self,
            _: &str,
        ) -> anyhow::Result<Option<Box<dyn tokio::io::AsyncBufRead + Send + Unpin>>> {
            unreachable!()
        }
    }

    #[derive(Default)]
    struct StubPeers {
        statuses: StdMutex<HashMap<i32, pb::NodeStatus>>,
    }

    #[async_trait]
    impl PeerRegistry for StubPeers {
        async fn client(&self, node: &NodeConfig) -> anyhow::Result<Arc<dyn PeerClient>> {
            match self.statuses.lock().unwrap().get(&node.id) {
                Some(s) => Ok(Arc::new(StubPeer(s.clone()))),
                None => anyhow::bail!("stub: peer {} unreachable", node.id),
            }
        }
        async fn close(&self) -> anyhow::Result<()> {
            Ok(())
        }
    }

    fn node(id: i32, up: bool, role: &str) -> NodeInfo {
        NodeInfo {
            id,
            hostname: format!("db{id}"),
            port: 5432,
            status_code: if up { 2 } else { 3 },
            lb_weight: 0.333,
            status_name: if up { "up" } else { "down" }.into(),
            actual_status: "up".into(),
            role: role.into(),
            actual_role: role.into(),
            replication_delay: "0".into(),
            replication_state: if role == "standby" {
                "streaming"
            } else {
                "none"
            }
            .into(),
            sync_state: "none".into(),
        }
    }

    fn pool(local: i32) -> NodePool {
        NodePool {
            members: (0..3)
                .map(|id| NodeConfig {
                    id,
                    hostname: format!("db{id}"),
                })
                .collect(),
            local_node_id: local,
        }
    }

    fn streaming() -> pb::NodeStatus {
        pb::NodeStatus {
            is_postgres_running: true,
            is_in_recovery: true,
            replication_state: "streaming".into(),
            ..Default::default()
        }
    }

    fn primary() -> pb::NodeStatus {
        pb::NodeStatus {
            is_postgres_running: true,
            is_in_recovery: false,
            ..Default::default()
        }
    }

    async fn store_with_holder(holder: i32) -> Arc<dyn ConsensusStore> {
        let s = InMemoryConsensusStore::default();
        s.install(ClusterState {
            lease: Some(Lease {
                holder,
                term: 1,
                since: chrono::Utc::now(),
            }),
            ..Default::default()
        });
        Arc::new(s)
    }

    struct Fixture {
        pcp: Arc<StubPcp>,
        peers: Arc<StubPeers>,
        r: PgpoolReconciler,
    }

    async fn fixture(local: i32, map: Vec<NodeInfo>, holder: i32, local_db: StubDb) -> Fixture {
        let pcp = StubPcp::with_map(map);
        let peers = Arc::new(StubPeers::default());
        let r = PgpoolReconciler::with_params(
            pcp.clone(),
            Arc::new(local_db),
            peers.clone(),
            pool(local),
            Some(store_with_holder(holder).await),
            Duration::from_millis(10),
            Duration::from_millis(100),
            Duration::from_millis(400),
        );
        Fixture { pcp, peers, r }
    }

    fn standby_db() -> StubDb {
        StubDb {
            in_recovery: true,
            state: "streaming",
        }
    }

    // ----- the field report's three maps ------------------------------------

    /// db1's map on 2026-11-02: its own backend down, db0 and db2 up.
    /// Local evidence says streaming; attach.
    #[tokio::test]
    async fn attaches_own_backend_when_locally_streaming() {
        let f = fixture(
            1,
            vec![
                node(0, true, "standby"),
                node(1, false, "standby"),
                node(2, true, "primary"),
            ],
            2,
            standby_db(),
        )
        .await;
        let report = f.r.reconcile_once(false, None).await;
        assert_eq!(f.pcp.attaches(), vec![1]);
        assert_eq!(report.attached(), vec![1]);
    }

    /// db0's map: db0 (self) and db1 down. Peer evidence for db1.
    #[tokio::test]
    async fn attaches_a_peer_the_cluster_reports_streaming() {
        let f = fixture(
            0,
            vec![
                node(0, false, "standby"),
                node(1, false, "standby"),
                node(2, true, "primary"),
            ],
            2,
            standby_db(),
        )
        .await;
        f.peers.statuses.lock().unwrap().insert(1, streaming());
        f.peers.statuses.lock().unwrap().insert(2, primary());
        let report = f.r.reconcile_once(false, None).await;
        let mut got = f.pcp.attaches();
        got.sort_unstable();
        assert_eq!(got, vec![0, 1]);
        assert_eq!(report.actions.len(), 2);
    }

    #[tokio::test]
    async fn leaves_a_stopped_standby_down() {
        let f = fixture(
            2,
            vec![
                node(0, true, "standby"),
                node(1, false, "standby"),
                node(2, true, "primary"),
            ],
            2,
            StubDb {
                in_recovery: false,
                state: "",
            },
        )
        .await;
        f.peers.statuses.lock().unwrap().insert(
            1,
            pb::NodeStatus {
                is_postgres_running: false,
                ..Default::default()
            },
        );
        let report = f.r.reconcile_once(false, None).await;
        assert!(f.pcp.attaches().is_empty());
        assert!(matches!(report.actions[0].action, Action::LeftDown { .. }));
    }

    #[tokio::test]
    async fn unreachable_peer_is_left_down_not_attached() {
        let f = fixture(
            2,
            vec![
                node(0, true, "standby"),
                node(1, false, "standby"),
                node(2, true, "primary"),
            ],
            2,
            StubDb {
                in_recovery: false,
                state: "",
            },
        )
        .await;
        // No status registered for node 1 → registry errors.
        let report = f.r.reconcile_once(false, None).await;
        assert!(f.pcp.attaches().is_empty());
        match &report.actions[0].action {
            Action::LeftDown { reason } => assert!(reason.contains("unreachable"), "{reason}"),
            other => panic!("{other:?}"),
        }
    }

    // ----- primary handling ---------------------------------------------------

    #[tokio::test]
    async fn primary_attaches_first_then_standbys() {
        let f = fixture(
            0,
            vec![
                node(0, false, "standby"),
                node(1, true, "standby"),
                node(2, false, "primary"),
            ],
            2,
            standby_db(),
        )
        .await;
        f.peers.statuses.lock().unwrap().insert(2, primary());
        f.r.reconcile_once(false, None).await;
        assert_eq!(f.pcp.attaches(), vec![2, 0], "holder first");
    }

    #[tokio::test]
    async fn a_primary_that_is_not_the_holder_stays_down() {
        // Node 2 runs as primary but the lease names node 1: a phantom.
        // It stays down. Node 0 (streaming) still attaches, because the
        // holder's backend (1) is up in this map — the primary-first
        // block only applies when the HOLDER is down here.
        let pcp = StubPcp::with_map(vec![
            node(0, false, "standby"),
            node(1, true, "standby"),
            node(2, false, "primary"),
        ]);
        let peers = Arc::new(StubPeers::default());
        peers.statuses.lock().unwrap().insert(1, streaming());
        peers.statuses.lock().unwrap().insert(2, primary());
        let r = PgpoolReconciler::new(
            pcp.clone(),
            Arc::new(standby_db()),
            peers,
            pool(0),
            Some(store_with_holder(1).await),
        );
        let report = r.reconcile_once(false, None).await;
        assert_eq!(pcp.attaches(), vec![0]);
        let two = report.actions.iter().find(|a| a.node_id == 2).unwrap();
        match &two.action {
            Action::LeftDown { reason } => assert!(reason.contains("without holding"), "{reason}"),
            other => panic!("{other:?}"),
        }
    }

    #[tokio::test]
    async fn holder_down_here_and_unreachable_blocks_every_standby_attach() {
        let f = fixture(
            0,
            vec![
                node(0, false, "standby"),
                node(1, true, "standby"),
                node(2, false, "primary"),
            ],
            2,
            standby_db(),
        )
        .await;
        // Holder 2 unreachable → Unknown; node 0 is streaming locally.
        let report = f.r.reconcile_once(false, None).await;
        assert!(f.pcp.attaches().is_empty());
        let zero = report.actions.iter().find(|a| a.node_id == 0).unwrap();
        assert!(
            matches!(zero.action, Action::BlockedOnPrimary { .. }),
            "{:?}",
            zero.action
        );
    }

    #[tokio::test]
    async fn without_a_store_no_primary_evidence_exists() {
        let pcp = StubPcp::with_map(vec![node(0, true, "standby"), node(2, false, "primary")]);
        let peers = Arc::new(StubPeers::default());
        peers.statuses.lock().unwrap().insert(2, primary());
        let r = PgpoolReconciler::new(pcp.clone(), Arc::new(standby_db()), peers, pool(0), None);
        let report = r.reconcile_once(false, None).await;
        assert!(pcp.attaches().is_empty());
        assert!(matches!(report.actions[0].action, Action::LeftDown { .. }));
    }

    // ----- revert backoff -----------------------------------------------------

    #[tokio::test(start_paused = true)]
    async fn a_reverted_attach_waits_and_doubles() {
        let f = fixture(
            1,
            vec![
                node(0, true, "standby"),
                node(1, false, "standby"),
                node(2, true, "primary"),
            ],
            2,
            standby_db(),
        )
        .await;
        f.pcp.revert_ids.lock().unwrap().push(1); // pgpool's health check keeps failing
        f.r.reconcile_once(false, None).await;
        assert_eq!(f.pcp.attaches().len(), 1);
        // Immediately after: seen down again after our attach → revert
        // #1, backoff 100 ms.
        let report = f.r.reconcile_once(false, None).await;
        assert_eq!(f.pcp.attaches().len(), 1, "no attach inside the backoff");
        assert!(matches!(
            report.actions[0].action,
            Action::Deferred { reverts: 1, .. }
        ));
        tokio::time::advance(Duration::from_millis(150)).await;
        f.r.reconcile_once(false, None).await;
        assert_eq!(f.pcp.attaches().len(), 2);
        // Revert #2 → 200 ms.
        f.r.reconcile_once(false, None).await;
        tokio::time::advance(Duration::from_millis(150)).await;
        let report = f.r.reconcile_once(false, None).await;
        assert_eq!(f.pcp.attaches().len(), 2);
        assert!(matches!(
            report.actions[0].action,
            Action::Deferred { reverts: 2, .. }
        ));
        tokio::time::advance(Duration::from_millis(100)).await;
        f.r.reconcile_once(false, None).await;
        assert_eq!(f.pcp.attaches().len(), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn force_ignores_the_backoff() {
        let f = fixture(
            1,
            vec![
                node(0, true, "standby"),
                node(1, false, "standby"),
                node(2, true, "primary"),
            ],
            2,
            standby_db(),
        )
        .await;
        f.pcp.revert_ids.lock().unwrap().push(1);
        f.r.reconcile_once(false, None).await;
        f.r.reconcile_once(false, None).await; // revert noted
        f.r.reconcile_once(true, None).await;
        assert_eq!(f.pcp.attaches().len(), 2);
    }

    #[tokio::test]
    async fn a_backend_that_stays_up_clears_its_history() {
        let f = fixture(
            1,
            vec![
                node(0, true, "standby"),
                node(1, false, "standby"),
                node(2, true, "primary"),
            ],
            2,
            standby_db(),
        )
        .await;
        f.r.reconcile_once(false, None).await; // attach lands (not in revert_ids)
        f.r.reconcile_once(false, None).await; // seen up → history cleared
        assert!(f.r.reverts.lock().unwrap().is_empty());
    }

    // ----- plumbing -----------------------------------------------------------

    #[tokio::test]
    async fn only_restricts_the_pass() {
        let f = fixture(
            0,
            vec![
                node(0, false, "standby"),
                node(1, false, "standby"),
                node(2, true, "primary"),
            ],
            2,
            standby_db(),
        )
        .await;
        f.peers.statuses.lock().unwrap().insert(1, streaming());
        f.r.reconcile_once(false, Some(1)).await;
        assert_eq!(f.pcp.attaches(), vec![1]);
    }

    #[tokio::test]
    async fn pgpool_down_is_a_quiet_no_op() {
        let pcp = Arc::new(StubPcp::default());
        struct Boom;
        #[async_trait]
        impl Pcp for Boom {
            async fn attach_node(&self, _: i32) -> anyhow::Result<()> {
                unreachable!()
            }
            async fn detach_node(&self, _: i32) -> anyhow::Result<()> {
                unreachable!()
            }
            async fn node_count(&self) -> anyhow::Result<i32> {
                anyhow::bail!("down")
            }
            async fn node_info_all(&self) -> anyhow::Result<Vec<NodeInfo>> {
                anyhow::bail!("pcp_node_info: connection refused")
            }
        }
        let _ = pcp;
        let r = PgpoolReconciler::new(
            Arc::new(Boom),
            Arc::new(standby_db()),
            Arc::new(StubPeers::default()),
            pool(0),
            None,
        );
        let report = r.reconcile_once(false, None).await;
        assert!(report.map_error.is_some());
        assert!(report.actions.is_empty());
    }

    #[tokio::test]
    async fn run_reacts_to_a_kick_and_stops_on_shutdown() {
        let f = fixture(
            1,
            vec![
                node(0, true, "standby"),
                node(1, false, "standby"),
                node(2, true, "primary"),
            ],
            2,
            standby_db(),
        )
        .await;
        let r = Arc::new(f.r);
        let shutdown = CancellationToken::new();
        let handle = tokio::spawn(r.clone().run(shutdown.clone()));
        r.kick();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!f.pcp.attaches().is_empty());
        shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("run() exits on shutdown")
            .unwrap();
    }

    #[test]
    fn classify_matches_the_precondition_predicate() {
        assert_eq!(
            classify(true, true, "streaming", false),
            Evidence::StreamingStandby
        );
        assert_eq!(classify(true, false, "", true), Evidence::Primary);
        assert!(matches!(
            classify(true, false, "", false),
            Evidence::NotServing { .. }
        ));
        assert!(matches!(
            classify(true, true, "", false),
            Evidence::NotServing { .. }
        ));
        assert!(matches!(
            classify(false, true, "streaming", false),
            Evidence::NotServing { .. }
        ));
    }
}
