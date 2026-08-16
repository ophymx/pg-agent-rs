//! `PgAgentLocal` tonic service — Unix-socket gRPC for `pg_agentc` (the
//! hook-script binary pgpool execs) and `pg_agentctl` (operator CLI).
//!
//! Auth is filesystem permissions: the socket is mode `0600 root:postgres`,
//! so any caller with a connection is implicitly authorised.
//!
//! # Surface
//!
//! - **Hook orchestration** — `Failover`, `FollowPrimary`,
//!   `RecoveryFirstStage`, `RemoteStart`, `RestoreWal`, plus the
//!   no-op `Escalation` (HAProxy fronts the cluster; no VIP to move).
//! - **Reads** — `GetStatus`, `GetNodeConfig`.
//! - **Cluster ops** — `ClusterInit`, `ClusterStatus`,
//!   `GetPgpoolBackends`. Each is a single-shot fan-out the daemon
//!   does on the CLI's behalf; `pg_agentctl` never dials peers itself.
//! - **Maintenance queue** — `ListMaintenance`, `GetMaintenance`,
//!   `RetryMaintenance`.
//!
//! Every RPC validates its inputs against SPEC §3.3's regex set
//! before touching the local DB or the peer pool.

use crate::agent::NodeInfo;
use crate::config::{NodeConfig, NodePool, PostgresRuntime};
use crate::consensus::ConsensusStore as _;
use crate::localdb::LocalDb;
use crate::maintenance::{
    MaintenanceIntent as CoreIntent, MaintenancePayload, MaintenanceStatus, MaintenanceStore,
    SkippedIntent,
};
use crate::pcp::Pcp;
use crate::peers::{PeerClient, PeerRegistry};
use crate::pgstandby::{BasebackupOpts, RewindOpts, StandbyOps, WriteRecoveryConfOpts};
use crate::replay_markers::ReplayMarkerStore;
use crate::systemd::Systemd;
use crate::walstore::WalStore;
use chrono::SecondsFormat;
use pg_agent_proto::pgagentpb::{
    pg_agent_local_server::{PgAgentLocal, PgAgentLocalServer},
    AbandonInflightOpRequest, AllowAsyncRequest, ClusterHandoffRequest, ClusterInitRequest,
    ClusterInitResponse, ClusterInitStandbyResult, ClusterRecoverRequest, ClusterStatusEntry,
    ClusterStatusRequest, ClusterStatusResponse, EscalationRequest, FailoverRequest,
    FollowPrimaryRequest, GetInflightOpRequest, GetMaintenanceRequest, GetPgpoolBackendsRequest,
    GetPgpoolBackendsResponse, GetStatusRequest, InflightOp as ProtoInflightOp,
    ListInflightOpsRequest, ListInflightOpsResponse, ListMaintenanceRequest,
    ListMaintenanceResponse, MaintenanceIntent as ProtoIntent, NodeConfigRequest,
    NodeConfigResponse, NodeRef, NodeStatus, OpResult, PgpoolBackendEntry, RecoveryRequest,
    RemoteStartRequest, RestoreWalRequest, ResumeInflightOpRequest, RetryMaintenanceRequest,
    SkippedInflightOp as ProtoSkippedInflightOp, SkippedMaintenanceIntent,
};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::UnixListener;
use tokio_stream::wrappers::UnixListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::{transport::Server, Request, Response, Status};
use tracing::{debug, info, warn};

/// Per-peer FetchWal deadline. Bounds the hook against a single
/// partitioned peer wedging the entire fan-out. A WAL segment is 16 MiB,
/// so 30 s is wide slack for handshake + transfer on a healthy LAN while
/// still letting an unresponsive peer fail fast.
const RESTORE_WAL_PER_PEER_TIMEOUT: Duration = Duration::from_secs(30);

/// After a peer times out (or errors) on FetchWal, skip it for this
/// long. PostgreSQL invokes `restore_command` once per file, back to
/// back — at promotion, several times in a row — and without a
/// cooldown each invocation re-pays the full timeout for the same
/// partitioned peer. Acceptance E2 measured the cost: a promotion
/// stalled ~40 s re-probing an isolated node, wide enough for a rival
/// to depose the winner and promote a second primary. The cooldown is
/// process-local and short: a peer that recovers is retried within
/// seconds, and a false skip only means the segment comes from another
/// peer or the primary.
const RESTORE_WAL_PEER_COOLDOWN: Duration = Duration::from_secs(10);

// ---------------------------------------------------------------------------
// Handoff phase ladder
// ---------------------------------------------------------------------------
//
// Recorded in `InflightOp.phase` so a crashed orchestration can resume
// (`ResumeInflightOp`). Constants instead of an enum so the journal
// store stays generic over future ops. Order is the ladder; the
// `run_handoff_from_phase` function steps through them.

pub(crate) const HANDOFF_PHASE_PREFLIGHT_DONE: &str = "preflight_done";
pub(crate) const HANDOFF_PHASE_TARGET_PROMOTED: &str = "target_promoted";
pub(crate) const HANDOFF_PHASE_SLOT_CREATED: &str = "slot_created";
pub(crate) const HANDOFF_PHASE_LOCAL_STOPPED: &str = "local_stopped";
pub(crate) const HANDOFF_PHASE_DATA_COPIED: &str = "data_copied";
pub(crate) const HANDOFF_PHASE_RECOVERY_CONF_WRITTEN: &str = "recovery_conf_written";
pub(crate) const HANDOFF_PHASE_LOCAL_STARTED: &str = "local_started";
pub(crate) const HANDOFF_PHASE_ATTACHED: &str = "attached";

const HANDOFF_PHASES: &[&str] = &[
    HANDOFF_PHASE_PREFLIGHT_DONE,
    HANDOFF_PHASE_TARGET_PROMOTED,
    HANDOFF_PHASE_SLOT_CREATED,
    HANDOFF_PHASE_LOCAL_STOPPED,
    HANDOFF_PHASE_DATA_COPIED,
    HANDOFF_PHASE_RECOVERY_CONF_WRITTEN,
    HANDOFF_PHASE_LOCAL_STARTED,
    HANDOFF_PHASE_ATTACHED,
];

/// Index of `phase` in [`HANDOFF_PHASES`], or `None` if unknown.
pub(crate) fn handoff_phase_index(phase: &str) -> Option<usize> {
    HANDOFF_PHASES.iter().position(|p| *p == phase)
}

// ---------------------------------------------------------------------------
// FollowPrimary phase ladder (journaled / resumable)
// ---------------------------------------------------------------------------
//
// One per standby being rebased onto a primary. Driven by
// `drive_follow_primary`, which both the post-handoff fan-out and the
// operator-initiated resume path enter. Resume re-enters the driver at
// the recorded phase. The existing `PgAgentLocal::FollowPrimary` RPC
// (the pgpool hook) does NOT yet use these phases — it's still on the
// binary replay marker. Convergence is tracked as the "follow_primary
// unification" item; the two should eventually share this driver so
// orchestration is agnostic about which node pulled the trigger.

// `recovery_1st_stage` ladder. Mirrors the handoff/follow_primary
// shape: one phase per externally-visible step, recorded before the
// step's effect is durable so a crash leaves the journal pointing at
// the step that may be half-done rather than one too early.
pub(crate) const REC_PHASE_STARTED: &str = "started";
pub(crate) const REC_PHASE_SLOT_CREATED: &str = "slot_created";
pub(crate) const REC_PHASE_DATA_COPIED: &str = "data_copied";
pub(crate) const REC_PHASE_STANDBY_CONFIGURED: &str = "standby_configured";

/// How long a completed recovery suppresses an identical re-run.
/// Preserves the semantics of the 24 h replay marker this ladder
/// replaces (SPEC §5.12): pgpool may re-fire `recovery_1st_stage`
/// after a partial success, and a second destructive reclone is not
/// what it is asking for. `bypass_replay_marker` on the request
/// overrides it, which is how `cluster recover` re-runs deliberately.
pub(crate) const RECOVERY_DEDUP_WINDOW: chrono::Duration = chrono::Duration::hours(24);

/// BACKSTOP bound on how long a *finished* orchestration still counts
/// as owning its target for `failover`'s cross-op consult. Ownership
/// normally ends at an **event**, not this clock: the moment the
/// rebuilt node is observed alive (its slot active), the op is
/// discharged and a later destructive request proceeds immediately —
/// see [`crate::inflight_ops::owner_of_slot_observing`]. This constant
/// only bounds the case where that evidence never arrives.
///
/// Why finished ops own anything at all: pgpool's `failover_command`
/// is a delayed reaction — health-check detection
/// (`health_check_period` × retries) plus the hook's own exec time
/// means a hook caused by "the recovery stopped its target" routinely
/// arrives *after* the recovery has completed. Dropping the slot then
/// is exactly as destructive as dropping it mid-flight, and the
/// precondition check does not catch it either — the rebuilt standby
/// has been started but has not necessarily reached `streaming` yet,
/// so it reads as a legitimately-down node.
///
/// Two minutes comfortably clears a default pgpool detection window.
/// The cost of the backstop is bounded: for a node that never comes
/// up, its slot stays protected this long before the maintenance
/// queue can reclaim it.
pub const CROSS_OP_GRACE: chrono::Duration = chrono::Duration::seconds(120);

pub(crate) const FP_PHASE_QUEUED: &str = "queued";
pub(crate) const FP_PHASE_DIALING: &str = "dialing";
pub(crate) const FP_PHASE_DETACHED_STOPPED: &str = "detached_stopped";
pub(crate) const FP_PHASE_SLOT_CREATED: &str = "slot_created";
pub(crate) const FP_PHASE_DATA_COPIED: &str = "data_copied";
pub(crate) const FP_PHASE_RECOVERY_CONF_WRITTEN: &str = "recovery_conf_written";
pub(crate) const FP_PHASE_DETACHED_STARTED: &str = "detached_started";
pub(crate) const FP_PHASE_ATTACHED: &str = "attached";

const FP_PHASES: &[&str] = &[
    FP_PHASE_QUEUED,
    FP_PHASE_DIALING,
    FP_PHASE_DETACHED_STOPPED,
    FP_PHASE_SLOT_CREATED,
    FP_PHASE_DATA_COPIED,
    FP_PHASE_RECOVERY_CONF_WRITTEN,
    FP_PHASE_DETACHED_STARTED,
    FP_PHASE_ATTACHED,
];

pub(crate) fn fp_phase_index(phase: &str) -> Option<usize> {
    FP_PHASES.iter().position(|p| *p == phase)
}

pub struct LocalServer {
    node_info: Arc<dyn NodeInfo>,
    db: Arc<dyn LocalDb>,
    peers: Arc<dyn PeerRegistry>,
    maint: Arc<dyn MaintenanceStore>,
    wal: Arc<dyn WalStore>,
    replay: Arc<dyn ReplayMarkerStore>,
    /// Durable journal for multi-phase state-change orchestrations.
    /// See [`crate::inflight_ops`] for the contract. Consumed by
    /// `cluster_handoff` (single-flight gate + phase journal) and by
    /// the failover handler (cross-op consult before mutating cluster
    /// state). Currently unused in this commit; wired through for
    /// upcoming `cluster_handoff` refactor.
    #[allow(dead_code)]
    inflight: Arc<dyn crate::inflight_ops::InflightOpStore>,
    pcp: Arc<dyn Pcp>,
    /// Local systemd. `cluster_handoff` is the first handler that needs
    /// to stop/start the local PG service (every other handler runs
    /// after pgpool has already taken local down, or only touches
    /// remote peers via their `start`/`stop` peer RPCs). Threaded here
    /// rather than on the peer-side because handoff is local-driven —
    /// the primary's daemon owns the demotion.
    sd: Arc<dyn Systemd>,
    standby: Arc<dyn StandbyOps>,
    node_pool: NodePool,
    pg: PostgresRuntime,
    /// Peers recently failed/timed out on FetchWal, and when. See
    /// [`RESTORE_WAL_PEER_COOLDOWN`].
    wal_peer_cooldown: std::sync::Mutex<std::collections::HashMap<i32, std::time::Instant>>,
    /// Present when `[raft] enabled = true`. `ClusterInit` uses it to
    /// form the Raft cluster's initial membership — the one moment
    /// where an operator, not the protocol, decides who the members
    /// are.
    raft: Option<Arc<crate::raftconsensus::RaftRuntime>>,
}

impl LocalServer {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_info: Arc<dyn NodeInfo>,
        db: Arc<dyn LocalDb>,
        peers: Arc<dyn PeerRegistry>,
        maint: Arc<dyn MaintenanceStore>,
        wal: Arc<dyn WalStore>,
        replay: Arc<dyn ReplayMarkerStore>,
        inflight: Arc<dyn crate::inflight_ops::InflightOpStore>,
        pcp: Arc<dyn Pcp>,
        sd: Arc<dyn Systemd>,
        standby: Arc<dyn StandbyOps>,
        node_pool: NodePool,
        pg: PostgresRuntime,
    ) -> Self {
        Self {
            node_info,
            db,
            peers,
            maint,
            wal,
            replay,
            inflight,
            pcp,
            sd,
            standby,
            node_pool,
            pg,
            wal_peer_cooldown: std::sync::Mutex::new(std::collections::HashMap::new()),
            raft: None,
        }
    }

    /// Let `ClusterInit` bootstrap Raft membership.
    pub fn with_raft(mut self, raft: Arc<crate::raftconsensus::RaftRuntime>) -> Self {
        self.raft = Some(raft);
        self
    }

    /// Serve until `shutdown` cancels. `listener` is consumed.
    pub async fn serve(
        self,
        listener: UnixListener,
        shutdown: CancellationToken,
    ) -> anyhow::Result<()> {
        info!("local server: starting");
        let incoming = UnixListenerStream::new(listener);
        let result = Server::builder()
            .add_service(PgAgentLocalServer::new(self))
            .serve_with_incoming_shutdown(incoming, async move { shutdown.cancelled().await })
            .await;
        match result {
            Ok(()) => {
                info!("local server: shut down cleanly");
                Ok(())
            }
            Err(e) => {
                warn!(?e, "local server: shut down with error");
                Err(e.into())
            }
        }
    }
}

#[tonic::async_trait]
impl PgAgentLocal for LocalServer {
    // ----- read-only --------------------------------------------------------

    async fn get_status(
        &self,
        _req: Request<GetStatusRequest>,
    ) -> Result<Response<NodeStatus>, Status> {
        self.node_info
            .get_status()
            .await
            .map(Response::new)
            .map_err(internal)
    }

    async fn get_node_config(
        &self,
        _req: Request<NodeConfigRequest>,
    ) -> Result<Response<NodeConfigResponse>, Status> {
        self.node_info
            .get_node_config()
            .await
            .map(Response::new)
            .map_err(internal)
    }

    // ----- hook orchestration -----------------------------------------------

    /// `failover_command` — pgpool fires this on a surviving node when
    /// a backend goes down. Two branches:
    ///
    /// **Primary down** (`detached.id == old_primary.id`): advisory,
    /// period. pgpool's failure report is a hint, never an order —
    /// promotion is the lease's decision (the HA loop), so this branch
    /// logs and returns `ok=true` without touching cluster state.
    ///
    /// **Standby down** (`detached.id != old_primary.id`): we're the
    /// primary; drop the detached standby's replication slot from our
    /// local PG — mechanism, not authority. Returns `Ok` (with replay
    /// marker written) even when the slot drop itself fails — the drop
    /// is queued to maintenance for retry. pgpool doesn't need to
    /// re-fire the hook just because a slot cleanup got hung up.
    ///
    /// The one early-out without a marker: `new_main.id == -1` means
    /// pgpool found no standby candidates. We return `OpResult { ok =
    /// false }` (not a gRPC error) so pgpool can retry once a
    /// candidate exists.
    async fn failover(&self, req: Request<FailoverRequest>) -> Result<Response<OpResult>, Status> {
        let req = req.into_inner();
        let detached_ref = req
            .detached
            .ok_or_else(|| Status::invalid_argument("failover: detached is required"))?;
        let new_main_ref = req
            .new_main
            .ok_or_else(|| Status::invalid_argument("failover: new_main is required"))?;
        let old_primary_ref = req
            .old_primary
            .ok_or_else(|| Status::invalid_argument("failover: old_primary is required"))?;

        // SPEC §8: -1 is pgpool's "no candidates" sentinel.
        if new_main_ref.id == -1 {
            warn!("failover: no standby candidates available (new_main.id == -1)");
            return Ok(Response::new(OpResult {
                ok: false,
                message: "no standby candidates available".into(),
            }));
        }

        // The primary-down announcement is ALWAYS advisory: pgpool's
        // failure report is a hint, never an order, and the pgpool-led
        // promote path is gone — the lease decides who is primary
        // (promotion-authority §6; the legacy mode was removed
        // wholesale once greenfield deployment made it dead code).
        // Answered before the replay-marker check because the advisory
        // is stateless. Only standby-down slot hygiene below is real
        // work, and it keeps its guards.
        if detached_ref.id == old_primary_ref.id {
            info!(
                detached = %detached_ref.hostname,
                pgpool_pick = %new_main_ref.hostname,
                "failover: advisory under lease-driven roles; no action \
                 (the HA loop decides promotion)"
            );
            return Ok(Response::new(OpResult {
                ok: true,
                message: format!(
                    "failover: advisory — lease-driven roles; pgpool announced \
                     {} down (pick {}), promotion is the HA loop's decision",
                    detached_ref.hostname, new_main_ref.hostname
                ),
            }));
        }

        let replay_key = format!(
            "detached={},new_main={},old_primary={}",
            detached_ref.id, new_main_ref.id, old_primary_ref.id
        );
        match self.replay.has("failover", &replay_key).await {
            Ok(true) => {
                info!(%replay_key, "failover: replay detected, skipping");
                return Ok(Response::new(OpResult {
                    ok: true,
                    message: "failover: already processed; skipping duplicate".into(),
                }));
            }
            Ok(false) => {}
            Err(e) => {
                return Err(internal(anyhow::anyhow!(
                    "failover: idempotency marker check: {e}"
                )));
            }
        }

        let detached = self
            .node_pool
            .resolve_node(&detached_ref)
            .map_err(|e| Status::invalid_argument(format!("failover: detached: {e}")))?;
        let new_main = self
            .node_pool
            .resolve_node(&new_main_ref)
            .map_err(|e| Status::invalid_argument(format!("failover: new_main: {e}")))?;
        let old_primary = self
            .node_pool
            .resolve_node(&old_primary_ref)
            .map_err(|e| Status::invalid_argument(format!("failover: old_primary: {e}")))?;
        info!(
            detached = %detached.hostname,
            new_main = %new_main.hostname,
            old_primary = %old_primary.hostname,
            "failover"
        );

        let slot_name = detached.slot_name();

        // Standby down: the detached node is a standby whose slot on
        // this primary needs dropping (unless a guard says otherwise).
        // The advisory early-return above means this is the only path
        // that reaches here.
        // §1: standby down. We're the primary; drop the slot locally.
        //
        // Cross-op consult first: an in-flight orchestration that
        // owns this node's slot (recovery, follow_primary, handoff)
        // deliberately stops its target's PostgreSQL, which is what
        // made pgpool fire this hook. Dropping the slot now destroys
        // the one that orchestration created and leaves a standby
        // that can never stream — observed in the acceptance suite
        // before this consult existed.
        if let Some(owner) = self.inflight_owner_of(detached.id).await {
            info!(
                detached = %detached.hostname,
                slot = %slot_name,
                op = %owner.payload.op_name(),
                id = %owner.id,
                phase = %owner.phase,
                "failover: in-flight op owns this node; skipping slot drop"
            );
            return self
                .write_replay_marker_then_ok(
                    "failover",
                    &replay_key,
                    format!(
                        "standby failover: slot {slot_name} retained — in-flight {} \
                         (id={}, phase={}) owns node {}",
                        owner.payload.op_name(),
                        owner.id,
                        owner.phase,
                        detached.id
                    ),
                )
                .await;
        }
        //
        // Defense in depth, not the fix (promotion-authority §3): if
        // the announced-dead standby is reachable and demonstrably
        // streaming, pgpool's failure report is wrong and dropping
        // its slot would break healthy replication.
        match crate::preconditions::validate_cluster_preconditions(
            self.peers.clone(),
            crate::preconditions::ClusterIntent::DropSlotBecauseStandbyDown { detached },
        )
        .await
        {
            crate::preconditions::PreconditionOutcome::Refuse { message } => {
                warn!(detached = %detached.hostname, %message, "failover: precondition refused");
                return Ok(Response::new(OpResult { ok: false, message }));
            }
            crate::preconditions::PreconditionOutcome::Unverifiable { reason } => {
                crate::preconditions::log_unverifiable("standby_down", &reason);
            }
            crate::preconditions::PreconditionOutcome::Pass => {}
        }
        info!(
            detached = %detached.hostname,
            slot = %slot_name,
            "failover: standby down, dropping replication slot"
        );
        let message = match self.db.drop_slot(&slot_name).await {
            Ok(()) => format!("standby failover: slot {slot_name} dropped"),
            Err(drop_err) => {
                self.queue_drop_slot_cleanup(
                    &slot_name,
                    &old_primary.hostname,
                    "standby_down_local_drop_error",
                    &drop_err,
                )
                .await
            }
        };
        return self
            .write_replay_marker_then_ok("failover", &replay_key, message)
            .await;
    }

    /// `follow_primary_command` — pgpool runs this on the new primary
    /// after a failover, telling each surviving standby to rebase onto
    /// the new primary. Per-standby flow on this primary:
    ///
    /// 1. Idempotency: skip if `(detached_id, new_primary_id)` already
    ///    has a replay marker. pgpool may re-fire after a partial
    ///    success — without this the rewind/basebackup runs twice.
    /// 2. Resolve detached + new_primary from the pool.
    /// 3. Ask the detached peer for its status; if PG isn't running on
    ///    it (deliberate shutdown / hardware failure), skip — the
    ///    operator will reattach manually.
    /// 4. `peer.stop()` on detached — pg_rewind / pg_basebackup refuse
    ///    a running target.
    /// 5. Local `db.checkpoint()` (so the slot we create immediately
    ///    has a fresh restart_lsn).
    /// 6. `db.create_slot(detached.slot_name())` on this primary.
    /// 7. Try `peer.rewind()`. On failure fall back to
    ///    `peer.basebackup()` — full clone.
    /// 8. `peer.configure_standby()` writes recovery conf on detached.
    /// 9. `peer.start()` brings the standby up.
    /// 10. `pcp.attach_node(detached.id)` re-attaches to pgpool.
    /// 11. Write the replay marker.
    ///
    /// If anything between (6) and (9) fails, drop the slot we just
    /// created. If the drop itself fails, queue a maintenance intent
    /// (the worker retries with backoff). After (10), the slot is in
    /// use by the now-running standby — do NOT drop on `attach_node`
    /// failure; the slot is correct, only pgpool's view is stale.
    async fn follow_primary(
        &self,
        req: Request<FollowPrimaryRequest>,
    ) -> Result<Response<OpResult>, Status> {
        let req = req.into_inner();
        let detached_ref = req
            .detached
            .ok_or_else(|| Status::invalid_argument("follow_primary: detached is required"))?;
        let new_primary_ref = req
            .new_primary
            .ok_or_else(|| Status::invalid_argument("follow_primary: new_primary is required"))?;

        let replay_key = format!(
            "detached={},new_primary={}",
            detached_ref.id, new_primary_ref.id
        );
        match self.replay.has("follow_primary", &replay_key).await {
            Ok(true) => {
                info!(%replay_key, "follow_primary: replay detected, skipping");
                return Ok(Response::new(OpResult {
                    ok: true,
                    message: "follow_primary: already processed; skipping duplicate".into(),
                }));
            }
            Ok(false) => {}
            Err(e) => {
                return Err(internal(anyhow::anyhow!(
                    "follow_primary: idempotency marker check: {e}"
                )));
            }
        }

        let detached = self
            .node_pool
            .resolve_node(&detached_ref)
            .map_err(|e| Status::invalid_argument(format!("follow_primary: detached: {e}")))?;
        let new_primary = self
            .node_pool
            .resolve_node(&new_primary_ref)
            .map_err(|e| Status::invalid_argument(format!("follow_primary: new_primary: {e}")))?;
        info!(
            detached = %detached.hostname,
            new_primary = %new_primary.hostname,
            "follow_primary"
        );

        let peer = self.peers.client(detached).await.map_err(|e| {
            internal(anyhow::anyhow!(
                "follow_primary: peer client for {}: {e}",
                detached.hostname
            ))
        })?;

        // SPEC §10: skip a deliberately-stopped detached node.
        let status = peer.get_status().await.map_err(|e| {
            internal(anyhow::anyhow!(
                "follow_primary: get_status {}: {e}",
                detached.hostname
            ))
        })?;
        if !status.is_running {
            info!(detached = %detached.hostname, "follow_primary: detached not running, skipping");
            return Ok(Response::new(OpResult {
                ok: true,
                message: "detached node is stopped, skipping".into(),
            }));
        }

        peer.stop().await.map_err(|e| {
            internal(anyhow::anyhow!(
                "follow_primary: stop {}: {e}",
                detached.hostname
            ))
        })?;

        // SPEC §2: checkpoint before slot creation so the slot's
        // restart_lsn is at the current WAL position, not whatever was
        // there when the primary was promoted.
        self.db
            .checkpoint()
            .await
            .map_err(|e| internal(anyhow::anyhow!("follow_primary: checkpoint: {e}")))?;

        let slot_name = detached.slot_name();
        self.db.create_slot(&slot_name).await.map_err(|e| {
            internal(anyhow::anyhow!(
                "follow_primary: create slot {slot_name}: {e}"
            ))
        })?;

        // From here through `peer.start()`, any failure should drop the
        // slot — it would otherwise pin WAL on this primary forever.
        // After attach_node, the slot is in use by the standby; leave it.

        // §7: prefer rewind, fall back to basebackup on any rewind failure.
        let rewind_opts = RewindOpts {
            primary_host: new_primary.hostname.clone(),
            primary_port: self.pg.port,
            repl_user: self.pg.repl_user.clone(),
        };
        let rewind_ok = match peer.rewind(rewind_opts).await {
            Ok(()) => true,
            Err(e) => {
                warn!(
                    detached = %detached.hostname,
                    err = %e,
                    "follow_primary: rewind failed; falling back to basebackup"
                );
                false
            }
        };

        if !rewind_ok {
            let bb_opts = BasebackupOpts {
                primary_host: new_primary.hostname.clone(),
                primary_port: self.pg.port,
                repl_user: self.pg.repl_user.clone(),
                slot_name: slot_name.clone(),
            };
            if let Err(e) = peer.basebackup(bb_opts).await {
                let err = anyhow::anyhow!("follow_primary: basebackup {}: {e}", detached.hostname);
                self.cleanup_slot_after_failure(
                    &slot_name,
                    &new_primary.hostname,
                    "follow_primary_basebackup_failed",
                    &err,
                )
                .await;
                return Err(internal(err));
            }
        }

        let cfg_opts = WriteRecoveryConfOpts {
            primary_host: new_primary.hostname.clone(),
            primary_port: self.pg.port,
            repl_user: self.pg.repl_user.clone(),
            slot_name: slot_name.clone(),
        };
        if let Err(e) = peer.configure_standby(cfg_opts).await {
            let err = anyhow::anyhow!(
                "follow_primary: configure_standby {}: {e}",
                detached.hostname
            );
            self.cleanup_slot_after_failure(
                &slot_name,
                &new_primary.hostname,
                "follow_primary_configure_standby_failed",
                &err,
            )
            .await;
            return Err(internal(err));
        }

        if let Err(e) = peer.start().await {
            let err = anyhow::anyhow!("follow_primary: start {}: {e}", detached.hostname);
            self.cleanup_slot_after_failure(
                &slot_name,
                &new_primary.hostname,
                "follow_primary_start_failed",
                &err,
            )
            .await;
            return Err(internal(err));
        }

        // SPEC §5: pcp_attach_node is FollowPrimary-only.
        // From this point the slot is in use by the standby — don't drop
        // on attach failure; only pgpool's view is wrong, slot is correct.
        if let Err(e) = self.pcp.attach_node(detached.id).await {
            return Err(internal(anyhow::anyhow!(
                "follow_primary: pcp_attach_node {}: {e}",
                detached.id
            )));
        }

        if let Err(e) = self.replay.mark_done("follow_primary", &replay_key).await {
            return Err(internal(anyhow::anyhow!(
                "follow_primary: idempotency marker write: {e}"
            )));
        }
        info!(
            detached = %detached.hostname,
            new_primary = %new_primary.hostname,
            "follow_primary: complete"
        );
        Ok(Response::new(OpResult {
            ok: true,
            message: format!("follow_primary complete for {}", detached.hostname),
        }))
    }
    /// `recovery_1st_stage_command` — operator-initiated standby rebuild
    /// from this primary (the local node). Flow on this primary:
    ///
    /// 1. Replay marker on `(primary_id, standby_id)`; skip on hit.
    /// 2. Resolve `primary` via `resolve_local_node` — defensive check
    ///    that we ARE the configured primary. The request comes from
    ///    pgpool's recovery extension which only runs it on the primary,
    ///    but a misconfigured operator could fire it elsewhere.
    /// 3. `db.checkpoint()` + `db.create_slot(standby.slot_name())`.
    /// 4. `peer.basebackup(opts)` — wipes the standby's $PGDATA and
    ///    streams ours into it. Streamed via OpProgress; drained by
    ///    `PeerClient::basebackup` waiting for `phase="done"`.
    /// 5. `peer.configure_standby(opts)` writes recovery.conf on top
    ///    of the streamed $PGDATA. Order matters — basebackup wipes,
    ///    configure_standby populates after.
    /// 6. Write replay marker.
    ///
    /// What we do NOT do (vs. FollowPrimary):
    ///   - no `peer.stop()` first — the standby is offline awaiting
    ///     basebackup; pgpool wouldn't be running this otherwise.
    ///   - no rewind fallback — operator-initiated means we go straight
    ///     to a full clone.
    ///   - no `peer.start()` — pgpool starts the standby via
    ///     `pgpool_remote_start` in 2nd stage.
    ///   - no `pcp.attach_node()` — pgpool drives re-attach in 2nd stage.
    ///
    /// Failure between (3) and (5) drops the slot via the shared
    /// `cleanup_slot_after_failure` helper; if the drop itself fails it
    /// queues a `DropSlotCleanup` maintenance intent.
    async fn recovery_first_stage(
        &self,
        req: Request<RecoveryRequest>,
    ) -> Result<Response<OpResult>, Status> {
        let req = req.into_inner();
        let primary_ref = req
            .primary
            .ok_or_else(|| Status::invalid_argument("recovery_1st_stage: primary is required"))?;
        let standby_ref = req
            .standby
            .ok_or_else(|| Status::invalid_argument("recovery_1st_stage: standby is required"))?;

        let dedup_key = format!("primary={},standby={}", primary_ref.id, standby_ref.id);
        if req.bypass_replay_marker {
            info!(
                %dedup_key,
                "recovery_1st_stage: bypass_replay_marker=true; running unconditionally"
            );
        } else {
            // Dedup against a recently-completed run of the same
            // orchestration. (An *in-flight* duplicate is rejected
            // structurally by `inflight.begin` below, which the replay
            // marker could never do — it was only written after
            // success.)
            match self.inflight.find("recovery", &dedup_key).await {
                Ok(Some(op))
                    if op.status == crate::inflight_ops::InflightStatus::Done
                        && op
                            .completed_at
                            .is_some_and(|t| chrono::Utc::now() - t < RECOVERY_DEDUP_WINDOW) =>
                {
                    info!(%dedup_key, id = %op.id, "recovery_1st_stage: recent completion, skipping");
                    // Distinct message from the basebackup-ran success
                    // case so operator-facing callers (cluster_recover)
                    // can flag this as "no work done" rather than the
                    // identical-looking "complete" wording that masked
                    // a silent skip on db2 on 2026-06-12.
                    return Ok(Response::new(OpResult {
                        ok: true,
                        message: format!(
                            "recovery_1st_stage: skipped — an identical recovery completed \
                             within the last {}h (op {}); pass --stop-target-pg via \
                             `cluster recover` to force a fresh one",
                            RECOVERY_DEDUP_WINDOW.num_hours(),
                            op.id
                        ),
                    }));
                }
                Ok(_) => {}
                Err(e) => {
                    return Err(internal(anyhow::anyhow!(
                        "recovery_1st_stage: inflight lookup: {e}"
                    )));
                }
            }
        }

        let primary = self
            .node_pool
            .resolve_local_node(&primary_ref)
            .map_err(|e| Status::invalid_argument(format!("recovery_1st_stage: primary: {e}")))?;
        let standby = self
            .node_pool
            .resolve_node(&standby_ref)
            .map_err(|e| Status::invalid_argument(format!("recovery_1st_stage: standby: {e}")))?;
        info!(
            primary = %primary.hostname,
            standby = %standby.hostname,
            "recovery_1st_stage"
        );

        let slot_name = standby.slot_name();

        // Journal before touching anything. The op is what tells a
        // concurrent `failover` (fired by pgpool the moment this
        // orchestration stops the target's PostgreSQL) that this node's
        // slot belongs to an operation in progress.
        let op = self
            .inflight
            .begin(
                crate::inflight_ops::InflightPayload::Recovery {
                    primary_node_id: primary.id,
                    standby_node_id: standby.id,
                    standby_hostname: standby.hostname.clone(),
                    slot_name: slot_name.clone(),
                },
                REC_PHASE_STARTED,
                false,
            )
            .await
            .map_err(|e| internal(anyhow::anyhow!("recovery_1st_stage: journal begin: {e}")))?;
        let op_id = op.id.clone();

        // Abandon the journal entry on any failure below, so a later
        // attempt isn't rejected as a duplicate of a run that died.
        macro_rules! fail {
            ($err:expr) => {{
                let err = $err;
                if let Err(je) = self.inflight.abandon(&op_id, &err.to_string()).await {
                    warn!(id = %op_id, ?je, "recovery_1st_stage: journal abandon failed");
                }
                return Err(internal(err));
            }};
        }

        // SPEC §2: checkpoint then create_slot so the slot's restart_lsn
        // sits at the current WAL position. Without this, basebackup
        // could start from an older checkpoint and the slot would
        // immediately need WAL we no longer keep.
        if let Err(e) = self.db.checkpoint().await {
            fail!(anyhow::anyhow!("recovery_1st_stage: checkpoint: {e}"));
        }

        if let Err(e) = self.db.create_slot(&slot_name).await {
            fail!(anyhow::anyhow!(
                "recovery_1st_stage: create slot {slot_name}: {e}"
            ));
        }
        if let Err(e) = self
            .inflight
            .update_phase(&op_id, REC_PHASE_SLOT_CREATED, None)
            .await
        {
            warn!(id = %op_id, ?e, "recovery_1st_stage: journal phase update failed");
        }

        // From here through `configure_standby`, any failure drops the
        // slot — it would otherwise pin WAL forever on this primary.
        let peer = match self.peers.client(standby).await {
            Ok(p) => p,
            Err(e) => {
                let err = anyhow::anyhow!(
                    "recovery_1st_stage: peer client for {}: {e}",
                    standby.hostname
                );
                self.cleanup_slot_after_failure(
                    &slot_name,
                    &primary.hostname,
                    "recovery_1st_stage_peer_dial_failed",
                    &err,
                )
                .await;
                fail!(err);
            }
        };

        let bb_opts = BasebackupOpts {
            primary_host: primary.hostname.clone(),
            primary_port: self.pg.port,
            repl_user: self.pg.repl_user.clone(),
            slot_name: slot_name.clone(),
        };
        if let Err(e) = peer.basebackup(bb_opts).await {
            let err = anyhow::anyhow!("recovery_1st_stage: basebackup {}: {e}", standby.hostname);
            self.cleanup_slot_after_failure(
                &slot_name,
                &primary.hostname,
                "recovery_1st_stage_basebackup_failed",
                &err,
            )
            .await;
            fail!(err);
        }
        if let Err(e) = self
            .inflight
            .update_phase(&op_id, REC_PHASE_DATA_COPIED, None)
            .await
        {
            warn!(id = %op_id, ?e, "recovery_1st_stage: journal phase update failed");
        }

        let cfg_opts = WriteRecoveryConfOpts {
            primary_host: primary.hostname.clone(),
            primary_port: self.pg.port,
            repl_user: self.pg.repl_user.clone(),
            slot_name: slot_name.clone(),
        };
        if let Err(e) = peer.configure_standby(cfg_opts).await {
            let err = anyhow::anyhow!(
                "recovery_1st_stage: configure_standby {}: {e}",
                standby.hostname
            );
            self.cleanup_slot_after_failure(
                &slot_name,
                &primary.hostname,
                "recovery_1st_stage_configure_standby_failed",
                &err,
            )
            .await;
            fail!(err);
        }
        if let Err(e) = self
            .inflight
            .update_phase(&op_id, REC_PHASE_STANDBY_CONFIGURED, None)
            .await
        {
            warn!(id = %op_id, ?e, "recovery_1st_stage: journal phase update failed");
        }

        // SPEC §5: do NOT call pcp_attach_node here — pgpool drives
        // re-attachment after 2nd stage completes (which is triggered
        // by pgpool itself via pgpool_remote_start, not us).
        self.inflight
            .complete(&op_id)
            .await
            .map_err(|e| internal(anyhow::anyhow!("recovery_1st_stage: journal complete: {e}")))?;

        info!(
            primary = %primary.hostname,
            standby = %standby.hostname,
            slot = %slot_name,
            "recovery_1st_stage: complete"
        );
        Ok(Response::new(OpResult {
            ok: true,
            message: format!("recovery complete for {}", standby.hostname),
        }))
    }
    /// `restore_command` — PostgreSQL calls `pg_agentc restore-wal %f %p`
    /// when a WAL segment is missing from its local pg_wal. We fan out
    /// across the pool in declaration order, returning the first peer's
    /// content that fits in `dest_path`. Per-peer attempts are bounded
    /// by `RESTORE_WAL_PER_PEER_TIMEOUT` (private const) so a single
    /// partitioned peer can't wedge the whole hook.
    ///
    /// `DestOutsidePgData` is fatal — it's a config / caller bug and
    /// every peer would fail the same way. NotFound on a peer is "try
    /// next". Generic peer errors are logged and treated as TryNext so
    /// transient failures don't bring down PG's restore loop.
    async fn restore_wal(
        &self,
        req: Request<RestoreWalRequest>,
    ) -> Result<Response<OpResult>, Status> {
        let req = req.into_inner();
        if req.wal_file.is_empty() {
            return Err(Status::invalid_argument(
                "restore_wal: wal_file is required",
            ));
        }
        if req.dest_path.is_empty() {
            return Err(Status::invalid_argument(
                "restore_wal: dest_path is required",
            ));
        }
        info!(wal_file = %req.wal_file, dest_path = %req.dest_path, "restore_wal");

        let local = self
            .node_pool
            .local_node()
            .map_err(|e| internal(anyhow::anyhow!("restore_wal: resolve local node: {e}")))?;
        let local_id = local.id;

        for node in &self.node_pool.members {
            if node.id == local_id {
                continue;
            }
            {
                let cooldown = self.wal_peer_cooldown.lock().unwrap();
                if let Some(since) = cooldown.get(&node.id) {
                    if since.elapsed() < RESTORE_WAL_PEER_COOLDOWN {
                        debug!(peer = %node.hostname, "restore_wal: peer in cooldown; skipping");
                        continue;
                    }
                }
            }
            match self
                .try_fetch_wal_from_peer(node, &req.wal_file, &req.dest_path)
                .await
            {
                FetchOutcome::Fetched => {
                    info!(wal_file = %req.wal_file, peer = %node.hostname, "restore_wal: fetched");
                    return Ok(Response::new(OpResult {
                        ok: true,
                        message: format!("restored {} from {}", req.wal_file, node.hostname),
                    }));
                }
                FetchOutcome::TryNext => continue,
                FetchOutcome::Fatal(s) => return Err(s),
            }
        }

        // Exhausted the pool — no peer had it. PG's restore_command
        // contract: non-zero exit signals "segment unavailable", which
        // pauses replay and retries. Maps cleanly to NotFound here.
        Err(Status::not_found(format!(
            "restore_wal: {} not found on any peer",
            req.wal_file
        )))
    }
    /// `pg_agentctl cluster init` — operator one-shot bootstrap. Must
    /// run on the chosen primary. Ensures `repl_user` exists, then for
    /// each non-local pool entry (or just the `only_node_id` if set):
    /// create_slot → stop → basebackup → configure_standby → start.
    /// Per-standby failures are collected; the overall response.ok is
    /// false if any standby failed.
    ///
    /// Each per-standby failure path drops the slot we just created
    /// (consistent with FollowPrimary/RecoveryFirstStage); if the drop
    /// itself fails, a `DropSlotCleanup` maintenance intent is queued.
    /// SPEC §5.7 spells out the cleanup rule.
    ///
    /// Does NOT call `pcp_attach_node` — pgpool isn't running yet
    /// during initial bootstrap. Adding-to-a-running-cluster is the
    /// future `pg_agentctl cluster attach <id>` command.
    async fn cluster_init(
        &self,
        req: Request<ClusterInitRequest>,
    ) -> Result<Response<ClusterInitResponse>, Status> {
        let req = req.into_inner();

        // SPEC §5.7 invariant: must run on the primary.
        let in_recovery =
            self.db.is_in_recovery().await.map_err(|e| {
                internal(anyhow::anyhow!("cluster_init: check primary status: {e}"))
            })?;
        if in_recovery {
            return Ok(Response::new(ClusterInitResponse {
                ok: false,
                message: "cluster_init: local node is not the primary (in recovery)".into(),
                repl_user: self.pg.repl_user.clone(),
                standbys: Vec::new(),
            }));
        }

        let primary = self
            .node_pool
            .local_node()
            .map_err(|e| internal(anyhow::anyhow!("cluster_init: resolve local node: {e}")))?;
        let primary_hostname = primary.hostname.clone();
        let primary_id = primary.id;

        info!(role = %self.pg.repl_user, "cluster_init: ensuring replication role");
        self.db
            .create_replication_role(&self.pg.repl_user)
            .await
            .map_err(|e| {
                internal(anyhow::anyhow!(
                    "cluster_init: create replication role {:?}: {e}",
                    self.pg.repl_user
                ))
            })?;

        let mut results: Vec<ClusterInitStandbyResult> = Vec::new();
        let mut failures = 0usize;
        for node in &self.node_pool.members {
            if node.id == primary_id {
                continue;
            }
            if let Some(only) = req.only_node_id {
                if only != node.id {
                    continue;
                }
            }
            let res = self.init_standby(node, &primary_hostname).await;
            if !res.ok {
                failures += 1;
            }
            results.push(res);
        }

        // Form Raft's initial membership from the same pool that just
        // got its replication set up. ClusterInit is the right place
        // because it is already the one operator-driven "this is the
        // cluster" moment; doing it at daemon startup instead would
        // have every node racing to declare a membership, and doing it
        // implicitly on first election would mean the set of members
        // depends on who booted first.
        //
        // Never fatal to cluster_init. Replication has by this point
        // actually been configured, and reporting that as a failure
        // because consensus bootstrap did not take would send the
        // operator back to re-run a destructive-ish command over work
        // that already succeeded.
        let raft_note = match &self.raft {
            Some(rt) => match rt.bootstrap_membership().await {
                Ok(outcome) => {
                    // Seed the lease for this primary (promotion-authority
                    // step 7: seeding replaces shadow-only vacant
                    // adoption as the bootstrap). CAS on observed
                    // vacancy: losing means a holder already exists,
                    // which is the goal state, not an error — exactly
                    // the membership-bootstrap idempotency argument
                    // again.
                    let seed = match rt.store.try_takeover(primary_id, None).await {
                        Ok(crate::consensus::TakeoverOutcome::Won { lease }) => {
                            info!(
                                term = lease.term,
                                "cluster_init: lease seeded for this primary"
                            );
                            format!("lease seeded (node {primary_id}, term {})", lease.term)
                        }
                        Ok(crate::consensus::TakeoverOutcome::Lost { current }) => format!(
                            "lease already held{}",
                            current
                                .map(|l| format!(" (node {}, term {})", l.holder, l.term))
                                .unwrap_or_default()
                        ),
                        Err(e) => {
                            warn!(?e, "cluster_init: lease seeding failed");
                            format!("lease seeding FAILED: {e}")
                        }
                    };
                    Some(format!("{}; {seed}", outcome.describe()))
                }
                Err(e) => {
                    warn!(?e, "cluster_init: raft membership bootstrap failed");
                    Some(format!("raft membership bootstrap FAILED: {e}"))
                }
            },
            None => None,
        };

        let (ok, message) = match (results.len(), failures) {
            (0, _) => (
                true,
                format!(
                    "cluster_init: replication role {} ensured; no standby nodes selected",
                    self.pg.repl_user
                ),
            ),
            (total, 0) => (
                true,
                format!("cluster_init complete: {total} standby(s) initialised"),
            ),
            (total, n) => (
                false,
                format!("cluster_init: {n} of {total} standby(s) failed"),
            ),
        };

        let message = match raft_note {
            Some(note) => format!("{message}; {note}"),
            None => message,
        };

        Ok(Response::new(ClusterInitResponse {
            ok,
            message,
            repl_user: self.pg.repl_user.clone(),
            standbys: results,
        }))
    }

    /// `pg_agentctl cluster recover --target <id>` — operator-initiated
    /// standby reclone. Same orchestration as pgpool's
    /// `recovery_1st_stage_command` hook (which routes through
    /// `pg_agentc recovery1`); different front door so the operator
    /// doesn't need to debug PCP auth (`~postgres/.pcppass`) to
    /// trigger a rebuild — the daemon already owns those credentials.
    ///
    /// Refuses on a node that's in recovery: a standby can't reclone
    /// another standby, and the error message names the operation so
    /// the caller can redirect.
    ///
    /// Preflights `peer.get_status` on the target. If PG is running
    /// there, the operator must opt in via `--stop-target-pg` (proto:
    /// `stop_target_pg=true`) to stop it first — otherwise the call
    /// returns `ok=false` with an actionable message instead of dying
    /// eight layers deep inside the basebackup safety check. With the
    /// flag, the daemon issues `peer.stop` and then proceeds.
    ///
    /// Resolves the local node as primary (its config-sourced
    /// `NodeConfig` — same source of truth as `cluster init`) and the
    /// target by pool id, then delegates to `recovery_first_stage`.
    /// Idempotency-marker behaviour is inherited — if a successful
    /// recovery has already been recorded for the same `(primary,
    /// standby)` pair within the retention window, the call returns
    /// `ok=true, message="…already processed…"` and does no work. The
    /// operator can re-run after retention sweeps the marker, or delete
    /// the marker file directly under `<state_dir>/replay/`.
    async fn cluster_recover(
        &self,
        req: Request<ClusterRecoverRequest>,
    ) -> Result<Response<OpResult>, Status> {
        let req = req.into_inner();

        let in_recovery = self.db.is_in_recovery().await.map_err(|e| {
            internal(anyhow::anyhow!(
                "cluster_recover: check primary status: {e}"
            ))
        })?;
        if in_recovery {
            return Ok(Response::new(OpResult {
                ok: false,
                message: "cluster_recover: local node is not the primary (in recovery); \
                          run this on the current primary"
                    .into(),
            }));
        }

        let primary = self
            .node_pool
            .local_node()
            .map_err(|e| internal(anyhow::anyhow!("cluster_recover: resolve local node: {e}")))?;

        let standby = self.node_pool.node_by_id(req.target_node_id).map_err(|e| {
            Status::invalid_argument(format!(
                "cluster_recover: target node {}: {e}",
                req.target_node_id
            ))
        })?;

        if self.node_pool.is_local(standby) {
            return Err(Status::invalid_argument(
                "cluster_recover: target is the local node — reclone target must be a peer",
            ));
        }

        // Preflight: assess PG state on the target. If PG is running we
        // either stop it now (operator opt-in via --stop-target-pg) or
        // refuse with an actionable message. Without this preflight the
        // basebackup safety check (peerserver.rs: "refusing to basebackup
        // while postgres is running") catches it, but eight layers deep
        // inside recovery_first_stage and with no opt-in path for the
        // operator. The peer channel is cached, so the get_status here
        // is "free" relative to the peer.basebackup recovery_first_stage
        // will run shortly.
        let peer = self.peers.client(standby).await.map_err(|e| {
            internal(anyhow::anyhow!(
                "cluster_recover: dial peer {}: {e}",
                standby.hostname
            ))
        })?;
        let target_status = peer.get_status().await.map_err(|e| {
            internal(anyhow::anyhow!(
                "cluster_recover: peer get_status {}: {e}",
                standby.hostname
            ))
        })?;
        if target_status.is_postgres_running {
            if !req.stop_target_pg {
                return Ok(Response::new(OpResult {
                    ok: false,
                    message: format!(
                        "cluster_recover: postgres is running on target {}; \
                         rerun with --stop-target-pg to stop it before reclone, \
                         or stop it manually on that host first",
                        standby.hostname
                    ),
                }));
            }
            info!(
                target = %standby.hostname,
                "cluster_recover: stopping postgres on target (--stop-target-pg)"
            );
            peer.stop().await.map_err(|e| {
                internal(anyhow::anyhow!(
                    "cluster_recover: stop postgres on {}: {e}",
                    standby.hostname
                ))
            })?;
        }

        info!(
            target = %standby.hostname,
            primary = %primary.hostname,
            "cluster_recover: delegating to recovery_first_stage"
        );

        let inner_req = RecoveryRequest {
            primary: Some(NodeRef {
                id: primary.id,
                hostname: primary.hostname.clone(),
                pg_port: 0,
                pg_data: String::new(),
            }),
            standby: Some(NodeRef {
                id: standby.id,
                hostname: standby.hostname.clone(),
                pg_port: 0,
                pg_data: String::new(),
            }),
            // Operator-driven `cluster recover` always means "actually
            // reclone now." Without this, a stale 24h replay marker
            // (set by a prior cluster_recover or pgpool hook) silently
            // short-circuits recovery_first_stage and the wrapper
            // reports "recovery complete" even though basebackup never
            // ran — observed live on db2 on 2026-06-12.
            bypass_replay_marker: true,
        };
        let rec_resp = self
            .recovery_first_stage(Request::new(inner_req))
            .await?
            .into_inner();
        if !rec_resp.ok {
            // recovery_first_stage handled cleanup; just surface.
            return Ok(Response::new(rec_resp));
        }

        // Post-recovery: bring the target back online.
        //
        // pgpool's own pcp_recovery_node flow would drive these via
        // recovery_2nd_stage + pgpool_remote_start; cluster_recover
        // doesn't go through pgpool so they don't happen automatically.
        // Operator otherwise has to ssh to the target, `systemctl start
        // postgresql`, then back to a pgpool-running host and
        // `pcp_attach_node`. That's three nodes worth of yak-shave in
        // the middle of recovery — fold it into one RPC.
        //
        // Each post-step is best-effort: a failure does NOT roll back
        // recovery_first_stage (it already succeeded) and does NOT
        // fail the whole RPC, because the operator can retry these
        // steps independently. The final message reports which steps
        // worked.
        let mut post_status = Vec::with_capacity(3);

        info!(target = %standby.hostname, "cluster_recover: starting postgres on target");
        match peer.start().await {
            Ok(()) => post_status.push("postgres started".to_string()),
            Err(e) => {
                warn!(
                    target = %standby.hostname,
                    ?e,
                    "cluster_recover: peer start (postgres) failed"
                );
                post_status.push(format!("postgres start failed: {e}"));
            }
        }

        info!(target = %standby.hostname, "cluster_recover: starting pgpool on target");
        match peer.start_pgpool().await {
            Ok(()) => post_status.push("pgpool started".to_string()),
            Err(e) => {
                warn!(
                    target = %standby.hostname,
                    ?e,
                    "cluster_recover: peer start_pgpool failed (target's local supervisor will retry)"
                );
                post_status.push(format!("pgpool start failed: {e}"));
            }
        }

        info!(
            target_id = standby.id,
            "cluster_recover: pgpool attach_node"
        );
        match self.pcp.attach_node(standby.id).await {
            Ok(()) => post_status.push(format!("attached node {} in pgpool", standby.id)),
            Err(e) => {
                warn!(
                    target_id = standby.id,
                    ?e,
                    "cluster_recover: pcp attach_node failed; operator may need pcp_attach_node manually"
                );
                post_status.push(format!("pgpool attach failed: {e}"));
            }
        }

        Ok(Response::new(OpResult {
            ok: true,
            message: format!(
                "recovery complete for {}; {}",
                standby.hostname,
                post_status.join("; ")
            ),
        }))
    }

    /// `pg_agentctl cluster handoff --target <id>` — planned primary
    /// handoff. Run from the current primary. Promotes the target
    /// standby, demotes the local node to a standby of the new primary.
    ///
    /// Distinct from `cluster recover` (rebuilds a broken target FROM
    /// the local primary): handoff REPLACES the local primary WITH the
    /// target. Distinct from the pgpool-driven `failover` (reactive):
    /// handoff is operator-initiated and chooses the target.
    ///
    /// Refusal cases (all returned as `ok=false`, not gRPC errors —
    /// operator can fix and re-run):
    /// - local is not the primary
    /// - target unreachable / PG not running on target
    /// - target is already a primary
    /// - target lag > `MAX_HANDOFF_LAG_BYTES` (16 MiB) unless
    ///   `allow_lag=true`
    ///
    /// Idempotent + crash-safe via [`crate::inflight_ops`]: every phase
    /// transition is journaled to `<state_dir>/inflight_ops/<id>.json`,
    /// so a crashed orchestration surfaces via `pg_agentctl ops list`
    /// and can be continued with `pg_agentctl ops resume <id>` or
    /// terminated with `pg_agentctl ops abandon <id>`. Single-flight
    /// is structural: `inflight.begin(.., exclusive=true)` atomically
    /// refuses if any state-change op is already in flight.
    async fn cluster_handoff(
        &self,
        req: Request<ClusterHandoffRequest>,
    ) -> Result<Response<OpResult>, Status> {
        let req = req.into_inner();

        // ----- Preflight ----------------------------------------------
        let in_recovery = self
            .db
            .is_in_recovery()
            .await
            .map_err(|e| internal(anyhow::anyhow!("cluster_handoff: is_in_recovery: {e}")))?;
        if in_recovery {
            return Ok(Response::new(OpResult {
                ok: false,
                message: "cluster_handoff: local node is not the primary (in recovery); \
                          run this on the current primary"
                    .into(),
            }));
        }

        let local = self
            .node_pool
            .local_node()
            .map_err(|e| internal(anyhow::anyhow!("cluster_handoff: resolve local: {e}")))?
            .clone();
        let target = self
            .node_pool
            .node_by_id(req.target_node_id)
            .map_err(|e| {
                Status::invalid_argument(format!(
                    "cluster_handoff: target node {}: {e}",
                    req.target_node_id
                ))
            })?
            .clone();
        if self.node_pool.is_local(&target) {
            return Err(Status::invalid_argument(
                "cluster_handoff: target is the local node — handoff target must be a peer",
            ));
        }

        // Idempotency: find the most-recent journal entry for this
        // (op, key). Done → "already processed"; InProgress → tell the
        // operator to resume or abandon (refusing the fresh begin
        // would happen at `begin` anyway, but a friendlier message
        // here saves a round-trip).
        let key = format!("from={},to={}", local.id, target.id);
        match self.inflight.find("handoff", &key).await {
            Ok(Some(op)) if op.status == crate::inflight_ops::InflightStatus::Done => {
                info!(%key, id = %op.id, "cluster_handoff: replay detected, skipping");
                return Ok(Response::new(OpResult {
                    ok: true,
                    message: "cluster_handoff: already processed; skipping duplicate".into(),
                }));
            }
            Ok(Some(op)) if op.status == crate::inflight_ops::InflightStatus::InProgress => {
                return Ok(Response::new(OpResult {
                    ok: false,
                    message: format!(
                        "cluster_handoff: handoff already in flight (id={}, phase={}); \
                         resume with `pg_agentctl ops resume {}` or abandon with \
                         `pg_agentctl ops abandon {}`",
                        op.id, op.phase, op.id, op.id
                    ),
                }));
            }
            Ok(_) => {} // None or Abandoned — fresh handoff.
            Err(e) => {
                return Err(internal(anyhow::anyhow!(
                    "cluster_handoff: inflight lookup: {e}"
                )));
            }
        }

        let peer = self.peers.client(&target).await.map_err(|e| {
            internal(anyhow::anyhow!(
                "cluster_handoff: dial peer {}: {e}",
                target.hostname
            ))
        })?;
        let target_status = peer.get_status().await.map_err(|e| {
            internal(anyhow::anyhow!(
                "cluster_handoff: peer get_status {}: {e}",
                target.hostname
            ))
        })?;
        if !target_status.is_postgres_running {
            return Ok(Response::new(OpResult {
                ok: false,
                message: format!(
                    "cluster_handoff: target {} has postgres stopped; \
                     start it first or pick a different target",
                    target.hostname
                ),
            }));
        }
        if !target_status.is_in_recovery {
            return Ok(Response::new(OpResult {
                ok: false,
                message: format!(
                    "cluster_handoff: target {} is not a standby (already primary); \
                     pick a standby as the handoff target",
                    target.hostname
                ),
            }));
        }
        // Lag pre-check: are we about to silently lose writes by
        // promoting a target that's behind us?
        //
        // The wire field `replication_lag_bytes` measures the
        // standby's *replay-vs-receive* (how far PG's recovery is
        // behind WAL it has already pulled from us). That number is
        // ZERO when the WAL receiver is disconnected — exactly the
        // failure mode we need to catch — so we can't trust it for
        // the "is target caught up to local primary?" question.
        //
        // The correct measure is `local.current_wal_lsn -
        // target.last_wal_replay_lsn`. NodeStatus.current_wal_lsn
        // already returns `pg_current_wal_lsn()` when populated from
        // a primary and `pg_last_wal_replay_lsn()` when populated
        // from a standby (added in 0.4.0 for the split-brain LEAD
        // marker) — so we already have both halves on the wire.
        if !req.allow_lag {
            let local_lsn = self.db.current_wal_lsn().await.map_err(|e| {
                internal(anyhow::anyhow!(
                    "cluster_handoff: local current_wal_lsn: {e}"
                ))
            })?;
            let target_replay_lsn = target_status.current_wal_lsn;
            // 0 from either side means the probe failed or the peer is
            // a pre-feature build; we can't safely measure lag in
            // either case. Make the operator opt in via --allow-lag.
            if local_lsn == 0 || target_replay_lsn == 0 {
                return Ok(Response::new(OpResult {
                    ok: false,
                    message: format!(
                        "cluster_handoff: cannot measure lag — local current_wal_lsn={} \
                         target last_replay_lsn={} (a 0 value means the LSN probe failed \
                         or the peer is on a pre-0.4.0 build); rerun with --allow-lag to \
                         override (will accept unknown data loss)",
                        local_lsn, target_replay_lsn
                    ),
                }));
            }
            let lag = local_lsn.saturating_sub(target_replay_lsn);
            if lag > crate::config::MAX_HANDOFF_LAG_BYTES as u64 {
                return Ok(Response::new(OpResult {
                    ok: false,
                    message: format!(
                        "cluster_handoff: target {} is {} bytes behind local primary \
                         (local LSN {:X}/{:08X}, target replay LSN {:X}/{:08X}); exceeds \
                         threshold {} bytes (one WAL segment); rerun with --allow-lag to \
                         override (will accept data loss for writes between the standby's \
                         replay LSN and the primary's current LSN)",
                        target.hostname,
                        lag,
                        local_lsn >> 32,
                        local_lsn as u32,
                        target_replay_lsn >> 32,
                        target_replay_lsn as u32,
                        crate::config::MAX_HANDOFF_LAG_BYTES
                    ),
                }));
            }
        }

        // ----- Begin journal entry (single-flight gate) ---------------
        let slot_name = local.slot_name();
        let payload = crate::inflight_ops::InflightPayload::Handoff {
            from_node_id: local.id,
            to_node_id: target.id,
            to_hostname: target.hostname.clone(),
            slot_name: slot_name.clone(),
            allow_lag: req.allow_lag,
        };
        let op = match self
            .inflight
            .begin(payload, HANDOFF_PHASE_PREFLIGHT_DONE, true)
            .await
        {
            Ok(op) => op,
            Err(e) => {
                // BeginRejected from the store maps to ok=false so the
                // operator can act on the structured message.
                return Ok(Response::new(OpResult {
                    ok: false,
                    message: format!("cluster_handoff: {e}"),
                }));
            }
        };

        info!(
            id = %op.id,
            local = %local.hostname,
            target = %target.hostname,
            target_lag_bytes = target_status.replication_lag_bytes,
            "cluster_handoff: preflight ok, starting orchestration"
        );

        let resp = self
            .run_handoff_from_phase(
                &op.id,
                HANDOFF_PHASE_PREFLIGHT_DONE,
                &peer,
                &target,
                &local,
                &slot_name,
            )
            .await?;
        // Demotion done; rebase any other standbys onto the new primary
        // in background tasks. Each follow-up is an independent
        // InflightOp visible via `pg_agentctl ops list` — failure on one
        // does not affect the others, and operator can resume any that
        // stall mid-way. See `drive_follow_primary`.
        if resp.get_ref().ok {
            self.fan_out_follow_primary(&local, &target).await;
        }
        Ok(resp)
    }

    async fn list_inflight_ops(
        &self,
        req: Request<ListInflightOpsRequest>,
    ) -> Result<Response<ListInflightOpsResponse>, Status> {
        let req = req.into_inner();
        let statuses = parse_inflight_statuses(&req.statuses)
            .map_err(|e| Status::invalid_argument(format!("list_inflight_ops: {e}")))?;
        let (ops, skipped) = self
            .inflight
            .list(&statuses)
            .await
            .map_err(|e| internal(anyhow::anyhow!("list_inflight_ops: {e}")))?;
        let mut out = Vec::with_capacity(ops.len());
        for op in ops {
            out.push(inflight_to_proto(&op)?);
        }
        Ok(Response::new(ListInflightOpsResponse {
            ops: out,
            skipped: skipped
                .into_iter()
                .map(|s| ProtoSkippedInflightOp {
                    path: s.path,
                    error: s.error,
                })
                .collect(),
        }))
    }

    async fn get_inflight_op(
        &self,
        req: Request<GetInflightOpRequest>,
    ) -> Result<Response<ProtoInflightOp>, Status> {
        let id = req.into_inner().id;
        if id.is_empty() {
            return Err(Status::invalid_argument("get_inflight_op: id is required"));
        }
        let op = self
            .inflight
            .get(&id)
            .await
            .map_err(|e| Status::not_found(format!("get_inflight_op: {e}")))?;
        Ok(Response::new(inflight_to_proto(&op)?))
    }

    async fn abandon_inflight_op(
        &self,
        req: Request<AbandonInflightOpRequest>,
    ) -> Result<Response<OpResult>, Status> {
        let req = req.into_inner();
        if req.id.is_empty() {
            return Err(Status::invalid_argument(
                "abandon_inflight_op: id is required",
            ));
        }
        let op = self
            .inflight
            .get(&req.id)
            .await
            .map_err(|e| Status::not_found(format!("abandon_inflight_op: {e}")))?;
        if op.status != crate::inflight_ops::InflightStatus::InProgress {
            return Ok(Response::new(OpResult {
                ok: false,
                message: format!(
                    "abandon_inflight_op: op {} is in status {}; only in-progress ops \
                     can be abandoned",
                    op.id,
                    op.status.as_wire()
                ),
            }));
        }
        let reason = if req.reason.is_empty() {
            "operator-abandoned".to_string()
        } else {
            req.reason
        };
        self.inflight
            .abandon(&req.id, &reason)
            .await
            .map_err(|e| internal(anyhow::anyhow!("abandon_inflight_op: {e}")))?;
        Ok(Response::new(OpResult {
            ok: true,
            message: format!("op {} abandoned (was at phase {})", op.id, op.phase),
        }))
    }

    /// The quorum-commit escape hatch (docs/quorum-commit.md §5):
    /// clear `synchronous_standby_names` on the current primary so
    /// commits stop requiring a standby ack. Journaled in
    /// `inflight_ops` for incident review — the executor re-arms at
    /// the next standby attach, which is what ends the window.
    async fn allow_async(
        &self,
        _req: Request<AllowAsyncRequest>,
    ) -> Result<Response<OpResult>, Status> {
        match self.db.is_in_recovery().await {
            Ok(false) => {}
            Ok(true) => {
                return Ok(Response::new(OpResult {
                    ok: false,
                    message: "allow-async: local node is not the primary (in recovery); \
                              run this on the current primary"
                        .into(),
                }));
            }
            Err(e) => {
                return Err(internal(anyhow::anyhow!(
                    "allow-async: is_in_recovery: {e}"
                )));
            }
        }
        let op = self
            .inflight
            .begin(
                crate::inflight_ops::InflightPayload::AllowAsync {
                    node_id: self.node_pool.local_node_id,
                },
                "disarming",
                false,
            )
            .await
            .map_err(|e| internal(anyhow::anyhow!("allow-async: journal begin: {e}")))?;
        if let Err(e) = self.db.set_synchronous_standby_names("").await {
            let _ = self.inflight.abandon(&op.id, &e.to_string()).await;
            return Err(internal(anyhow::anyhow!("allow-async: disarm: {e}")));
        }
        if let Err(e) = self.inflight.complete(&op.id).await {
            warn!(?e, id = %op.id, "allow-async: journal complete failed");
        }
        warn!(
            id = %op.id,
            "allow-async: QUORUM COMMIT DISARMED by operator — acknowledged writes are \
             single-copy promises until a standby attaches and the executor re-arms"
        );
        Ok(Response::new(OpResult {
            ok: true,
            message: "quorum commit disarmed (synchronous_standby_names cleared). The \
                      executor re-arms automatically when a standby attaches; /healthz \
                      shows sync_commit=disarmed until then."
                .into(),
        }))
    }

    async fn resume_inflight_op(
        &self,
        req: Request<ResumeInflightOpRequest>,
    ) -> Result<Response<OpResult>, Status> {
        let id = req.into_inner().id;
        if id.is_empty() {
            return Err(Status::invalid_argument(
                "resume_inflight_op: id is required",
            ));
        }
        let op = self
            .inflight
            .get(&id)
            .await
            .map_err(|e| Status::not_found(format!("resume_inflight_op: {e}")))?;
        if op.status != crate::inflight_ops::InflightStatus::InProgress {
            return Ok(Response::new(OpResult {
                ok: false,
                message: format!(
                    "resume_inflight_op: op {} is in status {}; only in-progress ops \
                     can be resumed",
                    op.id,
                    op.status.as_wire()
                ),
            }));
        }
        match &op.payload {
            crate::inflight_ops::InflightPayload::Handoff { .. } => self.resume_handoff(op).await,
            crate::inflight_ops::InflightPayload::FollowPrimary { .. } => {
                self.resume_follow_primary(op).await
            }
            // Recovery is journaled for visibility and for `failover`'s
            // cross-op consult, but has no resume driver yet: restarting
            // mid-ladder means re-running basebackup against a $PGDATA
            // in an unknown state, which `cluster recover` already does
            // correctly from the top. Point the operator at that rather
            // than pretending to resume.
            // A promote op has nothing to resume: the HA loop re-derives
            // its decision every tick and re-issues the (idempotent)
            // promotion if it is still the holder. The journal entry is
            // the record, not the driver.
            crate::inflight_ops::InflightPayload::Promote { .. } => Ok(Response::new(OpResult {
                ok: false,
                message: format!(
                    "op {} is a promotion; nothing to resume — the HA loop \
                     re-issues it while the lease is held",
                    op.id
                ),
            })),
            // Allow-async is a single applied action; the "resume" of
            // its state is the executor re-arming on standby attach.
            crate::inflight_ops::InflightPayload::AllowAsync { .. } => {
                Ok(Response::new(OpResult {
                    ok: false,
                    message: format!(
                        "op {} is an allow-async disarm; nothing to resume — the \
                         executor re-arms quorum commit when a standby attaches",
                        op.id
                    ),
                }))
            }
            crate::inflight_ops::InflightPayload::Recovery {
                standby_node_id, ..
            } => Ok(Response::new(OpResult {
                ok: false,
                message: format!(
                    "resume_inflight_op: op {} is a recovery, which has no resume driver. \
                     Abandon it (`pg_agentctl ops abandon {}`) and re-run \
                     `pg_agentctl cluster recover --target {} --stop-target-pg`, which \
                     restarts the orchestration from a known state.",
                    op.id, op.id, standby_node_id
                ),
            })),
        }
    }

    /// `cluster status` — fan-out `GetStatus` to every pool member and
    /// return one row per node. The local node uses the in-process
    /// `NodeInfo`; peers dial via the daemon's existing `PeerPool`
    /// (mTLS, cached connections, cert reload). Routing through the
    /// daemon keeps `pg_agentctl` from needing TLS material on disk
    /// and makes the daemon the single owner of peer connectivity.
    ///
    /// Per-node failure is captured as `reachable=false` with the
    /// error description in `error`; the RPC itself always succeeds.
    /// Exit-code semantics (any-node-down → caller fails) live in
    /// `pg_agentctl`'s renderer; the daemon's job is just to report.
    async fn cluster_status(
        &self,
        _req: Request<ClusterStatusRequest>,
    ) -> Result<Response<ClusterStatusResponse>, Status> {
        let mut entries: Vec<ClusterStatusEntry> = Vec::with_capacity(self.node_pool.members.len());
        let mut all_reachable = true;

        for node in &self.node_pool.members {
            let entry = if self.node_pool.is_local(node) {
                match self.node_info.get_status().await {
                    Ok(status) => ClusterStatusEntry {
                        node_id: node.id,
                        hostname: node.hostname.clone(),
                        reachable: true,
                        error: String::new(),
                        status: Some(status),
                    },
                    Err(e) => {
                        all_reachable = false;
                        ClusterStatusEntry {
                            node_id: node.id,
                            hostname: node.hostname.clone(),
                            reachable: false,
                            error: format!("local get_status: {e}"),
                            status: None,
                        }
                    }
                }
            } else {
                match self.peers.client(node).await {
                    Ok(peer) => match peer.get_status().await {
                        Ok(status) => ClusterStatusEntry {
                            node_id: node.id,
                            hostname: node.hostname.clone(),
                            reachable: true,
                            error: String::new(),
                            status: Some(status),
                        },
                        Err(e) => {
                            all_reachable = false;
                            ClusterStatusEntry {
                                node_id: node.id,
                                hostname: node.hostname.clone(),
                                reachable: false,
                                error: format!("get_status: {e}"),
                                status: None,
                            }
                        }
                    },
                    Err(e) => {
                        all_reachable = false;
                        ClusterStatusEntry {
                            node_id: node.id,
                            hostname: node.hostname.clone(),
                            reachable: false,
                            error: format!("dial peer: {e}"),
                            status: None,
                        }
                    }
                }
            };
            entries.push(entry);
        }
        entries.sort_by_key(|e| e.node_id);

        Ok(Response::new(ClusterStatusResponse {
            all_reachable,
            nodes: entries,
        }))
    }

    /// Backend data for rendering pgpool.conf's per-backend block
    /// (`backend_hostname{i}` / `backend_port{i}` / `backend_data_directory{i}`).
    /// Local node answered in-process via `NodeInfo`; peers dialed via
    /// the daemon's `PeerPool`. Per-node errors surface as
    /// `reachable=false` rows — the CLI decides whether to refuse to
    /// render a partial config.
    async fn get_pgpool_backends(
        &self,
        _req: Request<GetPgpoolBackendsRequest>,
    ) -> Result<Response<GetPgpoolBackendsResponse>, Status> {
        let mut backends: Vec<PgpoolBackendEntry> =
            Vec::with_capacity(self.node_pool.members.len());
        let mut all_reachable = true;

        for node in &self.node_pool.members {
            let cfg_result = if self.node_pool.is_local(node) {
                self.node_info
                    .get_node_config()
                    .await
                    .map_err(|e| format!("local get_node_config: {e}"))
            } else {
                match self.peers.client(node).await {
                    Ok(peer) => peer
                        .get_node_config()
                        .await
                        .map_err(|e| format!("get_node_config: {e}")),
                    Err(e) => Err(format!("dial peer: {e}")),
                }
            };

            let entry = match cfg_result {
                Ok(c) => PgpoolBackendEntry {
                    node_id: node.id,
                    hostname: node.hostname.clone(),
                    reachable: true,
                    error: String::new(),
                    pg_port: c.pg_port,
                    pg_data: c.pg_data_dir,
                },
                Err(e) => {
                    all_reachable = false;
                    PgpoolBackendEntry {
                        node_id: node.id,
                        hostname: node.hostname.clone(),
                        reachable: false,
                        error: e,
                        pg_port: 0,
                        pg_data: String::new(),
                    }
                }
            };
            backends.push(entry);
        }
        backends.sort_by_key(|b| b.node_id);

        Ok(Response::new(GetPgpoolBackendsResponse {
            all_reachable,
            backends,
        }))
    }

    // ----- simple hooks ---------------------------------------------------

    /// `pgpool_remote_start` — pgpool's `pgpool_recovery` C extension execs
    /// this on the primary to bring up a freshly-rebuilt standby. The
    /// dispatch path is: check we're the primary, resolve the target, dial
    /// the peer, invoke its `Start` RPC.
    ///
    /// "Not primary" is returned as `OpResult { ok=false, message=… }`
    /// rather than `Err(Status::failed_precondition)` because pgpool
    /// inspects the boolean — surfacing as a non-error is the correct
    /// contract per SPEC §3.6.
    async fn remote_start(
        &self,
        req: Request<RemoteStartRequest>,
    ) -> Result<Response<OpResult>, Status> {
        let req = req.into_inner();
        let target_ref = req
            .target
            .ok_or_else(|| Status::invalid_argument("remote_start: target is required"))?;
        let target = self
            .node_pool
            .resolve_node(&target_ref)
            .map_err(|e| Status::invalid_argument(format!("remote_start: target: {e}")))?;
        info!(target = %target.hostname, "remote_start");

        // SPEC §7: defense in depth — pgpool_remote_start is only ever
        // exec'd by `pgpool_recovery` on the primary. If we're in
        // recovery, something upstream is off.
        let in_recovery =
            self.db.is_in_recovery().await.map_err(|e| {
                internal(anyhow::anyhow!("remote_start: check primary status: {e}"))
            })?;
        if in_recovery {
            warn!("remote_start: local node is in recovery; refusing");
            return Ok(Response::new(OpResult {
                ok: false,
                message: "remote_start: local node is not the primary (in recovery)".to_string(),
            }));
        }

        let peer = self.peers.client(target).await.map_err(|e| {
            internal(anyhow::anyhow!(
                "remote_start: peer client for {}: {e}",
                target.hostname
            ))
        })?;
        peer.start().await.map_err(|e| {
            internal(anyhow::anyhow!(
                "remote_start: peer start on {}: {e}",
                target.hostname
            ))
        })?;
        Ok(Response::new(ok()))
    }

    /// `pgpool_escalation_command` — fires when this pgpool wins the
    /// watchdog leader election. This deployment uses HAProxy in front of
    /// pgpool (no VIP), so there is nothing to escalate. Return ok=true
    /// and log; pgpool tolerates a no-op escalation hook.
    async fn escalation(
        &self,
        _req: Request<EscalationRequest>,
    ) -> Result<Response<OpResult>, Status> {
        info!("escalation: no-op (HAProxy deployment has no VIP)");
        Ok(Response::new(ok()))
    }

    // ----- maintenance reads ----------------------------------------------

    async fn list_maintenance(
        &self,
        req: Request<ListMaintenanceRequest>,
    ) -> Result<Response<ListMaintenanceResponse>, Status> {
        let req = req.into_inner();
        let statuses = parse_status_filter(&req.statuses)
            .map_err(|e| Status::invalid_argument(format!("list_maintenance: {e}")))?;
        let (intents, skipped) = self
            .maint
            .list(&statuses)
            .await
            .map_err(|e| internal(anyhow::anyhow!("list_maintenance: {e}")))?;
        Ok(Response::new(ListMaintenanceResponse {
            intents: intents
                .iter()
                .map(intent_to_proto)
                .collect::<Result<Vec<_>, _>>()?,
            skipped: skipped.iter().map(skipped_to_proto).collect(),
        }))
    }

    async fn get_maintenance(
        &self,
        req: Request<GetMaintenanceRequest>,
    ) -> Result<Response<ProtoIntent>, Status> {
        let req = req.into_inner();
        if req.id.is_empty() {
            return Err(Status::invalid_argument("get_maintenance: id is required"));
        }
        match self.maint.get(&req.id).await {
            Ok(intent) => intent_to_proto(&intent).map(Response::new),
            Err(e) => Err(map_intent_lookup_error(&req.id, e)),
        }
    }

    async fn retry_maintenance(
        &self,
        req: Request<RetryMaintenanceRequest>,
    ) -> Result<Response<OpResult>, Status> {
        let req = req.into_inner();
        if req.id.is_empty() {
            return Err(Status::invalid_argument(
                "retry_maintenance: id is required",
            ));
        }
        let intent = match self.maint.get(&req.id).await {
            Ok(i) => i,
            Err(e) => return Err(map_intent_lookup_error(&req.id, e)),
        };
        if intent.status != MaintenanceStatus::Pending {
            return Ok(Response::new(OpResult {
                ok: false,
                message: format!(
                    "intent {} is in status {:?}; only pending intents can be retried",
                    intent.id,
                    intent.status.as_wire()
                ),
            }));
        }
        self.maint
            .reschedule(&req.id, chrono::Utc::now())
            .await
            .map_err(|e| map_intent_lookup_error(&req.id, e))?;
        Ok(Response::new(OpResult {
            ok: true,
            message: "intent rescheduled; the next maintenance sweep will pick it up".into(),
        }))
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Outcome of a single peer's WAL-fetch attempt within RestoreWal.
#[allow(clippy::large_enum_variant)]
enum FetchOutcome {
    /// Peer had the segment and `write_restore` succeeded.
    Fetched,
    /// Peer didn't have it / was unreachable / errored mid-stream.
    /// Loop continues to the next peer.
    TryNext,
    /// Caller-shaped failure (`dest_path` outside pgdata). Every peer
    /// would fail the same way, so abort immediately.
    Fatal(Status),
}

impl LocalServer {
    /// The orchestration that owns `node_id`'s data directory /
    /// replication slot, if any — in flight, or finished with the
    /// rebuilt node not yet observed alive (ownership ends at that
    /// event; [`CROSS_OP_GRACE`] is only the backstop for a node that
    /// never comes up).
    ///
    /// `failover` uses this to avoid acting destructively on a node
    /// another operation is mid-way through rebuilding. A journal read
    /// failure yields `None` — the same "proceed on absent evidence"
    /// posture the precondition check takes, since refusing every
    /// failover because the journal is unreadable would be worse than
    /// the race it guards.
    async fn inflight_owner_of(&self, node_id: i32) -> Option<crate::inflight_ops::InflightOp> {
        let slot = format!("node{node_id}");
        let db = self.db.clone();
        let s = slot.clone();
        crate::inflight_ops::owner_of_slot_observing(
            self.inflight.as_ref(),
            &slot,
            CROSS_OP_GRACE,
            move || async move { db.slot_active(&s).await },
        )
        .await
    }

    /// Queue a `DropSlotCleanup` maintenance intent after a slot drop
    /// failed in `failover` / `follow_primary`. Returns the human-
    /// readable message to bake into the hook's `OpResult`. The
    /// message always reads "succeeded queue" — if the queue itself
    /// failed, that's a tail-recursive error logged here, and the
    /// message still reports it so an operator sees both halves.
    async fn queue_drop_slot_cleanup(
        &self,
        slot_name: &str,
        target_hostname: &str,
        cause: &str,
        drop_err: &anyhow::Error,
    ) -> String {
        let payload = MaintenancePayload::DropSlotCleanup {
            slot_name: slot_name.to_string(),
            target_hostname: target_hostname.to_string(),
            cause: cause.to_string(),
            initial_error: drop_err.to_string(),
        };
        match self.maint.append(payload).await {
            Ok(intent) => {
                warn!(
                    slot = %slot_name,
                    target = %target_hostname,
                    cause,
                    intent_id = %intent.id,
                    drop_err = %drop_err,
                    "cleanup: queued maintenance for failed drop_slot"
                );
                format!(
                    "drop slot {slot_name} on {target_hostname} failed, queued maintenance cleanup"
                )
            }
            Err(queue_err) => {
                warn!(
                    slot = %slot_name,
                    target = %target_hostname,
                    cause,
                    drop_err = %drop_err,
                    queue_err = %queue_err,
                    "cleanup: drop_slot failed AND maintenance queue failed — slot is orphaned"
                );
                format!(
                    "drop slot {slot_name} on {target_hostname} failed; maintenance enqueue also failed"
                )
            }
        }
    }

    /// Write the replay marker then return an `Ok(OpResult)` with the
    /// given message. Failure to write the marker surfaces as Internal —
    /// without it pgpool may re-fire the whole hook on a retry, undoing
    /// what we just did. See SPEC §17.
    async fn write_replay_marker_then_ok(
        &self,
        op: &str,
        key: &str,
        message: String,
    ) -> Result<Response<OpResult>, Status> {
        if let Err(e) = self.replay.mark_done(op, key).await {
            return Err(internal(anyhow::anyhow!(
                "{op}: idempotency marker write: {e}"
            )));
        }
        Ok(Response::new(OpResult { ok: true, message }))
    }

    /// Per-standby init for `ClusterInit`. Returns a
    /// `ClusterInitStandbyResult` for the response array; never
    /// propagates errors as gRPC failures (cluster_init aggregates
    /// per-standby outcomes). On any failure between `create_slot` and
    /// `start`, the slot is dropped via the shared
    /// `cleanup_slot_after_failure` helper — failed cleanups queue a
    /// `DropSlotCleanup` maintenance intent. SPEC §5.7.
    async fn init_standby(
        &self,
        standby: &NodeConfig,
        primary_hostname: &str,
    ) -> ClusterInitStandbyResult {
        let slot_name = standby.slot_name();
        info!(
            standby = %standby.hostname,
            node_id = standby.id,
            slot = %slot_name,
            "cluster_init: initialising standby"
        );

        // Step 1: create slot locally. No slot to clean up if this fails.
        if let Err(e) = self.db.create_slot(&slot_name).await {
            return standby_result(standby, false, format!("create slot {slot_name}: {e}"));
        }

        // Helper: stage failed → clean up slot (best-effort) → return result.
        // Closure-with-async is finicky around lifetimes, so a plain match
        // ladder reads better than abstracting. Each arm: cleanup, format
        // the message, return the standby result.
        macro_rules! fail_with_cleanup {
            ($stage:expr, $err:expr) => {{
                let stage: &'static str = $stage;
                let err: anyhow::Error = $err;
                let msg = format!("{stage}: {err}");
                self.cleanup_slot_after_failure(
                    &slot_name,
                    primary_hostname,
                    &format!("cluster_init_{stage}_failed"),
                    &err,
                )
                .await;
                return standby_result(standby, false, msg);
            }};
        }

        // Step 2: dial peer.
        let peer = match self.peers.client(standby).await {
            Ok(p) => p,
            Err(e) => fail_with_cleanup!("peer_client", anyhow::anyhow!("{e}")),
        };

        // Step 3: peer.stop() — defensive; basebackup refuses non-empty pgdata.
        if let Err(e) = peer.stop().await {
            fail_with_cleanup!("stop", anyhow::anyhow!("{e}"));
        }

        // Step 4: peer.basebackup() — streams primary into standby's $PGDATA.
        let bb_opts = BasebackupOpts {
            primary_host: primary_hostname.to_string(),
            primary_port: self.pg.port,
            repl_user: self.pg.repl_user.clone(),
            slot_name: slot_name.clone(),
        };
        if let Err(e) = peer.basebackup(bb_opts).await {
            fail_with_cleanup!("basebackup", anyhow::anyhow!("{e}"));
        }

        // Step 5: peer.configure_standby() — write myrecovery.conf on top.
        let cfg_opts = WriteRecoveryConfOpts {
            primary_host: primary_hostname.to_string(),
            primary_port: self.pg.port,
            repl_user: self.pg.repl_user.clone(),
            slot_name: slot_name.clone(),
        };
        if let Err(e) = peer.configure_standby(cfg_opts).await {
            fail_with_cleanup!("configure_standby", anyhow::anyhow!("{e}"));
        }

        // Step 6: peer.start() — bring up as streaming replica.
        if let Err(e) = peer.start().await {
            fail_with_cleanup!("start", anyhow::anyhow!("{e}"));
        }

        standby_result(standby, true, "initialised".into())
    }

    /// Drop a slot that we created earlier in a failed orchestration.
    /// If the drop succeeds, we're done. If it fails, queue a
    /// maintenance intent so the worker retries with backoff — the
    /// hook itself still returns the original error, not a
    /// cleanup-side one.
    ///
    /// `cause` is the breadcrumb (`follow_primary_basebackup_failed`,
    /// etc.) that ends up on the maintenance payload.
    async fn cleanup_slot_after_failure(
        &self,
        slot_name: &str,
        target_hostname: &str,
        cause: &str,
        original_err: &anyhow::Error,
    ) {
        info!(slot = %slot_name, "cleanup: dropping slot after failure");
        match self.db.drop_slot(slot_name).await {
            Ok(()) => {
                debug!(slot = %slot_name, "cleanup: slot dropped");
            }
            Err(drop_err) => {
                let payload = MaintenancePayload::DropSlotCleanup {
                    slot_name: slot_name.to_string(),
                    target_hostname: target_hostname.to_string(),
                    cause: cause.to_string(),
                    initial_error: drop_err.to_string(),
                };
                match self.maint.append(payload).await {
                    Ok(intent) => {
                        warn!(
                            slot = %slot_name,
                            target = %target_hostname,
                            cause,
                            intent_id = %intent.id,
                            drop_err = %drop_err,
                            original_err = %original_err,
                            "cleanup: queued maintenance for failed drop_slot"
                        );
                    }
                    Err(queue_err) => {
                        warn!(
                            slot = %slot_name,
                            target = %target_hostname,
                            cause,
                            drop_err = %drop_err,
                            queue_err = %queue_err,
                            original_err = %original_err,
                            "cleanup: drop_slot failed AND maintenance queue failed — slot is orphaned"
                        );
                    }
                }
            }
        }
    }

    /// Post-handoff fan-out: for every member of the pool that's
    /// neither `local` (the just-demoted node) nor `new_primary` (the
    /// freshly-promoted target), enqueue an
    /// [`crate::inflight_ops::InflightPayload::FollowPrimary`] in
    /// the journal and spawn a background driver task. The drivers are
    /// independent — one stuck standby cannot block another, and the
    /// operator can inspect/resume/abandon each via
    /// `pg_agentctl ops list`.
    ///
    /// Errors during `inflight.begin` (e.g. journal write failure) are
    /// logged but do NOT block the other fan-outs or the handoff's
    /// response. The detached node will simply not be rebased until an
    /// operator runs `pg_agentctl cluster recover --target N
    /// --stop-target-pg` or pgpool fires a follow_primary hook.
    async fn fan_out_follow_primary(&self, local: &NodeConfig, new_primary: &NodeConfig) {
        for member in &self.node_pool.members {
            if member.id == local.id || member.id == new_primary.id {
                continue;
            }
            let detached = member.clone();
            let np = new_primary.clone();
            let payload = crate::inflight_ops::InflightPayload::FollowPrimary {
                detached_node_id: detached.id,
                detached_hostname: detached.hostname.clone(),
                new_primary_node_id: np.id,
                new_primary_hostname: np.hostname.clone(),
            };
            let op = match self.inflight.begin(payload, FP_PHASE_QUEUED, false).await {
                Ok(op) => op,
                Err(e) => {
                    warn!(
                        detached = %detached.hostname,
                        new_primary = %np.hostname,
                        ?e,
                        "follow_primary: skip fan-out — inflight.begin failed \
                         (existing in-flight follow-up?); operator can resume manually"
                    );
                    continue;
                }
            };
            let inflight = self.inflight.clone();
            let peers = self.peers.clone();
            let pcp = self.pcp.clone();
            let maint = self.maint.clone();
            let pg = self.pg.clone();
            let id = op.id.clone();
            info!(
                id = %id,
                detached = %detached.hostname,
                new_primary = %np.hostname,
                "follow_primary: enqueued; driver task spawning"
            );
            tokio::spawn(async move {
                let outcome = drive_follow_primary(
                    &inflight,
                    &peers,
                    &pcp,
                    &maint,
                    &pg,
                    &id,
                    FP_PHASE_QUEUED,
                    &detached,
                    &np,
                )
                .await;
                match outcome {
                    Ok(()) => {
                        if let Err(e) = inflight.complete(&id).await {
                            warn!(id = %id, ?e, "follow_primary: journal complete failed");
                        }
                    }
                    Err(e) => {
                        warn!(
                            id = %id,
                            detached = %detached.hostname,
                            ?e,
                            "follow_primary: driver failed; marking abandoned"
                        );
                        if let Err(je) = inflight.abandon(&id, &e.to_string()).await {
                            warn!(id = %id, ?je, "follow_primary: journal abandon failed");
                        }
                    }
                }
            });
        }
    }

    /// Operator-driven resume of a stuck follow-other-standby. Verifies
    /// the recorded payload is still consistent with the current pool
    /// then re-enters [`drive_follow_primary`] at the recorded
    /// phase. On success the driver runs synchronously (resume blocks
    /// until done or refused) so the operator gets a definitive
    /// response.
    async fn resume_follow_primary(
        &self,
        op: crate::inflight_ops::InflightOp,
    ) -> Result<Response<OpResult>, Status> {
        let crate::inflight_ops::InflightPayload::FollowPrimary {
            detached_node_id,
            ref detached_hostname,
            new_primary_node_id,
            ref new_primary_hostname,
        } = op.payload
        else {
            return Err(Status::failed_precondition(
                "resume_follow_primary: payload is not a follow_primary",
            ));
        };

        let detached = match self.node_pool.node_by_id(detached_node_id) {
            Ok(n) if n.hostname == *detached_hostname => n.clone(),
            Ok(n) => {
                return Ok(Response::new(OpResult {
                    ok: false,
                    message: format!(
                        "resume_follow_primary: op recorded detached_hostname={:?} \
                         but current pool has node {} as {:?}; pool topology has changed",
                        detached_hostname, detached_node_id, n.hostname
                    ),
                }));
            }
            Err(e) => {
                return Ok(Response::new(OpResult {
                    ok: false,
                    message: format!(
                        "resume_follow_primary: detached node {detached_node_id} not in \
                         current pool: {e}"
                    ),
                }));
            }
        };
        let new_primary = match self.node_pool.node_by_id(new_primary_node_id) {
            Ok(n) if n.hostname == *new_primary_hostname => n.clone(),
            Ok(n) => {
                return Ok(Response::new(OpResult {
                    ok: false,
                    message: format!(
                        "resume_follow_primary: op recorded new_primary_hostname={:?} \
                         but current pool has node {} as {:?}; pool topology has changed",
                        new_primary_hostname, new_primary_node_id, n.hostname
                    ),
                }));
            }
            Err(e) => {
                return Ok(Response::new(OpResult {
                    ok: false,
                    message: format!(
                        "resume_follow_primary: new_primary node {new_primary_node_id} \
                         not in current pool: {e}"
                    ),
                }));
            }
        };

        info!(
            id = %op.id,
            phase = %op.phase,
            detached = %detached.hostname,
            new_primary = %new_primary.hostname,
            "resume_follow_primary: continuing"
        );

        let outcome = drive_follow_primary(
            &self.inflight,
            &self.peers,
            &self.pcp,
            &self.maint,
            &self.pg,
            &op.id,
            &op.phase,
            &detached,
            &new_primary,
        )
        .await;
        match outcome {
            Ok(()) => {
                self.inflight.complete(&op.id).await.map_err(|e| {
                    internal(anyhow::anyhow!(
                        "resume_follow_primary: complete journal: {e}"
                    ))
                })?;
                Ok(Response::new(OpResult {
                    ok: true,
                    message: format!(
                        "follow_primary complete for {} (resumed)",
                        detached.hostname
                    ),
                }))
            }
            Err(e) => {
                let reason = e.to_string();
                let _ = self.inflight.abandon(&op.id, &reason).await;
                Ok(Response::new(OpResult {
                    ok: false,
                    message: format!(
                        "resume_follow_primary: driver failed: {reason}; op marked abandoned"
                    ),
                }))
            }
        }
    }

    /// One peer's worth of restore_wal: dial → FetchWal → write to
    /// `dest_path` via `WalStore::write_restore`. Bounded by
    /// `RESTORE_WAL_PER_PEER_TIMEOUT`; timeouts and connect failures are
    /// logged and folded into `TryNext` so a single bad peer doesn't
    /// abort the loop.
    async fn try_fetch_wal_from_peer(
        &self,
        node: &NodeConfig,
        wal_file: &str,
        dest_path: &str,
    ) -> FetchOutcome {
        let attempt = async {
            let peer = match self.peers.client(node).await {
                Ok(p) => p,
                Err(e) => {
                    warn!(?e, peer = %node.hostname, "restore_wal: peer unavailable");
                    return FetchOutcome::TryNext;
                }
            };
            let reader = match peer.fetch_wal(wal_file).await {
                Ok(Some(r)) => r,
                Ok(None) => {
                    info!(peer = %node.hostname, %wal_file, "restore_wal: segment not on peer");
                    return FetchOutcome::TryNext;
                }
                Err(e) => {
                    warn!(?e, peer = %node.hostname, "restore_wal: fetch RPC failed");
                    self.wal_peer_cooldown
                        .lock()
                        .unwrap()
                        .insert(node.id, std::time::Instant::now());
                    return FetchOutcome::TryNext;
                }
            };
            match self.wal.write_restore(Path::new(dest_path), reader).await {
                Ok(()) => FetchOutcome::Fetched,
                Err(pgman::walstore::WalStoreError::DestOutsidePgData) => FetchOutcome::Fatal(
                    Status::invalid_argument("restore_wal: dest_path outside pg_data_dir"),
                ),
                Err(e) => {
                    warn!(?e, peer = %node.hostname, "restore_wal: write_restore failed");
                    FetchOutcome::TryNext
                }
            }
        };
        match tokio::time::timeout(RESTORE_WAL_PER_PEER_TIMEOUT, attempt).await {
            Ok(outcome) => outcome,
            Err(_) => {
                warn!(peer = %node.hostname, "restore_wal: per-peer timeout");
                self.wal_peer_cooldown
                    .lock()
                    .unwrap()
                    .insert(node.id, std::time::Instant::now());
                FetchOutcome::TryNext
            }
        }
    }

    /// Step through the handoff phase ladder from `start_phase`. Each
    /// successful step writes `update_phase(next_phase)` BEFORE moving
    /// on, so a crash leaves the journal at the last-completed phase
    /// and resume picks up at the correct boundary.
    ///
    /// `start_phase` is one of the [`HANDOFF_PHASES`] constants. The
    /// caller (`cluster_handoff` or `resume_handoff`) is responsible
    /// for ensuring the cluster state matches what `start_phase`
    /// implies — fresh handoffs always start from
    /// [`HANDOFF_PHASE_PREFLIGHT_DONE`] so the whole ladder runs;
    /// resume must call `Self::verify_handoff_state` first.
    #[allow(clippy::too_many_arguments)]
    async fn run_handoff_from_phase(
        &self,
        op_id: &str,
        start_phase: &str,
        peer: &Arc<dyn PeerClient>,
        target: &NodeConfig,
        local: &NodeConfig,
        slot_name: &str,
    ) -> Result<Response<OpResult>, Status> {
        let Some(start_idx) = handoff_phase_index(start_phase) else {
            return Err(Status::invalid_argument(format!(
                "cluster_handoff: unknown phase {start_phase:?}"
            )));
        };

        // Always run checkpoint when resuming from preflight_done.
        // Idempotent — re-checkpointing is harmless.
        if start_idx <= handoff_phase_index(HANDOFF_PHASE_PREFLIGHT_DONE).unwrap() {
            self.db
                .checkpoint()
                .await
                .map_err(|e| internal(anyhow::anyhow!("cluster_handoff: checkpoint: {e}")))?;

            info!(id = op_id, target = %target.hostname, "cluster_handoff: promoting target");
            peer.promote().await.map_err(|e| {
                internal(anyhow::anyhow!(
                    "cluster_handoff: promote {}: {e}",
                    target.hostname
                ))
            })?;
            self.inflight
                .update_phase(op_id, HANDOFF_PHASE_TARGET_PROMOTED, None)
                .await
                .map_err(|e| internal(anyhow::anyhow!("cluster_handoff: journal: {e}")))?;
        }

        if start_idx <= handoff_phase_index(HANDOFF_PHASE_TARGET_PROMOTED).unwrap() {
            info!(
                id = op_id,
                slot = %slot_name,
                on = %target.hostname,
                "cluster_handoff: creating slot for local"
            );
            peer.create_slot(slot_name).await.map_err(|e| {
                internal(anyhow::anyhow!(
                    "cluster_handoff: create_slot on {}: {e}",
                    target.hostname
                ))
            })?;
            self.inflight
                .update_phase(op_id, HANDOFF_PHASE_SLOT_CREATED, None)
                .await
                .map_err(|e| internal(anyhow::anyhow!("cluster_handoff: journal: {e}")))?;
        }

        if start_idx <= handoff_phase_index(HANDOFF_PHASE_SLOT_CREATED).unwrap() {
            info!(id = op_id, "cluster_handoff: stopping local postgres");
            self.sd
                .stop_postgres()
                .await
                .map_err(|e| internal(anyhow::anyhow!("cluster_handoff: stop_postgres: {e}")))?;
            self.inflight
                .update_phase(op_id, HANDOFF_PHASE_LOCAL_STOPPED, None)
                .await
                .map_err(|e| internal(anyhow::anyhow!("cluster_handoff: journal: {e}")))?;
        }

        let bb_opts = BasebackupOpts {
            primary_host: target.hostname.clone(),
            primary_port: self.pg.port,
            repl_user: self.pg.repl_user.clone(),
            slot_name: slot_name.to_string(),
        };
        let rewind_opts = RewindOpts {
            primary_host: target.hostname.clone(),
            primary_port: self.pg.port,
            repl_user: self.pg.repl_user.clone(),
        };
        let mut basebackup_ran = false;
        if start_idx <= handoff_phase_index(HANDOFF_PHASE_LOCAL_STOPPED).unwrap() {
            info!(
                id = op_id,
                target = %target.hostname,
                "cluster_handoff: attempting rewind against new primary"
            );
            if let Err(e) = self.standby.rewind(rewind_opts, None).await {
                warn!(
                    ?e,
                    "cluster_handoff: rewind failed; falling back to basebackup"
                );
                if let Err(bb_err) = self.standby.basebackup(bb_opts.clone(), None).await {
                    // Both paths failed. Drop the slot we created and
                    // abandon the journal entry so the operator sees
                    // exactly where it died.
                    let drop_err = peer.drop_slot(slot_name).await;
                    if let Err(drop_e) = drop_err {
                        warn!(
                            ?drop_e,
                            slot = %slot_name,
                            "cluster_handoff: drop_slot cleanup also failed; manual cleanup may be required"
                        );
                    }
                    let reason = format!("rewind+basebackup both failed: {bb_err}");
                    if let Err(ab_err) = self.inflight.abandon(op_id, &reason).await {
                        warn!(?ab_err, "cluster_handoff: abandon journal entry failed");
                    }
                    return Ok(Response::new(OpResult {
                        ok: false,
                        message: format!(
                            "cluster_handoff: rewind and basebackup both failed against {}: {bb_err}; \
                             local PG is stopped, target is now the primary; \
                             run `pg_agentctl cluster recover --target {} --stop-target-pg` from \
                             the new primary to finish converging the local node",
                            target.hostname, local.id
                        ),
                    }));
                }
                basebackup_ran = true;
            }
            self.inflight
                .update_phase(op_id, HANDOFF_PHASE_DATA_COPIED, None)
                .await
                .map_err(|e| internal(anyhow::anyhow!("cluster_handoff: journal: {e}")))?;
        }

        if start_idx <= handoff_phase_index(HANDOFF_PHASE_DATA_COPIED).unwrap() {
            let cfg_opts = WriteRecoveryConfOpts {
                primary_host: target.hostname.clone(),
                primary_port: self.pg.port,
                repl_user: self.pg.repl_user.clone(),
                slot_name: slot_name.to_string(),
            };
            self.standby
                .write_recovery_conf(cfg_opts)
                .await
                .map_err(|e| {
                    internal(anyhow::anyhow!("cluster_handoff: write_recovery_conf: {e}"))
                })?;
            self.inflight
                .update_phase(op_id, HANDOFF_PHASE_RECOVERY_CONF_WRITTEN, None)
                .await
                .map_err(|e| internal(anyhow::anyhow!("cluster_handoff: journal: {e}")))?;
        }

        if start_idx <= handoff_phase_index(HANDOFF_PHASE_RECOVERY_CONF_WRITTEN).unwrap() {
            info!(
                id = op_id,
                "cluster_handoff: starting local postgres as standby"
            );
            self.sd
                .start_postgres()
                .await
                .map_err(|e| internal(anyhow::anyhow!("cluster_handoff: start_postgres: {e}")))?;
            self.inflight
                .update_phase(op_id, HANDOFF_PHASE_LOCAL_STARTED, None)
                .await
                .map_err(|e| internal(anyhow::anyhow!("cluster_handoff: journal: {e}")))?;
        }

        let mut attach_note = String::new();
        if start_idx <= handoff_phase_index(HANDOFF_PHASE_LOCAL_STARTED).unwrap() {
            if let Err(e) = self.pcp.attach_node(local.id).await {
                warn!(
                    ?e,
                    local_id = local.id,
                    "cluster_handoff: pcp attach_node failed; operator may need pcp_attach_node manually"
                );
                attach_note = format!("; pgpool attach failed: {e}");
            }
            self.inflight
                .update_phase(op_id, HANDOFF_PHASE_ATTACHED, None)
                .await
                .map_err(|e| internal(anyhow::anyhow!("cluster_handoff: journal: {e}")))?;
        }

        // Final transition.
        self.inflight
            .complete(op_id)
            .await
            .map_err(|e| internal(anyhow::anyhow!("cluster_handoff: complete journal: {e}")))?;

        let how = if basebackup_ran {
            "basebackup"
        } else {
            "rewind"
        };
        info!(
            id = op_id,
            target = %target.hostname,
            method = how,
            "cluster_handoff: complete"
        );
        Ok(Response::new(OpResult {
            ok: true,
            message: format!(
                "handoff complete: primary is now {}; local demoted to standby via {}{}",
                target.hostname, how, attach_note
            ),
        }))
    }

    /// Operator-driven resume of a crashed handoff. Verifies that the
    /// cluster state still matches the recorded phase before
    /// continuing — if a peer node has moved underneath us (someone
    /// else promoted, the target reverted, etc.) we refuse rather than
    /// run destructive steps blindly.
    async fn resume_handoff(
        &self,
        op: crate::inflight_ops::InflightOp,
    ) -> Result<Response<OpResult>, Status> {
        // The dispatcher (`resume_inflight_op`) already matched on the
        // Handoff variant; this destructure is irrefutable today.
        // Adding a new InflightPayload variant in the future will turn
        // this into a refutable pattern again — compiler will catch it.
        #[allow(irrefutable_let_patterns)]
        let crate::inflight_ops::InflightPayload::Handoff {
            from_node_id,
            to_node_id,
            ref to_hostname,
            ref slot_name,
            ..
        } = op.payload
        else {
            return Err(Status::failed_precondition(
                "resume_handoff: payload is not a handoff",
            ));
        };

        // Resolve current pool state. Payload-recorded `to_hostname` is
        // the source of truth for the orchestration's target — if the
        // pool was renumbered we'd see a mismatch and refuse rather
        // than guessing.
        let local = self
            .node_pool
            .local_node()
            .map_err(|e| internal(anyhow::anyhow!("resume_handoff: resolve local: {e}")))?
            .clone();
        if local.id != from_node_id {
            return Ok(Response::new(OpResult {
                ok: false,
                message: format!(
                    "resume_handoff: op recorded from_node_id={} but local is node {}; \
                     pool topology has changed since the op began",
                    from_node_id, local.id
                ),
            }));
        }
        let target = match self.node_pool.node_by_id(to_node_id) {
            Ok(n) if n.hostname == *to_hostname => n.clone(),
            Ok(n) => {
                return Ok(Response::new(OpResult {
                    ok: false,
                    message: format!(
                        "resume_handoff: op recorded to_hostname={to_hostname:?} but current \
                         pool has node {} as {:?}; pool topology has changed since the op began",
                        to_node_id, n.hostname
                    ),
                }));
            }
            Err(e) => {
                return Ok(Response::new(OpResult {
                    ok: false,
                    message: format!(
                        "resume_handoff: target node {to_node_id} not in current pool: {e}"
                    ),
                }));
            }
        };
        let peer = self.peers.client(&target).await.map_err(|e| {
            internal(anyhow::anyhow!(
                "resume_handoff: dial peer {}: {e}",
                target.hostname
            ))
        })?;

        // Verify-then-resume.
        if let Some(diverged) = self.verify_handoff_state(&op.phase, &peer, &target).await? {
            return Ok(Response::new(OpResult {
                ok: false,
                message: format!(
                    "resume_handoff: cluster state diverged from recorded phase {:?}: {diverged}; \
                     run `pg_agentctl ops abandon {}` if this op is unrecoverable",
                    op.phase, op.id
                ),
            }));
        }

        info!(
            id = %op.id,
            phase = %op.phase,
            target = %target.hostname,
            "resume_handoff: continuing orchestration"
        );

        self.run_handoff_from_phase(&op.id, &op.phase, &peer, &target, &local, slot_name)
            .await
    }

    /// Returns `Ok(Some(reason))` when the cluster state has moved
    /// underneath us in a way that resume can't safely paper over.
    /// `Ok(None)` means the recorded phase is still consistent with
    /// observed cluster state and resume should proceed.
    async fn verify_handoff_state(
        &self,
        recorded_phase: &str,
        peer: &Arc<dyn PeerClient>,
        target: &NodeConfig,
    ) -> Result<Option<String>, Status> {
        let target_status = peer.get_status().await.map_err(|e| {
            internal(anyhow::anyhow!(
                "verify_handoff_state: peer get_status {}: {e}",
                target.hostname
            ))
        })?;

        // At phases prior to target_promoted, the target should still
        // be a standby (we haven't promoted it yet).
        if recorded_phase == HANDOFF_PHASE_PREFLIGHT_DONE {
            if !target_status.is_in_recovery {
                return Ok(Some(format!(
                    "target {} is already a primary; the original handoff was about to \
                     promote it but someone else promoted it first",
                    target.hostname
                )));
            }
            return Ok(None);
        }

        // From target_promoted onward, the target MUST be the primary.
        // If it's been demoted or restarted into recovery, the resume
        // assumptions don't hold.
        if !target_status.is_postgres_running {
            return Ok(Some(format!(
                "target {} has postgres stopped; original handoff had promoted it",
                target.hostname
            )));
        }
        if target_status.is_in_recovery {
            return Ok(Some(format!(
                "target {} is back in recovery; original handoff had promoted it to primary",
                target.hostname
            )));
        }

        // Verify local PG state for the post-stop phases. Errors here
        // are conservative — if systemd can't tell us, refuse rather
        // than guess.
        let pg_running = self.sd.status_postgres().await.map_err(|e| {
            internal(anyhow::anyhow!(
                "verify_handoff_state: status_postgres: {e}"
            ))
        })?;
        match recorded_phase {
            // local should still be primary (we haven't stopped it yet).
            HANDOFF_PHASE_TARGET_PROMOTED | HANDOFF_PHASE_SLOT_CREATED if !pg_running => {
                return Ok(Some(
                    "local PG is stopped but recorded phase is pre-stop; someone else \
                     stopped the old primary"
                        .into(),
                ));
            }
            // local should still be stopped.
            HANDOFF_PHASE_LOCAL_STOPPED
            | HANDOFF_PHASE_DATA_COPIED
            | HANDOFF_PHASE_RECOVERY_CONF_WRITTEN
                if pg_running =>
            {
                return Ok(Some(
                    "local PG is running but recorded phase is post-stop; \
                     someone else started the old primary"
                        .into(),
                ));
            }
            // local should be running as a standby.
            HANDOFF_PHASE_LOCAL_STARTED | HANDOFF_PHASE_ATTACHED => {
                if !pg_running {
                    return Ok(Some(
                        "local PG is stopped but recorded phase is post-start".into(),
                    ));
                }
                let in_recovery = self.db.is_in_recovery().await.map_err(|e| {
                    internal(anyhow::anyhow!("verify_handoff_state: is_in_recovery: {e}"))
                })?;
                if !in_recovery {
                    return Ok(Some(
                        "local PG is a primary but recorded phase implies standby".into(),
                    ));
                }
            }
            _ => {}
        }
        Ok(None)
    }
}

fn internal(e: anyhow::Error) -> Status {
    Status::internal(e.to_string())
}

/// Drive one third-standby's rebase onto the new primary. Used by both
/// the post-handoff fan-out (spawned, fire-and-forget) and the
/// operator-driven `ResumeInflightOp` handler (synchronous). Steps,
/// each recorded in the inflight journal so a crash + resume picks up
/// where it left off:
///
/// 1. `dialing` — open peer clients to detached and new_primary
/// 2. (skip if detached PG is deliberately stopped — operator owns it)
/// 3. `detached_stopped` — peer.stop on the orphaned standby
/// 4. `slot_created` — new_primary.create_slot for the detached
///    (idempotent — `42710` is treated as success peer-side)
/// 5. `data_copied` — rewind, then basebackup on rewind failure
/// 6. `recovery_conf_written` — repoint detached at new_primary
/// 7. `detached_started` — bring detached back up
/// 8. `attached` — pcp_attach_node so pgpool routes to it again
///
/// Per-phase failures between `slot_created` and `detached_started`
/// queue a `DropSlotCleanup` maintenance intent so the slot on the new
/// primary doesn't pin WAL forever. Once `detached_started`, the slot
/// is in use and must not be dropped — failure of `attach_node` leaves
/// only pgpool's view stale (the slot is correct).
#[allow(clippy::too_many_arguments)]
async fn drive_follow_primary(
    inflight: &Arc<dyn crate::inflight_ops::InflightOpStore>,
    peers: &Arc<dyn PeerRegistry>,
    pcp: &Arc<dyn Pcp>,
    maint: &Arc<dyn MaintenanceStore>,
    pg: &PostgresRuntime,
    op_id: &str,
    start_phase: &str,
    detached: &NodeConfig,
    new_primary: &NodeConfig,
) -> anyhow::Result<()> {
    let start_idx = fp_phase_index(start_phase)
        .ok_or_else(|| anyhow::anyhow!("follow_primary: unknown phase {start_phase:?}"))?;
    let slot_name = detached.slot_name();

    // ----- dialing -------------------------------------------------
    if start_idx <= fp_phase_index(FP_PHASE_QUEUED).unwrap() {
        inflight
            .update_phase(op_id, FP_PHASE_DIALING, None)
            .await
            .map_err(|e| anyhow::anyhow!("follow_primary: journal dialing: {e}"))?;
    }
    let detached_peer = peers
        .client(detached)
        .await
        .map_err(|e| anyhow::anyhow!("follow_primary: dial detached {}: {e}", detached.hostname))?;
    let new_primary_peer = peers.client(new_primary).await.map_err(|e| {
        anyhow::anyhow!(
            "follow_primary: dial new_primary {}: {e}",
            new_primary.hostname
        )
    })?;

    // ----- detached_stopped ----------------------------------------
    if start_idx <= fp_phase_index(FP_PHASE_DIALING).unwrap() {
        let status = detached_peer.get_status().await.map_err(|e| {
            anyhow::anyhow!("follow_primary: get_status {}: {e}", detached.hostname)
        })?;
        if !status.is_running {
            info!(
                detached = %detached.hostname,
                "follow_primary: detached PG not running, skipping (operator owns)"
            );
            // Skip the rest. The op completes — no work to do.
            return Ok(());
        }
        detached_peer
            .stop()
            .await
            .map_err(|e| anyhow::anyhow!("follow_primary: stop {}: {e}", detached.hostname))?;
        inflight
            .update_phase(op_id, FP_PHASE_DETACHED_STOPPED, None)
            .await
            .map_err(|e| anyhow::anyhow!("follow_primary: journal detached_stopped: {e}"))?;
    }

    // ----- slot_created --------------------------------------------
    if start_idx <= fp_phase_index(FP_PHASE_DETACHED_STOPPED).unwrap() {
        new_primary_peer
            .create_slot(&slot_name)
            .await
            .map_err(|e| {
                anyhow::anyhow!(
                    "follow_primary: create_slot {slot_name} on {}: {e}",
                    new_primary.hostname
                )
            })?;
        inflight
            .update_phase(op_id, FP_PHASE_SLOT_CREATED, None)
            .await
            .map_err(|e| anyhow::anyhow!("follow_primary: journal slot_created: {e}"))?;
    }

    // ----- data_copied (rewind → basebackup fallback) --------------
    if start_idx <= fp_phase_index(FP_PHASE_SLOT_CREATED).unwrap() {
        let rewind_opts = RewindOpts {
            primary_host: new_primary.hostname.clone(),
            primary_port: pg.port,
            repl_user: pg.repl_user.clone(),
        };
        let rewind_ok = match detached_peer.rewind(rewind_opts).await {
            Ok(()) => true,
            Err(e) => {
                warn!(
                    detached = %detached.hostname,
                    err = %e,
                    "follow_primary: rewind failed; falling back to basebackup"
                );
                false
            }
        };
        if !rewind_ok {
            let bb_opts = BasebackupOpts {
                primary_host: new_primary.hostname.clone(),
                primary_port: pg.port,
                repl_user: pg.repl_user.clone(),
                slot_name: slot_name.clone(),
            };
            if let Err(e) = detached_peer.basebackup(bb_opts).await {
                let err = anyhow::anyhow!("follow_primary: basebackup {}: {e}", detached.hostname);
                cleanup_peer_slot_after_failure(
                    maint,
                    &new_primary_peer,
                    &new_primary.hostname,
                    &slot_name,
                    "follow_primary_basebackup_failed",
                    &err,
                )
                .await;
                return Err(err);
            }
        }
        inflight
            .update_phase(op_id, FP_PHASE_DATA_COPIED, None)
            .await
            .map_err(|e| anyhow::anyhow!("follow_primary: journal data_copied: {e}"))?;
    }

    // ----- recovery_conf_written -----------------------------------
    if start_idx <= fp_phase_index(FP_PHASE_DATA_COPIED).unwrap() {
        let cfg_opts = WriteRecoveryConfOpts {
            primary_host: new_primary.hostname.clone(),
            primary_port: pg.port,
            repl_user: pg.repl_user.clone(),
            slot_name: slot_name.clone(),
        };
        if let Err(e) = detached_peer.configure_standby(cfg_opts).await {
            let err = anyhow::anyhow!(
                "follow_primary: configure_standby {}: {e}",
                detached.hostname
            );
            cleanup_peer_slot_after_failure(
                maint,
                &new_primary_peer,
                &new_primary.hostname,
                &slot_name,
                "follow_primary_configure_standby_failed",
                &err,
            )
            .await;
            return Err(err);
        }
        inflight
            .update_phase(op_id, FP_PHASE_RECOVERY_CONF_WRITTEN, None)
            .await
            .map_err(|e| anyhow::anyhow!("follow_primary: journal recovery_conf_written: {e}"))?;
    }

    // ----- detached_started ----------------------------------------
    if start_idx <= fp_phase_index(FP_PHASE_RECOVERY_CONF_WRITTEN).unwrap() {
        if let Err(e) = detached_peer.start().await {
            let err = anyhow::anyhow!("follow_primary: start {}: {e}", detached.hostname);
            cleanup_peer_slot_after_failure(
                maint,
                &new_primary_peer,
                &new_primary.hostname,
                &slot_name,
                "follow_primary_start_failed",
                &err,
            )
            .await;
            return Err(err);
        }
        inflight
            .update_phase(op_id, FP_PHASE_DETACHED_STARTED, None)
            .await
            .map_err(|e| anyhow::anyhow!("follow_primary: journal detached_started: {e}"))?;
    }

    // ----- attached (slot is now in use — DO NOT drop on failure) --
    if start_idx <= fp_phase_index(FP_PHASE_DETACHED_STARTED).unwrap() {
        if let Err(e) = pcp.attach_node(detached.id).await {
            // pgpool's view is stale, but the standby is up and
            // streaming from new_primary. Surface the error in the
            // journal; operator runs `pcp_attach_node` to finish.
            return Err(anyhow::anyhow!(
                "follow_primary: pcp_attach_node {}: {e}",
                detached.id
            ));
        }
        inflight
            .update_phase(op_id, FP_PHASE_ATTACHED, None)
            .await
            .map_err(|e| anyhow::anyhow!("follow_primary: journal attached: {e}"))?;
    }

    info!(
        id = op_id,
        detached = %detached.hostname,
        new_primary = %new_primary.hostname,
        "follow_primary: complete"
    );
    Ok(())
}

/// Same shape as [`LocalServer::cleanup_slot_after_failure`] but the
/// slot lives on a peer (the new primary), not on the local DB. Used
/// by [`drive_follow_primary`] when an orchestration phase
/// between `slot_created` and `detached_started` errors out.
async fn cleanup_peer_slot_after_failure(
    maint: &Arc<dyn MaintenanceStore>,
    new_primary_peer: &Arc<dyn PeerClient>,
    new_primary_hostname: &str,
    slot_name: &str,
    cause: &str,
    original_err: &anyhow::Error,
) {
    info!(slot = %slot_name, "follow_primary cleanup: dropping slot on new primary after failure");
    match new_primary_peer.drop_slot(slot_name).await {
        Ok(()) => debug!(slot = %slot_name, "follow_primary cleanup: slot dropped"),
        Err(drop_err) => {
            let payload = MaintenancePayload::DropSlotCleanup {
                slot_name: slot_name.to_string(),
                target_hostname: new_primary_hostname.to_string(),
                cause: cause.to_string(),
                initial_error: drop_err.to_string(),
            };
            match maint.append(payload).await {
                Ok(intent) => warn!(
                    slot = %slot_name,
                    target = %new_primary_hostname,
                    cause,
                    intent_id = %intent.id,
                    drop_err = %drop_err,
                    original_err = %original_err,
                    "follow_primary cleanup: queued maintenance for failed drop_slot"
                ),
                Err(queue_err) => warn!(
                    slot = %slot_name,
                    target = %new_primary_hostname,
                    cause,
                    drop_err = %drop_err,
                    queue_err = %queue_err,
                    original_err = %original_err,
                    "follow_primary cleanup: drop_slot failed AND maintenance queue failed — slot is orphaned"
                ),
            }
        }
    }
}

fn standby_result(node: &NodeConfig, ok: bool, message: String) -> ClusterInitStandbyResult {
    ClusterInitStandbyResult {
        node_id: node.id,
        hostname: node.hostname.clone(),
        ok,
        message,
    }
}

fn ok() -> OpResult {
    OpResult {
        ok: true,
        message: String::new(),
    }
}

/// Parse the wire `statuses` filter into the enum form
/// `InflightOpStore::list` expects. Empty list = no filter.
#[allow(clippy::result_large_err)]
fn parse_inflight_statuses(
    raw: &[String],
) -> Result<Vec<crate::inflight_ops::InflightStatus>, String> {
    raw.iter()
        .map(|s| match s.as_str() {
            "in_progress" => Ok(crate::inflight_ops::InflightStatus::InProgress),
            "done" => Ok(crate::inflight_ops::InflightStatus::Done),
            "abandoned" => Ok(crate::inflight_ops::InflightStatus::Abandoned),
            other => Err(format!(
                "unknown status filter {other:?}; \
                 expected one of: in_progress, done, abandoned"
            )),
        })
        .collect()
}

/// Project a core `InflightOp` to the wire shape. Payload is rendered
/// as JSON so callers don't need a discriminated decoder.
#[allow(clippy::result_large_err)]
fn inflight_to_proto(op: &crate::inflight_ops::InflightOp) -> Result<ProtoInflightOp, Status> {
    let payload = serde_json::to_vec(&op.payload)
        .map_err(|e| internal(anyhow::anyhow!("inflight_to_proto: marshal payload: {e}")))?;
    Ok(ProtoInflightOp {
        id: op.id.clone(),
        op: op.payload.op_name().to_string(),
        status: op.status.as_wire().to_string(),
        payload,
        phase: op.phase.clone(),
        started_at: op.started_at.to_rfc3339_opts(SecondsFormat::Nanos, true),
        updated_at: op.updated_at.to_rfc3339_opts(SecondsFormat::Nanos, true),
        completed_at: op
            .completed_at
            .map(|t| t.to_rfc3339_opts(SecondsFormat::Nanos, true))
            .unwrap_or_default(),
        last_error: op.last_error.clone().unwrap_or_default(),
    })
}

/// Parse the wire `statuses` filter into the enum form `MaintenanceStore::list`
/// expects. Empty list = no filter (all statuses).
#[allow(clippy::result_large_err)]
fn parse_status_filter(raw: &[String]) -> Result<Vec<MaintenanceStatus>, String> {
    raw.iter()
        .map(|s| match s.as_str() {
            "pending" => Ok(MaintenanceStatus::Pending),
            "done" => Ok(MaintenanceStatus::Done),
            "abandoned" => Ok(MaintenanceStatus::Abandoned),
            other => Err(format!(
                "invalid status filter {other:?}: expected one of pending/done/abandoned"
            )),
        })
        .collect()
}

/// Project the core intent shape onto the wire `MaintenanceIntent`.
/// Payload is JSON-serialised — proto carries it as `bytes` so the wire
/// is forward-compatible with payload-shape additions that the operator
/// surfaces can hand back to the worker.
#[allow(clippy::result_large_err)]
fn intent_to_proto(intent: &CoreIntent) -> Result<ProtoIntent, Status> {
    let payload = serde_json::to_vec(&intent.payload)
        .map_err(|e| Status::internal(format!("serialize maintenance payload: {e}")))?;
    Ok(ProtoIntent {
        id: intent.id.clone(),
        op: intent.payload.op_name().to_string(),
        status: intent.status.as_wire().to_string(),
        payload,
        attempts: intent.attempts as i32,
        last_error: intent.last_error.clone(),
        created_at: intent
            .created_at
            .to_rfc3339_opts(SecondsFormat::Nanos, true),
        updated_at: intent
            .updated_at
            .to_rfc3339_opts(SecondsFormat::Nanos, true),
        next_retry_at: intent
            .next_retry_at
            .map(|t| t.to_rfc3339_opts(SecondsFormat::Nanos, true))
            .unwrap_or_default(),
    })
}

fn skipped_to_proto(s: &SkippedIntent) -> SkippedMaintenanceIntent {
    SkippedMaintenanceIntent {
        path: s.path.clone(),
        error: s.error.clone(),
    }
}

/// Best-effort mapping of intent-store errors to gRPC codes. The store
/// returns `anyhow::Error` (boxed underneath); we string-match on the
/// io-kind suffix since `not-found` is the only case that warrants a
/// non-Internal code, and there's no cleaner introspection without an
/// IoErrorKind helper on the trait.
fn map_intent_lookup_error(id: &str, err: anyhow::Error) -> Status {
    let chain = err.to_string();
    if chain.contains("No such file or directory") || chain.to_lowercase().contains("not found") {
        Status::not_found(format!("maintenance intent {id:?} not found"))
    } else {
        Status::internal(format!("maintenance store: {chain}"))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::NodeConfig;
    use crate::inflight_ops::InflightOpStore;
    use crate::localdb::ReplicationLag;
    use crate::maintenance::{MaintenancePayload, MaintenanceStatus};
    use crate::peers::PeerClient;
    use async_trait::async_trait;
    use chrono::{TimeZone, Utc};
    use pg_agent_proto::pgagentpb::NodeRef;
    use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
    use std::sync::Mutex as StdMutex;

    /// Plausible non-zero WAL position used to satisfy the handoff lag
    /// check's "cannot measure lag" guard (0 from either side is
    /// treated as "probe failed / pre-feature peer"). Tests that
    /// exercise lag behaviour pick offsets relative to this.
    const BASE_LSN: u64 = 0x1_0000_0000;

    // ----- FakeNodeInfo ----------------------------------------------------

    struct FakeNodeInfo;

    #[async_trait]
    impl NodeInfo for FakeNodeInfo {
        async fn get_status(&self) -> anyhow::Result<NodeStatus> {
            Ok(NodeStatus {
                is_running: true,
                is_in_recovery: false,
                is_ready: true,
                replication_lag_bytes: 0,
                replication_state: String::new(),
                is_postgres_running: true,
                is_pgpool_running: true,
                is_postgres_status_ok: true,
                is_pgpool_status_ok: true,
                timeline_id: 0,
                current_wal_lsn: 0,
                last_flush_lsn: 0,
            })
        }
        async fn get_node_config(&self) -> anyhow::Result<NodeConfigResponse> {
            Ok(NodeConfigResponse {
                pg_port: 5432,
                pg_data_dir: "/var/lib/postgresql/17/main".into(),
            })
        }
    }

    // ----- Stub deps ----------------------------------------------------

    #[derive(Default)]
    struct StubDb {
        in_recovery: AtomicBool,
        is_in_recovery_fails: AtomicBool,
        checkpoint_calls: AtomicUsize,
        created_slots: StdMutex<Vec<String>>,
        dropped_slots: StdMutex<Vec<String>>,
        drop_slot_fails: AtomicBool,
        created_repl_roles: StdMutex<Vec<String>>,
        create_repl_role_fails: AtomicBool,
        /// What `slot_active` reports (default false — no walreceiver).
        /// Tests exercising the cross-op ownership discharge flip it.
        slot_is_active: AtomicBool,
        /// Local PG's `pg_current_wal_lsn()` value. Tests set this to a
        /// non-zero value when exercising the handoff lag check so the
        /// "cannot measure lag" guard doesn't trip.
        current_wal_lsn: std::sync::atomic::AtomicU64,
    }

    #[async_trait]
    impl LocalDb for StubDb {
        async fn promote(&self) -> anyhow::Result<()> {
            Ok(())
        }
        async fn checkpoint(&self) -> anyhow::Result<()> {
            self.checkpoint_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        async fn slot_active(&self, _: &str) -> anyhow::Result<bool> {
            Ok(self.slot_is_active.load(Ordering::SeqCst))
        }
        async fn set_synchronous_standby_names(&self, _: &str) -> anyhow::Result<()> {
            Ok(())
        }
        async fn connected_standby_names(&self) -> anyhow::Result<Vec<String>> {
            Ok(Vec::new())
        }
        async fn flush_lsn(&self) -> anyhow::Result<u64> {
            Ok(self.current_wal_lsn.load(Ordering::SeqCst))
        }
        async fn create_slot(&self, name: &str) -> anyhow::Result<()> {
            self.created_slots.lock().unwrap().push(name.to_string());
            Ok(())
        }
        async fn drop_slot(&self, name: &str) -> anyhow::Result<()> {
            self.dropped_slots.lock().unwrap().push(name.to_string());
            if self.drop_slot_fails.load(Ordering::SeqCst) {
                anyhow::bail!("stub: drop_slot boom");
            }
            Ok(())
        }
        async fn is_in_recovery(&self) -> anyhow::Result<bool> {
            if self.is_in_recovery_fails.load(Ordering::SeqCst) {
                anyhow::bail!("stub: is_in_recovery boom");
            }
            Ok(self.in_recovery.load(Ordering::SeqCst))
        }
        async fn timeline_id(&self) -> anyhow::Result<i32> {
            Ok(0)
        }
        async fn current_wal_lsn(&self) -> anyhow::Result<u64> {
            Ok(self.current_wal_lsn.load(Ordering::SeqCst))
        }
        async fn replication_lag(&self) -> anyhow::Result<ReplicationLag> {
            Ok(ReplicationLag::default())
        }
        async fn setting(&self, _: &str) -> anyhow::Result<String> {
            Ok(String::new())
        }
        async fn extension_exists(&self, _: &str) -> anyhow::Result<bool> {
            Ok(false)
        }
        async fn role_exists(&self, _: &str) -> anyhow::Result<bool> {
            Ok(false)
        }
        async fn create_replication_role(&self, name: &str) -> anyhow::Result<()> {
            self.created_repl_roles
                .lock()
                .unwrap()
                .push(name.to_string());
            if self.create_repl_role_fails.load(Ordering::SeqCst) {
                anyhow::bail!("stub: create_replication_role boom");
            }
            Ok(())
        }
    }

    #[derive(Default)]
    struct StubReplay {
        recorded: StdMutex<std::collections::HashSet<(String, String)>>,
        has_fails: AtomicBool,
        mark_fails: AtomicBool,
    }

    impl StubReplay {
        fn mark(&self, op: &str, key: &str) {
            self.recorded
                .lock()
                .unwrap()
                .insert((op.to_string(), key.to_string()));
        }
    }

    #[async_trait]
    impl ReplayMarkerStore for StubReplay {
        async fn has(&self, op: &str, key: &str) -> anyhow::Result<bool> {
            if self.has_fails.load(Ordering::SeqCst) {
                anyhow::bail!("stub: replay.has boom");
            }
            Ok(self
                .recorded
                .lock()
                .unwrap()
                .contains(&(op.to_string(), key.to_string())))
        }
        async fn mark_done(&self, op: &str, key: &str) -> anyhow::Result<()> {
            if self.mark_fails.load(Ordering::SeqCst) {
                anyhow::bail!("stub: replay.mark_done boom");
            }
            self.mark(op, key);
            Ok(())
        }
        async fn sweep(&self, _: chrono::DateTime<Utc>) {}
    }

    /// In-memory `InflightOpStore` for localserver tests. Mirrors
    /// `FileInflightOpStore`'s contract closely enough that handler
    /// tests can drive begin/find/update_phase/complete/abandon
    /// without spinning up a tempdir per test.
    #[derive(Default)]
    struct StubInflight {
        ops: StdMutex<Vec<crate::inflight_ops::InflightOp>>,
        seq: std::sync::atomic::AtomicU64,
    }

    impl StubInflight {
        fn next_id(&self, op: &str) -> String {
            let n = self.seq.fetch_add(1, Ordering::SeqCst);
            format!("stub-{op}-{n}")
        }
        /// Seed an op directly without going through `begin` — used by
        /// failover/handoff resume tests to set up a pre-existing
        /// in-flight handoff at a specific phase.
        #[allow(dead_code)]
        fn seed(&self, op: crate::inflight_ops::InflightOp) {
            self.ops.lock().unwrap().push(op);
        }
        /// Age a completed op's `completed_at` backwards, so tests can
        /// cross a time-based window without sleeping.
        fn backdate_completion(&self, id: &str, by: chrono::Duration) {
            let mut ops = self.ops.lock().unwrap();
            if let Some(op) = ops.iter_mut().find(|o| o.id == id) {
                op.completed_at = op.completed_at.map(|t| t - by);
            }
        }
    }

    #[async_trait]
    impl crate::inflight_ops::InflightOpStore for StubInflight {
        async fn begin(
            &self,
            payload: crate::inflight_ops::InflightPayload,
            phase: &str,
            exclusive: bool,
        ) -> anyhow::Result<crate::inflight_ops::InflightOp> {
            let mut ops = self.ops.lock().unwrap();
            let op_name = payload.op_name();
            let key = payload.key();
            for existing in ops.iter() {
                if existing.status != crate::inflight_ops::InflightStatus::InProgress {
                    continue;
                }
                if existing.payload.op_name() == op_name && existing.payload.key() == key {
                    anyhow::bail!(
                        "stub: duplicate in-progress op (id={}, phase={})",
                        existing.id,
                        existing.phase
                    );
                }
                if exclusive {
                    anyhow::bail!(
                        "stub: exclusive blocked by op (id={}, name={}, phase={})",
                        existing.id,
                        existing.payload.op_name(),
                        existing.phase
                    );
                }
            }
            let now = Utc::now();
            let op = crate::inflight_ops::InflightOp {
                id: self.next_id(op_name),
                status: crate::inflight_ops::InflightStatus::InProgress,
                payload,
                phase: phase.to_string(),
                started_at: now,
                updated_at: now,
                completed_at: None,
                discharged_at: None,
                last_error: None,
            };
            ops.push(op.clone());
            Ok(op)
        }
        async fn update_phase(
            &self,
            id: &str,
            phase: &str,
            last_error: Option<String>,
        ) -> anyhow::Result<()> {
            let mut ops = self.ops.lock().unwrap();
            let op = ops
                .iter_mut()
                .find(|o| o.id == id)
                .ok_or_else(|| anyhow::anyhow!("stub: id not found: {id}"))?;
            if op.status != crate::inflight_ops::InflightStatus::InProgress {
                anyhow::bail!("stub: update_phase on terminal op {id}");
            }
            op.phase = phase.to_string();
            op.updated_at = Utc::now();
            op.last_error = last_error;
            Ok(())
        }
        async fn complete(&self, id: &str) -> anyhow::Result<()> {
            let mut ops = self.ops.lock().unwrap();
            let op = ops
                .iter_mut()
                .find(|o| o.id == id)
                .ok_or_else(|| anyhow::anyhow!("stub: id not found: {id}"))?;
            op.status = crate::inflight_ops::InflightStatus::Done;
            op.phase = "done".to_string();
            let now = Utc::now();
            op.updated_at = now;
            op.completed_at = Some(now);
            op.last_error = None;
            Ok(())
        }
        async fn abandon(&self, id: &str, reason: &str) -> anyhow::Result<()> {
            let mut ops = self.ops.lock().unwrap();
            let op = ops
                .iter_mut()
                .find(|o| o.id == id)
                .ok_or_else(|| anyhow::anyhow!("stub: id not found: {id}"))?;
            op.status = crate::inflight_ops::InflightStatus::Abandoned;
            let now = Utc::now();
            op.updated_at = now;
            op.completed_at = Some(now);
            op.last_error = Some(reason.to_string());
            Ok(())
        }
        async fn discharge(&self, id: &str) -> anyhow::Result<()> {
            let mut ops = self.ops.lock().unwrap();
            let op = ops
                .iter_mut()
                .find(|o| o.id == id)
                .ok_or_else(|| anyhow::anyhow!("stub: id not found: {id}"))?;
            op.discharged_at = Some(Utc::now());
            Ok(())
        }
        async fn find(
            &self,
            op_name: &str,
            key: &str,
        ) -> anyhow::Result<Option<crate::inflight_ops::InflightOp>> {
            let ops = self.ops.lock().unwrap();
            Ok(ops
                .iter()
                .filter(|o| o.payload.op_name() == op_name && o.payload.key() == key)
                .cloned()
                .max_by_key(|o| o.started_at))
        }
        async fn get(&self, id: &str) -> anyhow::Result<crate::inflight_ops::InflightOp> {
            let ops = self.ops.lock().unwrap();
            ops.iter()
                .find(|o| o.id == id)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("stub: id not found: {id}"))
        }
        async fn list(
            &self,
            statuses: &[crate::inflight_ops::InflightStatus],
        ) -> anyhow::Result<(
            Vec<crate::inflight_ops::InflightOp>,
            Vec<crate::inflight_ops::SkippedInflightOp>,
        )> {
            let ops = self.ops.lock().unwrap();
            let mut out: Vec<crate::inflight_ops::InflightOp> = if statuses.is_empty() {
                ops.clone()
            } else {
                ops.iter()
                    .filter(|o| statuses.contains(&o.status))
                    .cloned()
                    .collect()
            };
            out.sort_by_key(|o| o.started_at);
            Ok((out, vec![]))
        }
        async fn sweep(&self, _: chrono::DateTime<Utc>) {}
    }

    #[derive(Default)]
    struct StubPcp {
        attach_calls: StdMutex<Vec<i32>>,
        attach_fails: AtomicBool,
    }

    #[async_trait]
    impl Pcp for StubPcp {
        async fn attach_node(&self, node_id: i32) -> anyhow::Result<()> {
            self.attach_calls.lock().unwrap().push(node_id);
            if self.attach_fails.load(Ordering::SeqCst) {
                anyhow::bail!("stub: pcp attach_node boom");
            }
            Ok(())
        }
        async fn detach_node(&self, _: i32) -> anyhow::Result<()> {
            unreachable!("no handler under test detaches")
        }
        async fn node_count(&self) -> anyhow::Result<i32> {
            Ok(0)
        }
        async fn node_info_all(&self) -> anyhow::Result<Vec<crate::pcp::NodeInfo>> {
            Ok(Vec::new())
        }
    }

    #[derive(Default)]
    struct StubPeerClient {
        start_calls: AtomicUsize,
        start_fails: AtomicBool,
        start_pgpool_calls: AtomicUsize,
        start_pgpool_fails: AtomicBool,
        /// Map wal_file → content the peer "has" in its archive.
        wal_content: StdMutex<std::collections::HashMap<String, Vec<u8>>>,
        /// Make fetch_wal err out (transport/RPC failure shape).
        fetch_wal_errors: AtomicBool,
        fetch_wal_calls: AtomicUsize,
        /// Hang fetch_wal indefinitely — exercises the per-peer timeout.
        fetch_wal_hangs: AtomicBool,
        // FollowPrimary surface — counters + failure switches per method.
        is_running: AtomicBool,
        is_in_recovery: AtomicBool,
        replication_lag_bytes: AtomicI64,
        /// Peer's `current_wal_lsn` (on a standby = `pg_last_wal_replay_lsn()`).
        /// Used by the handoff lag check after 0.6.1; tests set this
        /// to a non-zero value to bypass the "cannot measure lag" guard.
        current_wal_lsn: std::sync::atomic::AtomicU64,
        /// Peer's live timeline. Defaults to 0 (= unknown).
        timeline_id: std::sync::atomic::AtomicI32,
        /// `pg_stat_wal_receiver.status` as reported via GetStatus.
        /// Defaults to "" (= no receiver), which keeps the standby-down
        /// precondition check passing in tests that aren't about it.
        replication_state: StdMutex<String>,
        stop_calls: AtomicUsize,
        stop_fails: AtomicBool,
        rewind_calls: AtomicUsize,
        rewind_fails: AtomicBool,
        basebackup_calls: AtomicUsize,
        basebackup_fails: AtomicBool,
        configure_standby_calls: AtomicUsize,
        configure_standby_fails: AtomicBool,
        configure_standby_opts: StdMutex<Vec<WriteRecoveryConfOpts>>,
        // Failover surface.
        promote_calls: AtomicUsize,
        drop_slot_calls: StdMutex<Vec<String>>,
        drop_slot_fails: AtomicBool,
        create_slot_calls: StdMutex<Vec<String>>,
        create_slot_fails: AtomicBool,
    }

    impl StubPeerClient {
        fn stage_wal(&self, name: &str, content: Vec<u8>) {
            self.wal_content
                .lock()
                .unwrap()
                .insert(name.to_string(), content);
        }
        fn mark_running(&self) -> &Self {
            self.is_running.store(true, Ordering::SeqCst);
            self
        }
        fn mark_standby(&self) -> &Self {
            self.is_running.store(true, Ordering::SeqCst);
            self.is_in_recovery.store(true, Ordering::SeqCst);
            self
        }
        fn set_lag(&self, bytes: i64) -> &Self {
            self.replication_lag_bytes.store(bytes, Ordering::SeqCst);
            self
        }
        /// Standby's `pg_last_wal_replay_lsn()` as a 64-bit value.
        fn set_replay_lsn(&self, lsn: u64) -> &Self {
            self.current_wal_lsn.store(lsn, Ordering::SeqCst);
            self
        }
        /// Mark the peer as actively streaming (healthy standby) — the
        /// state the standby-down precondition check refuses to break.
        fn set_streaming(&self) -> &Self {
            *self.replication_state.lock().unwrap() = "streaming".to_string();
            self
        }
    }

    #[async_trait]
    impl PeerClient for StubPeerClient {
        async fn drop_slot(&self, name: &str) -> anyhow::Result<()> {
            self.drop_slot_calls.lock().unwrap().push(name.to_string());
            if self.drop_slot_fails.load(Ordering::SeqCst) {
                anyhow::bail!("stub peer drop_slot boom");
            }
            Ok(())
        }
        async fn create_slot(&self, name: &str) -> anyhow::Result<()> {
            self.create_slot_calls
                .lock()
                .unwrap()
                .push(name.to_string());
            if self.create_slot_fails.load(Ordering::SeqCst) {
                anyhow::bail!("stub peer create_slot boom");
            }
            Ok(())
        }
        async fn get_node_config(&self) -> anyhow::Result<NodeConfigResponse> {
            Ok(NodeConfigResponse {
                pg_port: 5432,
                pg_data_dir: "/d".into(),
            })
        }
        async fn start(&self) -> anyhow::Result<()> {
            self.start_calls.fetch_add(1, Ordering::SeqCst);
            if self.start_fails.load(Ordering::SeqCst) {
                anyhow::bail!("stub peer start boom");
            }
            Ok(())
        }
        async fn start_pgpool(&self) -> anyhow::Result<()> {
            self.start_pgpool_calls.fetch_add(1, Ordering::SeqCst);
            if self.start_pgpool_fails.load(Ordering::SeqCst) {
                anyhow::bail!("stub peer start_pgpool boom");
            }
            Ok(())
        }
        async fn fetch_wal(
            &self,
            wal_file: &str,
        ) -> anyhow::Result<Option<Box<dyn tokio::io::AsyncRead + Send + Unpin>>> {
            self.fetch_wal_calls.fetch_add(1, Ordering::SeqCst);
            if self.fetch_wal_hangs.load(Ordering::SeqCst) {
                std::future::pending::<()>().await;
            }
            if self.fetch_wal_errors.load(Ordering::SeqCst) {
                anyhow::bail!("stub peer fetch_wal boom");
            }
            let content = self.wal_content.lock().unwrap().get(wal_file).cloned();
            Ok(content.map(|c| {
                Box::new(std::io::Cursor::new(c)) as Box<dyn tokio::io::AsyncRead + Send + Unpin>
            }))
        }
        async fn get_status(&self) -> anyhow::Result<NodeStatus> {
            let running = self.is_running.load(Ordering::SeqCst);
            let in_recovery = self.is_in_recovery.load(Ordering::SeqCst);
            let lag = self.replication_lag_bytes.load(Ordering::SeqCst);
            let lsn = self.current_wal_lsn.load(Ordering::SeqCst);
            Ok(NodeStatus {
                is_running: running,
                is_in_recovery: in_recovery,
                is_ready: false,
                replication_lag_bytes: lag,
                replication_state: self.replication_state.lock().unwrap().clone(),
                is_postgres_running: running,
                is_pgpool_running: true,
                is_postgres_status_ok: true,
                is_pgpool_status_ok: true,
                timeline_id: self.timeline_id.load(Ordering::SeqCst),
                current_wal_lsn: lsn,
                last_flush_lsn: lsn,
            })
        }
        async fn stop(&self) -> anyhow::Result<()> {
            self.stop_calls.fetch_add(1, Ordering::SeqCst);
            if self.stop_fails.load(Ordering::SeqCst) {
                anyhow::bail!("stub peer stop boom");
            }
            Ok(())
        }
        async fn rewind(&self, _: RewindOpts) -> anyhow::Result<()> {
            self.rewind_calls.fetch_add(1, Ordering::SeqCst);
            if self.rewind_fails.load(Ordering::SeqCst) {
                anyhow::bail!("stub peer rewind boom");
            }
            Ok(())
        }
        async fn basebackup(&self, _: BasebackupOpts) -> anyhow::Result<()> {
            self.basebackup_calls.fetch_add(1, Ordering::SeqCst);
            if self.basebackup_fails.load(Ordering::SeqCst) {
                anyhow::bail!("stub peer basebackup boom");
            }
            Ok(())
        }
        async fn configure_standby(&self, opts: WriteRecoveryConfOpts) -> anyhow::Result<()> {
            self.configure_standby_calls.fetch_add(1, Ordering::SeqCst);
            self.configure_standby_opts.lock().unwrap().push(opts);
            if self.configure_standby_fails.load(Ordering::SeqCst) {
                anyhow::bail!("stub peer configure_standby boom");
            }
            Ok(())
        }
        async fn promote(&self) -> anyhow::Result<()> {
            self.promote_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[derive(Default)]
    struct StubSd {
        stop_postgres_calls: AtomicUsize,
        start_postgres_calls: AtomicUsize,
        stop_postgres_fails: AtomicBool,
        start_postgres_fails: AtomicBool,
    }

    #[async_trait]
    impl Systemd for StubSd {
        async fn start_postgres(&self) -> anyhow::Result<()> {
            self.start_postgres_calls.fetch_add(1, Ordering::SeqCst);
            if self.start_postgres_fails.load(Ordering::SeqCst) {
                anyhow::bail!("stub sd: start_postgres boom");
            }
            Ok(())
        }
        async fn stop_postgres(&self) -> anyhow::Result<()> {
            self.stop_postgres_calls.fetch_add(1, Ordering::SeqCst);
            if self.stop_postgres_fails.load(Ordering::SeqCst) {
                anyhow::bail!("stub sd: stop_postgres boom");
            }
            Ok(())
        }
        async fn start_pgpool(&self) -> anyhow::Result<()> {
            Ok(())
        }
        async fn status_postgres(&self) -> anyhow::Result<bool> {
            Ok(true)
        }
        async fn status_pgpool(&self) -> anyhow::Result<bool> {
            Ok(true)
        }
        async fn reload_or_restart_postgres(&self) -> anyhow::Result<()> {
            Ok(())
        }
        async fn reload_or_restart_pgpool(&self) -> anyhow::Result<()> {
            Ok(())
        }
    }

    #[derive(Default)]
    struct StubStandby {
        rewind_calls: AtomicUsize,
        rewind_fails: AtomicBool,
        basebackup_calls: AtomicUsize,
        basebackup_fails: AtomicBool,
        write_recovery_conf_calls: AtomicUsize,
    }

    #[async_trait]
    impl StandbyOps for StubStandby {
        async fn rewind(
            &self,
            _: RewindOpts,
            _: Option<crate::pgstandby::ProgressCb>,
        ) -> anyhow::Result<()> {
            self.rewind_calls.fetch_add(1, Ordering::SeqCst);
            if self.rewind_fails.load(Ordering::SeqCst) {
                anyhow::bail!("stub standby: rewind boom");
            }
            Ok(())
        }
        async fn basebackup(
            &self,
            _: BasebackupOpts,
            _: Option<crate::pgstandby::ProgressCb>,
        ) -> anyhow::Result<()> {
            self.basebackup_calls.fetch_add(1, Ordering::SeqCst);
            if self.basebackup_fails.load(Ordering::SeqCst) {
                anyhow::bail!("stub standby: basebackup boom");
            }
            Ok(())
        }
        async fn write_recovery_conf(&self, _: WriteRecoveryConfOpts) -> anyhow::Result<()> {
            self.write_recovery_conf_calls
                .fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[derive(Default)]
    struct StubPeers {
        /// Returned for any node that lacks an override.
        default_client: Arc<StubPeerClient>,
        /// Per-node-id override; lets tests put different staged content
        /// on different peers (used by restore_wal fan-out tests).
        overrides: StdMutex<std::collections::HashMap<i32, Arc<StubPeerClient>>>,
        /// Node ids whose `client()` call returns Err (simulates an
        /// unreachable / partitioned peer).
        unreachable: StdMutex<std::collections::HashSet<i32>>,
        /// Global failure switch (legacy of remote_start tests).
        fail_client: AtomicBool,
    }

    impl StubPeers {
        fn override_client(&self, node_id: i32, client: Arc<StubPeerClient>) {
            self.overrides.lock().unwrap().insert(node_id, client);
        }
        fn mark_unreachable(&self, node_id: i32) {
            self.unreachable.lock().unwrap().insert(node_id);
        }
    }

    #[async_trait]
    impl PeerRegistry for StubPeers {
        async fn client(&self, node: &NodeConfig) -> anyhow::Result<Arc<dyn PeerClient>> {
            if self.fail_client.load(Ordering::SeqCst) {
                anyhow::bail!("stub: peer client unreachable");
            }
            if self.unreachable.lock().unwrap().contains(&node.id) {
                anyhow::bail!("stub: peer {} unreachable", node.id);
            }
            let overrides = self.overrides.lock().unwrap();
            Ok(overrides
                .get(&node.id)
                .cloned()
                .unwrap_or_else(|| self.default_client.clone())
                as Arc<dyn PeerClient>)
        }
        async fn close(&self) -> anyhow::Result<()> {
            Ok(())
        }
    }

    #[derive(Default)]
    struct StubMaint {
        intents: StdMutex<Vec<CoreIntent>>,
        skipped: StdMutex<Vec<SkippedIntent>>,
        rescheduled: StdMutex<Vec<(String, chrono::DateTime<Utc>)>>,
    }

    impl StubMaint {
        fn insert(&self, intent: CoreIntent) {
            self.intents.lock().unwrap().push(intent);
        }
    }

    #[async_trait]
    impl MaintenanceStore for StubMaint {
        async fn append(&self, payload: MaintenancePayload) -> anyhow::Result<CoreIntent> {
            let intent = CoreIntent {
                id: format!("stub-{}", self.intents.lock().unwrap().len()),
                status: MaintenanceStatus::Pending,
                payload,
                attempts: 0,
                last_error: String::new(),
                created_at: Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
                updated_at: Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
                next_retry_at: None,
            };
            self.intents.lock().unwrap().push(intent.clone());
            Ok(intent)
        }
        async fn list_pending(&self) -> anyhow::Result<Vec<CoreIntent>> {
            Ok(self
                .intents
                .lock()
                .unwrap()
                .iter()
                .filter(|i| i.status == MaintenanceStatus::Pending)
                .cloned()
                .collect())
        }
        async fn list(
            &self,
            statuses: &[MaintenanceStatus],
        ) -> anyhow::Result<(Vec<CoreIntent>, Vec<SkippedIntent>)> {
            let all = self.intents.lock().unwrap();
            let filtered: Vec<_> = if statuses.is_empty() {
                all.clone()
            } else {
                all.iter()
                    .filter(|i| statuses.contains(&i.status))
                    .cloned()
                    .collect()
            };
            let skipped = self.skipped.lock().unwrap().clone();
            Ok((filtered, skipped))
        }
        async fn get(&self, id: &str) -> anyhow::Result<CoreIntent> {
            let all = self.intents.lock().unwrap();
            all.iter()
                .find(|i| i.id == id)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("not found: {id}"))
        }
        async fn mark_attempt(
            &self,
            _: &str,
            _: &str,
            _: chrono::DateTime<Utc>,
        ) -> anyhow::Result<()> {
            Ok(())
        }
        async fn mark_done(&self, _: &str) -> anyhow::Result<()> {
            Ok(())
        }
        async fn mark_abandoned(&self, _: &str, _: &str) -> anyhow::Result<()> {
            Ok(())
        }
        async fn reschedule(&self, id: &str, when: chrono::DateTime<Utc>) -> anyhow::Result<()> {
            // Ensure intent exists (mirrors FileMaintenanceStore semantics).
            let exists = self.intents.lock().unwrap().iter().any(|i| i.id == id);
            if !exists {
                anyhow::bail!("not found: {id}");
            }
            self.rescheduled
                .lock()
                .unwrap()
                .push((id.to_string(), when));
            Ok(())
        }
    }

    /// WalStore stub that records `write_restore` calls and can be made
    /// to return any of the three failure modes restore_wal cares about
    /// (`DestOutsidePgData` — fatal; anything else — `TryNext`).
    #[derive(Default)]
    struct StubWal {
        written: StdMutex<Vec<(std::path::PathBuf, Vec<u8>)>>,
        dest_outside_pgdata: AtomicBool,
        write_errors: AtomicBool,
    }

    #[async_trait]
    impl WalStore for StubWal {
        async fn open_archive(
            &self,
            _: &str,
        ) -> Result<Box<dyn tokio::io::AsyncRead + Send + Unpin>, pgman::walstore::WalStoreError>
        {
            // LocalServer doesn't call open_archive — only the inbound
            // PeerServer.FetchWal does, and that path is exercised in
            // peerserver tests.
            Err(pgman::walstore::WalStoreError::WalNotFound(
                "stub: not used here".into(),
            ))
        }
        async fn write_restore(
            &self,
            dest_path: &Path,
            mut src: Box<dyn tokio::io::AsyncRead + Send + Unpin>,
        ) -> Result<(), pgman::walstore::WalStoreError> {
            if self.dest_outside_pgdata.load(Ordering::SeqCst) {
                return Err(pgman::walstore::WalStoreError::DestOutsidePgData);
            }
            if self.write_errors.load(Ordering::SeqCst) {
                return Err(pgman::walstore::WalStoreError::WalNotFound(
                    "stub: write_restore boom".into(),
                ));
            }
            use tokio::io::AsyncReadExt;
            let mut bytes = Vec::new();
            src.read_to_end(&mut bytes)
                .await
                .map_err(pgman::walstore::WalStoreError::Io)?;
            self.written
                .lock()
                .unwrap()
                .push((dest_path.to_path_buf(), bytes));
            Ok(())
        }
    }

    // ----- builders ----------------------------------------------------

    fn make_pool() -> NodePool {
        NodePool {
            members: vec![
                NodeConfig {
                    id: 0,
                    hostname: "local".into(),
                },
                NodeConfig {
                    id: 1,
                    hostname: "peer1.local".into(),
                },
            ],
            local_node_id: 0,
        }
    }

    fn make_pg() -> PostgresRuntime {
        PostgresRuntime {
            port: 5432,
            data_dir: std::path::PathBuf::from("/var/lib/postgresql/17/main"),
            repl_user: "repl".into(),
        }
    }

    #[allow(clippy::type_complexity)]
    fn make_server() -> (
        LocalServer,
        Arc<StubDb>,
        Arc<StubPeers>,
        Arc<StubMaint>,
        Arc<StubWal>,
        Arc<StubReplay>,
        Arc<StubPcp>,
        Arc<StubSd>,
        Arc<StubStandby>,
        Arc<StubInflight>,
    ) {
        let db = Arc::new(StubDb::default());
        let peers = Arc::new(StubPeers::default());
        let maint = Arc::new(StubMaint::default());
        let wal = Arc::new(StubWal::default());
        let replay = Arc::new(StubReplay::default());
        let pcp = Arc::new(StubPcp::default());
        let sd = Arc::new(StubSd::default());
        let standby = Arc::new(StubStandby::default());
        let inflight = Arc::new(StubInflight::default());
        let server = LocalServer::new(
            Arc::new(FakeNodeInfo),
            db.clone(),
            peers.clone(),
            maint.clone(),
            wal.clone(),
            replay.clone(),
            inflight.clone(),
            pcp.clone(),
            sd.clone(),
            standby.clone(),
            make_pool(),
            make_pg(),
        );
        (
            server, db, peers, maint, wal, replay, pcp, sd, standby, inflight,
        )
    }

    fn pending_intent(id: &str) -> CoreIntent {
        CoreIntent {
            id: id.to_string(),
            status: MaintenanceStatus::Pending,
            payload: MaintenancePayload::DropSlotCleanup {
                slot_name: "node1".into(),
                target_hostname: "peer1.local".into(),
                cause: "rpc_error".into(),
                initial_error: "boom".into(),
            },
            attempts: 0,
            last_error: String::new(),
            created_at: Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
            updated_at: Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
            next_retry_at: None,
        }
    }

    // ----- read-only ---------------------------------------------------

    #[tokio::test]
    async fn get_status_routes_to_node_info() {
        let (s, ..) = make_server();
        let resp = s
            .get_status(Request::new(GetStatusRequest {}))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.is_running);
        assert!(resp.is_ready);
    }

    #[tokio::test]
    async fn get_node_config_routes_to_node_info() {
        let (s, ..) = make_server();
        let resp = s
            .get_node_config(Request::new(NodeConfigRequest {}))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.pg_port, 5432);
    }

    // ----- escalation ---------------------------------------------------

    #[tokio::test]
    async fn escalation_is_no_op_ok() {
        let (s, ..) = make_server();
        let resp = s
            .escalation(Request::new(EscalationRequest::default()))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.ok);
    }

    // ----- remote_start --------------------------------------------------

    #[tokio::test]
    async fn remote_start_calls_peer_start() {
        let (s, _db, peers, _maint, _wal, _replay, _pcp, _sd, _standby, _inflight) = make_server();
        let resp = s
            .remote_start(Request::new(RemoteStartRequest {
                target: Some(NodeRef {
                    id: 1,
                    hostname: "peer1.local".into(),
                    pg_port: 0,
                    pg_data: String::new(),
                }),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.ok);
        assert_eq!(peers.default_client.start_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn remote_start_refuses_when_local_is_replica() {
        let (s, db, peers, _maint, _wal, _replay, _pcp, _sd, _standby, _inflight) = make_server();
        db.in_recovery.store(true, Ordering::SeqCst);
        let resp = s
            .remote_start(Request::new(RemoteStartRequest {
                target: Some(NodeRef {
                    id: 1,
                    hostname: "peer1.local".into(),
                    pg_port: 0,
                    pg_data: String::new(),
                }),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(!resp.ok);
        assert!(resp.message.contains("not the primary"));
        assert_eq!(peers.default_client.start_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn remote_start_propagates_peer_error_as_internal() {
        let (s, _db, peers, _maint, _wal, _replay, _pcp, _sd, _standby, _inflight) = make_server();
        peers
            .default_client
            .start_fails
            .store(true, Ordering::SeqCst);
        let err = s
            .remote_start(Request::new(RemoteStartRequest {
                target: Some(NodeRef {
                    id: 1,
                    hostname: "peer1.local".into(),
                    pg_port: 0,
                    pg_data: String::new(),
                }),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Internal);
        assert!(err.message().contains("peer start"));
    }

    #[tokio::test]
    async fn remote_start_rejects_missing_target() {
        let (s, ..) = make_server();
        let err = s
            .remote_start(Request::new(RemoteStartRequest { target: None }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn remote_start_rejects_unknown_target() {
        let (s, ..) = make_server();
        let err = s
            .remote_start(Request::new(RemoteStartRequest {
                target: Some(NodeRef {
                    id: 99,
                    hostname: "nope".into(),
                    pg_port: 0,
                    pg_data: String::new(),
                }),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    // ----- maintenance reads --------------------------------------------

    #[tokio::test]
    async fn list_maintenance_no_filter_returns_all() {
        let (s, _db, _peers, maint, _wal, _replay, _pcp, _sd, _standby, _inflight) = make_server();
        maint.insert(pending_intent("a"));
        let mut done_intent = pending_intent("b");
        done_intent.status = MaintenanceStatus::Done;
        maint.insert(done_intent);

        let resp = s
            .list_maintenance(Request::new(ListMaintenanceRequest { statuses: vec![] }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.intents.len(), 2);
    }

    #[tokio::test]
    async fn list_maintenance_filters_by_status() {
        let (s, _db, _peers, maint, _wal, _replay, _pcp, _sd, _standby, _inflight) = make_server();
        maint.insert(pending_intent("pending-1"));
        let mut done_intent = pending_intent("done-1");
        done_intent.status = MaintenanceStatus::Done;
        maint.insert(done_intent);

        let resp = s
            .list_maintenance(Request::new(ListMaintenanceRequest {
                statuses: vec!["pending".to_string()],
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.intents.len(), 1);
        assert_eq!(resp.intents[0].id, "pending-1");
        assert_eq!(resp.intents[0].status, "pending");
    }

    #[tokio::test]
    async fn list_maintenance_rejects_unknown_filter() {
        let (s, ..) = make_server();
        let err = s
            .list_maintenance(Request::new(ListMaintenanceRequest {
                statuses: vec!["fishy".to_string()],
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn list_maintenance_surfaces_skipped() {
        let (s, _db, _peers, maint, _wal, _replay, _pcp, _sd, _standby, _inflight) = make_server();
        maint.skipped.lock().unwrap().push(SkippedIntent {
            path: "/tmp/corrupt.json".into(),
            error: "bad json".into(),
        });
        let resp = s
            .list_maintenance(Request::new(ListMaintenanceRequest { statuses: vec![] }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.skipped.len(), 1);
        assert_eq!(resp.skipped[0].path, "/tmp/corrupt.json");
    }

    #[tokio::test]
    async fn get_maintenance_returns_existing_intent() {
        let (s, _db, _peers, maint, _wal, _replay, _pcp, _sd, _standby, _inflight) = make_server();
        maint.insert(pending_intent("alpha"));
        let resp = s
            .get_maintenance(Request::new(GetMaintenanceRequest { id: "alpha".into() }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.id, "alpha");
        assert_eq!(resp.op, "drop_slot_cleanup");
        assert_eq!(resp.status, "pending");
        assert!(!resp.payload.is_empty());
    }

    #[tokio::test]
    async fn get_maintenance_missing_id_returns_not_found() {
        let (s, ..) = make_server();
        let err = s
            .get_maintenance(Request::new(GetMaintenanceRequest { id: "nope".into() }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn get_maintenance_empty_id_returns_invalid_argument() {
        let (s, ..) = make_server();
        let err = s
            .get_maintenance(Request::new(GetMaintenanceRequest { id: String::new() }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn retry_maintenance_reschedules_pending_intent() {
        let (s, _db, _peers, maint, _wal, _replay, _pcp, _sd, _standby, _inflight) = make_server();
        maint.insert(pending_intent("alpha"));
        let resp = s
            .retry_maintenance(Request::new(RetryMaintenanceRequest { id: "alpha".into() }))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.ok);
        let resched = maint.rescheduled.lock().unwrap();
        assert_eq!(resched.len(), 1);
        assert_eq!(resched[0].0, "alpha");
    }

    #[tokio::test]
    async fn retry_maintenance_refuses_non_pending_intent() {
        let (s, _db, _peers, maint, _wal, _replay, _pcp, _sd, _standby, _inflight) = make_server();
        let mut done = pending_intent("alpha");
        done.status = MaintenanceStatus::Done;
        maint.insert(done);
        let resp = s
            .retry_maintenance(Request::new(RetryMaintenanceRequest { id: "alpha".into() }))
            .await
            .unwrap()
            .into_inner();
        assert!(!resp.ok);
        assert!(resp.message.contains("only pending intents"));
        assert!(maint.rescheduled.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn retry_maintenance_missing_id_returns_not_found() {
        let (s, ..) = make_server();
        let err = s
            .retry_maintenance(Request::new(RetryMaintenanceRequest { id: "nope".into() }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::NotFound);
    }

    // ----- failover -----------------------------------------------------

    fn failover_req(detached: i32, new_main: i32, old_primary: i32) -> FailoverRequest {
        FailoverRequest {
            detached: Some(NodeRef {
                id: detached,
                hostname: pool_hostname(detached),
                pg_port: 0,
                pg_data: String::new(),
            }),
            new_main: Some(NodeRef {
                id: new_main,
                hostname: if new_main >= 0 {
                    pool_hostname(new_main)
                } else {
                    String::new()
                },
                pg_port: 0,
                pg_data: String::new(),
            }),
            old_primary: Some(NodeRef {
                id: old_primary,
                hostname: pool_hostname(old_primary),
                pg_port: 0,
                pg_data: String::new(),
            }),
            old_main: None,
        }
    }

    #[tokio::test]
    async fn failover_no_candidates_returns_ok_false_without_marker() {
        let (s, _db, _peers, _maint, _wal, replay, _pcp, _sd, _standby, _inflight) = make_server();
        // new_main.id == -1 — pgpool's no-candidate sentinel.
        let resp = s
            .failover(Request::new(failover_req(1, -1, 0)))
            .await
            .unwrap()
            .into_inner();
        assert!(!resp.ok);
        assert!(resp.message.contains("no standby candidates"));
        // No replay marker — pgpool may retry once a candidate exists.
        assert!(!replay.has("failover", "any").await.unwrap());
    }

    #[tokio::test]
    async fn failover_standby_down_drops_slot_locally() {
        // detached=1 (peer1), new_main=0 (us, the primary), old_primary=0.
        // Since detached.id != old_primary.id, this is the standby-down
        // branch: we drop the slot locally and don't dial a peer.
        let (s, db, _peers, _maint, _wal, replay, _pcp, _sd, _standby, _inflight) = make_server();
        let resp = s
            .failover(Request::new(failover_req(1, 0, 0)))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.ok);
        assert!(resp.message.contains("slot node1 dropped"));
        assert_eq!(*db.dropped_slots.lock().unwrap(), vec!["node1".to_string()]);
        assert!(replay
            .has("failover", "detached=1,new_main=0,old_primary=0")
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn failover_standby_down_local_drop_failure_queues_maintenance() {
        let (s, db, _peers, maint, _wal, replay, _pcp, _sd, _standby, _inflight) = make_server();
        db.drop_slot_fails.store(true, Ordering::SeqCst);
        let resp = s
            .failover(Request::new(failover_req(1, 0, 0)))
            .await
            .unwrap()
            .into_inner();
        // Hook still returns Ok — pgpool doesn't retry on slot-cleanup.
        assert!(resp.ok);
        assert!(resp.message.contains("queued maintenance"));
        {
            let intents = maint.intents.lock().unwrap();
            assert_eq!(intents.len(), 1);
            match &intents[0].payload {
                MaintenancePayload::DropSlotCleanup { cause, .. } => {
                    assert_eq!(cause, "standby_down_local_drop_error");
                }
            }
        }
        // Marker IS written — the hook completed (just with a queued cleanup).
        assert!(replay
            .has("failover", "detached=1,new_main=0,old_primary=0")
            .await
            .unwrap());
    }

    /// The hook contract: the primary-down branch is a notify-only
    /// poke — no promotion, no slot drop, the HA loop decides. The
    /// standby-down branch is mechanism, not authority, and must keep
    /// working unchanged.
    #[tokio::test]
    async fn failover_primary_down_is_always_advisory() {
        let (s, _db, peers, _maint, _wal, replay, _pcp, _sd, _standby, _inflight) = make_server();
        let new_main_client = Arc::new(StubPeerClient::default());
        peers.override_client(0, new_main_client.clone());

        let resp = s
            .failover(Request::new(failover_req(1, 0, 1)))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.ok, "advisory answer must be ok=true: {}", resp.message);
        assert!(resp.message.contains("advisory"), "{}", resp.message);
        assert_eq!(
            new_main_client.promote_calls.load(Ordering::SeqCst),
            0,
            "the hook must not promote under lease-driven roles"
        );
        assert!(
            new_main_client.drop_slot_calls.lock().unwrap().is_empty(),
            "no slot action from the advisory branch"
        );
        // No replay marker either: the advisory answer is stateless and
        // re-firing it is free.
        assert!(!replay
            .has("failover", "detached=1,new_main=0,old_primary=1")
            .await
            .unwrap());
    }

    /// A replay marker from a legacy-mode failover (same key — pgpool
    /// re-announces the same detached/new_main/old_primary shape) must
    /// not mask the advisory: run 8's E3 hit exactly this, with S9's
    /// marker answering "already processed" where the cutover contract
    /// should have said "advisory".
    #[tokio::test]
    async fn failover_advisory_wins_over_a_stale_legacy_replay_marker() {
        let (s, _db, peers, _maint, _wal, replay, _pcp, _sd, _standby, _inflight) = make_server();
        replay
            .mark_done("failover", "detached=1,new_main=0,old_primary=1")
            .await
            .unwrap();
        let new_main_client = Arc::new(StubPeerClient::default());
        peers.override_client(0, new_main_client.clone());

        let resp = s
            .failover(Request::new(failover_req(1, 0, 1)))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.ok);
        assert!(
            resp.message.contains("advisory"),
            "marker masked the advisory: {}",
            resp.message
        );
        assert_eq!(new_main_client.promote_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn failover_standby_down_still_drops_slot() {
        let (s, db, _peers, _maint, _wal, _replay, _pcp, _sd, _standby, _inflight) = make_server();
        let resp = s
            .failover(Request::new(failover_req(1, 0, 0)))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.ok);
        assert_eq!(
            db.dropped_slots.lock().unwrap().as_slice(),
            ["node1"],
            "standby-down slot hygiene is mechanism and survives the cutover"
        );
    }

    // ----- failover preconditions (defense in depth, TODO.md / §3) ----------

    #[tokio::test]
    async fn failover_skips_slot_drop_while_a_recovery_owns_the_node() {
        // The acceptance-suite race: `cluster recover --stop-target-pg`
        // stops db1's PostgreSQL, pgpool fires failover_command with
        // db1 detached, and the standby-down branch would drop the very
        // slot the in-flight recovery just created. The detached node
        // really IS down, so the precondition check cannot help — only
        // the cross-op consult can.
        let (s, db, _peers, _maint, _wal, replay, _pcp, _sd, _standby, inflight) = make_server();
        inflight
            .begin(
                crate::inflight_ops::InflightPayload::Recovery {
                    primary_node_id: 0,
                    standby_node_id: 1,
                    standby_hostname: "peer1.local".into(),
                    slot_name: "node1".into(),
                },
                REC_PHASE_SLOT_CREATED,
                false,
            )
            .await
            .unwrap();

        let resp = s
            .failover(Request::new(failover_req(1, 0, 0)))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.ok, "{}", resp.message);
        assert!(resp.message.contains("retained"), "{}", resp.message);
        assert!(
            db.dropped_slots.lock().unwrap().is_empty(),
            "the in-flight recovery's slot must survive"
        );
        assert!(replay
            .has("failover", "detached=1,new_main=0,old_primary=0")
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn failover_skips_slot_drop_just_after_a_recovery_completes() {
        // pgpool's hook is a delayed reaction: health-check detection
        // plus exec time means the hook caused by "recovery stopped the
        // target" routinely lands after the recovery finished. The
        // rebuilt standby has been started but may not be `streaming`
        // yet, so the precondition check reads it as legitimately down —
        // only the grace window saves the slot.
        let (s, db, _peers, _maint, _wal, _replay, _pcp, _sd, _standby, inflight) = make_server();
        let op = inflight
            .begin(
                crate::inflight_ops::InflightPayload::Recovery {
                    primary_node_id: 0,
                    standby_node_id: 1,
                    standby_hostname: "peer1.local".into(),
                    slot_name: "node1".into(),
                },
                REC_PHASE_SLOT_CREATED,
                false,
            )
            .await
            .unwrap();
        inflight.complete(&op.id).await.unwrap();

        let resp = s
            .failover(Request::new(failover_req(1, 0, 0)))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.ok, "{}", resp.message);
        assert!(resp.message.contains("retained"), "{}", resp.message);
        assert!(
            db.dropped_slots.lock().unwrap().is_empty(),
            "a just-completed recovery's slot must survive the lagging hook"
        );
    }

    #[tokio::test]
    async fn failover_drops_slot_once_the_grace_window_has_passed() {
        // Same shape, but the recovery completed long ago — nothing owns
        // node 1 any more, so a genuine standby-down failover proceeds
        // and the stale slot is reclaimed.
        let (s, db, _peers, _maint, _wal, _replay, _pcp, _sd, _standby, inflight) = make_server();
        let op = inflight
            .begin(
                crate::inflight_ops::InflightPayload::Recovery {
                    primary_node_id: 0,
                    standby_node_id: 1,
                    standby_hostname: "peer1.local".into(),
                    slot_name: "node1".into(),
                },
                REC_PHASE_SLOT_CREATED,
                false,
            )
            .await
            .unwrap();
        inflight.complete(&op.id).await.unwrap();
        inflight.backdate_completion(&op.id, CROSS_OP_GRACE + chrono::Duration::seconds(1));

        let resp = s
            .failover(Request::new(failover_req(1, 0, 0)))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.ok, "{}", resp.message);
        assert_eq!(*db.dropped_slots.lock().unwrap(), vec!["node1".to_string()]);
    }

    #[tokio::test]
    async fn failover_discharges_ownership_when_the_rebuilt_standby_streams() {
        // The event that ends a completed op's ownership: the consult
        // observes the slot ACTIVE (the rebuilt standby came up),
        // records the discharge, and stops shielding the node — the
        // ordinary guards own the decision from here.
        let (s, db, _peers, _maint, _wal, _replay, _pcp, _sd, _standby, inflight) = make_server();
        db.slot_is_active.store(true, Ordering::SeqCst);
        let op = inflight
            .begin(
                crate::inflight_ops::InflightPayload::Recovery {
                    primary_node_id: 0,
                    standby_node_id: 1,
                    standby_hostname: "peer1.local".into(),
                    slot_name: "node1".into(),
                },
                REC_PHASE_SLOT_CREATED,
                false,
            )
            .await
            .unwrap();
        inflight.complete(&op.id).await.unwrap();

        let resp = s
            .failover(Request::new(failover_req(1, 0, 0)))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.ok, "{}", resp.message);
        assert!(
            !resp.message.contains("retained"),
            "consult must not shield a discharged op: {}",
            resp.message
        );
        assert!(
            inflight.get(&op.id).await.unwrap().discharged_at.is_some(),
            "the observed-alive event must be recorded on the op"
        );
        // The drop proceeded (stub PG has no active-slot refusal; in
        // production an active slot cannot be dropped anyway).
        assert_eq!(*db.dropped_slots.lock().unwrap(), vec!["node1".to_string()]);
    }

    #[tokio::test]
    async fn failover_reclaims_immediately_once_discharged() {
        // Came up (discharged), then genuinely died: the next hook
        // reclaims the slot NOW — no waiting out the grace window.
        let (s, db, _peers, _maint, _wal, _replay, _pcp, _sd, _standby, inflight) = make_server();
        let op = inflight
            .begin(
                crate::inflight_ops::InflightPayload::Recovery {
                    primary_node_id: 0,
                    standby_node_id: 1,
                    standby_hostname: "peer1.local".into(),
                    slot_name: "node1".into(),
                },
                REC_PHASE_SLOT_CREATED,
                false,
            )
            .await
            .unwrap();
        inflight.complete(&op.id).await.unwrap();
        inflight.discharge(&op.id).await.unwrap();
        // Slot inactive now (the standby died) — well within the grace.

        let resp = s
            .failover(Request::new(failover_req(1, 0, 0)))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.ok, "{}", resp.message);
        assert_eq!(
            *db.dropped_slots.lock().unwrap(),
            vec!["node1".to_string()],
            "a discharged op must not delay reclaim until the grace expires"
        );
    }

    #[tokio::test]
    async fn failover_refuses_slot_drop_when_detached_standby_streaming() {
        // Standby-down flavor of the same false report: the standby is
        // reachable, in recovery, and streaming — dropping its slot
        // would break replication that is demonstrably healthy.
        let (s, db, peers, _maint, _wal, replay, _pcp, _sd, _standby, _inflight) = make_server();
        let detached_client = Arc::new(StubPeerClient::default());
        detached_client.mark_standby().set_streaming();
        peers.override_client(1, detached_client);

        let resp = s
            .failover(Request::new(failover_req(1, 0, 0)))
            .await
            .unwrap()
            .into_inner();
        assert!(!resp.ok, "expected refusal, got: {}", resp.message);
        assert!(resp.message.contains("streaming"), "{}", resp.message);
        assert!(db.dropped_slots.lock().unwrap().is_empty());
        assert!(!replay
            .has("failover", "detached=1,new_main=0,old_primary=0")
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn failover_drops_slot_when_detached_standby_up_but_not_streaming() {
        // A standby that is up but has no WAL receiver is exactly what a
        // legitimate detach looks like (its replication broke) — the
        // precondition check must not shield it.
        let (s, db, peers, _maint, _wal, _replay, _pcp, _sd, _standby, _inflight) = make_server();
        let detached_client = Arc::new(StubPeerClient::default());
        detached_client.mark_standby(); // in recovery, replication_state ""
        peers.override_client(1, detached_client);

        let resp = s
            .failover(Request::new(failover_req(1, 0, 0)))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.ok, "expected drop, got: {}", resp.message);
        assert_eq!(*db.dropped_slots.lock().unwrap(), vec!["node1".to_string()]);
    }

    #[tokio::test]
    async fn failover_skips_when_replay_marker_present() {
        // Markers only matter for the standby-down branch now — the
        // primary-down advisory is stateless and answers before the
        // marker check.
        let (s, db, _peers, _maint, _wal, replay, _pcp, _sd, _standby, _inflight) = make_server();
        replay.mark("failover", "detached=1,new_main=0,old_primary=0");

        let resp = s
            .failover(Request::new(failover_req(1, 0, 0)))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.ok);
        assert!(resp.message.contains("already processed"));
        // No downstream calls happened.
        assert!(db.dropped_slots.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn failover_rejects_missing_refs() {
        let (s, ..) = make_server();
        let mut req = failover_req(1, 0, 0);
        req.detached = None;
        let err = s.failover(Request::new(req)).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    // ----- recovery_first_stage ------------------------------------------

    fn recovery_req(primary: i32, standby: i32) -> RecoveryRequest {
        RecoveryRequest {
            primary: Some(NodeRef {
                id: primary,
                hostname: pool_hostname(primary),
                pg_port: 0,
                pg_data: String::new(),
            }),
            standby: Some(NodeRef {
                id: standby,
                hostname: pool_hostname(standby),
                pg_port: 0,
                pg_data: String::new(),
            }),
            // Default to pgpool's path (dedup on). Tests that exercise
            // operator-bypass override this explicitly.
            bypass_replay_marker: false,
        }
    }

    /// Default setup: local node (id 0) is the primary; peer1 (id 1) is
    /// the standby being recovered. Returns the StubPeerClient that
    /// represents peer1 so tests can configure it, plus the StubPcp,
    /// StubSd, StubStandby, and StubInflight for cluster_recover /
    /// cluster_handoff assertions.
    #[allow(clippy::type_complexity)]
    fn make_recovery_setup() -> (
        LocalServer,
        Arc<StubDb>,
        Arc<StubPeers>,
        Arc<StubMaint>,
        Arc<StubReplay>,
        Arc<StubPeerClient>,
        Arc<StubPcp>,
        Arc<StubSd>,
        Arc<StubStandby>,
        Arc<StubInflight>,
    ) {
        let (s, db, peers, maint, _wal, replay, pcp, sd, standby, inflight) = make_server();
        let standby_client = Arc::new(StubPeerClient::default());
        peers.override_client(1, standby_client.clone());
        (
            s,
            db,
            peers,
            maint,
            replay,
            standby_client,
            pcp,
            sd,
            standby,
            inflight,
        )
    }

    #[tokio::test]
    async fn recovery_first_stage_happy_path() {
        let (s, db, _peers, _maint, _replay, standby, _pcp, _sd, _standby, _inflight) =
            make_recovery_setup();
        let resp = s
            .recovery_first_stage(Request::new(recovery_req(0, 1)))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.ok);
        assert!(resp.message.contains("recovery complete"));
        // Sequence: checkpoint → create_slot → basebackup → configure_standby
        // → mark_done. No promote/start/attach.
        assert_eq!(db.checkpoint_calls.load(Ordering::SeqCst), 1);
        assert_eq!(*db.created_slots.lock().unwrap(), vec!["node1".to_string()]);
        assert_eq!(standby.basebackup_calls.load(Ordering::SeqCst), 1);
        assert_eq!(standby.configure_standby_calls.load(Ordering::SeqCst), 1);
        assert_eq!(standby.start_calls.load(Ordering::SeqCst), 0);
        assert_eq!(standby.promote_calls.load(Ordering::SeqCst), 0);
        assert!(db.dropped_slots.lock().unwrap().is_empty());
        // Journaled as a completed `recovery` op, walking the ladder.
        let op = _inflight
            .find("recovery", "primary=0,standby=1")
            .await
            .unwrap()
            .expect("recovery op journaled");
        assert_eq!(op.status, crate::inflight_ops::InflightStatus::Done);
        assert_eq!(op.payload.target_node_id(), Some(1));
    }

    #[tokio::test]
    async fn recovery_first_stage_skips_after_a_recent_completion() {
        let (s, db, _peers, _maint, _replay, standby, _pcp, _sd, _standby, inflight) =
            make_recovery_setup();
        // A completed run of the same orchestration, moments ago.
        let prior = inflight
            .begin(
                crate::inflight_ops::InflightPayload::Recovery {
                    primary_node_id: 0,
                    standby_node_id: 1,
                    standby_hostname: "peer1.local".into(),
                    slot_name: "node1".into(),
                },
                REC_PHASE_STARTED,
                false,
            )
            .await
            .unwrap();
        inflight.complete(&prior.id).await.unwrap();
        let resp = s
            .recovery_first_stage(Request::new(recovery_req(0, 1)))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.ok);
        // Distinct message so cluster_recover-wrapped callers can flag
        // a silent skip vs. an actual basebackup. Two halves so a future
        // refactor of one of the strings doesn't break both tests.
        assert!(resp.message.contains("skipped"), "got: {}", resp.message);
        assert!(
            resp.message.contains("within the last 24h"),
            "got: {}",
            resp.message
        );
        // Nothing downstream of the dedup check touched.
        assert_eq!(db.checkpoint_calls.load(Ordering::SeqCst), 0);
        assert_eq!(standby.basebackup_calls.load(Ordering::SeqCst), 0);
    }

    /// `bypass_replay_marker=true` makes recovery_first_stage ignore an
    /// existing marker and run basebackup. Operator-driven
    /// `cluster_recover` relies on this — without it, a 24h-old marker
    /// silently short-circuited the recover and reported "complete"
    /// without actually reclonating (observed live on db2 2026-06-12).
    #[tokio::test]
    async fn recovery_first_stage_bypass_runs_even_with_marker_present() {
        let (s, db, _peers, _maint, replay, standby, _pcp, _sd, _standby, _inflight) =
            make_recovery_setup();
        replay.mark("recovery_1st_stage", "primary=0,standby=1");
        let mut req = recovery_req(0, 1);
        req.bypass_replay_marker = true;
        let resp = s
            .recovery_first_stage(Request::new(req))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.ok, "{}", resp.message);
        assert!(
            !resp.message.contains("skipped"),
            "bypass should not produce skip message: {}",
            resp.message
        );
        // Basebackup actually ran.
        assert_eq!(db.checkpoint_calls.load(Ordering::SeqCst), 1);
        assert_eq!(standby.basebackup_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn recovery_first_stage_rejects_non_local_primary() {
        // Pool: id=0 is local. Request claims id=1 is primary — rejected.
        let (s, ..) = make_server();
        let err = s
            .recovery_first_stage(Request::new(recovery_req(1, 0)))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("primary"));
    }

    #[tokio::test]
    async fn recovery_first_stage_rejects_missing_primary() {
        let (s, ..) = make_server();
        let mut req = recovery_req(0, 1);
        req.primary = None;
        let err = s.recovery_first_stage(Request::new(req)).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn recovery_first_stage_basebackup_failure_drops_slot() {
        let (s, db, _peers, _maint, replay, standby, _pcp, _sd, _standby, _inflight) =
            make_recovery_setup();
        standby.basebackup_fails.store(true, Ordering::SeqCst);

        let err = s
            .recovery_first_stage(Request::new(recovery_req(0, 1)))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Internal);
        assert!(err.message().contains("basebackup"));
        // Slot was dropped during cleanup; no replay marker.
        assert_eq!(*db.dropped_slots.lock().unwrap(), vec!["node1".to_string()]);
        assert!(!replay
            .has("recovery_1st_stage", "primary=0,standby=1")
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn recovery_first_stage_configure_standby_failure_drops_slot() {
        let (s, db, _peers, _maint, _replay, standby, _pcp, _sd, _standby, _inflight) =
            make_recovery_setup();
        standby
            .configure_standby_fails
            .store(true, Ordering::SeqCst);

        let err = s
            .recovery_first_stage(Request::new(recovery_req(0, 1)))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Internal);
        assert!(err.message().contains("configure_standby"));
        assert_eq!(*db.dropped_slots.lock().unwrap(), vec!["node1".to_string()]);
    }

    #[tokio::test]
    async fn recovery_first_stage_drop_slot_failure_queues_maintenance() {
        let (s, db, _peers, maint, _replay, standby, _pcp, _sd, _standby, _inflight) =
            make_recovery_setup();
        standby.basebackup_fails.store(true, Ordering::SeqCst);
        db.drop_slot_fails.store(true, Ordering::SeqCst);

        let err = s
            .recovery_first_stage(Request::new(recovery_req(0, 1)))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Internal);

        // drop_slot was attempted (and failed) → intent appended.
        assert_eq!(*db.dropped_slots.lock().unwrap(), vec!["node1".to_string()]);
        let intents = maint.intents.lock().unwrap();
        assert_eq!(intents.len(), 1);
        match &intents[0].payload {
            MaintenancePayload::DropSlotCleanup { cause, .. } => {
                assert!(cause.contains("basebackup_failed"));
            }
        }
    }

    // ----- cluster_recover -----------------------------------------------

    #[tokio::test]
    async fn cluster_recover_delegates_to_recovery_first_stage() {
        let (s, db, _peers, _maint, _replay, standby, pcp, _sd, _standby, _inflight) =
            make_recovery_setup();
        let resp = s
            .cluster_recover(Request::new(ClusterRecoverRequest {
                target_node_id: 1,
                stop_target_pg: false,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.ok);
        assert!(resp.message.contains("recovery complete"));
        // Same downstream effects as a direct recovery_first_stage call.
        assert_eq!(db.checkpoint_calls.load(Ordering::SeqCst), 1);
        assert_eq!(*db.created_slots.lock().unwrap(), vec!["node1".to_string()]);
        assert_eq!(standby.basebackup_calls.load(Ordering::SeqCst), 1);
        assert_eq!(standby.configure_standby_calls.load(Ordering::SeqCst), 1);
        // Post-recovery: PG started on target, pgpool started on target,
        // node attached in pgpool.
        assert_eq!(standby.start_calls.load(Ordering::SeqCst), 1);
        assert_eq!(standby.start_pgpool_calls.load(Ordering::SeqCst), 1);
        assert_eq!(*pcp.attach_calls.lock().unwrap(), vec![1]);
        // Message echoes the post-recovery progress.
        assert!(resp.message.contains("postgres started"));
        assert!(resp.message.contains("pgpool started"));
        assert!(resp.message.contains("attached node 1"));
    }

    #[tokio::test]
    async fn cluster_recover_continues_when_post_steps_fail() {
        // recovery_first_stage succeeds; peer.start fails; pgpool start
        // fails; pcp attach fails. We must still return ok=true (the
        // recovery itself was destructive enough that rolling back is
        // worse than surfacing partial completion to the operator) and
        // report each failure in the message.
        let (s, _db, _peers, _maint, _replay, standby, pcp, _sd, _standby, _inflight) =
            make_recovery_setup();
        standby.start_fails.store(true, Ordering::SeqCst);
        standby.start_pgpool_fails.store(true, Ordering::SeqCst);
        pcp.attach_fails.store(true, Ordering::SeqCst);

        let resp = s
            .cluster_recover(Request::new(ClusterRecoverRequest {
                target_node_id: 1,
                stop_target_pg: false,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(
            resp.ok,
            "recovery_1st_stage succeeded; post-step failures must not fail the RPC"
        );
        assert!(resp.message.contains("postgres start failed"));
        assert!(resp.message.contains("pgpool start failed"));
        assert!(resp.message.contains("pgpool attach failed"));
        // All three were attempted (best-effort, no short-circuit on failure).
        assert_eq!(standby.start_calls.load(Ordering::SeqCst), 1);
        assert_eq!(standby.start_pgpool_calls.load(Ordering::SeqCst), 1);
        assert_eq!(*pcp.attach_calls.lock().unwrap(), vec![1]);
    }

    #[tokio::test]
    async fn cluster_recover_skips_post_steps_when_recovery_fails() {
        // recovery_first_stage hits basebackup failure → propagates Err.
        // Post-recovery steps must NOT run — there's nothing to start.
        let (s, _db, _peers, _maint, _replay, standby, pcp, _sd, _standby, _inflight) =
            make_recovery_setup();
        standby.basebackup_fails.store(true, Ordering::SeqCst);
        let err = s
            .cluster_recover(Request::new(ClusterRecoverRequest {
                target_node_id: 1,
                stop_target_pg: false,
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Internal);
        assert_eq!(standby.start_calls.load(Ordering::SeqCst), 0);
        assert_eq!(standby.start_pgpool_calls.load(Ordering::SeqCst), 0);
        assert!(pcp.attach_calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn cluster_recover_refuses_when_local_is_replica() {
        let (s, db, _peers, _maint, _replay, standby, _pcp, _sd, _standby, _inflight) =
            make_recovery_setup();
        db.in_recovery.store(true, Ordering::SeqCst);
        let resp = s
            .cluster_recover(Request::new(ClusterRecoverRequest {
                target_node_id: 1,
                stop_target_pg: false,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(!resp.ok);
        assert!(resp.message.contains("not the primary"));
        // Nothing downstream touched.
        assert_eq!(db.checkpoint_calls.load(Ordering::SeqCst), 0);
        assert_eq!(standby.basebackup_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn cluster_recover_rejects_unknown_target_id() {
        let (s, ..) = make_recovery_setup();
        let err = s
            .cluster_recover(Request::new(ClusterRecoverRequest {
                target_node_id: 99,
                stop_target_pg: false,
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("target node 99"));
    }

    #[tokio::test]
    async fn cluster_recover_rejects_local_node_as_target() {
        // Local is node 0; asking to reclone it from itself is nonsense.
        let (s, db, _peers, _maint, _replay, standby, _pcp, _sd, _standby, _inflight) =
            make_recovery_setup();
        let err = s
            .cluster_recover(Request::new(ClusterRecoverRequest {
                target_node_id: 0,
                stop_target_pg: false,
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("target is the local node"));
        // No work happened.
        assert_eq!(db.checkpoint_calls.load(Ordering::SeqCst), 0);
        assert_eq!(standby.basebackup_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn cluster_recover_refuses_when_target_pg_running_without_flag() {
        // Operator forgot --stop-target-pg; target reports PG running.
        // We must refuse cleanly here, not let recovery_first_stage run
        // headlong into the deeper basebackup safety check.
        let (s, db, _peers, _maint, _replay, standby, _pcp, _sd, _standby, _inflight) =
            make_recovery_setup();
        standby.mark_running(); // is_postgres_running=true on the peer's get_status
        let resp = s
            .cluster_recover(Request::new(ClusterRecoverRequest {
                target_node_id: 1,
                stop_target_pg: false,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(!resp.ok);
        assert!(
            resp.message.contains("postgres is running on target"),
            "unexpected: {}",
            resp.message
        );
        assert!(resp.message.contains("--stop-target-pg"));
        // No downstream work — we didn't even checkpoint.
        assert_eq!(db.checkpoint_calls.load(Ordering::SeqCst), 0);
        assert_eq!(standby.basebackup_calls.load(Ordering::SeqCst), 0);
        assert_eq!(standby.stop_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn cluster_recover_stops_target_pg_when_flag_set() {
        let (s, db, _peers, _maint, _replay, standby, _pcp, _sd, _standby, _inflight) =
            make_recovery_setup();
        standby.mark_running();
        let resp = s
            .cluster_recover(Request::new(ClusterRecoverRequest {
                target_node_id: 1,
                stop_target_pg: true,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.ok, "{}", resp.message);
        // peer.stop fired exactly once; then recovery_first_stage ran.
        assert_eq!(standby.stop_calls.load(Ordering::SeqCst), 1);
        assert_eq!(db.checkpoint_calls.load(Ordering::SeqCst), 1);
        assert_eq!(standby.basebackup_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn cluster_recover_skips_stop_when_target_pg_already_stopped() {
        // Flag set, but target already reports PG stopped → no peer.stop
        // (idempotent, but skipping avoids the systemd D-Bus round-trip
        // for no reason).
        let (s, _db, _peers, _maint, _replay, standby, _pcp, _sd, _standby, _inflight) =
            make_recovery_setup();
        // standby.is_running default false; do not mark_running().
        let resp = s
            .cluster_recover(Request::new(ClusterRecoverRequest {
                target_node_id: 1,
                stop_target_pg: true,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.ok, "{}", resp.message);
        assert_eq!(standby.stop_calls.load(Ordering::SeqCst), 0);
    }

    /// cluster_recover bypasses a stale replay marker on
    /// recovery_first_stage. Without this, a 24h-old marker silently
    /// short-circuited the recover and the operator got back
    /// "recovery complete" with no work actually done — observed live
    /// on db2 on 2026-06-12.
    #[tokio::test]
    async fn cluster_recover_bypasses_stale_replay_marker() {
        let (s, db, _peers, _maint, replay, standby, _pcp, _sd, _standby, _inflight) =
            make_recovery_setup();
        // Plant a marker that would short-circuit recovery_first_stage.
        replay.mark("recovery_1st_stage", "primary=0,standby=1");
        let resp = s
            .cluster_recover(Request::new(ClusterRecoverRequest {
                target_node_id: 1,
                stop_target_pg: false,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.ok, "{}", resp.message);
        // Basebackup actually ran — the wrapper set bypass_replay_marker.
        assert_eq!(db.checkpoint_calls.load(Ordering::SeqCst), 1);
        assert_eq!(standby.basebackup_calls.load(Ordering::SeqCst), 1);
        // And the response is NOT the silent-skip message.
        assert!(
            !resp.message.contains("skipped via replay marker"),
            "got: {}",
            resp.message
        );
    }

    // ----- cluster_handoff -----------------------------------------------

    #[tokio::test]
    async fn cluster_handoff_happy_path() {
        let (s, db, _peers, _maint, _replay, peer, pcp, sd, standby, inflight) =
            make_recovery_setup();
        // Both sides synced at BASE_LSN → lag = 0; gate passes.
        db.current_wal_lsn.store(BASE_LSN, Ordering::SeqCst);
        peer.mark_standby().set_lag(0).set_replay_lsn(BASE_LSN);
        let resp = s
            .cluster_handoff(Request::new(ClusterHandoffRequest {
                target_node_id: 1,
                allow_lag: false,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.ok, "{}", resp.message);
        assert!(resp.message.contains("handoff complete"));
        // Promote target, create slot on target, stop local PG, rewind,
        // write recovery conf, start local PG, attach in pgpool.
        assert_eq!(peer.promote_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            *peer.create_slot_calls.lock().unwrap(),
            vec!["node0".to_string()]
        );
        assert_eq!(sd.stop_postgres_calls.load(Ordering::SeqCst), 1);
        assert_eq!(standby.rewind_calls.load(Ordering::SeqCst), 1);
        // Rewind succeeded → basebackup NOT called.
        assert_eq!(standby.basebackup_calls.load(Ordering::SeqCst), 0);
        assert_eq!(standby.write_recovery_conf_calls.load(Ordering::SeqCst), 1);
        assert_eq!(sd.start_postgres_calls.load(Ordering::SeqCst), 1);
        assert_eq!(*pcp.attach_calls.lock().unwrap(), vec![0]);
        // Journal: op was begun + transitioned through phases + completed.
        let op = inflight
            .find("handoff", "from=0,to=1")
            .await
            .unwrap()
            .expect("inflight op should exist after handoff");
        assert_eq!(op.status, crate::inflight_ops::InflightStatus::Done);
        assert_eq!(op.phase, "done");
    }

    #[tokio::test]
    async fn cluster_handoff_refuses_when_local_is_standby() {
        let (s, db, _peers, _maint, _replay, _peer, _pcp, sd, standby, _inflight) =
            make_recovery_setup();
        db.in_recovery.store(true, Ordering::SeqCst);
        let resp = s
            .cluster_handoff(Request::new(ClusterHandoffRequest {
                target_node_id: 1,
                allow_lag: false,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(!resp.ok);
        assert!(
            resp.message.contains("not the primary"),
            "unexpected: {}",
            resp.message
        );
        // Nothing happened locally.
        assert_eq!(sd.stop_postgres_calls.load(Ordering::SeqCst), 0);
        assert_eq!(standby.rewind_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn cluster_handoff_rejects_local_node_as_target() {
        let (s, _db, _peers, _maint, _replay, _peer, _pcp, _sd, _standby, _inflight) =
            make_recovery_setup();
        let err = s
            .cluster_handoff(Request::new(ClusterHandoffRequest {
                target_node_id: 0,
                allow_lag: false,
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("target is the local node"));
    }

    #[tokio::test]
    async fn cluster_handoff_rejects_unknown_target_id() {
        let (s, ..) = make_recovery_setup();
        let err = s
            .cluster_handoff(Request::new(ClusterHandoffRequest {
                target_node_id: 99,
                allow_lag: false,
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("target node 99"));
    }

    #[tokio::test]
    async fn cluster_handoff_refuses_when_target_pg_not_running() {
        // peer.is_running defaults to false → is_postgres_running=false.
        let (s, _db, _peers, _maint, _replay, peer, _pcp, sd, _standby, _inflight) =
            make_recovery_setup();
        // Explicitly DO NOT mark_running.
        let resp = s
            .cluster_handoff(Request::new(ClusterHandoffRequest {
                target_node_id: 1,
                allow_lag: false,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(!resp.ok);
        assert!(resp.message.contains("postgres stopped"));
        assert_eq!(peer.promote_calls.load(Ordering::SeqCst), 0);
        assert_eq!(sd.stop_postgres_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn cluster_handoff_refuses_when_target_is_already_primary() {
        // peer reports is_postgres_running=true && is_in_recovery=false.
        let (s, _db, _peers, _maint, _replay, peer, _pcp, sd, _standby, _inflight) =
            make_recovery_setup();
        peer.mark_running(); // running=true, in_recovery stays false
        let resp = s
            .cluster_handoff(Request::new(ClusterHandoffRequest {
                target_node_id: 1,
                allow_lag: false,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(!resp.ok);
        assert!(resp.message.contains("not a standby"));
        assert_eq!(peer.promote_calls.load(Ordering::SeqCst), 0);
        assert_eq!(sd.stop_postgres_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn cluster_handoff_refuses_when_lag_exceeds_threshold() {
        let (s, db, _peers, _maint, _replay, peer, _pcp, sd, _standby, _inflight) =
            make_recovery_setup();
        // Local primary is 16 MiB + 1 ahead of target's replay LSN.
        let max = crate::config::MAX_HANDOFF_LAG_BYTES as u64;
        db.current_wal_lsn
            .store(BASE_LSN + max + 1, Ordering::SeqCst);
        peer.mark_standby().set_replay_lsn(BASE_LSN);
        let resp = s
            .cluster_handoff(Request::new(ClusterHandoffRequest {
                target_node_id: 1,
                allow_lag: false,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(!resp.ok);
        assert!(
            resp.message.contains("--allow-lag"),
            "expected --allow-lag hint, got: {}",
            resp.message
        );
        assert!(
            resp.message.contains("bytes behind local primary"),
            "expected accurate primary→standby framing, got: {}",
            resp.message
        );
        assert_eq!(sd.stop_postgres_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn cluster_handoff_catches_silent_lag_when_receive_lsn_is_stale() {
        // The motivating bug: a standby whose WAL receiver has been
        // disconnected reports replication_lag_bytes=0 (replay caught
        // up to what was received) but is missing every primary write
        // since the disconnect. The OLD check (which used
        // replication_lag_bytes) would have accepted the handoff and
        // silently lost data. The NEW check compares
        // `local.current_wal_lsn` to `target.current_wal_lsn`
        // (=last_replay_lsn on a standby), so it sees the truth.
        let (s, db, _peers, _maint, _replay, peer, _pcp, _sd, _standby, _inflight) =
            make_recovery_setup();
        let max = crate::config::MAX_HANDOFF_LAG_BYTES as u64;
        // Primary has written far ahead.
        db.current_wal_lsn
            .store(BASE_LSN + max + 1024, Ordering::SeqCst);
        // Target reports zero replication_lag_bytes (receiver was
        // disconnected; replay caught up to its stale receive_lsn) but
        // its actual replay LSN is far behind local.
        peer.mark_standby().set_lag(0).set_replay_lsn(BASE_LSN);
        let resp = s
            .cluster_handoff(Request::new(ClusterHandoffRequest {
                target_node_id: 1,
                allow_lag: false,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(!resp.ok, "old behaviour would have accepted this silently");
        assert!(
            resp.message.contains("bytes behind local primary"),
            "expected the new framing in the refusal: {}",
            resp.message
        );
    }

    #[tokio::test]
    async fn cluster_handoff_refuses_when_lsn_probe_unknown() {
        // local.current_wal_lsn=0 OR target.current_wal_lsn=0 means
        // the LSN data isn't trustworthy — refuse rather than guess.
        let (s, _db, _peers, _maint, _replay, peer, _pcp, _sd, _standby, _inflight) =
            make_recovery_setup();
        peer.mark_standby(); // do NOT set_replay_lsn
        let resp = s
            .cluster_handoff(Request::new(ClusterHandoffRequest {
                target_node_id: 1,
                allow_lag: false,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(!resp.ok);
        assert!(
            resp.message.contains("cannot measure lag"),
            "expected 'cannot measure lag' refusal, got: {}",
            resp.message
        );
        assert!(resp.message.contains("--allow-lag"));
    }

    #[tokio::test]
    async fn cluster_handoff_unknown_lsns_with_allow_lag_proceed() {
        // Operator's "I know what I'm doing" escape hatch: even with
        // both LSNs unknown, --allow-lag lets the handoff proceed.
        let (s, _db, _peers, _maint, _replay, peer, _pcp, sd, _standby, _inflight) =
            make_recovery_setup();
        peer.mark_standby();
        let resp = s
            .cluster_handoff(Request::new(ClusterHandoffRequest {
                target_node_id: 1,
                allow_lag: true,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.ok, "{}", resp.message);
        assert_eq!(sd.stop_postgres_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn cluster_handoff_proceeds_with_high_lag_when_flag_set() {
        let (s, db, _peers, _maint, _replay, peer, _pcp, sd, _standby, _inflight) =
            make_recovery_setup();
        let max = crate::config::MAX_HANDOFF_LAG_BYTES as u64;
        db.current_wal_lsn
            .store(BASE_LSN + max + 1, Ordering::SeqCst);
        peer.mark_standby().set_replay_lsn(BASE_LSN);
        let resp = s
            .cluster_handoff(Request::new(ClusterHandoffRequest {
                target_node_id: 1,
                allow_lag: true,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.ok, "{}", resp.message);
        assert_eq!(peer.promote_calls.load(Ordering::SeqCst), 1);
        assert_eq!(sd.stop_postgres_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn cluster_handoff_falls_back_to_basebackup_when_rewind_fails() {
        let (s, db, _peers, _maint, _replay, peer, _pcp, _sd, standby, _inflight) =
            make_recovery_setup();
        db.current_wal_lsn.store(BASE_LSN, Ordering::SeqCst);
        peer.mark_standby().set_replay_lsn(BASE_LSN);
        standby.rewind_fails.store(true, Ordering::SeqCst);
        let resp = s
            .cluster_handoff(Request::new(ClusterHandoffRequest {
                target_node_id: 1,
                allow_lag: false,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.ok, "{}", resp.message);
        assert_eq!(standby.rewind_calls.load(Ordering::SeqCst), 1);
        assert_eq!(standby.basebackup_calls.load(Ordering::SeqCst), 1);
        assert!(resp.message.contains("basebackup"));
    }

    #[tokio::test]
    async fn cluster_handoff_drops_slot_when_both_data_copies_fail() {
        let (s, db, _peers, _maint, _replay, peer, _pcp, _sd, standby, _inflight) =
            make_recovery_setup();
        db.current_wal_lsn.store(BASE_LSN, Ordering::SeqCst);
        peer.mark_standby().set_replay_lsn(BASE_LSN);
        standby.rewind_fails.store(true, Ordering::SeqCst);
        standby.basebackup_fails.store(true, Ordering::SeqCst);
        let resp = s
            .cluster_handoff(Request::new(ClusterHandoffRequest {
                target_node_id: 1,
                allow_lag: false,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(!resp.ok);
        // Slot was created on target then dropped during cleanup.
        assert_eq!(
            *peer.create_slot_calls.lock().unwrap(),
            vec!["node0".to_string()]
        );
        assert_eq!(
            *peer.drop_slot_calls.lock().unwrap(),
            vec!["node0".to_string()]
        );
        assert!(
            resp.message.contains("cluster recover"),
            "actionable retry hint missing: {}",
            resp.message
        );
    }

    #[tokio::test]
    async fn cluster_handoff_is_idempotent_via_inflight_done() {
        // Pre-seed a Done op for (from=0,to=1) — the preflight should
        // detect it and short-circuit with "already processed".
        let (s, _db, _peers, _maint, _replay, peer, _pcp, sd, _standby, inflight) =
            make_recovery_setup();
        peer.mark_standby();
        let now = chrono::Utc::now();
        inflight.seed(crate::inflight_ops::InflightOp {
            id: "preseeded".into(),
            status: crate::inflight_ops::InflightStatus::Done,
            payload: crate::inflight_ops::InflightPayload::Handoff {
                from_node_id: 0,
                to_node_id: 1,
                to_hostname: "peer1.local".into(),
                slot_name: "node0".into(),
                allow_lag: false,
            },
            phase: "done".into(),
            started_at: now,
            updated_at: now,
            completed_at: Some(now),
            discharged_at: None,
            last_error: None,
        });
        let resp = s
            .cluster_handoff(Request::new(ClusterHandoffRequest {
                target_node_id: 1,
                allow_lag: false,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.ok);
        assert!(resp.message.contains("already processed"));
        // No-op: nothing called.
        assert_eq!(peer.promote_calls.load(Ordering::SeqCst), 0);
        assert_eq!(sd.stop_postgres_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn cluster_handoff_refuses_when_an_inflight_handoff_exists() {
        // Pre-seed an InProgress op — preflight must return ok=false
        // with a "resume or abandon" message, NOT start a parallel
        // handoff.
        let (s, _db, _peers, _maint, _replay, peer, _pcp, sd, _standby, inflight) =
            make_recovery_setup();
        peer.mark_standby();
        let now = chrono::Utc::now();
        inflight.seed(crate::inflight_ops::InflightOp {
            id: "stuck-handoff".into(),
            status: crate::inflight_ops::InflightStatus::InProgress,
            payload: crate::inflight_ops::InflightPayload::Handoff {
                from_node_id: 0,
                to_node_id: 1,
                to_hostname: "peer1.local".into(),
                slot_name: "node0".into(),
                allow_lag: false,
            },
            phase: "target_promoted".into(),
            started_at: now,
            updated_at: now,
            completed_at: None,
            discharged_at: None,
            last_error: None,
        });
        let resp = s
            .cluster_handoff(Request::new(ClusterHandoffRequest {
                target_node_id: 1,
                allow_lag: false,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(!resp.ok);
        assert!(
            resp.message.contains("already in flight"),
            "unexpected: {}",
            resp.message
        );
        assert!(resp.message.contains("stuck-handoff"));
        assert!(resp.message.contains("ops resume"));
        // No destructive work happened.
        assert_eq!(peer.promote_calls.load(Ordering::SeqCst), 0);
        assert_eq!(sd.stop_postgres_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn cluster_handoff_attach_failure_does_not_fail_rpc() {
        let (s, db, _peers, _maint, _replay, peer, pcp, _sd, _standby, _inflight) =
            make_recovery_setup();
        db.current_wal_lsn.store(BASE_LSN, Ordering::SeqCst);
        peer.mark_standby().set_replay_lsn(BASE_LSN);
        pcp.attach_fails.store(true, Ordering::SeqCst);
        let resp = s
            .cluster_handoff(Request::new(ClusterHandoffRequest {
                target_node_id: 1,
                allow_lag: false,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.ok, "{}", resp.message);
        assert!(resp.message.contains("attach failed"));
    }

    // ----- inflight ops RPCs + resume -----------------------------------

    fn seed_handoff_at_phase(
        inflight: &Arc<StubInflight>,
        id: &str,
        phase: &str,
        status: crate::inflight_ops::InflightStatus,
    ) {
        let now = chrono::Utc::now();
        inflight.seed(crate::inflight_ops::InflightOp {
            id: id.into(),
            status,
            payload: crate::inflight_ops::InflightPayload::Handoff {
                from_node_id: 0,
                to_node_id: 1,
                to_hostname: "peer1.local".into(),
                slot_name: "node0".into(),
                allow_lag: false,
            },
            phase: phase.into(),
            started_at: now,
            updated_at: now,
            completed_at: if status == crate::inflight_ops::InflightStatus::InProgress {
                None
            } else {
                Some(now)
            },
            discharged_at: None,
            last_error: None,
        });
    }

    #[tokio::test]
    async fn list_inflight_ops_returns_seeded_op_and_filters_by_status() {
        let (s, _db, _peers, _maint, _replay, _peer, _pcp, _sd, _standby, inflight) =
            make_recovery_setup();
        seed_handoff_at_phase(
            &inflight,
            "alpha",
            "slot_created",
            crate::inflight_ops::InflightStatus::InProgress,
        );
        seed_handoff_at_phase(
            &inflight,
            "bravo",
            "done",
            crate::inflight_ops::InflightStatus::Done,
        );

        let all = s
            .list_inflight_ops(Request::new(ListInflightOpsRequest { statuses: vec![] }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(all.ops.len(), 2);

        let only_in_progress = s
            .list_inflight_ops(Request::new(ListInflightOpsRequest {
                statuses: vec!["in_progress".into()],
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(only_in_progress.ops.len(), 1);
        assert_eq!(only_in_progress.ops[0].id, "alpha");
        assert_eq!(only_in_progress.ops[0].phase, "slot_created");
        assert_eq!(only_in_progress.ops[0].op, "handoff");
    }

    #[tokio::test]
    async fn get_inflight_op_returns_full_payload() {
        let (s, _db, _peers, _maint, _replay, _peer, _pcp, _sd, _standby, inflight) =
            make_recovery_setup();
        seed_handoff_at_phase(
            &inflight,
            "abc",
            "local_stopped",
            crate::inflight_ops::InflightStatus::InProgress,
        );
        let op = s
            .get_inflight_op(Request::new(GetInflightOpRequest { id: "abc".into() }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(op.id, "abc");
        assert_eq!(op.phase, "local_stopped");
        assert!(!op.payload.is_empty(), "payload should be JSON-encoded");
    }

    #[tokio::test]
    async fn abandon_inflight_op_marks_terminal_and_includes_reason() {
        let (s, _db, _peers, _maint, _replay, _peer, _pcp, _sd, _standby, inflight) =
            make_recovery_setup();
        seed_handoff_at_phase(
            &inflight,
            "abc",
            "local_stopped",
            crate::inflight_ops::InflightStatus::InProgress,
        );
        let resp = s
            .abandon_inflight_op(Request::new(AbandonInflightOpRequest {
                id: "abc".into(),
                reason: "operator triage decided to roll back".into(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.ok, "{}", resp.message);
        assert!(resp.message.contains("abandoned"));
        let op = inflight.get("abc").await.unwrap();
        assert_eq!(op.status, crate::inflight_ops::InflightStatus::Abandoned);
        assert_eq!(
            op.last_error.as_deref(),
            Some("operator triage decided to roll back")
        );
    }

    #[tokio::test]
    async fn abandon_inflight_op_refuses_terminal_op() {
        let (s, _db, _peers, _maint, _replay, _peer, _pcp, _sd, _standby, inflight) =
            make_recovery_setup();
        seed_handoff_at_phase(
            &inflight,
            "abc",
            "done",
            crate::inflight_ops::InflightStatus::Done,
        );
        let resp = s
            .abandon_inflight_op(Request::new(AbandonInflightOpRequest {
                id: "abc".into(),
                reason: String::new(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(!resp.ok);
        assert!(resp.message.contains("only in-progress"));
    }

    #[tokio::test]
    async fn resume_inflight_op_continues_handoff_from_slot_created() {
        // Seed a handoff at phase=slot_created. Resume should pick up
        // from stop_postgres onward and reach completion.
        let (s, _db, _peers, _maint, _replay, peer, pcp, sd, standby, inflight) =
            make_recovery_setup();
        peer.mark_running(); // peer has already been promoted
        seed_handoff_at_phase(
            &inflight,
            "resume-me",
            "slot_created",
            crate::inflight_ops::InflightStatus::InProgress,
        );
        let resp = s
            .resume_inflight_op(Request::new(ResumeInflightOpRequest {
                id: "resume-me".into(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.ok, "{}", resp.message);
        // Promote was NOT called (we resumed past it).
        assert_eq!(peer.promote_calls.load(Ordering::SeqCst), 0);
        // create_slot was NOT called (we resumed past it).
        assert!(peer.create_slot_calls.lock().unwrap().is_empty());
        // stop, rewind, write_recovery_conf, start, attach were called.
        assert_eq!(sd.stop_postgres_calls.load(Ordering::SeqCst), 1);
        assert_eq!(standby.rewind_calls.load(Ordering::SeqCst), 1);
        assert_eq!(standby.write_recovery_conf_calls.load(Ordering::SeqCst), 1);
        assert_eq!(sd.start_postgres_calls.load(Ordering::SeqCst), 1);
        assert_eq!(*pcp.attach_calls.lock().unwrap(), vec![0]);
        // Journal completed.
        let op = inflight.get("resume-me").await.unwrap();
        assert_eq!(op.status, crate::inflight_ops::InflightStatus::Done);
    }

    #[tokio::test]
    async fn resume_inflight_op_refuses_when_cluster_state_diverged() {
        // Seed at target_promoted but the peer reports is_in_recovery=true
        // (target is somehow back to standby). Verify should refuse.
        let (s, _db, _peers, _maint, _replay, peer, _pcp, sd, _standby, inflight) =
            make_recovery_setup();
        peer.mark_standby(); // running but in_recovery=true
        seed_handoff_at_phase(
            &inflight,
            "diverged",
            "target_promoted",
            crate::inflight_ops::InflightStatus::InProgress,
        );
        let resp = s
            .resume_inflight_op(Request::new(ResumeInflightOpRequest {
                id: "diverged".into(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(!resp.ok);
        assert!(
            resp.message.contains("diverged from recorded phase"),
            "unexpected: {}",
            resp.message
        );
        // No destructive work.
        assert_eq!(sd.stop_postgres_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn resume_inflight_op_refuses_terminal_op() {
        let (s, _db, _peers, _maint, _replay, _peer, _pcp, _sd, _standby, inflight) =
            make_recovery_setup();
        seed_handoff_at_phase(
            &inflight,
            "already-done",
            "done",
            crate::inflight_ops::InflightStatus::Done,
        );
        let resp = s
            .resume_inflight_op(Request::new(ResumeInflightOpRequest {
                id: "already-done".into(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(!resp.ok);
        assert!(resp.message.contains("only in-progress"));
    }

    // ----- restore_wal ---------------------------------------------------

    /// Three-node pool: local + peer1 + peer2 — supports fan-out tests.
    fn make_pool_3() -> NodePool {
        NodePool {
            members: vec![
                NodeConfig {
                    id: 0,
                    hostname: "local".into(),
                },
                NodeConfig {
                    id: 1,
                    hostname: "peer1.local".into(),
                },
                NodeConfig {
                    id: 2,
                    hostname: "peer2.local".into(),
                },
            ],
            local_node_id: 0,
        }
    }

    #[allow(clippy::type_complexity)]
    fn make_server_3() -> (
        LocalServer,
        Arc<StubDb>,
        Arc<StubPeers>,
        Arc<StubMaint>,
        Arc<StubWal>,
    ) {
        let db = Arc::new(StubDb::default());
        let peers = Arc::new(StubPeers::default());
        let maint = Arc::new(StubMaint::default());
        let wal = Arc::new(StubWal::default());
        let replay = Arc::new(StubReplay::default());
        let pcp = Arc::new(StubPcp::default());
        let sd = Arc::new(StubSd::default());
        let standby = Arc::new(StubStandby::default());
        let inflight = Arc::new(StubInflight::default());
        let server = LocalServer::new(
            Arc::new(FakeNodeInfo),
            db.clone(),
            peers.clone(),
            maint.clone(),
            wal.clone(),
            replay,
            inflight,
            pcp,
            sd,
            standby,
            make_pool_3(),
            make_pg(),
        );
        (server, db, peers, maint, wal)
    }

    fn valid_restore_req() -> RestoreWalRequest {
        RestoreWalRequest {
            wal_file: "000000010000000000000001".into(),
            dest_path: "/var/lib/postgresql/17/main/pg_wal/000000010000000000000001".into(),
        }
    }

    #[tokio::test]
    async fn restore_wal_rejects_empty_wal_file() {
        let (s, ..) = make_server();
        let err = s
            .restore_wal(Request::new(RestoreWalRequest {
                wal_file: String::new(),
                dest_path: "/d".into(),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn restore_wal_rejects_empty_dest_path() {
        let (s, ..) = make_server();
        let err = s
            .restore_wal(Request::new(RestoreWalRequest {
                wal_file: "wal".into(),
                dest_path: String::new(),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn restore_wal_fetches_from_first_peer_with_segment() {
        let (s, _db, peers, _maint, wal, _replay, _pcp, _sd, _standby, _inflight) = make_server();
        // Default client has the segment.
        let content = b"WAL_BYTES".to_vec();
        peers
            .default_client
            .stage_wal("000000010000000000000001", content.clone());

        let resp = s
            .restore_wal(Request::new(valid_restore_req()))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.ok);
        assert!(resp.message.contains("peer1.local"));

        let written = wal.written.lock().unwrap();
        assert_eq!(written.len(), 1);
        assert_eq!(written[0].1, content);
    }

    #[tokio::test]
    async fn restore_wal_skips_to_next_peer_when_segment_missing() {
        let (s, _db, peers, _maint, wal) = make_server_3();
        // peer1 (default) has nothing staged. peer2 has the segment.
        let peer2_client = Arc::new(StubPeerClient::default());
        peer2_client.stage_wal("000000010000000000000001", b"GOT_IT".to_vec());
        peers.override_client(2, peer2_client);

        let resp = s
            .restore_wal(Request::new(valid_restore_req()))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.ok);
        assert!(resp.message.contains("peer2.local"));
        assert_eq!(wal.written.lock().unwrap()[0].1, b"GOT_IT");
    }

    #[tokio::test]
    async fn restore_wal_skips_unreachable_peer() {
        let (s, _db, peers, _maint, _wal) = make_server_3();
        peers.mark_unreachable(1);
        let peer2_client = Arc::new(StubPeerClient::default());
        peer2_client.stage_wal("000000010000000000000001", b"GOT_IT".to_vec());
        peers.override_client(2, peer2_client);

        let resp = s
            .restore_wal(Request::new(valid_restore_req()))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.ok);
        assert!(resp.message.contains("peer2.local"));
    }

    #[tokio::test]
    async fn restore_wal_returns_not_found_when_no_peer_has_segment() {
        let (s, _db, _peers, _maint, _wal) = make_server_3();
        // Default + no overrides → nobody has it staged.
        let err = s
            .restore_wal(Request::new(valid_restore_req()))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::NotFound);
        assert!(err.message().contains("not found on any peer"));
    }

    #[tokio::test]
    async fn restore_wal_dest_outside_pgdata_is_fatal() {
        let (s, _db, peers, _maint, wal, _replay, _pcp, _sd, _standby, _inflight) = make_server();
        peers
            .default_client
            .stage_wal("000000010000000000000001", b"_".to_vec());
        wal.dest_outside_pgdata.store(true, Ordering::SeqCst);

        let err = s
            .restore_wal(Request::new(valid_restore_req()))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("outside pg_data_dir"));
    }

    #[tokio::test]
    async fn restore_wal_propagates_peer_rpc_error_as_try_next() {
        let (s, _db, peers, _maint, _wal) = make_server_3();
        // peer1 errors on fetch_wal; peer2 has the segment.
        peers
            .default_client
            .fetch_wal_errors
            .store(true, Ordering::SeqCst);
        let peer2_client = Arc::new(StubPeerClient::default());
        peer2_client.stage_wal("000000010000000000000001", b"_".to_vec());
        peers.override_client(2, peer2_client);

        let resp = s
            .restore_wal(Request::new(valid_restore_req()))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.ok);
        assert!(resp.message.contains("peer2.local"));
    }

    /// A peer that just failed a fetch is skipped for
    /// RESTORE_WAL_PEER_COOLDOWN. PostgreSQL calls restore_command once
    /// per file back to back — at promotion, several times in a row —
    /// and without the cooldown every invocation re-pays the full
    /// per-peer timeout for the same partitioned peer. Acceptance E2
    /// measured that as a ~40 s promotion stall, wide enough for a
    /// rival to depose the winner (finding 14).
    #[tokio::test]
    async fn restore_wal_cools_down_a_failed_peer_across_invocations() {
        let (s, _db, peers, _maint, _wal) = make_server_3();
        peers
            .default_client
            .fetch_wal_errors
            .store(true, Ordering::SeqCst);
        let peer2_client = Arc::new(StubPeerClient::default());
        peer2_client.stage_wal("000000010000000000000001", b"_".to_vec());
        peers.override_client(2, peer2_client);

        let resp = s
            .restore_wal(Request::new(valid_restore_req()))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.ok);
        let calls_after_first = peers.default_client.fetch_wal_calls.load(Ordering::SeqCst);
        assert_eq!(calls_after_first, 1, "failing peer probed once");

        // Second restore_command invocation, immediately after: the
        // failed peer must be in cooldown and not probed again.
        let resp = s
            .restore_wal(Request::new(valid_restore_req()))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.ok);
        assert_eq!(
            peers.default_client.fetch_wal_calls.load(Ordering::SeqCst),
            calls_after_first,
            "cooldown must skip the peer that just failed"
        );
    }

    // ----- follow_primary -------------------------------------------------

    /// Map a node id from `make_pool()` to the pool's hostname. id=0 →
    /// "local" (this primary). Tests using id=99 trigger the unknown-node
    /// rejection path on purpose.
    fn pool_hostname(id: i32) -> String {
        match id {
            0 => "local".to_string(),
            _ => format!("peer{id}.local"),
        }
    }

    fn follow_primary_req(detached: i32, new_primary: i32) -> FollowPrimaryRequest {
        FollowPrimaryRequest {
            detached: Some(NodeRef {
                id: detached,
                hostname: pool_hostname(detached),
                pg_port: 0,
                pg_data: String::new(),
            }),
            new_primary: Some(NodeRef {
                id: new_primary,
                hostname: pool_hostname(new_primary),
                pg_port: 0,
                pg_data: String::new(),
            }),
            old_main: None,
            old_primary: None,
        }
    }

    /// Three-node setup with peer1 as the "detached" target. peer1's
    /// stub client starts marked as running (the happy-path precondition).
    #[allow(clippy::type_complexity)]
    fn make_follow_setup() -> (
        LocalServer,
        Arc<StubDb>,
        Arc<StubPeers>,
        Arc<StubMaint>,
        Arc<StubReplay>,
        Arc<StubPcp>,
        Arc<StubPeerClient>,
    ) {
        let (s, db, peers, maint, _wal, replay, pcp, _sd, _standby, _inflight) = make_server();
        let detached_client = Arc::new(StubPeerClient::default());
        detached_client.mark_running();
        peers.override_client(1, detached_client.clone());
        (s, db, peers, maint, replay, pcp, detached_client)
    }

    #[tokio::test]
    async fn follow_primary_happy_path_with_rewind() {
        let (s, db, _peers, _maint, replay, pcp, detached) = make_follow_setup();
        let resp = s
            .follow_primary(Request::new(follow_primary_req(1, 0)))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.ok);
        // Sequence: get_status → stop → checkpoint → create_slot →
        // rewind (succeeds) → configure_standby → start → attach_node →
        // mark_done.
        assert_eq!(detached.stop_calls.load(Ordering::SeqCst), 1);
        assert_eq!(db.checkpoint_calls.load(Ordering::SeqCst), 1);
        assert_eq!(*db.created_slots.lock().unwrap(), vec!["node1".to_string()]);
        assert_eq!(detached.rewind_calls.load(Ordering::SeqCst), 1);
        assert_eq!(detached.basebackup_calls.load(Ordering::SeqCst), 0);
        assert_eq!(detached.configure_standby_calls.load(Ordering::SeqCst), 1);
        assert_eq!(detached.start_calls.load(Ordering::SeqCst), 1);
        assert_eq!(*pcp.attach_calls.lock().unwrap(), vec![1]);
        // Replay marker was written.
        assert!(replay
            .has("follow_primary", "detached=1,new_primary=0")
            .await
            .unwrap());
        // No slot drop happened.
        assert!(db.dropped_slots.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn follow_primary_falls_back_to_basebackup_when_rewind_fails() {
        let (s, _db, _peers, _maint, _replay, _pcp, detached) = make_follow_setup();
        detached.rewind_fails.store(true, Ordering::SeqCst);
        let resp = s
            .follow_primary(Request::new(follow_primary_req(1, 0)))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.ok);
        assert_eq!(detached.rewind_calls.load(Ordering::SeqCst), 1);
        assert_eq!(detached.basebackup_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn follow_primary_skips_when_replay_marker_present() {
        let (s, db, _peers, _maint, replay, _pcp, detached) = make_follow_setup();
        replay.mark("follow_primary", "detached=1,new_primary=0");
        let resp = s
            .follow_primary(Request::new(follow_primary_req(1, 0)))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.ok);
        assert!(resp.message.contains("already processed"));
        // Nothing on the peer touched.
        assert_eq!(detached.stop_calls.load(Ordering::SeqCst), 0);
        assert_eq!(db.checkpoint_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn follow_primary_skips_when_detached_not_running() {
        let (s, db, peers, _maint, replay, _pcp, _det) = make_follow_setup();
        // Replace detached client with one that reports not-running.
        let detached_off = Arc::new(StubPeerClient::default()); // is_running defaults to false
        peers.override_client(1, detached_off.clone());

        let resp = s
            .follow_primary(Request::new(follow_primary_req(1, 0)))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.ok);
        assert!(resp.message.contains("stopped"));
        // Past stop / checkpoint untouched; the replay marker is NOT set
        // (operator may bring the node back up later and re-fire the hook).
        assert_eq!(detached_off.stop_calls.load(Ordering::SeqCst), 0);
        assert_eq!(db.checkpoint_calls.load(Ordering::SeqCst), 0);
        assert!(!replay
            .has("follow_primary", "detached=1,new_primary=0")
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn follow_primary_rejects_missing_detached() {
        let (s, ..) = make_server();
        let mut req = follow_primary_req(1, 0);
        req.detached = None;
        let err = s.follow_primary(Request::new(req)).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn follow_primary_rejects_unknown_detached() {
        let (s, ..) = make_server();
        let req = follow_primary_req(99, 0);
        let err = s.follow_primary(Request::new(req)).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn follow_primary_basebackup_failure_drops_slot() {
        let (s, db, _peers, _maint, replay, _pcp, detached) = make_follow_setup();
        detached.rewind_fails.store(true, Ordering::SeqCst);
        detached.basebackup_fails.store(true, Ordering::SeqCst);

        let err = s
            .follow_primary(Request::new(follow_primary_req(1, 0)))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Internal);
        assert!(err.message().contains("basebackup"));

        // Slot was dropped during cleanup.
        assert_eq!(*db.dropped_slots.lock().unwrap(), vec!["node1".to_string()]);
        // No replay marker on failure.
        assert!(!replay
            .has("follow_primary", "detached=1,new_primary=0")
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn follow_primary_start_failure_drops_slot() {
        let (s, db, _peers, _maint, _replay, _pcp, detached) = make_follow_setup();
        detached.start_fails.store(true, Ordering::SeqCst);

        let err = s
            .follow_primary(Request::new(follow_primary_req(1, 0)))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Internal);
        assert!(err.message().contains("start"));
        assert_eq!(*db.dropped_slots.lock().unwrap(), vec!["node1".to_string()]);
    }

    #[tokio::test]
    async fn follow_primary_attach_failure_does_not_drop_slot() {
        let (s, db, _peers, _maint, replay, pcp, _det) = make_follow_setup();
        pcp.attach_fails.store(true, Ordering::SeqCst);

        let err = s
            .follow_primary(Request::new(follow_primary_req(1, 0)))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Internal);
        assert!(err.message().contains("pcp_attach_node"));
        // Slot stays — the now-running standby is using it.
        assert!(db.dropped_slots.lock().unwrap().is_empty());
        // No replay marker either; pgpool may retry attach later.
        assert!(!replay
            .has("follow_primary", "detached=1,new_primary=0")
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn follow_primary_drop_slot_failure_queues_maintenance() {
        let (s, db, _peers, maint, _replay, _pcp, detached) = make_follow_setup();
        // Force basebackup to fail (so we hit cleanup) AND drop_slot
        // itself to fail (so the maintenance fallback kicks in).
        detached.rewind_fails.store(true, Ordering::SeqCst);
        detached.basebackup_fails.store(true, Ordering::SeqCst);
        db.drop_slot_fails.store(true, Ordering::SeqCst);

        let err = s
            .follow_primary(Request::new(follow_primary_req(1, 0)))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Internal);

        // drop_slot was attempted (and failed) — maintenance intent queued.
        assert_eq!(*db.dropped_slots.lock().unwrap(), vec!["node1".to_string()]);
        let intents = maint.intents.lock().unwrap();
        assert_eq!(intents.len(), 1);
        match &intents[0].payload {
            MaintenancePayload::DropSlotCleanup {
                slot_name, cause, ..
            } => {
                assert_eq!(slot_name, "node1");
                assert!(cause.contains("basebackup_failed"));
            }
        }
    }

    // ----- cluster_init ---------------------------------------------------

    /// 3-node pool with peer1 + peer2 as standbys for cluster_init fan-out.
    /// Both standby clients are mounted as overrides so tests can drive
    /// their per-method failure switches independently.
    #[allow(clippy::type_complexity)]
    fn make_cluster_init_setup() -> (
        LocalServer,
        Arc<StubDb>,
        Arc<StubPeers>,
        Arc<StubMaint>,
        Arc<StubPeerClient>,
        Arc<StubPeerClient>,
    ) {
        let db = Arc::new(StubDb::default());
        let peers = Arc::new(StubPeers::default());
        let maint = Arc::new(StubMaint::default());
        let wal = Arc::new(StubWal::default());
        let replay = Arc::new(StubReplay::default());
        let pcp = Arc::new(StubPcp::default());
        let sd = Arc::new(StubSd::default());
        let standby = Arc::new(StubStandby::default());
        let inflight = Arc::new(StubInflight::default());
        let server = LocalServer::new(
            Arc::new(FakeNodeInfo),
            db.clone(),
            peers.clone(),
            maint.clone(),
            wal,
            replay,
            inflight,
            pcp,
            sd,
            standby,
            make_pool_3(),
            make_pg(),
        );
        let peer1 = Arc::new(StubPeerClient::default());
        let peer2 = Arc::new(StubPeerClient::default());
        peers.override_client(1, peer1.clone());
        peers.override_client(2, peer2.clone());
        (server, db, peers, maint, peer1, peer2)
    }

    #[tokio::test]
    async fn cluster_init_happy_path_initialises_all_standbys() {
        let (s, db, _peers, _maint, peer1, peer2) = make_cluster_init_setup();
        let resp = s
            .cluster_init(Request::new(ClusterInitRequest { only_node_id: None }))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.ok);
        assert_eq!(resp.repl_user, "repl");
        assert_eq!(resp.standbys.len(), 2);
        for r in &resp.standbys {
            assert!(r.ok, "standby {} failed: {}", r.node_id, r.message);
            assert_eq!(r.message, "initialised");
        }
        // Sequence per standby: create_slot → stop → basebackup →
        // configure_standby → start. No pcp_attach.
        assert_eq!(
            *db.created_repl_roles.lock().unwrap(),
            vec!["repl".to_string()]
        );
        let created = db.created_slots.lock().unwrap().clone();
        assert!(created.contains(&"node1".to_string()));
        assert!(created.contains(&"node2".to_string()));
        assert_eq!(peer1.stop_calls.load(Ordering::SeqCst), 1);
        assert_eq!(peer1.basebackup_calls.load(Ordering::SeqCst), 1);
        assert_eq!(peer1.configure_standby_calls.load(Ordering::SeqCst), 1);
        assert_eq!(peer1.start_calls.load(Ordering::SeqCst), 1);
        assert_eq!(peer2.start_calls.load(Ordering::SeqCst), 1);
        // No slot drops.
        assert!(db.dropped_slots.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn cluster_init_refuses_when_local_is_replica() {
        let (s, db, _peers, _maint, peer1, _peer2) = make_cluster_init_setup();
        db.in_recovery.store(true, Ordering::SeqCst);
        let resp = s
            .cluster_init(Request::new(ClusterInitRequest::default()))
            .await
            .unwrap()
            .into_inner();
        assert!(!resp.ok);
        assert!(resp.message.contains("not the primary"));
        assert_eq!(resp.standbys.len(), 0);
        // Nothing touched.
        assert!(db.created_repl_roles.lock().unwrap().is_empty());
        assert_eq!(peer1.stop_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn cluster_init_only_node_id_targets_one_standby() {
        let (s, db, _peers, _maint, peer1, peer2) = make_cluster_init_setup();
        let resp = s
            .cluster_init(Request::new(ClusterInitRequest {
                only_node_id: Some(2),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.ok);
        assert_eq!(resp.standbys.len(), 1);
        assert_eq!(resp.standbys[0].node_id, 2);
        // peer1 untouched, peer2 fully driven.
        assert_eq!(peer1.start_calls.load(Ordering::SeqCst), 0);
        assert_eq!(peer2.start_calls.load(Ordering::SeqCst), 1);
        // Only one slot created.
        assert_eq!(*db.created_slots.lock().unwrap(), vec!["node2".to_string()]);
    }

    #[tokio::test]
    async fn cluster_init_partial_failure_aggregates() {
        let (s, db, _peers, _maint, peer1, peer2) = make_cluster_init_setup();
        // peer2 basebackup fails; peer1 succeeds.
        peer2.basebackup_fails.store(true, Ordering::SeqCst);

        let resp = s
            .cluster_init(Request::new(ClusterInitRequest { only_node_id: None }))
            .await
            .unwrap()
            .into_inner();
        assert!(!resp.ok);
        assert!(resp.message.contains("1 of 2 standby(s) failed"));
        // Order in standbys[] follows pool declaration order.
        let by_id: std::collections::HashMap<i32, &ClusterInitStandbyResult> =
            resp.standbys.iter().map(|r| (r.node_id, r)).collect();
        assert!(by_id[&1].ok, "peer1 should have succeeded");
        assert!(!by_id[&2].ok, "peer2 should have failed");
        assert!(by_id[&2].message.contains("basebackup"));
        // peer2's slot was dropped during cleanup.
        let dropped = db.dropped_slots.lock().unwrap();
        assert!(dropped.contains(&"node2".to_string()));
        // peer1's slot stays.
        assert!(!dropped.contains(&"node1".to_string()));
        // peer1 fully driven.
        assert_eq!(peer1.start_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn cluster_init_create_repl_role_failure_is_fatal() {
        let (s, db, _peers, _maint, _peer1, _peer2) = make_cluster_init_setup();
        db.create_repl_role_fails.store(true, Ordering::SeqCst);

        let err = s
            .cluster_init(Request::new(ClusterInitRequest::default()))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Internal);
        assert!(err.message().contains("create replication role"));
    }

    #[tokio::test]
    async fn cluster_init_stop_failure_drops_slot() {
        let (s, db, _peers, _maint, peer1, _peer2) = make_cluster_init_setup();
        peer1.stop_fails.store(true, Ordering::SeqCst);
        let resp = s
            .cluster_init(Request::new(ClusterInitRequest {
                only_node_id: Some(1),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(!resp.ok);
        assert!(!resp.standbys[0].ok);
        assert!(resp.standbys[0].message.contains("stop"));
        // Slot dropped.
        assert_eq!(*db.dropped_slots.lock().unwrap(), vec!["node1".to_string()]);
        // Downstream not reached.
        assert_eq!(peer1.basebackup_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn cluster_init_drop_slot_cleanup_failure_queues_maintenance() {
        let (s, db, _peers, maint, peer1, _peer2) = make_cluster_init_setup();
        peer1.basebackup_fails.store(true, Ordering::SeqCst);
        db.drop_slot_fails.store(true, Ordering::SeqCst);

        let resp = s
            .cluster_init(Request::new(ClusterInitRequest {
                only_node_id: Some(1),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(!resp.ok);
        // drop_slot was attempted (and failed) → maintenance intent appended.
        assert_eq!(*db.dropped_slots.lock().unwrap(), vec!["node1".to_string()]);
        let intents = maint.intents.lock().unwrap();
        assert_eq!(intents.len(), 1);
        match &intents[0].payload {
            MaintenancePayload::DropSlotCleanup { cause, .. } => {
                assert!(cause.contains("basebackup_failed"));
            }
        }
    }

    #[tokio::test]
    async fn cluster_init_local_only_pool_emits_no_standby_results() {
        // Single-node pool: just the primary, no peers to init.
        let db = Arc::new(StubDb::default());
        let peers = Arc::new(StubPeers::default());
        let maint = Arc::new(StubMaint::default());
        let wal = Arc::new(StubWal::default());
        let replay = Arc::new(StubReplay::default());
        let pcp = Arc::new(StubPcp::default());
        let single_pool = NodePool {
            members: vec![NodeConfig {
                id: 0,
                hostname: "local".into(),
            }],
            local_node_id: 0,
        };
        let sd = Arc::new(StubSd::default());
        let standby = Arc::new(StubStandby::default());
        let inflight = Arc::new(StubInflight::default());
        let s = LocalServer::new(
            Arc::new(FakeNodeInfo),
            db.clone(),
            peers,
            maint,
            wal,
            replay,
            inflight,
            pcp,
            sd,
            standby,
            single_pool,
            make_pg(),
        );
        let resp = s
            .cluster_init(Request::new(ClusterInitRequest::default()))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.ok);
        assert!(resp.message.contains("no standby nodes selected"));
        assert_eq!(resp.standbys.len(), 0);
        // Repl role still ensured.
        assert_eq!(
            *db.created_repl_roles.lock().unwrap(),
            vec!["repl".to_string()]
        );
    }

    #[test]
    fn internal_maps_anyhow_to_internal_status() {
        let err = internal(anyhow::anyhow!("boom"));
        assert_eq!(err.code(), tonic::Code::Internal);
        assert_eq!(err.message(), "boom");
    }

    #[test]
    fn intent_proto_round_trips_payload_shape() {
        let intent = pending_intent("alpha");
        let proto = intent_to_proto(&intent).unwrap();
        assert_eq!(proto.id, "alpha");
        assert_eq!(proto.op, "drop_slot_cleanup");
        assert_eq!(proto.status, "pending");
        // payload is JSON-encoded; parse it back.
        let parsed: serde_json::Value = serde_json::from_slice(&proto.payload).unwrap();
        assert_eq!(parsed["op"], "drop_slot_cleanup");
        assert_eq!(parsed["slot_name"], "node1");
    }

    // ----- follow_primary driver + post-handoff fan-out ---------------

    /// 3-node pool with a shared inflight + pcp + maint stash so the
    /// follow_primary tests can assert post-conditions.
    #[allow(clippy::type_complexity)]
    fn make_fp_fixture() -> (
        LocalServer,
        Arc<StubPeers>,
        Arc<StubPcp>,
        Arc<StubMaint>,
        Arc<StubInflight>,
    ) {
        let db = Arc::new(StubDb::default());
        let peers = Arc::new(StubPeers::default());
        let maint = Arc::new(StubMaint::default());
        let wal = Arc::new(StubWal::default());
        let replay = Arc::new(StubReplay::default());
        let pcp = Arc::new(StubPcp::default());
        let sd = Arc::new(StubSd::default());
        let standby = Arc::new(StubStandby::default());
        let inflight = Arc::new(StubInflight::default());
        let server = LocalServer::new(
            Arc::new(FakeNodeInfo),
            db,
            peers.clone(),
            maint.clone(),
            wal,
            replay,
            inflight.clone(),
            pcp.clone(),
            sd,
            standby,
            make_pool_3(),
            make_pg(),
        );
        (server, peers, pcp, maint, inflight)
    }

    fn fp_detached() -> NodeConfig {
        NodeConfig {
            id: 2,
            hostname: "peer2.local".into(),
        }
    }

    fn fp_new_primary() -> NodeConfig {
        NodeConfig {
            id: 1,
            hostname: "peer1.local".into(),
        }
    }

    /// Driver happy path: rewind succeeds end-to-end. Every phase
    /// transition lands in the journal; pcp.attach_node fires once.
    #[tokio::test]
    async fn drive_follow_primary_happy_path() {
        let (s, peers, pcp, _maint, inflight) = make_fp_fixture();
        let detached = fp_detached();
        let new_primary = fp_new_primary();

        let detached_client = Arc::new(StubPeerClient::default());
        detached_client.mark_running();
        peers.override_client(detached.id, detached_client.clone());

        let np_client = Arc::new(StubPeerClient::default());
        peers.override_client(new_primary.id, np_client.clone());

        // Seed a journal entry the driver will advance.
        let op = inflight
            .begin(
                crate::inflight_ops::InflightPayload::FollowPrimary {
                    detached_node_id: detached.id,
                    detached_hostname: detached.hostname.clone(),
                    new_primary_node_id: new_primary.id,
                    new_primary_hostname: new_primary.hostname.clone(),
                },
                FP_PHASE_QUEUED,
                false,
            )
            .await
            .unwrap();

        drive_follow_primary(
            &s.inflight,
            &s.peers,
            &s.pcp,
            &s.maint,
            &s.pg,
            &op.id,
            FP_PHASE_QUEUED,
            &detached,
            &new_primary,
        )
        .await
        .expect("driver should succeed");

        // Detached side: stop, rewind, configure_standby, start.
        assert_eq!(detached_client.stop_calls.load(Ordering::SeqCst), 1);
        assert_eq!(detached_client.rewind_calls.load(Ordering::SeqCst), 1);
        assert_eq!(detached_client.basebackup_calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            detached_client
                .configure_standby_calls
                .load(Ordering::SeqCst),
            1
        );
        assert_eq!(detached_client.start_calls.load(Ordering::SeqCst), 1);
        // New primary side: slot created for detached, no drop.
        assert_eq!(
            np_client.create_slot_calls.lock().unwrap().as_slice(),
            &["node2".to_string()]
        );
        assert!(np_client.drop_slot_calls.lock().unwrap().is_empty());
        // pcp attach for detached.
        assert_eq!(pcp.attach_calls.lock().unwrap().as_slice(), &[2]);
        // Journal advanced through attached (driver leaves complete()
        // to the caller — the spawn wrapper or resume RPC).
        let final_op = inflight.get(&op.id).await.unwrap();
        assert_eq!(final_op.phase, FP_PHASE_ATTACHED);
    }

    /// Driver falls back to basebackup when peer.rewind returns Err.
    #[tokio::test]
    async fn drive_follow_primary_falls_back_to_basebackup() {
        let (s, peers, _pcp, _maint, inflight) = make_fp_fixture();
        let detached = fp_detached();
        let new_primary = fp_new_primary();

        let detached_client = Arc::new(StubPeerClient::default());
        detached_client.mark_running();
        detached_client.rewind_fails.store(true, Ordering::SeqCst);
        peers.override_client(detached.id, detached_client.clone());

        let np_client = Arc::new(StubPeerClient::default());
        peers.override_client(new_primary.id, np_client.clone());

        let op = inflight
            .begin(
                crate::inflight_ops::InflightPayload::FollowPrimary {
                    detached_node_id: detached.id,
                    detached_hostname: detached.hostname.clone(),
                    new_primary_node_id: new_primary.id,
                    new_primary_hostname: new_primary.hostname.clone(),
                },
                FP_PHASE_QUEUED,
                false,
            )
            .await
            .unwrap();

        drive_follow_primary(
            &s.inflight,
            &s.peers,
            &s.pcp,
            &s.maint,
            &s.pg,
            &op.id,
            FP_PHASE_QUEUED,
            &detached,
            &new_primary,
        )
        .await
        .expect("driver should succeed via basebackup");

        assert_eq!(detached_client.rewind_calls.load(Ordering::SeqCst), 1);
        assert_eq!(detached_client.basebackup_calls.load(Ordering::SeqCst), 1);
    }

    /// Detached PG not running → driver returns Ok early without
    /// touching the detached or the new primary. Slot is NOT created
    /// because there's nothing to rebase yet.
    #[tokio::test]
    async fn drive_follow_primary_skips_when_detached_stopped() {
        let (s, peers, pcp, _maint, inflight) = make_fp_fixture();
        let detached = fp_detached();
        let new_primary = fp_new_primary();

        let detached_client = Arc::new(StubPeerClient::default());
        // NOT marked running — is_running is false by default.
        peers.override_client(detached.id, detached_client.clone());

        let np_client = Arc::new(StubPeerClient::default());
        peers.override_client(new_primary.id, np_client.clone());

        let op = inflight
            .begin(
                crate::inflight_ops::InflightPayload::FollowPrimary {
                    detached_node_id: detached.id,
                    detached_hostname: detached.hostname.clone(),
                    new_primary_node_id: new_primary.id,
                    new_primary_hostname: new_primary.hostname.clone(),
                },
                FP_PHASE_QUEUED,
                false,
            )
            .await
            .unwrap();

        drive_follow_primary(
            &s.inflight,
            &s.peers,
            &s.pcp,
            &s.maint,
            &s.pg,
            &op.id,
            FP_PHASE_QUEUED,
            &detached,
            &new_primary,
        )
        .await
        .expect("driver should return Ok early");

        assert_eq!(detached_client.stop_calls.load(Ordering::SeqCst), 0);
        assert!(np_client.create_slot_calls.lock().unwrap().is_empty());
        assert!(pcp.attach_calls.lock().unwrap().is_empty());
    }

    /// Basebackup failure after slot creation queues a maintenance
    /// intent to drop the slot on the new primary, and surfaces the
    /// original error so the caller marks the op Abandoned.
    #[tokio::test]
    async fn drive_follow_primary_queues_maintenance_on_basebackup_failure() {
        let (s, peers, _pcp, maint, inflight) = make_fp_fixture();
        let detached = fp_detached();
        let new_primary = fp_new_primary();

        let detached_client = Arc::new(StubPeerClient::default());
        detached_client.mark_running();
        detached_client.rewind_fails.store(true, Ordering::SeqCst);
        detached_client
            .basebackup_fails
            .store(true, Ordering::SeqCst);
        peers.override_client(detached.id, detached_client.clone());

        let np_client = Arc::new(StubPeerClient::default());
        // drop_slot also fails — forces the maintenance queue path.
        np_client.drop_slot_fails.store(true, Ordering::SeqCst);
        peers.override_client(new_primary.id, np_client.clone());

        let op = inflight
            .begin(
                crate::inflight_ops::InflightPayload::FollowPrimary {
                    detached_node_id: detached.id,
                    detached_hostname: detached.hostname.clone(),
                    new_primary_node_id: new_primary.id,
                    new_primary_hostname: new_primary.hostname.clone(),
                },
                FP_PHASE_QUEUED,
                false,
            )
            .await
            .unwrap();

        let err = drive_follow_primary(
            &s.inflight,
            &s.peers,
            &s.pcp,
            &s.maint,
            &s.pg,
            &op.id,
            FP_PHASE_QUEUED,
            &detached,
            &new_primary,
        )
        .await
        .expect_err("driver should bubble basebackup failure");
        assert!(err.to_string().contains("basebackup"), "{err}");

        // Slot drop was attempted on new primary, failed, so a
        // DropSlotCleanup intent was queued.
        assert_eq!(
            np_client.drop_slot_calls.lock().unwrap().as_slice(),
            &["node2".to_string()]
        );
        let intents = maint.intents.lock().unwrap();
        assert_eq!(intents.len(), 1);
        match &intents[0].payload {
            MaintenancePayload::DropSlotCleanup {
                slot_name,
                target_hostname,
                cause,
                ..
            } => {
                assert_eq!(slot_name, "node2");
                // Slot lives on the NEW PRIMARY, not the detached.
                assert_eq!(target_hostname, "peer1.local");
                assert_eq!(cause, "follow_primary_basebackup_failed");
            }
        }
    }

    /// Driver resumes from a recorded phase mid-ladder. Seed at
    /// `slot_created`; driver should skip the stop+create_slot phases
    /// and pick up at rewind/basebackup forward.
    #[tokio::test]
    async fn drive_follow_primary_resumes_from_recorded_phase() {
        let (s, peers, _pcp, _maint, inflight) = make_fp_fixture();
        let detached = fp_detached();
        let new_primary = fp_new_primary();

        let detached_client = Arc::new(StubPeerClient::default());
        detached_client.mark_running();
        peers.override_client(detached.id, detached_client.clone());

        let np_client = Arc::new(StubPeerClient::default());
        peers.override_client(new_primary.id, np_client.clone());

        // Pre-seed at slot_created (e.g. the daemon crashed right
        // after writing the journal entry for slot_created).
        let op = inflight
            .begin(
                crate::inflight_ops::InflightPayload::FollowPrimary {
                    detached_node_id: detached.id,
                    detached_hostname: detached.hostname.clone(),
                    new_primary_node_id: new_primary.id,
                    new_primary_hostname: new_primary.hostname.clone(),
                },
                FP_PHASE_SLOT_CREATED,
                false,
            )
            .await
            .unwrap();

        drive_follow_primary(
            &s.inflight,
            &s.peers,
            &s.pcp,
            &s.maint,
            &s.pg,
            &op.id,
            FP_PHASE_SLOT_CREATED,
            &detached,
            &new_primary,
        )
        .await
        .expect("resume from slot_created should succeed");

        // Stop and create_slot SKIPPED — already done before crash.
        assert_eq!(detached_client.stop_calls.load(Ordering::SeqCst), 0);
        assert!(np_client.create_slot_calls.lock().unwrap().is_empty());
        // But rewind and downstream phases DID run.
        assert_eq!(detached_client.rewind_calls.load(Ordering::SeqCst), 1);
        assert_eq!(detached_client.start_calls.load(Ordering::SeqCst), 1);
    }

    /// fan_out_follow_primary enqueues exactly one InflightOp per
    /// non-local non-target standby. With a 3-node pool and target=1,
    /// node 2 gets exactly one FollowPrimary op.
    #[tokio::test]
    async fn fan_out_follow_primary_enqueues_one_per_other_standby() {
        let (s, peers, _pcp, _maint, inflight) = make_fp_fixture();
        let local = NodeConfig {
            id: 0,
            hostname: "local".into(),
        };
        let new_primary = fp_new_primary();

        // Make peer calls slow enough that we can observe the
        // InProgress op before the spawned task completes. Without
        // overrides the driver would race to completion.
        peers.mark_unreachable(2); // detached unreachable → driver errs out fast

        s.fan_out_follow_primary(&local, &new_primary).await;

        // Give the spawned task a tick to record the enqueue (the
        // spawn happens AFTER inflight.begin, so the op is already
        // present even before the task runs).
        let (ops, _) = inflight.list(&[]).await.unwrap();
        let fp_ops: Vec<_> = ops
            .iter()
            .filter(|o| {
                matches!(
                    &o.payload,
                    crate::inflight_ops::InflightPayload::FollowPrimary { .. }
                )
            })
            .collect();
        assert_eq!(fp_ops.len(), 1, "expected exactly one FollowPrimary op");
        match &fp_ops[0].payload {
            crate::inflight_ops::InflightPayload::FollowPrimary {
                detached_node_id,
                new_primary_node_id,
                ..
            } => {
                assert_eq!(*detached_node_id, 2);
                assert_eq!(*new_primary_node_id, 1);
            }
            _ => unreachable!(),
        }
    }

    /// resume_follow_primary verifies pool topology, then drives to
    /// completion. The op must end up Done.
    #[tokio::test]
    async fn resume_follow_primary_completes_via_resume_inflight_op() {
        let (s, peers, _pcp, _maint, inflight) = make_fp_fixture();
        let detached = fp_detached();
        let new_primary = fp_new_primary();

        let detached_client = Arc::new(StubPeerClient::default());
        detached_client.mark_running();
        peers.override_client(detached.id, detached_client.clone());
        let np_client = Arc::new(StubPeerClient::default());
        peers.override_client(new_primary.id, np_client.clone());

        // Seed at queued. (`seed` bypasses `begin` so we can pick any
        // phase / id.)
        inflight.seed(crate::inflight_ops::InflightOp {
            id: "fp-resume".into(),
            status: crate::inflight_ops::InflightStatus::InProgress,
            payload: crate::inflight_ops::InflightPayload::FollowPrimary {
                detached_node_id: detached.id,
                detached_hostname: detached.hostname.clone(),
                new_primary_node_id: new_primary.id,
                new_primary_hostname: new_primary.hostname.clone(),
            },
            phase: FP_PHASE_QUEUED.into(),
            started_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            completed_at: None,
            discharged_at: None,
            last_error: None,
        });

        let resp = s
            .resume_inflight_op(Request::new(ResumeInflightOpRequest {
                id: "fp-resume".into(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.ok, "{}", resp.message);

        let final_op = inflight.get("fp-resume").await.unwrap();
        assert_eq!(final_op.status, crate::inflight_ops::InflightStatus::Done);
    }

    /// resume refuses when the pool topology has shifted — detached
    /// hostname no longer matches the recorded one.
    #[tokio::test]
    async fn resume_follow_primary_refuses_on_topology_mismatch() {
        let (s, _peers, _pcp, _maint, inflight) = make_fp_fixture();
        inflight.seed(crate::inflight_ops::InflightOp {
            id: "fp-bad-topo".into(),
            status: crate::inflight_ops::InflightStatus::InProgress,
            payload: crate::inflight_ops::InflightPayload::FollowPrimary {
                detached_node_id: 2,
                detached_hostname: "renamed.example".into(), // does NOT match make_pool_3
                new_primary_node_id: 1,
                new_primary_hostname: "peer1.local".into(),
            },
            phase: FP_PHASE_QUEUED.into(),
            started_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            completed_at: None,
            discharged_at: None,
            last_error: None,
        });
        let resp = s
            .resume_inflight_op(Request::new(ResumeInflightOpRequest {
                id: "fp-bad-topo".into(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(!resp.ok);
        assert!(
            resp.message.contains("pool topology has changed"),
            "got: {}",
            resp.message
        );
    }
}
