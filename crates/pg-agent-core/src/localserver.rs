//! `PgAgentLocal` tonic service — Unix-socket gRPC for `pg_agentc` (the
//! hook-script binary pgpool execs) and `pg_agentctl` (operator CLI).
//!
//! Auth is filesystem permissions: the socket is mode `0600 root:postgres`,
//! so any caller with a connection is implicitly authorised.
//!
//! # Scope
//!
//! Implemented today: read-only RPCs (`GetStatus`, `GetNodeConfig`), the
//! maintenance reads (`ListMaintenance`, `GetMaintenance`,
//! `RetryMaintenance`), the trivial `Escalation` (deliberate no-op — no
//! VIP), and `RemoteStart` (forwards `pgpool_remote_start` to the target
//! peer's `Systemd::start_postgres`).
//!
//! Still `Status::unimplemented`: `Failover`, `FollowPrimary`,
//! `RecoveryFirstStage`, `RestoreWal`, `ClusterInit`. Each is a separate
//! orchestration shape that lands in its own commit.

use crate::agent::NodeInfo;
use crate::config::{NodeConfig, NodePool, PostgresRuntime};
use crate::errors::AgentError;
use crate::localdb::LocalDb;
use crate::maintenance::{
    MaintenanceIntent as CoreIntent, MaintenancePayload, MaintenanceStatus, MaintenanceStore,
    SkippedIntent,
};
use crate::pcp::Pcp;
use crate::peers::PeerRegistry;
use crate::pgstandby::{BasebackupOpts, RewindOpts, WriteRecoveryConfOpts};
use crate::replay_markers::ReplayMarkerStore;
use crate::walstore::WalStore;
use chrono::SecondsFormat;
use pg_agent_proto::pgagentpb::{
    pg_agent_local_server::{PgAgentLocal, PgAgentLocalServer},
    ClusterInitRequest, ClusterInitResponse, ClusterInitStandbyResult, EscalationRequest,
    FailoverRequest, FollowPrimaryRequest, GetMaintenanceRequest, GetStatusRequest,
    ListMaintenanceRequest, ListMaintenanceResponse, MaintenanceIntent as ProtoIntent,
    NodeConfigRequest, NodeConfigResponse, NodeStatus, OpResult, RecoveryRequest,
    RemoteStartRequest, RestoreWalRequest, RetryMaintenanceRequest, SkippedMaintenanceIntent,
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

pub struct LocalServer {
    node_info: Arc<dyn NodeInfo>,
    db: Arc<dyn LocalDb>,
    peers: Arc<dyn PeerRegistry>,
    maint: Arc<dyn MaintenanceStore>,
    wal: Arc<dyn WalStore>,
    replay: Arc<dyn ReplayMarkerStore>,
    pcp: Arc<dyn Pcp>,
    node_pool: NodePool,
    pg: PostgresRuntime,
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
        pcp: Arc<dyn Pcp>,
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
            pcp,
            node_pool,
            pg,
        }
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

    // ----- hook orchestration (TODO(v1)) ------------------------------------

    /// `failover_command` — pgpool fires this on a surviving node when
    /// a backend goes down. Two branches:
    ///
    /// **Standby down** (`detached.id != old_primary.id`): we're the
    /// primary; drop the detached standby's replication slot from our
    /// local PG. No promotion involved.
    ///
    /// **Primary down** (`detached.id == old_primary.id`): the detached
    /// node IS the failed primary. Dial `new_main` (the chosen
    /// successor), tell it to `Promote()`, then drop the old primary's
    /// slot on the newly-promoted node.
    ///
    /// Either branch returns `Ok` (with replay marker written) even
    /// when the slot drop itself fails — the drop is queued to
    /// maintenance for retry. pgpool doesn't need to re-fire the hook
    /// just because a slot cleanup got hung up.
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

        if detached.id != old_primary.id {
            // §1: standby down. We're the primary; drop the slot locally.
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

        // Primary down — promote new main, then drop old primary's slot
        // on the newly promoted node.
        info!(
            new_main = %new_main.hostname,
            "failover: primary down, promoting new main"
        );

        let peer = self.peers.client(new_main).await.map_err(|e| {
            internal(anyhow::anyhow!(
                "failover: peer client for {}: {e}",
                new_main.hostname
            ))
        })?;

        if let Err(e) = peer.promote().await {
            return Err(internal(anyhow::anyhow!(
                "failover: promote {}: {e}",
                new_main.hostname
            )));
        }

        info!(
            slot = %slot_name,
            on = %new_main.hostname,
            "failover: dropping old primary's replication slot on new primary"
        );
        let message = match peer.drop_slot(&slot_name).await {
            Ok(()) => "primary failover: promoted and slot dropped".to_string(),
            Err(drop_err) => {
                self.queue_drop_slot_cleanup(&slot_name, &new_main.hostname, "rpc_error", &drop_err)
                    .await
            }
        };
        self.write_replay_marker_then_ok("failover", &replay_key, message)
            .await
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

        let replay_key = format!("primary={},standby={}", primary_ref.id, standby_ref.id);
        match self.replay.has("recovery_1st_stage", &replay_key).await {
            Ok(true) => {
                info!(%replay_key, "recovery_1st_stage: replay detected, skipping");
                return Ok(Response::new(OpResult {
                    ok: true,
                    message: "recovery_1st_stage: already processed; skipping duplicate".into(),
                }));
            }
            Ok(false) => {}
            Err(e) => {
                return Err(internal(anyhow::anyhow!(
                    "recovery_1st_stage: idempotency marker check: {e}"
                )));
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

        // SPEC §2: checkpoint then create_slot so the slot's restart_lsn
        // sits at the current WAL position. Without this, basebackup
        // could start from an older checkpoint and the slot would
        // immediately need WAL we no longer keep.
        self.db
            .checkpoint()
            .await
            .map_err(|e| internal(anyhow::anyhow!("recovery_1st_stage: checkpoint: {e}")))?;

        let slot_name = standby.slot_name();
        self.db.create_slot(&slot_name).await.map_err(|e| {
            internal(anyhow::anyhow!(
                "recovery_1st_stage: create slot {slot_name}: {e}"
            ))
        })?;

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
                return Err(internal(err));
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
            return Err(internal(err));
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
            return Err(internal(err));
        }

        // SPEC §5: do NOT call pcp_attach_node here — pgpool drives
        // re-attachment after 2nd stage completes (which is triggered
        // by pgpool itself via pgpool_remote_start, not us).
        self.replay
            .mark_done("recovery_1st_stage", &replay_key)
            .await
            .map_err(|e| {
                internal(anyhow::anyhow!(
                    "recovery_1st_stage: idempotency marker write: {e}"
                ))
            })?;

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

        Ok(Response::new(ClusterInitResponse {
            ok,
            message,
            repl_user: self.pg.repl_user.clone(),
            standbys: results,
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
                    return FetchOutcome::TryNext;
                }
            };
            match self.wal.write_restore(Path::new(dest_path), reader).await {
                Ok(()) => FetchOutcome::Fetched,
                Err(AgentError::DestOutsidePgData) => FetchOutcome::Fatal(
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
                FetchOutcome::TryNext
            }
        }
    }
}

fn internal(e: anyhow::Error) -> Status {
    Status::internal(e.to_string())
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
    use crate::localdb::ReplicationLag;
    use crate::maintenance::{MaintenancePayload, MaintenanceStatus};
    use crate::peers::PeerClient;
    use async_trait::async_trait;
    use chrono::{TimeZone, Utc};
    use pg_agent_proto::pgagentpb::NodeRef;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Mutex as StdMutex;

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
        /// Map wal_file → content the peer "has" in its archive.
        wal_content: StdMutex<std::collections::HashMap<String, Vec<u8>>>,
        /// Make fetch_wal err out (transport/RPC failure shape).
        fetch_wal_errors: AtomicBool,
        /// Hang fetch_wal indefinitely — exercises the per-peer timeout.
        fetch_wal_hangs: AtomicBool,
        // FollowPrimary surface — counters + failure switches per method.
        is_running: AtomicBool,
        get_status_fails: AtomicBool,
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
        promote_fails: AtomicBool,
        drop_slot_calls: StdMutex<Vec<String>>,
        drop_slot_fails: AtomicBool,
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
        async fn fetch_wal(
            &self,
            wal_file: &str,
        ) -> anyhow::Result<Option<Box<dyn tokio::io::AsyncRead + Send + Unpin>>> {
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
            if self.get_status_fails.load(Ordering::SeqCst) {
                anyhow::bail!("stub peer get_status boom");
            }
            let running = self.is_running.load(Ordering::SeqCst);
            Ok(NodeStatus {
                is_running: running,
                is_in_recovery: false,
                is_ready: false,
                replication_lag_bytes: 0,
                replication_state: String::new(),
                is_postgres_running: running,
                is_pgpool_running: true,
                is_postgres_status_ok: true,
                is_pgpool_status_ok: true,
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
            if self.promote_fails.load(Ordering::SeqCst) {
                anyhow::bail!("stub peer promote boom");
            }
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
        ) -> Result<Box<dyn tokio::io::AsyncRead + Send + Unpin>, AgentError> {
            // LocalServer doesn't call open_archive — only the inbound
            // PeerServer.FetchWal does, and that path is exercised in
            // peerserver tests.
            Err(AgentError::WalNotFound("stub: not used here".into()))
        }
        async fn write_restore(
            &self,
            dest_path: &Path,
            mut src: Box<dyn tokio::io::AsyncRead + Send + Unpin>,
        ) -> Result<(), AgentError> {
            if self.dest_outside_pgdata.load(Ordering::SeqCst) {
                return Err(AgentError::DestOutsidePgData);
            }
            if self.write_errors.load(Ordering::SeqCst) {
                return Err(AgentError::WalNotFound("stub: write_restore boom".into()));
            }
            use tokio::io::AsyncReadExt;
            let mut bytes = Vec::new();
            src.read_to_end(&mut bytes).await.map_err(AgentError::Io)?;
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
    ) {
        let db = Arc::new(StubDb::default());
        let peers = Arc::new(StubPeers::default());
        let maint = Arc::new(StubMaint::default());
        let wal = Arc::new(StubWal::default());
        let replay = Arc::new(StubReplay::default());
        let pcp = Arc::new(StubPcp::default());
        let server = LocalServer::new(
            Arc::new(FakeNodeInfo),
            db.clone(),
            peers.clone(),
            maint.clone(),
            wal.clone(),
            replay.clone(),
            pcp.clone(),
            make_pool(),
            make_pg(),
        );
        (server, db, peers, maint, wal, replay, pcp)
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
        let (s, _db, peers, _maint, _wal, _replay, _pcp) = make_server();
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
        let (s, db, peers, _maint, _wal, _replay, _pcp) = make_server();
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
        let (s, _db, peers, _maint, _wal, _replay, _pcp) = make_server();
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
        let (s, _db, _peers, maint, _wal, _replay, _pcp) = make_server();
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
        let (s, _db, _peers, maint, _wal, _replay, _pcp) = make_server();
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
        let (s, _db, _peers, maint, _wal, _replay, _pcp) = make_server();
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
        let (s, _db, _peers, maint, _wal, _replay, _pcp) = make_server();
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
        let (s, _db, _peers, maint, _wal, _replay, _pcp) = make_server();
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
        let (s, _db, _peers, maint, _wal, _replay, _pcp) = make_server();
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
        let (s, _db, _peers, _maint, _wal, replay, _pcp) = make_server();
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
        let (s, db, _peers, _maint, _wal, replay, _pcp) = make_server();
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
        let (s, db, _peers, maint, _wal, replay, _pcp) = make_server();
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

    /// Two-peer pool where peer1 acts as the new main. detached=peer1
    /// and old_primary=peer1 simulates "the primary went down and we're
    /// running on the *other* standby that's becoming new_main". The
    /// pool's local node (id 0) is the new_main target — so we override
    /// id=0's peer client. (`peers.client()` is called for new_main.)
    #[tokio::test]
    async fn failover_primary_down_promotes_and_drops_slot() {
        let (s, db, peers, _maint, _wal, replay, _pcp) = make_server();
        let new_main_client = Arc::new(StubPeerClient::default());
        peers.override_client(0, new_main_client.clone());

        // detached=1 (the failed primary), new_main=0, old_primary=1.
        // detached.id == old_primary.id → primary-down branch.
        let resp = s
            .failover(Request::new(failover_req(1, 0, 1)))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.ok);
        assert!(resp.message.contains("promoted and slot dropped"));
        assert_eq!(new_main_client.promote_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            *new_main_client.drop_slot_calls.lock().unwrap(),
            vec!["node1".to_string()]
        );
        // Local drop_slot was NOT called — drop happens on the peer.
        assert!(db.dropped_slots.lock().unwrap().is_empty());
        assert!(replay
            .has("failover", "detached=1,new_main=0,old_primary=1")
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn failover_primary_down_promote_failure_is_internal_no_marker() {
        let (s, _db, peers, _maint, _wal, replay, _pcp) = make_server();
        let new_main_client = Arc::new(StubPeerClient::default());
        new_main_client.promote_fails.store(true, Ordering::SeqCst);
        peers.override_client(0, new_main_client.clone());

        let err = s
            .failover(Request::new(failover_req(1, 0, 1)))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Internal);
        assert!(err.message().contains("promote"));
        // No marker — pgpool re-fires the hook so we can retry promote.
        assert!(!replay
            .has("failover", "detached=1,new_main=0,old_primary=1")
            .await
            .unwrap());
        // No drop_slot since promote failed.
        assert!(new_main_client.drop_slot_calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn failover_primary_down_drop_slot_failure_queues_maintenance() {
        let (s, _db, peers, maint, _wal, replay, _pcp) = make_server();
        let new_main_client = Arc::new(StubPeerClient::default());
        new_main_client
            .drop_slot_fails
            .store(true, Ordering::SeqCst);
        peers.override_client(0, new_main_client.clone());

        let resp = s
            .failover(Request::new(failover_req(1, 0, 1)))
            .await
            .unwrap()
            .into_inner();
        // Hook returns Ok — promote succeeded, drop cleanup queued.
        assert!(resp.ok);
        assert!(resp.message.contains("queued maintenance"));
        {
            let intents = maint.intents.lock().unwrap();
            assert_eq!(intents.len(), 1);
            match &intents[0].payload {
                MaintenancePayload::DropSlotCleanup { cause, .. } => {
                    assert_eq!(cause, "rpc_error");
                }
            }
        }
        // Marker IS written — promote succeeded; only slot cleanup is async.
        assert!(replay
            .has("failover", "detached=1,new_main=0,old_primary=1")
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn failover_skips_when_replay_marker_present() {
        let (s, db, peers, _maint, _wal, replay, _pcp) = make_server();
        let new_main_client = Arc::new(StubPeerClient::default());
        peers.override_client(0, new_main_client.clone());
        replay.mark("failover", "detached=1,new_main=0,old_primary=1");

        let resp = s
            .failover(Request::new(failover_req(1, 0, 1)))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.ok);
        assert!(resp.message.contains("already processed"));
        // No downstream calls happened.
        assert_eq!(new_main_client.promote_calls.load(Ordering::SeqCst), 0);
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
        }
    }

    /// Default setup: local node (id 0) is the primary; peer1 (id 1) is
    /// the standby being recovered. Returns the StubPeerClient that
    /// represents peer1 so tests can configure it.
    #[allow(clippy::type_complexity)]
    fn make_recovery_setup() -> (
        LocalServer,
        Arc<StubDb>,
        Arc<StubPeers>,
        Arc<StubMaint>,
        Arc<StubReplay>,
        Arc<StubPeerClient>,
    ) {
        let (s, db, peers, maint, _wal, replay, _pcp) = make_server();
        let standby_client = Arc::new(StubPeerClient::default());
        peers.override_client(1, standby_client.clone());
        (s, db, peers, maint, replay, standby_client)
    }

    #[tokio::test]
    async fn recovery_first_stage_happy_path() {
        let (s, db, _peers, _maint, replay, standby) = make_recovery_setup();
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
        assert!(replay
            .has("recovery_1st_stage", "primary=0,standby=1")
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn recovery_first_stage_skips_when_replay_marker_present() {
        let (s, db, _peers, _maint, replay, standby) = make_recovery_setup();
        replay.mark("recovery_1st_stage", "primary=0,standby=1");
        let resp = s
            .recovery_first_stage(Request::new(recovery_req(0, 1)))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.ok);
        assert!(resp.message.contains("already processed"));
        // Nothing downstream of the marker touched.
        assert_eq!(db.checkpoint_calls.load(Ordering::SeqCst), 0);
        assert_eq!(standby.basebackup_calls.load(Ordering::SeqCst), 0);
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
        let (s, db, _peers, _maint, replay, standby) = make_recovery_setup();
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
        let (s, db, _peers, _maint, _replay, standby) = make_recovery_setup();
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
        let (s, db, _peers, maint, _replay, standby) = make_recovery_setup();
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
        let server = LocalServer::new(
            Arc::new(FakeNodeInfo),
            db.clone(),
            peers.clone(),
            maint.clone(),
            wal.clone(),
            replay,
            pcp,
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
        let (s, _db, peers, _maint, wal, _replay, _pcp) = make_server();
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
        let (s, _db, peers, _maint, wal, _replay, _pcp) = make_server();
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
        let (s, db, peers, maint, _wal, replay, pcp) = make_server();
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
        let server = LocalServer::new(
            Arc::new(FakeNodeInfo),
            db.clone(),
            peers.clone(),
            maint.clone(),
            wal,
            replay,
            pcp,
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
        let s = LocalServer::new(
            Arc::new(FakeNodeInfo),
            db.clone(),
            peers,
            maint,
            wal,
            replay,
            pcp,
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
}
