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
use crate::config::{NodeConfig, NodePool};
use crate::errors::AgentError;
use crate::localdb::LocalDb;
use crate::maintenance::{
    MaintenanceIntent as CoreIntent, MaintenanceStatus, MaintenanceStore, SkippedIntent,
};
use crate::peers::PeerRegistry;
use crate::walstore::WalStore;
use chrono::SecondsFormat;
use pg_agent_proto::pgagentpb::{
    pg_agent_local_server::{PgAgentLocal, PgAgentLocalServer},
    ClusterInitRequest, ClusterInitResponse, EscalationRequest, FailoverRequest,
    FollowPrimaryRequest, GetMaintenanceRequest, GetStatusRequest, ListMaintenanceRequest,
    ListMaintenanceResponse, MaintenanceIntent as ProtoIntent, NodeConfigRequest,
    NodeConfigResponse, NodeStatus, OpResult, RecoveryRequest, RemoteStartRequest,
    RestoreWalRequest, RetryMaintenanceRequest, SkippedMaintenanceIntent,
};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::UnixListener;
use tokio_stream::wrappers::UnixListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::{transport::Server, Request, Response, Status};
use tracing::{info, warn};

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
    node_pool: NodePool,
    // TODO(v1): Arc<dyn StandbyOps>, Arc<dyn Pcp>, Arc<dyn ReplayMarkerStore>,
    // PostgresRuntime — added as the remaining orchestration handlers land.
}

impl LocalServer {
    pub fn new(
        node_info: Arc<dyn NodeInfo>,
        db: Arc<dyn LocalDb>,
        peers: Arc<dyn PeerRegistry>,
        maint: Arc<dyn MaintenanceStore>,
        wal: Arc<dyn WalStore>,
        node_pool: NodePool,
    ) -> Self {
        Self {
            node_info,
            db,
            peers,
            maint,
            wal,
            node_pool,
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

    async fn failover(&self, _req: Request<FailoverRequest>) -> Result<Response<OpResult>, Status> {
        Err(Status::unimplemented("failover"))
    }
    async fn follow_primary(
        &self,
        _req: Request<FollowPrimaryRequest>,
    ) -> Result<Response<OpResult>, Status> {
        Err(Status::unimplemented("follow_primary"))
    }
    async fn recovery_first_stage(
        &self,
        _req: Request<RecoveryRequest>,
    ) -> Result<Response<OpResult>, Status> {
        Err(Status::unimplemented("recovery_first_stage"))
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
    async fn cluster_init(
        &self,
        _req: Request<ClusterInitRequest>,
    ) -> Result<Response<ClusterInitResponse>, Status> {
        Err(Status::unimplemented("cluster_init"))
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
    }

    #[async_trait]
    impl LocalDb for StubDb {
        async fn promote(&self) -> anyhow::Result<()> {
            Ok(())
        }
        async fn checkpoint(&self) -> anyhow::Result<()> {
            Ok(())
        }
        async fn create_slot(&self, _: &str) -> anyhow::Result<()> {
            Ok(())
        }
        async fn drop_slot(&self, _: &str) -> anyhow::Result<()> {
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
        async fn create_replication_role(&self, _: &str) -> anyhow::Result<()> {
            Ok(())
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
    }

    impl StubPeerClient {
        fn stage_wal(&self, name: &str, content: Vec<u8>) {
            self.wal_content
                .lock()
                .unwrap()
                .insert(name.to_string(), content);
        }
    }

    #[async_trait]
    impl PeerClient for StubPeerClient {
        async fn drop_slot(&self, _: &str) -> anyhow::Result<()> {
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
        async fn append(&self, _: MaintenancePayload) -> anyhow::Result<CoreIntent> {
            anyhow::bail!("stub")
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

    #[allow(clippy::type_complexity)]
    fn make_server() -> (
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
        let server = LocalServer::new(
            Arc::new(FakeNodeInfo),
            db.clone(),
            peers.clone(),
            maint.clone(),
            wal.clone(),
            make_pool(),
        );
        (server, db, peers, maint, wal)
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
        let (s, _db, peers, _maint, _wal) = make_server();
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
        let (s, db, peers, _maint, _wal) = make_server();
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
        let (s, _db, peers, _maint, _wal) = make_server();
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
        let (s, _db, _peers, maint, _wal) = make_server();
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
        let (s, _db, _peers, maint, _wal) = make_server();
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
        let (s, _db, _peers, maint, _wal) = make_server();
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
        let (s, _db, _peers, maint, _wal) = make_server();
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
        let (s, _db, _peers, maint, _wal) = make_server();
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
        let (s, _db, _peers, maint, _wal) = make_server();
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

    // ----- unimplemented still ------------------------------------------

    #[tokio::test]
    async fn failover_returns_unimplemented() {
        let (s, ..) = make_server();
        let err = s
            .failover(Request::new(FailoverRequest::default()))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unimplemented);
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
        let server = LocalServer::new(
            Arc::new(FakeNodeInfo),
            db.clone(),
            peers.clone(),
            maint.clone(),
            wal.clone(),
            make_pool_3(),
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
        let (s, _db, peers, _maint, wal) = make_server();
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
        let (s, _db, peers, _maint, wal) = make_server();
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
