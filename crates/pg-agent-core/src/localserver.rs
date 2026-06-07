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
use crate::config::NodePool;
use crate::localdb::LocalDb;
use crate::maintenance::{
    MaintenanceIntent as CoreIntent, MaintenanceStatus, MaintenanceStore, SkippedIntent,
};
use crate::peers::PeerRegistry;
use chrono::SecondsFormat;
use pg_agent_proto::pgagentpb::{
    pg_agent_local_server::{PgAgentLocal, PgAgentLocalServer},
    ClusterInitRequest, ClusterInitResponse, EscalationRequest, FailoverRequest,
    FollowPrimaryRequest, GetMaintenanceRequest, GetStatusRequest, ListMaintenanceRequest,
    ListMaintenanceResponse, MaintenanceIntent as ProtoIntent, NodeConfigRequest,
    NodeConfigResponse, NodeStatus, OpResult, RecoveryRequest, RemoteStartRequest,
    RestoreWalRequest, RetryMaintenanceRequest, SkippedMaintenanceIntent,
};
use std::sync::Arc;
use tokio::net::UnixListener;
use tokio_stream::wrappers::UnixListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::{transport::Server, Request, Response, Status};
use tracing::{info, warn};

pub struct LocalServer {
    node_info: Arc<dyn NodeInfo>,
    db: Arc<dyn LocalDb>,
    peers: Arc<dyn PeerRegistry>,
    maint: Arc<dyn MaintenanceStore>,
    node_pool: NodePool,
    // TODO(v1): Arc<dyn StandbyOps>, Arc<dyn WalStore>, Arc<dyn Pcp>,
    // Arc<dyn ReplayMarkerStore>, PostgresRuntime — added as the
    // remaining orchestration handlers land.
}

impl LocalServer {
    pub fn new(
        node_info: Arc<dyn NodeInfo>,
        db: Arc<dyn LocalDb>,
        peers: Arc<dyn PeerRegistry>,
        maint: Arc<dyn MaintenanceStore>,
        node_pool: NodePool,
    ) -> Self {
        Self {
            node_info,
            db,
            peers,
            maint,
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
    async fn restore_wal(
        &self,
        _req: Request<RestoreWalRequest>,
    ) -> Result<Response<OpResult>, Status> {
        Err(Status::unimplemented("restore_wal"))
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
    }

    #[derive(Default)]
    struct StubPeers {
        client: Arc<StubPeerClient>,
        fail_client: AtomicBool,
    }

    #[async_trait]
    impl PeerRegistry for StubPeers {
        async fn client(&self, _: &NodeConfig) -> anyhow::Result<Arc<dyn PeerClient>> {
            if self.fail_client.load(Ordering::SeqCst) {
                anyhow::bail!("stub: peer client unreachable");
            }
            Ok(self.client.clone() as Arc<dyn PeerClient>)
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
    fn make_server() -> (LocalServer, Arc<StubDb>, Arc<StubPeers>, Arc<StubMaint>) {
        let db = Arc::new(StubDb::default());
        let peers = Arc::new(StubPeers::default());
        let maint = Arc::new(StubMaint::default());
        let server = LocalServer::new(
            Arc::new(FakeNodeInfo),
            db.clone(),
            peers.clone(),
            maint.clone(),
            make_pool(),
        );
        (server, db, peers, maint)
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
        let (s, _db, peers, _maint) = make_server();
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
        assert_eq!(peers.client.start_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn remote_start_refuses_when_local_is_replica() {
        let (s, db, peers, _maint) = make_server();
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
        assert_eq!(peers.client.start_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn remote_start_propagates_peer_error_as_internal() {
        let (s, _db, peers, _maint) = make_server();
        peers.client.start_fails.store(true, Ordering::SeqCst);
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
        let (s, _db, _peers, maint) = make_server();
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
        let (s, _db, _peers, maint) = make_server();
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
        let (s, _db, _peers, maint) = make_server();
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
        let (s, _db, _peers, maint) = make_server();
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
        let (s, _db, _peers, maint) = make_server();
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
        let (s, _db, _peers, maint) = make_server();
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

    #[tokio::test]
    async fn restore_wal_returns_unimplemented() {
        let (s, ..) = make_server();
        let err = s
            .restore_wal(Request::new(RestoreWalRequest::default()))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unimplemented);
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
