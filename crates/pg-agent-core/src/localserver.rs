//! `PgAgentLocal` tonic service — Unix-socket gRPC for `pg_agentc` (the
//! hook-script binary pgpool execs) and `pg_agentctl` (operator CLI).
//!
//! Auth is filesystem permissions: the socket is mode `0600 root:postgres`,
//! so any caller with a connection is implicitly authorised.
//!
//! # Scope of this skeleton
//!
//! Only the read-only RPCs (`GetStatus`, `GetNodeConfig`) are wired through
//! to [`NodeInfo`]. All action RPCs (`Failover`, `FollowPrimary`,
//! `RecoveryFirstStage`, `RemoteStart`, `Escalation`, `RestoreWal`,
//! `ClusterInit`, and the three `*Maintenance` ones) return
//! `Status::unimplemented` until SPEC §3 lands handler-by-handler. The
//! plumbing (service registration, transport, shutdown wiring) is complete
//! — only the per-RPC bodies are pending.

use crate::agent::NodeInfo;
use pg_agent_proto::pgagentpb::{
    pg_agent_local_server::{PgAgentLocal, PgAgentLocalServer},
    ClusterInitRequest, ClusterInitResponse, EscalationRequest, FailoverRequest,
    FollowPrimaryRequest, GetMaintenanceRequest, GetStatusRequest, ListMaintenanceRequest,
    ListMaintenanceResponse, MaintenanceIntent, NodeConfigRequest, NodeConfigResponse, NodeStatus,
    OpResult, RecoveryRequest, RemoteStartRequest, RestoreWalRequest, RetryMaintenanceRequest,
};
use std::sync::Arc;
use tokio::net::UnixListener;
use tokio_stream::wrappers::UnixListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::{transport::Server, Request, Response, Status};
use tracing::{info, warn};

/// Local gRPC server. Construction is cheap (Arc clone of [`NodeInfo`]);
/// [`LocalServer::serve`] consumes `self` so the spawned task owns it.
pub struct LocalServer {
    node_info: Arc<dyn NodeInfo>,
    // TODO(v1): Arc<dyn LocalDb>, Arc<dyn Pcp>, Arc<dyn Systemd>,
    // Arc<dyn PeerRegistry>, Arc<dyn ReplayMarkerStore>, Arc<dyn WalStore>,
    // Arc<dyn MaintenanceStore>, NodePool, PostgresRuntime — added as
    // action-handler implementations land.
}

impl LocalServer {
    pub fn new(node_info: Arc<dyn NodeInfo>) -> Self {
        Self { node_info }
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

    // ----- action surface (TODO(v1)) ----------------------------------------

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
    async fn remote_start(
        &self,
        _req: Request<RemoteStartRequest>,
    ) -> Result<Response<OpResult>, Status> {
        Err(Status::unimplemented("remote_start"))
    }
    async fn escalation(
        &self,
        _req: Request<EscalationRequest>,
    ) -> Result<Response<OpResult>, Status> {
        Err(Status::unimplemented("escalation"))
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

    async fn list_maintenance(
        &self,
        _req: Request<ListMaintenanceRequest>,
    ) -> Result<Response<ListMaintenanceResponse>, Status> {
        Err(Status::unimplemented("list_maintenance"))
    }
    async fn get_maintenance(
        &self,
        _req: Request<GetMaintenanceRequest>,
    ) -> Result<Response<MaintenanceIntent>, Status> {
        Err(Status::unimplemented("get_maintenance"))
    }
    async fn retry_maintenance(
        &self,
        _req: Request<RetryMaintenanceRequest>,
    ) -> Result<Response<OpResult>, Status> {
        Err(Status::unimplemented("retry_maintenance"))
    }
}

fn internal(e: anyhow::Error) -> Status {
    Status::internal(e.to_string())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------
//
// The transport (tonic + UnixListenerStream + graceful shutdown) is exercised
// end-to-end by `agent::tests::serve_exits_promptly_on_shutdown`. Here we test
// the per-RPC routing on the trait impl directly — no wire, no dial, no hyper.

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;

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

    fn server() -> LocalServer {
        LocalServer::new(Arc::new(FakeNodeInfo))
    }

    #[tokio::test]
    async fn get_status_routes_to_node_info() {
        let s = server();
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
        let s = server();
        let resp = s
            .get_node_config(Request::new(NodeConfigRequest {}))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.pg_port, 5432);
        assert_eq!(resp.pg_data_dir, "/var/lib/postgresql/17/main");
    }

    #[tokio::test]
    async fn failover_returns_unimplemented() {
        let s = server();
        let err = s
            .failover(Request::new(FailoverRequest::default()))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unimplemented);
    }

    #[tokio::test]
    async fn cluster_init_returns_unimplemented() {
        let s = server();
        let err = s
            .cluster_init(Request::new(ClusterInitRequest::default()))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unimplemented);
    }

    #[tokio::test]
    async fn restore_wal_returns_unimplemented() {
        let s = server();
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
}
