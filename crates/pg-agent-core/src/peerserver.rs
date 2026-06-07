//! `PgAgentPeer` tonic service — mTLS TCP gRPC for the persistent peer mesh.
//!
//! Inbound auth is mTLS: the client must present a cert whose SAN matches a
//! hostname in the configured pool. The SAN allowlist check lives on the
//! [`crate::certreload::CertReloader`] surface and is wired into the TLS
//! `ClientCertVerifier` (TODO — see [`PeerServer::serve`] below).
//!
//! # Scope of this skeleton
//!
//! Only the read-only RPCs (`GetStatus`, `GetNodeConfig`) are wired through
//! to [`NodeInfo`]. The 12 action RPCs return `Status::unimplemented` until
//! the corresponding hook handlers in `agent.rs` land. The streaming RPCs
//! (`Basebackup`, `Rewind`, `FetchWal`) declare their associated stream
//! types as `Pin<Box<dyn Stream + Send>>` so the trait impl compiles; the
//! handler bodies just return `unimplemented` for now — no stream is ever
//! constructed.
//!
//! # TLS plumbing — TODO(v1)
//!
//! [`PeerServer::serve`] currently accepts an `Option<Arc<CertReloader>>`
//! but only the plain-TCP branch is wired. When `Some`, it returns an
//! error today — the production path is to install a
//! [`crate::certreload::ReloadingServerCertResolver`] +
//! `verify_peer_san()` `ClientCertVerifier` on a `rustls::ServerConfig`,
//! wrap each accepted TCP stream through `tokio_rustls::TlsAcceptor`, and
//! feed the resulting stream into tonic's `serve_with_incoming_shutdown`.
//! Plain TCP is permitted only when the daemon was started with `--dev`
//! AND no remote peers are configured.

use crate::agent::NodeInfo;
use crate::certreload::CertReloader;
use futures_core::Stream;
use pg_agent_proto::pgagentpb::{
    pg_agent_peer_server::{PgAgentPeer, PgAgentPeerServer},
    BasebackupRequest, ConfigureStandbyRequest, CreateSlotRequest, DropSlotRequest,
    FetchWalRequest, GetStatusRequest, NodeConfigRequest, NodeConfigResponse, NodeStatus,
    OpProgress, OpResult, PromoteRequest, ReloadPgpoolRequest, ReloadRequest, RemoveVipRequest,
    RewindRequest, StartRequest, StopRequest, WalChunk,
};
use std::pin::Pin;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::{transport::Server, Request, Response, Status};
use tracing::{info, warn};

pub struct PeerServer {
    node_info: Arc<dyn NodeInfo>,
    // TODO(v1): action deps — Arc<dyn Systemd>, Arc<dyn LocalDb>,
    // Arc<dyn StandbyOps>, Arc<dyn WalStore>.
}

impl PeerServer {
    pub fn new(node_info: Arc<dyn NodeInfo>) -> Self {
        Self { node_info }
    }

    /// Serve until `shutdown` cancels. `listener` is consumed.
    ///
    /// `cert_reloader == None` is the dev-mode path: plain TCP, no client
    /// cert verification. Passing `Some(...)` returns an error today —
    /// the production mTLS path is TODO; see the module-level docs.
    pub async fn serve(
        self,
        listener: TcpListener,
        cert_reloader: Option<Arc<CertReloader>>,
        shutdown: CancellationToken,
    ) -> anyhow::Result<()> {
        if cert_reloader.is_some() {
            // TODO(v1): wrap incoming streams with tokio_rustls::TlsAcceptor
            // driven by ReloadingServerCertResolver + verify_peer_san.
            anyhow::bail!(
                "peer server: mTLS path not yet implemented; \
                 pass --dev to run without TLS (no remote peers allowed)"
            );
        }

        info!("peer server: starting (plain TCP, --dev)");
        let incoming = TcpListenerStream::new(listener);
        let result = Server::builder()
            .add_service(PgAgentPeerServer::new(self))
            .serve_with_incoming_shutdown(incoming, async move { shutdown.cancelled().await })
            .await;
        match result {
            Ok(()) => {
                info!("peer server: shut down cleanly");
                Ok(())
            }
            Err(e) => {
                warn!(?e, "peer server: shut down with error");
                Err(e.into())
            }
        }
    }
}

// Associated stream types for the three server-streaming RPCs. We never
// construct values of these in the skeleton (every handler returns
// unimplemented), but the trait requires the types to exist.
type ProgressStream = Pin<Box<dyn Stream<Item = Result<OpProgress, Status>> + Send + 'static>>;
type WalChunkStream = Pin<Box<dyn Stream<Item = Result<WalChunk, Status>> + Send + 'static>>;

#[tonic::async_trait]
impl PgAgentPeer for PeerServer {
    type BasebackupStream = ProgressStream;
    type RewindStream = ProgressStream;
    type FetchWalStream = WalChunkStream;

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

    async fn start(&self, _req: Request<StartRequest>) -> Result<Response<OpResult>, Status> {
        Err(Status::unimplemented("start"))
    }
    async fn stop(&self, _req: Request<StopRequest>) -> Result<Response<OpResult>, Status> {
        Err(Status::unimplemented("stop"))
    }
    async fn reload(&self, _req: Request<ReloadRequest>) -> Result<Response<OpResult>, Status> {
        Err(Status::unimplemented("reload"))
    }
    async fn reload_pgpool(
        &self,
        _req: Request<ReloadPgpoolRequest>,
    ) -> Result<Response<OpResult>, Status> {
        Err(Status::unimplemented("reload_pgpool"))
    }
    async fn promote(&self, _req: Request<PromoteRequest>) -> Result<Response<OpResult>, Status> {
        Err(Status::unimplemented("promote"))
    }
    async fn create_slot(
        &self,
        _req: Request<CreateSlotRequest>,
    ) -> Result<Response<OpResult>, Status> {
        Err(Status::unimplemented("create_slot"))
    }
    async fn drop_slot(
        &self,
        _req: Request<DropSlotRequest>,
    ) -> Result<Response<OpResult>, Status> {
        Err(Status::unimplemented("drop_slot"))
    }
    async fn configure_standby(
        &self,
        _req: Request<ConfigureStandbyRequest>,
    ) -> Result<Response<OpResult>, Status> {
        Err(Status::unimplemented("configure_standby"))
    }
    async fn remove_vip(
        &self,
        _req: Request<RemoveVipRequest>,
    ) -> Result<Response<OpResult>, Status> {
        Err(Status::unimplemented("remove_vip"))
    }

    async fn basebackup(
        &self,
        _req: Request<BasebackupRequest>,
    ) -> Result<Response<Self::BasebackupStream>, Status> {
        Err(Status::unimplemented("basebackup"))
    }
    async fn rewind(
        &self,
        _req: Request<RewindRequest>,
    ) -> Result<Response<Self::RewindStream>, Status> {
        Err(Status::unimplemented("rewind"))
    }
    async fn fetch_wal(
        &self,
        _req: Request<FetchWalRequest>,
    ) -> Result<Response<Self::FetchWalStream>, Status> {
        Err(Status::unimplemented("fetch_wal"))
    }
}

fn internal(e: anyhow::Error) -> Status {
    Status::internal(e.to_string())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

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
                is_in_recovery: true,
                is_ready: true,
                replication_lag_bytes: 42,
                replication_state: "streaming".into(),
                is_postgres_running: true,
                is_pgpool_running: true,
                is_postgres_status_ok: true,
                is_pgpool_status_ok: true,
            })
        }
        async fn get_node_config(&self) -> anyhow::Result<NodeConfigResponse> {
            Ok(NodeConfigResponse {
                pg_port: 5433,
                pg_data_dir: "/d".into(),
            })
        }
    }

    fn server() -> PeerServer {
        PeerServer::new(Arc::new(FakeNodeInfo))
    }

    #[tokio::test]
    async fn get_status_routes_to_node_info() {
        let s = server();
        let resp = s
            .get_status(Request::new(GetStatusRequest {}))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.is_in_recovery);
        assert_eq!(resp.replication_lag_bytes, 42);
    }

    #[tokio::test]
    async fn get_node_config_routes_to_node_info() {
        let s = server();
        let resp = s
            .get_node_config(Request::new(NodeConfigRequest {}))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.pg_port, 5433);
    }

    #[tokio::test]
    async fn promote_returns_unimplemented() {
        let s = server();
        let err = s
            .promote(Request::new(PromoteRequest::default()))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unimplemented);
    }

    #[tokio::test]
    async fn basebackup_returns_unimplemented() {
        // `BasebackupStream` is `Pin<Box<dyn Stream>>` which doesn't impl
        // Debug, so unwrap_err() won't compile — match instead.
        let s = server();
        match s
            .basebackup(Request::new(BasebackupRequest::default()))
            .await
        {
            Err(e) => assert_eq!(e.code(), tonic::Code::Unimplemented),
            Ok(_) => panic!("expected unimplemented"),
        }
    }

    #[test]
    fn internal_maps_anyhow_to_internal_status() {
        let err = internal(anyhow::anyhow!("boom"));
        assert_eq!(err.code(), tonic::Code::Internal);
        assert_eq!(err.message(), "boom");
    }
}
